import { describe, expect, it, vi } from "vitest";
import type { AnyTask, Conductor, TaskContext } from "pgconductor-js";
import { metadataOf } from "../../../src/workflows/metadata.ts";
import { ExecutionError, startAndWait } from "../../../src/execution.ts";
import { trigger, type Json, type JsonObject } from "../../internals.ts";
import { until } from "../../conductor/helper.ts";
import {
	alice,
	approvalWorkflow,
	defineWorkflow,
	metadata,
	once,
	useCoreApp,
	workflow,
} from "./setup.ts";

// How an app's workflows run as Conductor tasks: triggers, schedules, child calls and shutdown.
const { db, setup, enter } = useCoreApp();

describe("registration", () => {
	it("routes concurrent emits of one delivery once, through the router's event trigger", async () => {
		const seen: JsonObject[] = [];
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event: "source.ping" })],
			retries: once,
			run: async (input) => {
				seen.push(input);
			},
		});
		const test = setup([owner]);
		await test.app.start();
		await Promise.all(
			Array.from({ length: 8 }, () =>
				test.app.emit("source.ping", { key: "yes" }, { id: "delivery" }),
			),
		);
		await (await test.execution()).output();
		await test.app.emit("source.ping", { key: "yes" }, { id: "delivery" });
		await test.app.emit("source.ping", { key: "no" }, { id: "other-delivery" });
		await (await test.execution("owner", 1)).output();
		expect(seen).toEqual([{ key: "yes" }, { key: "no" }]);
		expect(await test.routes()).toHaveLength(2);
	});

	it("registers a workflow's cron schedule; a tick runs it with the schedule's name and no requester", async () => {
		let seen: { input: Json; metadata: unknown } | undefined;
		const scheduled = defineWorkflow({
			name: "scheduled",
			cron: [{ schedule: "* * * * *", name: "tick" }],
			retries: once,
			run: async (input, ctx) => {
				seen = { input, metadata: metadataOf(ctx as never) };
			},
		});
		const test = setup([scheduled]);
		await test.app.start();
		const [tick] = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions
			where queue = ${test.client.queue} and task_key = 'scheduled' and cron_expression = '* * * * *'`;
		expect(tick).toBeDefined();
		await db.sql`update pgconductor._private_executions set run_at = pgconductor._private_current_time()
			where id = ${tick!.id}::uuid`;
		await db.execution(tick!.id).output();
		expect(seen?.input).toEqual({ name: "tick" });
		expect(seen?.metadata).toMatchObject({ origin: null, requestedBy: null });
	});
});

describe("child calls", () => {
	it("a failed child reaches the caller as a catchable ExecutionError", async () => {
		const failing = defineWorkflow({
			name: "failing",
			retries: once,
			run: async () => {
				throw new Error("broken");
			},
		});
		let caught: unknown;
		const test = setup([
			workflow(async (_input, ctx) => {
				try {
					await startAndWait(ctx as unknown as TaskContext, "child", "failing", {});
				} catch (error) {
					caught = error;
				}
				return "caught";
			}),
			failing,
		]);
		expect(await (await enter(test)).output()).toBe("caught");
		expect(caught).toBeInstanceOf(ExecutionError);
		expect((caught as ExecutionError).result).toEqual({ status: "failed", error: "broken" });
	});

	it("a timed-out call throws and cancels its attached child", async () => {
		let child: string | undefined;
		const slow = defineWorkflow({
			name: "slow",
			retries: once,
			run: async (_input, ctx) => {
				child = ctx.executionId;
				await ctx.waitForEvent("never", { event: "source.ping" });
			},
		});
		const test = setup([
			workflow(async (_input, ctx) => {
				try {
					await startAndWait(
						ctx as unknown as TaskContext,
						"child",
						"slow",
						{},
						{ timeoutMs: 100 },
					);
				} catch (error) {
					return (error as Error).message;
				}
				return "answered";
			}),
			slow,
		]);
		expect(await (await enter(test)).output()).toBe("Child execution timed out after 100ms");
		const id = await until("the child to run", async () => child);
		expect(await db.execution(id).settled()).toMatchObject({ status: "cancelled" });
	});
});

describe("approvals", () => {
	it("accepts an authenticated click during card delivery", async () => {
		const test = setup([approvalWorkflow()]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = dm.post.bind(dm);
		let clicks = 0;
		// Alice clicks before the card's post returned: before the delivery recorded its receipt.
		dm.post = async (content) => {
			const ref = await post(content);
			const action =
				typeof content === "object" && "card" in content
					? content.card.actions?.find((action) =>
							action.id.startsWith("fabrial.approval.approve:"),
						)
					: undefined;
			if (action) {
				clicks++;
				await test.chat.click(ref, action.id, alice.identities[0]!, { dedupeId: "fast-click" });
			}
			return ref;
		};
		expect(await (await enter(test)).output()).toBe("approved");
		expect(clicks).toBe(1);
		expect(dm.posts).toHaveLength(1);
		// The cleanup workflow relabels the card after the decision.
		await until("the card to be relabelled", async () => {
			const content = dm.posts[0]!.content;
			return typeof content === "object" && "card" in content && content.card.actions?.length === 0
				? true
				: undefined;
		});
	});
});

describe("shutdown", () => {
	it("stop releases a fetch that claims queued executions after the worker aborted; the next deploy runs them", async () => {
		let handlerCalls = 0;
		const queued = defineWorkflow({
			name: "queued",
			retries: once,
			run: async (_input, ctx) => {
				handlerCalls++;
				return { value: await ctx.step("effect", () => "done") };
			},
		});
		const test = setup([queued]);
		await test.app.start();
		const conductor = test.app.host.conductor as Conductor & {
			db: { getExecutions(...args: unknown[]): Promise<{ executions: { task_key: string }[] }> };
		};
		const worker = (
			test.app.host.orchestrator as unknown as {
				workers: {
					queueName: string;
					tasks: Map<string, AnyTask>;
					_abortController: AbortController;
				}[];
			}
		).workers.find((worker) => worker.queueName === test.client.queue)!;
		// Hold a real fetch after the database claimed the execution, before it reaches the worker's queue:
		// shutdown must release the claim without entering the handler.
		let fetched!: () => void;
		const claimed = new Promise<void>((resolve) => (fetched = resolve));
		let deliver!: () => void;
		const delivery = new Promise<void>((resolve) => (deliver = resolve));
		const original = conductor.db.getExecutions.bind(conductor.db);
		const fetch = vi.spyOn(conductor.db, "getExecutions").mockImplementation(async (...args) => {
			const rows = await original(...args);
			if (rows.executions.some((row) => row.task_key === "queued")) {
				fetched();
				await delivery;
			}
			return rows;
		});
		// A bailout lets a broken implementation clean up after the bounded assertion, rather than leave
		// the fixture's stop hung on Conductor's hang-up promise.
		const task = worker.tasks.get("queued")!;
		let bailOut!: () => void;
		const bailout = new Promise<void>((resolve) => (bailOut = resolve));
		const execute = task.execute.bind(task);
		const handler = vi
			.spyOn(task, "execute")
			.mockImplementation(
				(...args) =>
					Promise.race([
						execute(...args),
						bailout.then(() => ({ __pgconductorTaskAborted: true, reason: "parent-aborted" })),
					]) as ReturnType<typeof task.execute>,
			);
		let stopping: Promise<void> | undefined;
		let id: string | undefined;
		try {
			id = await test.client.invoke("queued", {}, { metadata });
			await claimed;
			stopping = test.app.stop();
			await until("the worker to abort", async () =>
				worker._abortController.signal.aborted ? true : undefined,
			);
			deliver();
			expect(
				await Promise.race([
					stopping.then(() => "stopped"),
					new Promise<string>((resolve) => setTimeout(() => resolve("hung"), 1000)),
				]),
			).toBe("stopped");
			expect(handlerCalls).toBe(0);
		} finally {
			deliver();
			bailOut();
			await stopping;
			handler.mockRestore();
			fetch.mockRestore();
		}
		const next = setup([queued], { queue: test.client.queue });
		await next.app.start();
		expect(await db.execution(id).output()).toEqual({ value: "done" });
		expect(handlerCalls).toBe(1);
	});
});
