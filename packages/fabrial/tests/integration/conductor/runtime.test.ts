import { metadataOf } from "../../../src/context.ts";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import postgres from "postgres";
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from "vitest";
import {
	createFabrial,
	defineEvent,
	defineUser,
	definePlugin,
	trigger,
	type DurableExecution,
	type DurableRuntime,
	type InvocationMetadata,
	type Json,
	type RuntimeWorkflow,
	type WaitBranch,
} from "../../internals.ts";
import type { EventFilter } from "../../../src/events.ts";
import { FakeChat, testApp } from "../../../src/testing-internal.ts";
import { z } from "zod";
import type { AnyTask, Conductor } from "pgconductor-js";
import { conductor, ExecutionError } from "../../../src/runtime/conductor/index.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

let container: StartedPostgreSqlContainer;
let admin: postgres.Sql;
let sql: postgres.Sql;
let connectionString: string;
let runtimes: DurableRuntime[];
const metadata: InvocationMetadata = {
	requestId: "interaction",
	origin: { provider: "test" },
	replyTo: null,
	requestedBy: {
		id: "alice",
		name: "Alice",
		known: true,
		identities: [{ provider: "test", installationId: "workspace", subjectId: "user" }],
	},
};
const logger = { debug() {}, info() {}, warn() {}, error() {} };
const poll = { timeout: 15_000, interval: 20 };

beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17").start();
	admin = postgres(container.getConnectionUri());
});
beforeEach(async () => {
	const database = `test_${crypto.randomUUID().replaceAll("-", "")}`;
	await admin.unsafe(`create database ${database}`);
	const url = new URL(container.getConnectionUri());
	url.pathname = database;
	connectionString = url.toString();
	sql = postgres(connectionString, { max: 15 });
	runtimes = [];
});
afterEach(async () => {
	for (const runtime of runtimes) await runtime.stop();
	await sql.end();
});
afterAll(async () => {
	await admin?.end();
	await container?.stop();
});

function workflow(
	name: string,
	handler: RuntimeWorkflow["handler"],
	extra: Partial<RuntimeWorkflow> = {},
): RuntimeWorkflow {
	return { name, triggers: [], handler, ...extra };
}
async function setup(
	workflows: RuntimeWorkflow[],
	options: { queue?: string; concurrency?: number; ownedPool?: boolean } = {},
): Promise<DurableRuntime> {
	const runtime = conductor(options.ownedPool ? { connectionString } : { sql }, {
		queue: options.queue,
		logger,
		worker: {
			pollIntervalMs: 20,
			flushIntervalMs: 20,
			concurrency: options.concurrency ?? 8,
			fetchBatchSize: 10,
		},
	});
	runtime.register({
		workflows,
		events: [
			{ name: "test.event", filterable: ["key", "amount"] },
			{ name: "test.other", filterable: ["key"] },
		],
	});
	runtimes.push(runtime);
	await runtime.start();
	return runtime;
}
/** Suspend until one matching event, or null after `timeoutMs`: subscribe, then wait (native). */
async function waitForEvent(
	ctx: DurableExecution,
	id: string,
	options: { event: string; filter?: EventFilter; timeoutMs?: number },
): Promise<{ name: string; payload: Json } | null> {
	const event = await ctx.subscribe(`${id}:subscribe`, options);
	const winner = await ctx.waitForAny(
		id,
		{ event },
		options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : undefined,
	);
	return winner.kind === "event" ? winner.event : null;
}
async function completed(id: string): Promise<boolean> {
	const [row] = await sql<
		{ done: boolean }[]
	>`select completed_at is not null as done from pgconductor._private_executions where id = ${id}::uuid`;
	return row?.done ?? false;
}
async function waiting(id: string, count = 1): Promise<void> {
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_custom_event_subscriptions s
			join pgconductor._private_executions e on e.id = s.execution_id
			where s.execution_id = ${id}::uuid and e.locked_by is null`;
			return row?.count;
		}, poll)
		.toBe(count);
}
async function terminal(id: string): Promise<{ cancelled: boolean; failed: boolean }> {
	const [row] = await sql<
		{ cancelled: boolean; failed: boolean }[]
	>`select cancelled, failed_at is not null as failed from pgconductor._private_executions where id = ${id}::uuid`;
	return row ?? { cancelled: false, failed: false };
}

test("step and sleep replay from the top, preserving metadata", async () => {
	let attempts = 0;
	let effects = 0;
	const seen: InvocationMetadata[] = [];
	const runtime = await setup([
		workflow("replay", async (input, ctx) => {
			attempts++;
			seen.push(ctx.metadata);
			expect(ctx.executionId).toBeTruthy();
			expect(ctx.workflow).toBe("replay");
			expect(ctx.signal).toBeInstanceOf(AbortSignal);
			const output = await ctx.step("once", () => {
				effects++;
				return input;
			});
			await ctx.sleep("sleep", 50);
			return output;
		}),
	]);
	const id = await runtime.invoke("replay", [1, "two"], { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(attempts).toBe(2);
	expect(effects).toBe(1);
	expect(seen).toEqual([metadata, metadata]);
});

test("event wait suspends, filters payload fields, resumes and returns a clean event; timeout returns null", async () => {
	const received: Json[] = [];
	let effects = 0;
	const runtime = await setup([
		workflow("wait", async (_input, ctx) => {
			await ctx.step("effect", () => {
				effects++;
			});
			const event = await waitForEvent(ctx, "wait", {
				event: "test.event",
				filter: { key: ["yes"], amount: [{ numeric: [">", 2] }] },
				timeoutMs: 5_000,
			});
			received.push(event);
			return event;
		}),
		workflow("timeout", async (_input, ctx) => {
			const result = await waitForEvent(ctx, "wait", {
				event: "test.other",
				timeoutMs: 30,
			});
			received.push(result);
			return result;
		}),
	]);
	const id = await runtime.invoke("wait", null, { metadata });
	await waiting(id);
	await runtime.emit("test.event", { key: "no", amount: 4 }, { metadata });
	await runtime.emit("test.event", { key: "yes", amount: 4 }, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(received).toEqual([{ name: "test.event", payload: { key: "yes", amount: 4 } }]);
	expect(effects).toBe(1);
	const timeout = await runtime.invoke("timeout", {}, { metadata });
	await expect.poll(() => completed(timeout), poll).toBe(true);
	expect(received.at(-1)).toBeNull();
});

test("event triggers start each matching workflow with the event's payload and metadata; concurrent emits dedupe", async () => {
	const seen: { workflow: string; input: Json; metadata: InvocationMetadata }[] = [];
	const handler: RuntimeWorkflow["handler"] = async (input, ctx) => {
		seen.push({ workflow: ctx.workflow, input, metadata: ctx.metadata });
		return null;
	};
	const runtime = await setup([
		workflow("overlapping", handler, {
			triggers: [{ event: "test.event", filter: { key: ["yes", "no"] } }],
		}),
		workflow("other", handler, { triggers: [{ event: "test.event" }] }),
		workflow("filtered", handler, { triggers: [{ event: "test.event", filter: { key: ["no"] } }] }),
	]);
	await Promise.all(
		Array.from({ length: 8 }, () =>
			runtime.emit("test.event", { key: "yes" }, { metadata, id: "delivery" }),
		),
	);
	await expect.poll(() => seen.length, poll).toBe(2);
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_executions where queue = 'pgconductor.internal' and payload ->> 'eventKey' = 'test.event'`;
			return row?.count;
		}, poll)
		.toBe(1);
	await runtime.emit("test.event", { key: "yes" }, { metadata, id: "delivery" });
	for (const workflow of ["overlapping", "other"])
		expect(seen.find((entry) => entry.workflow === workflow)).toEqual({
			workflow,
			input: { key: "yes" },
			metadata,
		});
	await runtime.emit("test.event", { key: "no" }, { metadata, id: "other-delivery" });
	await expect.poll(() => seen.length, poll).toBe(5);
	expect(
		seen
			.slice(2)
			.map((entry) => entry.workflow)
			.sort(),
	).toEqual(["filtered", "other", "overlapping"]);
});
test("invoke/start/emit inherit metadata; overrides affect only the child; starts dedupe after completion", async () => {
	const seen: InvocationMetadata[] = [];
	let starts = 0;
	let invokes = 0;
	let childId: string | undefined;
	const emitted: InvocationMetadata[] = [];
	const resumed: InvocationMetadata[] = [];
	const runtime = await setup([
		workflow("child", async (input, ctx) => {
			seen.push(ctx.metadata);
			starts++;
			return input;
		}),
		workflow("resumed", async (_input, ctx) => {
			resumed.push(ctx.metadata);
			await ctx.sleep("pause", 30);
			return null;
		}),
		workflow("parent", async (_input, ctx) => {
			invokes++;
			const output = await ctx.invoke("call", "child", "output", {
				metadata: { requestId: "override" },
			});
			expect(output).toBe("output");
			childId = await ctx.start("start", "child", { data: true });
			await ctx.start("override-start", "child", {}, { metadata: { requestId: "start-override" } });
			await ctx.emit("test.other", { key: "metadata" }, { id: `${ctx.executionId}:emit` });
			await ctx.emit(
				"test.other",
				{ key: "override" },
				{ id: `${ctx.executionId}:override-emit`, metadata: { requestId: "emit-override" } },
			);
			await ctx.invoke("resume", "resumed", {}, { metadata: { requestId: "resumed" } });
			await ctx.sleep("replay", 30);
			return null;
		}),
		workflow(
			"observer",
			async (_input, ctx) => {
				emitted.push(ctx.metadata);
				return null;
			},
			{ triggers: [{ event: "test.other" }] },
		),
	]);
	const ids = [await runtime.invoke("parent", {}, { metadata })];
	await expect.poll(() => completed(ids[0]!), poll).toBe(true);
	await expect.poll(() => starts, poll).toBe(3);
	expect(invokes).toBeGreaterThan(1);
	expect(seen).toContainEqual({ ...metadata, requestId: "override" });
	expect(seen).toContainEqual(metadata);
	expect(seen).toContainEqual({ ...metadata, requestId: "start-override" });
	await expect.poll(() => emitted.length, poll).toBe(2);
	expect(emitted).toContainEqual({ ...metadata });
	expect(emitted).toContainEqual({
		...metadata,
		requestId: "emit-override",
	});
	expect(await completed(childId!)).toBe(true);
	// Native metadata is replayed on every attempt, including the resume after a sleep.
	expect(resumed.length).toBeGreaterThanOrEqual(2);
	for (const attempt of resumed) expect(attempt).toEqual({ ...metadata, requestId: "resumed" });
	const [row] = await sql<
		{ payload: Record<string, unknown> }[]
	>`select payload from pgconductor._private_executions where task_key = 'resumed'`;
	expect(row?.payload).toEqual({ input: {} });
});

for (const winner of ["first", "second", "child", "timer"] as const) {
	test(`waitForAny: ${winner} wins, caches winner and removes losing subscriptions`, async () => {
		let result: Json | undefined;
		let childId: string | undefined;
		const runtime = await setup([
			workflow("child", async (_input, ctx) => {
				await waitForEvent(ctx, "done", { event: "test.other", filter: { key: ["child"] } });
				return 42;
			}),
			workflow("race", async (_input, ctx) => {
				childId = await ctx.start("child", "child", {});
				const branches: Record<string, WaitBranch> = {
					first: await ctx.subscribe("first", { event: "test.event", filter: { key: ["first"] } }),
					second: await ctx.subscribe("second", {
						event: "test.event",
						filter: { key: ["second"] },
					}),
					child: { execution: childId },
				};
				result = await ctx.waitForAny("race", branches, {
					timeoutMs: winner === "timer" ? 1_000 : 10_000,
				});
				await ctx.sleep("replay-winner", 30);
				return result;
			}),
		]);
		const id = await runtime.invoke("race", {}, { metadata });
		// The native race holds its branches itself while it waits.
		await expect
			.poll(async () => {
				const [row] = await sql<{ waiting: boolean }[]>`
					select locked_by is null and waiting_step_key = 'race' as waiting
					from pgconductor._private_executions where id = ${id}::uuid`;
				return row?.waiting;
			}, poll)
			.toBe(true);
		if (winner === "child") {
			await waiting(childId!);
			await runtime.emit("test.other", { key: "child" }, { metadata });
		} else if (winner !== "timer") await runtime.emit("test.event", { key: winner }, { metadata });
		await expect.poll(() => completed(id), poll).toBe(true);
		expect(result).toEqual(
			winner === "child"
				? { key: "child", kind: "execution", result: { status: "completed", output: 42 } }
				: winner === "timer"
					? { key: "timeout", kind: "timeout" }
					: {
							key: winner,
							kind: "event",
							event: { name: "test.event", payload: { key: winner } },
						},
		);
		const [row] = await sql<
			{ count: number }[]
		>`select count(*)::int as count from pgconductor._private_custom_event_subscriptions where execution_id = ${id}::uuid`;
		expect(row?.count).toBe(0);
	});
}

for (const detached of [false, true]) {
	test(`cancel suspended parent: detached=${detached}`, async () => {
		let childId: string | undefined;
		const runtime = await setup([
			workflow("child", async (_input, ctx) => {
				childId = ctx.executionId;
				await waitForEvent(ctx, "wait", { event: "test.event" });
				return "survived";
			}),
			workflow("parent", async (_input, ctx) => ctx.invoke("child", "child", {}, { detached })),
		]);
		const parent = await runtime.invoke("parent", {}, { metadata });
		await expect.poll(() => childId, poll).toBeDefined();
		await waiting(childId!);
		expect(await runtime.cancel(parent, "user stopped")).toBe(true);
		await expect.poll(() => terminal(parent), poll).toEqual({ failed: true, cancelled: true });
		if (detached) {
			expect(await terminal(childId!)).toEqual({ failed: false, cancelled: false });
			await runtime.emit("test.event", { key: "resume" }, { metadata });
			await expect.poll(() => completed(childId!), poll).toBe(true);
		} else
			await expect.poll(() => terminal(childId!), poll).toEqual({ failed: true, cancelled: true });
		expect(await runtime.cancel(parent)).toBe(false);
	});
}

test("cancel via execution, distinguish failed/cancelled child results, timeout, and independent start survival", async () => {
	let caught: unknown;
	let timeoutError: string | undefined;
	let timedOutChild: string | undefined;
	const runtime = await setup([
		workflow("suspended", async (_input, ctx) => {
			await waitForEvent(ctx, "wait", { event: "test.event" });
			return null;
		}),
		workflow(
			"failure",
			async () => {
				throw new Error("broken");
			},
			{ retries: { maxAttempts: 1 } },
		),
		workflow("cancel-call", async (_input, ctx) => {
			const child = await ctx.start("independent", "suspended", {});
			await runtime.cancel(child, "deliberate");
			return ctx.waitForAny("result", { child: { execution: child } });
		}),
		workflow("failure-call", async (_input, ctx) => {
			try {
				await ctx.invoke("child", "failure", {});
			} catch (error) {
				caught = error;
			}
			return null;
		}),
		workflow("timed-out", async (_input, ctx) => {
			timedOutChild = ctx.executionId;
			await waitForEvent(ctx, "wait", { event: "test.event" });
			return null;
		}),
		workflow("timeout-call", async (_input, ctx) => {
			try {
				await ctx.invoke("child", "timed-out", {}, { timeoutMs: 100 });
			} catch (error) {
				timeoutError = (error as Error).message;
			}
			return null;
		}),
		workflow("starter", async (_input, ctx) => {
			await ctx.start("child", "suspended", {}, { dedupeKey: "independent" });
			await waitForEvent(ctx, "wait", { event: "test.other" });
			return null;
		}),
	]);
	const cancelled = await runtime.invoke("cancel-call", {}, { metadata });
	await expect.poll(() => completed(cancelled), poll).toBe(true);
	const [receipt] = await sql<
		{ result: { output: { result: { status: string; reason: string } } } }[]
	>`select result from pgconductor._private_executions where id = ${cancelled}::uuid`;
	expect(receipt?.result.output.result).toEqual({ status: "cancelled", reason: "deliberate" });
	const failed = await runtime.invoke("failure-call", {}, { metadata });
	await expect.poll(() => completed(failed), poll).toBe(true);
	expect(caught).toBeInstanceOf(ExecutionError);
	expect((caught as ExecutionError).result).toEqual({ status: "failed", error: "broken" });
	const timeout = await runtime.invoke("timeout-call", {}, { metadata });
	await expect.poll(() => completed(timeout), poll).toBe(true);
	expect(timeoutError).toBe("Child execution timed out after 100ms");
	// A native result timeout leaves the child running; an attached call cancels it.
	await expect
		.poll(() => terminal(timedOutChild!), poll)
		.toEqual({ failed: true, cancelled: true });
	const parent = await runtime.invoke("starter", {}, { metadata });
	await waiting(parent);
	const [independent] = await sql<
		{ id: string }[]
	>`select id from pgconductor._private_executions where dedupe_key = 'independent'`;
	await waiting(independent!.id);
	await runtime.cancel(parent);
	expect(await terminal(independent!.id)).toEqual({ failed: false, cancelled: false });
});

test("restart preserves native wait subscriptions and memoized effects", async () => {
	let effects = 0;
	let result: Json | undefined;
	const definition = workflow("restart", async (_input, ctx) => {
		await ctx.step("once", () => {
			effects++;
		});
		result = await waitForEvent(ctx, "wait", { event: "test.event", filter: { key: ["restart"] } });
		return result;
	});
	const first = await setup([definition], { queue: "restart" });
	const id = await first.invoke("restart", {}, { metadata });
	await waiting(id);
	await first.stop();
	runtimes = runtimes.filter((runtime) => runtime !== first);
	const second = await setup([definition], { queue: "restart" });
	await second.emit("test.event", { key: "restart" }, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(effects).toBe(1);
	expect(result).toEqual({
		name: "test.event",
		payload: { key: "restart" },
	});
});

test("cron registers native schedules and executes through the metadata wrapper", async () => {
	let seen: { input: Json; metadata: InvocationMetadata } | undefined;
	const runtime = await setup([
		workflow(
			"cron",
			async (input, ctx) => {
				seen = { input, metadata: ctx.metadata };
				return null;
			},
			{
				cron: [{ schedule: "* * * * *", name: "tick" }],
			},
		),
	]);
	void runtime;
	const rows = await sql<
		{ id: string }[]
	>`select id from pgconductor._private_executions where task_key = 'cron' and cron_expression = '* * * * *'`;
	expect(rows.length).toBeGreaterThan(0);
	await sql`update pgconductor._private_executions set run_at = now() where id = ${rows[0]!.id}::uuid`;
	await expect.poll(() => seen, poll).toBeDefined();
	expect(seen?.input).toEqual({ name: "tick" });
	expect(seen?.metadata).toMatchObject({
		replyTo: null,
		origin: null,
		requestedBy: null,
	});
});

test("waitForAny races concurrent events, replays a single winner, and excludes pre-wait events", async () => {
	let releaseId: string | undefined;
	const results: Json[] = [];
	const runtime = await setup([
		workflow("race", async (_input, ctx) => {
			releaseId = ctx.executionId;
			await waitForEvent(ctx, "gate", { event: "test.other" });
			const result = await ctx.waitForAny("race", {
				first: await ctx.subscribe("first", {
					event: "test.event",
					filter: { key: [{ prefix: "first" }], amount: [{ numeric: [">=", 2, "<", 5] }] },
				}),
				second: await ctx.subscribe("second", {
					event: "test.event",
					filter: { key: [{ "anything-but": "first" }], amount: [{ exists: false }] },
				}),
			});
			results.push(result);
			await ctx.sleep("replay", 20);
			return result;
		}),
	]);
	await runtime.emit("test.event", { key: "first", amount: 3 }, { metadata, id: "stale" });
	const id = await runtime.invoke("race", {}, { metadata });
	await waiting(id);
	expect(releaseId).toBe(id);
	await runtime.emit("test.other", {}, { metadata });
	await waiting(id, 2);
	await Promise.all([
		runtime.emit("test.event", { key: "first-new", amount: 3 }, { metadata }),
		runtime.emit("test.event", { key: "second" }, { metadata }),
	]);
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(results.length).toBe(2);
	expect(results[1]).toEqual(results[0]);
	expect(results[0]).toMatchObject({ kind: "event" });
	const event = (results[0] as { event: { payload: { key: string } } }).event;
	expect(["first-new", "second"]).toContain(event.payload.key);
});

test("structured cancellation is recursive, removes waits, and reports cancelled execution branches", async () => {
	let childId: string | undefined;
	let grandchildId: string | undefined;
	let result: Json | undefined;
	const runtime = await setup([
		workflow("grandchild", async (_input, ctx) => {
			grandchildId = ctx.executionId;
			await waitForEvent(ctx, "wait", { event: "test.event" });
			return null;
		}),
		workflow("child", async (_input, ctx) => {
			childId = ctx.executionId;
			return ctx.invoke("grandchild", "grandchild", {});
		}),
		workflow("parent", async (_input, ctx) => ctx.invoke("child", "child", {})),
		workflow("watch", async (input, ctx) => {
			result = await ctx.waitForAny("child", {
				child: { execution: input as string },
			});
			return result;
		}),
	]);
	const parent = await runtime.invoke("parent", {}, { metadata });
	await expect.poll(() => grandchildId, poll).toBeDefined();
	await waiting(grandchildId!);
	const watch = await runtime.invoke("watch", childId!, { metadata });
	await runtime.cancel(parent, "stop tree");
	for (const id of [parent, childId!, grandchildId!]) {
		await expect.poll(() => terminal(id), poll).toEqual({ failed: true, cancelled: true });
		const [row] = await sql<
			{ count: number }[]
		>`select count(*)::int as count from pgconductor._private_custom_event_subscriptions where execution_id = ${id}::uuid`;
		expect(row?.count).toBe(0);
	}
	await expect.poll(() => completed(watch), poll).toBe(true);
	expect(result).toEqual({
		key: "child",
		kind: "execution",
		result: { status: "cancelled", reason: "stop tree" },
	});
});

test("cancelling a pending execution marks it cancelled without running it", async () => {
	let ran = 0;
	const runtime = await setup([
		workflow("idle", async () => {
			ran++;
			return null;
		}),
	]);
	// Scheduled in the future: still pending when cancelled.
	const [pending] = await sql<{ id: string }[]>`
		insert into pgconductor._private_executions (task_key, queue, payload, run_at)
		values ('idle', 'default', ${sql.json({ input: {} })}, now() + interval '1 hour') returning id`;
	expect(await runtime.cancel(pending!.id, "not needed")).toBe(true);
	expect(await terminal(pending!.id)).toEqual({ failed: true, cancelled: true });
	expect(await runtime.cancel(pending!.id)).toBe(false);
	expect(ran).toBe(0);
});

test("a permanently failed parent cancels its attached child, not its detached one", async () => {
	const children: Record<string, string> = {};
	const runtime = await setup([
		workflow("child", async (input, ctx) => {
			children[input as string] = ctx.executionId;
			await waitForEvent(ctx, "wait", { event: "test.event", filter: { key: [input as string] } });
			return null;
		}),
		workflow(
			"parent",
			async (_input, ctx) => {
				await ctx.start("detached", "child", "detached");
				await ctx.waitForAny(
					"attached",
					{
						child: {
							// The attached call's child, started without waiting.
							execution: await ctx.start("attached", "child", "attached", {
								cancelWithParent: true,
							}),
						},
					},
					{ timeoutMs: 200 },
				);
				throw new Error("parent broke");
			},
			{ retries: { maxAttempts: 1 } },
		),
	]);
	const parent = await runtime.invoke("parent", {}, { metadata });
	await expect.poll(() => terminal(parent), poll).toEqual({ failed: true, cancelled: false });
	await expect
		.poll(() => terminal(children.attached!), poll)
		.toEqual({ failed: true, cancelled: true });
	expect(await terminal(children.detached!)).toEqual({ failed: false, cancelled: false });
	await runtime.emit("test.event", { key: "detached" }, { metadata });
	await expect.poll(() => completed(children.detached!), poll).toBe(true);
});

test("detached invoke timeout leaves the child running", async () => {
	let childId: string | undefined;
	let timedOut = false;
	const runtime = await setup([
		workflow("child", async (_input, ctx) => {
			childId = ctx.executionId;
			await waitForEvent(ctx, "wait", { event: "test.event" });
			return 5;
		}),
		workflow("parent", async (_input, ctx) => {
			try {
				await ctx.invoke("child", "child", {}, { detached: true, timeoutMs: 1_000 });
			} catch (error) {
				timedOut = (error as Error).message.includes("timed out");
			}
			return null;
		}),
	]);
	const parent = await runtime.invoke("parent", {}, { metadata });
	await expect.poll(() => childId, poll).toBeDefined();
	await waiting(childId!);
	await expect.poll(() => completed(parent), poll).toBe(true);
	expect(timedOut).toBe(true);
	expect(await terminal(childId!)).toEqual({ failed: false, cancelled: false });
	await runtime.emit("test.event", {}, { metadata });
	await expect.poll(() => completed(childId!), poll).toBe(true);
});

test("connectionString pools run workflows; injected pools stay usable after stop", async () => {
	const runtime = await setup([workflow("pool", async (input) => input)], { ownedPool: true });
	const id = await runtime.invoke("pool", 123, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	await runtime.stop();
	runtimes = runtimes.filter((entry) => entry !== runtime);
	const injected = await setup([workflow("pool", async (input) => input)]);
	await injected.stop();
	runtimes = runtimes.filter((entry) => entry !== injected);
	const [row] = await sql<{ value: number }[]>`select 1 as value`;
	expect(row?.value).toBe(1);
});

test("emit dedupe is event-scoped and retains the first payload across restart", async () => {
	const seen: Json[] = [];
	const definitions = [
		workflow(
			"observer",
			async (input) => {
				seen.push(input);
				return null;
			},
			{
				triggers: [{ event: "test.event" }, { event: "test.other" }],
			},
		),
	];
	const first = await setup(definitions);
	await first.emit("test.event", { key: "first" }, { metadata, id: "same" });
	await first.emit("test.other", { key: "second" }, { metadata, id: "same" });
	await expect.poll(() => seen.length, poll).toBe(2);
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	const second = await setup(definitions);
	await second.emit(
		"test.event",
		{ key: "different" },
		{ metadata: { ...metadata, requestId: "different" }, id: "same" },
	);
	const rows = await sql<{ payload: { payload: { key: string } }; metadata: InvocationMetadata }[]>`
		select payload, metadata from pgconductor._private_executions where task_key = 'pgconductor.event-dispatch' order by created_at
	`;
	expect(rows).toHaveLength(2);
	expect(rows[0]?.payload.payload.key).toBe("first");
	expect(rows[0]?.metadata).toEqual(metadata);
});

test("running cancellation aborts the native signal and preserves the caller's reason in terminal results", async () => {
	let signal: AbortSignal | undefined;
	let result: Json | undefined;
	const runtime = await setup([
		workflow("running", async (_input, ctx) => {
			signal = ctx.signal;
			await ctx.step(
				"in-flight",
				() =>
					new Promise<void>((resolve) => {
						ctx.signal.addEventListener("abort", () => resolve(), { once: true });
					}),
			);
			return null;
		}),
		workflow("watch", async (input, ctx) => {
			result = await ctx.waitForAny("result", {
				child: { execution: input as string },
			});
			return result;
		}),
	]);
	const id = await runtime.invoke("running", {}, { metadata });
	await expect.poll(() => signal, poll).toBeDefined();
	const watch = await runtime.invoke("watch", id, { metadata });
	expect(await runtime.cancel(id, "caller supplied reason")).toBe(true);
	// Native cancellation signals are delivered by Conductor's 30-second heartbeat.
	await expect.poll(() => signal?.aborted, { timeout: 45_000, interval: 50 }).toBe(true);
	await expect.poll(() => terminal(id), poll).toEqual({ failed: true, cancelled: true });
	await expect.poll(() => completed(watch), poll).toBe(true);
	expect(result).toEqual({
		key: "child",
		kind: "execution",
		result: { status: "cancelled", reason: "caller supplied reason" },
	});
});

test("core fabrial composes event routing, steps, child invocation, native wait, replay and terminal cleanup", async () => {
	const runtime = conductor(
		{ sql },
		{
			logger,
			worker: { pollIntervalMs: 20, flushIntervalMs: 20, concurrency: 8 },
		},
	);
	runtimes.push(runtime);
	const chat = new FakeChat();
	const surface = { kind: "thread", provider: "test", threadId: "test:general:thread" } as const;
	const attempts = new Map<string, number>();
	let effects = 0;
	let childEffects = 0;
	let parentId: string | undefined;
	const plugin = definePlugin({
		id: "test",
		events: {
			begin: defineEvent({ payload: z.object({ key: z.string() }), filterable: ["key"] }),
			reply: defineEvent({ payload: z.object({ key: z.string() }), filterable: ["key"] }),
		},
		hooks: {
			workflow: [
				async (info, ctx, next) => {
					attempts.set(info.workflow, (attempts.get(info.workflow) ?? 0) + 1);
					expect(metadataOf(ctx).requestId).toBe("test.begin:delivery");
					return next(ctx);
				},
			],
		},
	});
	const child = defineWorkflow({
		name: "core-child",
		input: z.string(),
		run: async (input, ctx) => {
			await ctx.step("effect", () => {
				childEffects++;
			});
			return `${input}:child`;
		},
	});
	const parent = defineWorkflow({
		name: "core-parent",
		triggers: [trigger<{ key: string }>({ event: "test.begin", filter: { key: ["go"] } })],
		run: async (input, ctx) => {
			parentId = ctx.executionId;
			await ctx.step("effect", () => {
				effects++;
			});
			await ctx.thread!.post("ack", "working");
			const output = await ctx.invoke("child", child, input.key);
			// Runtime operations are internal; this test captures them from the runtime port.
			const execution = executions.get(ctx.executionId)!;
			const event = await waitForEvent(execution, "reply", {
				event: "test.reply",
				filter: { key: ["reply"] },
				timeoutMs: 5_000,
			});
			return { output, event };
		},
	});
	const executions = new Map<string, import("../../internals.ts").DurableExecution>();
	const register = runtime.register.bind(runtime);
	runtime.register = (definition) =>
		register({
			...definition,
			workflows: definition.workflows.map((workflow) => ({
				...workflow,
				handler: (input, execution, attempt) => {
					executions.set(execution.executionId, execution);
					return workflow.handler(input, execution, attempt);
				},
			})),
		});
	const app = testApp(createFabrial({ plugins: [plugin] }), {
		runtime,
		chat,
		workflows: [parent, child],
	});
	await app.start();
	await app.emit("test.begin", { key: "go" }, { id: "delivery", replyTo: surface });
	await expect.poll(() => parentId, poll).toBeDefined();
	// The child's result wait is a native subscription too: wait for the reply wait itself.
	await expect
		.poll(async () => {
			const [row] = await sql<{ count: number }[]>`
				select count(*)::int as count from pgconductor._private_custom_event_subscriptions s
				join pgconductor._private_executions e on e.id = s.execution_id
				where s.execution_id = ${parentId!}::uuid and s.step_key = 'reply:subscribe' and e.locked_by is null`;
			return row?.count;
		}, poll)
		.toBe(1);
	const thread = await chat.thread(surface);
	expect(thread.posts).toHaveLength(1);
	// The wait starts at its subscription: earlier events do not match.
	await app.emit("test.reply", { key: "wrong" });
	await app.emit("test.reply", { key: "reply" }, { id: "reply" });
	await expect.poll(() => completed(parentId!), poll).toBe(true);
	await expect.poll(() => thread.getState(), poll).toBeNull();
	expect(effects).toBe(1);
	expect(childEffects).toBe(1);
	expect(attempts.get(parent.name)).toBeGreaterThanOrEqual(2);
	expect(attempts.get(child.name)).toBe(1);
	const [row] = await sql<{ result: { output: Json } }[]>`
		select result from pgconductor._private_executions where id = ${parentId!}::uuid
	`;
	expect(row?.result.output).toEqual({
		output: "go:child",
		event: { name: "test.reply", payload: { key: "reply" } },
	});
	await app.emit("test.begin", { key: "go" }, { id: "delivery", replyTo: surface });
	const rows = await sql<
		{ id: string }[]
	>`select id from pgconductor._private_executions where task_key = ${parent.name}`;
	expect(rows.map((row) => row.id)).toEqual([parentId]);
	await app.stop();
	runtimes = runtimes.filter((entry) => entry !== runtime);
});

test("start reconnects to its completed child after losing the memoized submission step", async () => {
	const children: string[] = [];
	let effects = 0;
	const definitions = [
		workflow("child", async (_input, ctx) => {
			await ctx.step("effect", () => {
				effects++;
			});
			return 7;
		}),
		workflow("starter", async (_input, ctx) => {
			children.push(await ctx.start("submission", "child", {}));
			await waitForEvent(ctx, "gate", { event: "test.other" });
			return null;
		}),
	];
	const first = await setup(definitions);
	const parent = await first.invoke("starter", {}, { metadata });
	await waiting(parent);
	await expect.poll(() => completed(children[0]!), poll).toBe(true);
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	// Recreate the durable state of a process lost after INSERT committed but before saveStep.
	await sql`delete from pgconductor._private_steps where execution_id = ${parent}::uuid and key = 'submission'`;
	const second = await setup(definitions);
	await second.emit("test.other", {}, { metadata });
	await expect.poll(() => completed(parent), poll).toBe(true);
	// Replays are at-least-once; every replay after the lost step must reconnect to the same child.
	expect(children.length).toBeGreaterThanOrEqual(2);
	expect(new Set(children)).toEqual(new Set([children[0]]));
	expect(effects).toBe(1);
	const rows = await sql<
		{ id: string }[]
	>`select id from pgconductor._private_executions where task_key = 'child'`;
	expect(rows).toHaveLength(1);
});

test("core approval accepts an authenticated click during card delivery, before registration", async () => {
	const runtime = conductor(
		{ sql },
		{
			logger,
			worker: { pollIntervalMs: 20, flushIntervalMs: 20, concurrency: 8 },
		},
	);
	runtimes.push(runtime);
	const chat = new FakeChat();
	const alice = defineUser({
		id: "alice",
		name: "Alice",
		identities: [{ provider: "test", installationId: "workspace", subjectId: "alice" }],
	});
	const dm = await chat.openDM(alice.identities[0]!);
	const post = dm.post.bind(dm);
	let clicks = 0;
	dm.post = async (content) => {
		const ref = await post(content);
		if (typeof content === "object" && "card" in content) {
			const action = content.card.actions?.find((action) =>
				action.id.startsWith("fabrial.approval.approve:"),
			);
			if (action) {
				const [row] = await sql<
					{ count: number }[]
				>`select count(*)::int as count from pgconductor._private_custom_event_subscriptions
					where execution_id = ${parentId!}::uuid and event_key = 'fabrial.approval.decided'`;
				expect(row?.count).toBe(0);
				clicks++;
				await chat.click(ref, action.id, alice.identities[0]!, { dedupeId: "fast-click" });
			}
		}
		return ref;
	};
	let parentId: string | undefined;
	let decision: Json | undefined;
	const plugin = definePlugin({
		id: "test",
		events: { begin: defineEvent({ payload: z.object({ key: z.string() }) }) },
	});
	const parent = defineWorkflow({
		name: "core-approval",
		triggers: [trigger({ event: "test.begin" })],
		run: async (_input, ctx) => {
			parentId = ctx.executionId;
			decision = (
				await ctx.waitForApproval("approve-sql", {
					title: "Run SQL?",
					details: "select 1",
					approvers: alice,
					timeout: 5_000,
				})
			).status;
			await ctx.sleep("replay", 20);
			return decision;
		},
	});
	const app = testApp(createFabrial({ plugins: [plugin], identity: [alice] }), {
		runtime,
		chat,
		workflows: [parent],
	});
	await app.start();
	await app.emit("test.begin", { key: "go" }, { id: "approval" });
	await expect.poll(() => parentId, poll).toBeDefined();
	await expect.poll(() => completed(parentId!), poll).toBe(true);
	expect(decision).toBe("approved");
	expect(clicks).toBe(1);
	expect(dm.posts).toHaveLength(1);
	// The separate cleanup workflow relabels the card after the decision.
	await expect
		.poll(() => dm.posts[0]!.content, poll)
		.toMatchObject({ card: { text: "Approved by Alice", actions: [] } });
	await app.stop();
	runtimes = runtimes.filter((entry) => entry !== runtime);
});

test("stop drains a fetch that delivers queued executions after worker abort", async () => {
	let handlerCalls = 0;
	const queued = workflow("queued", async (_input, execution) => {
		handlerCalls++;
		return execution.step("effect", () => "done");
	});
	const runtime = await setup([queued]);
	// Hold a real fetch after the database has claimed work, but before it reaches
	// the worker queue. Shutdown must release this claim without entering a handler.
	const native = runtime as unknown as {
		client: Conductor;
		orchestrator: {
			workers: {
				queueName: string;
				tasks: Map<string, AnyTask>;
				_abortController: AbortController;
			}[];
		};
	};
	let fetched!: () => void;
	const claimed = new Promise<void>((resolve) => {
		fetched = resolve;
	});
	let deliver!: () => void;
	const delivery = new Promise<void>((resolve) => {
		deliver = resolve;
	});
	const original = native.client.db.getExecutions.bind(native.client.db);
	const fetch = vi.spyOn(native.client.db, "getExecutions").mockImplementation(async (...args) => {
		const rows = await original(...args);
		if (rows.executions.some((row) => row.task_key === "queued")) {
			fetched();
			await delivery;
		}
		return rows;
	});
	const worker = native.orchestrator.workers.find((worker) => worker.queueName === "default")!;
	const task = worker.tasks.get("queued")!;
	// A bailout lets the broken implementation clean up after the bounded assertion,
	// rather than leaving afterEach hung on Conductor's intentional hang-up promise.
	let bailOut!: () => void;
	const bailout = new Promise<void>((resolve) => {
		bailOut = resolve;
	});
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
		id = await runtime.invoke("queued", null, { metadata });
		await claimed;
		stopping = runtime.stop();
		await expect.poll(() => worker._abortController.signal.aborted, poll).toBe(true);
		deliver();
		await expect(
			Promise.race([
				stopping.then(() => "stopped"),
				new Promise<string>((resolve) => setTimeout(() => resolve("hung"), 1000)),
			]),
		).resolves.toBe("stopped");
		expect(handlerCalls).toBe(0);
	} finally {
		deliver();
		bailOut();
		await stopping;
		handler.mockRestore();
		fetch.mockRestore();
	}
	await setup([queued]);
	await expect.poll(() => completed(id!), poll).toBe(true);
	expect(handlerCalls).toBe(1);
});

/** Collects "input" signals until one carries "stop". */
function session(received: Json[]): RuntimeWorkflow {
	return workflow("session", async (_input, ctx) => {
		await new Promise<void>((resolve) =>
			ctx.onSignal("input", (payload) => {
				received.push(payload);
				if (payload === "stop") resolve();
			}),
		);
		return received;
	});
}
const input = (id: string, payload: Json = id) => ({ name: "input", id, payload });
async function output(id: string): Promise<Json> {
	const [row] = await sql<{ result: { output: Json } }[]>`
		select result from pgconductor._private_executions where id = ${id}::uuid`;
	return row?.result.output ?? null;
}

test("a running handler receives signals from outside; stored signals arrive first, in order, deduped by id", async () => {
	const received: Json[] = [];
	const runtime = await setup([session(received)]);
	const start = { workflow: "session", key: "thread-1", metadata };
	const id = await runtime.signalWithStart({ ...start, signal: input("1", "a") });
	expect(await runtime.signal(id, input("2", "b"))).toBe(true);
	expect(await runtime.signalWithStart({ ...start, signal: input("1", "dup") })).toBe(id);
	await expect.poll(() => received, poll).toEqual(["a", "b"]);
	const [running] = await sql<{ locked: boolean }[]>`
		select locked_by is not null as locked from pgconductor._private_executions where id = ${id}::uuid`;
	expect(running?.locked).toBe(true);
	expect(await runtime.signal(id, input("2", "dup"))).toBe(true);
	expect(await runtime.signal(id, input("3", "stop"))).toBe(true);
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(received).toEqual(["a", "b", "stop"]);
	expect(await output(id)).toEqual(["a", "b", "stop"]);
	expect(await runtime.signal(id, input("4"))).toBe(false);
});

test("concurrent signalWithStart calls start exactly one execution per key; ctx.signalWithStart is memoized and inherits metadata", async () => {
	const received: Json[] = [];
	const seen: InvocationMetadata[] = [];
	let replays = 0;
	const runtime = await setup([
		session(received),
		workflow("recorder", async (_input, ctx) => {
			seen.push(ctx.metadata);
			await new Promise<void>((resolve) => ctx.onSignal("input", () => resolve()));
		}),
		workflow("caller", async (_input, ctx) => {
			const options = { workflow: "recorder", key: "k", metadata: { origin: metadata.origin } };
			const first = await ctx.signalWithStart("send", { ...options, signal: input("a") });
			if (++replays === 1) await ctx.sleep("pause", 1);
			const again = await ctx.signalWithStart("send", { ...options, signal: input("b") });
			return [first, again];
		}),
	]);
	const send = (id: string) =>
		runtime.signalWithStart({ workflow: "session", key: "thread-1", signal: input(id), metadata });
	const ids = await Promise.all(Array.from({ length: 10 }, (_, i) => send(`s${i}`)));
	expect(new Set(ids).size).toBe(1);
	const [count] = await sql<{ count: number }[]>`
		select count(*)::int as count from pgconductor._private_executions where task_key = 'session'`;
	expect(count?.count).toBe(1);
	await send("stop");
	await expect.poll(() => completed(ids[0]!), poll).toBe(true);
	expect(new Set(received.slice(0, 10))).toEqual(new Set(ids.map((_, i) => `s${i}`)));
	// Settlement releases the key.
	const next = await send("next");
	expect(next).not.toBe(ids[0]);
	await send("stop");
	await expect.poll(() => completed(next), poll).toBe(true);

	const caller = await runtime.invoke("caller", null, { metadata });
	await expect.poll(() => completed(caller), poll).toBe(true);
	const [first, again] = (await output(caller)) as string[];
	expect(again).toBe(first);
	await expect.poll(() => completed(first!), poll).toBe(true);
	expect(seen[0]).toEqual({ ...metadata });
});

// Real time: the late signal must be stored after the worker's last signal poll of the attempt. A poll
// that reads it first delivers it while the attempt drains, which Conductor allows. No hook orders the
// two, so a retry reruns the race; a re-queue that loses the signal still fails every attempt.
test(
	"signals arriving while the handler returns re-queue it; the final attempt's output is kept",
	{ retry: 2 },
	async () => {
		let attempts = 0;
		const runtime = await setup([
			workflow(
				"returning",
				async (_input, ctx) => {
					attempts++;
					const received: Json[] = [];
					// Signals arrive on worker polls, so an attempt must run until one delivers.
					await new Promise<void>((resolve) =>
						ctx.onSignal("input", (payload) => {
							received.push(payload);
							resolve();
						}),
					);
					// Stored as this attempt stops reading signals: settlement re-queues it.
					if (attempts === 1) await runtime.signal(ctx.executionId, input("late"));
					return { attempts, received };
				},
				{ retries: { maxAttempts: 1 } },
			),
		]);
		const id = await runtime.signalWithStart({
			workflow: "returning",
			key: "k",
			signal: input("early"),
			metadata,
		});
		await expect.poll(() => completed(id), poll).toBe(true);
		expect(await output(id)).toEqual({ attempts: 2, received: ["late"] });
	},
);
