import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import postgres from "postgres";
import { afterAll, afterEach, beforeAll, beforeEach, expect, test } from "vitest";
import {
	EXECUTION_SETTLED_EVENT,
	defineEvent,
	defineUser,
	definePlugin,
	defineWorkflow,
	fabrial,
	trigger,
	type DurableExecution,
	type DurableRuntime,
	type InvocationMetadata,
	type Json,
	type RuntimeWorkflow,
	type WaitBranch,
	type EventCursor,
} from "fabrial";
import { FakeChat } from "fabrial/testing";
import { z } from "zod";
import { conductor, ExecutionError, type ConductorExecution } from "../../src/index.ts";

let container: StartedPostgreSqlContainer;
let admin: postgres.Sql;
let sql: postgres.Sql;
let connectionString: string;
let runtimes: DurableRuntime[];
const metadata: InvocationMetadata = {
	interactionId: "interaction",
	origin: { provider: "test" },
	replyTo: null,
	requestedBy: {
		id: "alice",
		name: "Alice",
		known: true,
		identities: [{ provider: "test", installationId: "workspace", subjectId: "user" }],
	},
	ownsThread: true,
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
		pollIntervalMs: 100,
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

test("step and sleep replay from the top, preserving metadata and native raw handles", async () => {
	let attempts = 0;
	let effects = 0;
	const seen: InvocationMetadata[] = [];
	const runtime = await setup([
		workflow("replay", async (input, ctx) => {
			attempts++;
			seen.push(ctx.metadata);
			expect((ctx as DurableExecution & { raw: unknown }).raw).toBeDefined();
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
			const event = await ctx.waitForEvent("wait", {
				event: "test.event",
				filter: { key: ["yes"], amount: [{ numeric: [">", 2] }] },
				deadline: await ctx.step("deadline", () => Date.now() + 5_000),
			});
			received.push(event);
			return event;
		}),
		workflow("timeout", async (_input, ctx) => {
			const result = await ctx.waitForEvent("wait", {
				event: "test.other",
				deadline: await ctx.step("deadline", () => Date.now() + 30),
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
	expect(received).toEqual([
		{ name: "test.event", payload: { key: "yes", amount: 4 }, cursor: expect.any(String) },
	]);
	expect(effects).toBe(1);
	const timeout = await runtime.invoke("timeout", {}, { metadata });
	await expect.poll(() => completed(timeout), poll).toBe(true);
	expect(received.at(-1)).toBeNull();
});

test("SHIM(conductor#1): pre-registration replies after a memoized cursor are buffered; chained cursors lose nothing", async () => {
	const cursors: EventCursor[] = [];
	const received: Json[] = [];
	let effects = 0;
	const runtime = await setup([
		workflow("approval", async (_input, ctx) => {
			await ctx.step("stale", () => ctx.emit("test.event", { key: "approval", amount: 0 }));
			const after = await ctx.cursor("before-card");
			cursors.push(after);
			const deadline = await ctx.step("deadline", () => Date.now() + 5_000);
			await ctx.step("post-card", async () => {
				effects++;
				// Two fast clicks arrive while posting the card, before either wait registers.
				await Promise.all(
					[1, 2].map((amount) => ctx.emit("test.event", { key: "approval", amount })),
				);
				await ctx.emit("test.event", { key: "different-approval", amount: 3 });
			});
			const first = await ctx.waitForEvent("first", {
				event: "test.event",
				filter: { key: ["approval"] },
				after,
				deadline,
			});
			expect(first).not.toBeNull();
			const second = await ctx.waitForEvent("second", {
				event: "test.event",
				filter: { key: ["approval"] },
				after: first!.cursor,
				deadline,
			});
			expect(second).not.toBeNull();
			received.push([first, second]);
			await ctx.sleep("replay", 20);
			return [first, second];
		}),
	]);
	const id = await runtime.invoke("approval", null, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(effects).toBe(1);
	expect(new Set(cursors).size).toBe(1);
	expect(received[1]).toEqual(received[0]);
	const events = received[0] as { payload: { amount: number }; cursor: string }[];
	expect(events.map((event) => event.payload.amount).sort((a, b) => a - b)).toEqual([1, 2]);
	expect(BigInt(events[0]!.cursor.split(":")[1]!)).toBeLessThan(
		BigInt(events[1]!.cursor.split(":")[1]!),
	);
});

test("cursor waits ignore pre-cursor events, even when retained and correlated", async () => {
	let received: Json | undefined;
	const runtime = await setup([
		workflow("stale", async (_input, ctx) => {
			await ctx.step("old-click", () => ctx.emit("test.event", { key: "approval" }));
			const after = await ctx.cursor("before-card");
			received = await ctx.waitForEvent("decision", {
				event: "test.event",
				filter: { key: ["approval"] },
				after,
				deadline: await ctx.step("deadline", () => Date.now() + 200),
			});
			return received;
		}),
	]);
	const id = await runtime.invoke("stale", null, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(received).toBeNull();
});

test("receipt retention supports correlated waits at 30 days and survives restart", async () => {
	let received: Json | undefined;
	const definition = workflow("retained", async (_input, ctx) => {
		const after = await ctx.cursor("before-card");
		await ctx.waitForEvent("gate", { event: "test.other" });
		received = await ctx.waitForEvent("approval", {
			event: "test.event",
			filter: { key: ["approval"] },
			after,
			deadline: await ctx.step("deadline", () => Date.now() + 1_000),
		});
		return received;
	});
	const first = await setup([definition]);
	const id = await first.invoke("retained", null, { metadata });
	await waiting(id);
	await first.emit("test.event", { key: "approval" }, { metadata, id: "click" });
	await sql`update pgconductor._private_executions set created_at = now() - interval '30 days'
		where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = 'test.event'`;
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	const second = await setup([definition]);
	await second.emit("test.other", {}, { metadata });
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(received).toEqual({
		name: "test.event",
		payload: { key: "approval" },
		cursor: expect.any(String),
	});
});

test("cursor capture waits for concurrent emitter commit, not just sequence allocation", async () => {
	const received: Json[] = [];
	const runtime = await setup([
		workflow("capture", async (_input, ctx) => {
			const after = await ctx.cursor("capture");
			received.push(
				await ctx.waitForEvent("wait", {
					event: "test.event",
					after,
					deadline: await ctx.step("deadline", () => Date.now() + 150),
				}),
			);
			return null;
		}),
	]);
	// Block an emit AFTER it allocates its position but BEFORE its receipt commits.
	await sql.unsafe(`create function public.delay_receipt() returns trigger language plpgsql as $$
		begin
			if new.task_key = 'pgconductor.event-dispatch' then perform pg_advisory_xact_lock(724831, 2); end if;
			return new;
		end $$;
		create trigger delay_receipt before insert on pgconductor._private_executions
		for each row execute function public.delay_receipt()`);
	let unblock!: () => void;
	let held!: () => void;
	const ready = new Promise<void>((resolve) => {
		held = resolve;
	});
	const blocking = sql.begin(async (transaction) => {
		await transaction`select pg_advisory_xact_lock(724831, 2)`;
		held();
		await new Promise<void>((resolve) => {
			unblock = resolve;
		});
	});
	await ready;
	let id: string | undefined;
	const emitting = runtime.emit("test.event", { key: "before-cursor" }, { metadata });
	try {
		await expect
			.poll(async () => {
				const [row] = await sql<{ waiting: boolean }[]>`select exists(select 1 from pg_locks
				where locktype = 'advisory' and classid = 724831 and objid = 2 and not granted) as waiting`;
				return row?.waiting;
			}, poll)
			.toBe(true);
		id = await runtime.invoke("capture", null, { metadata });
		await expect
			.poll(async () => {
				const [row] = await sql<{ waiting: boolean }[]>`select exists(select 1 from pg_locks
				where locktype = 'advisory' and classid = 724831 and objid = 1 and not granted) as waiting`;
				return row?.waiting;
			}, poll)
			.toBe(true);
	} finally {
		unblock();
		await blocking;
		await emitting;
	}
	await expect.poll(() => completed(id!), poll).toBe(true);
	// The delayed emit committed before capture completed: it must be excluded.
	expect(received).toEqual([null]);
});

for (const timely of [true, false]) {
	test(`buffered event emitted ${timely ? "before" : "after"} deadline competes deterministically with expired timer`, async () => {
		let received: Json | undefined;
		const runtime = await setup([
			workflow("deadline-race", async (_input, ctx) => {
				const after = await ctx.cursor("before-card");
				const deadline = await ctx.step("deadline", () => Date.now() + 150);
				await ctx.step("side-effect", async () => {
					if (!timely) await sql`select pg_sleep(0.2)`;
					await ctx.emit("test.event", { key: "approval" });
					if (timely) await sql`select pg_sleep(0.2)`;
				});
				received = await ctx.waitForEvent("decision", { event: "test.event", after, deadline });
				return received;
			}),
		]);
		const id = await runtime.invoke("deadline-race", null, { metadata });
		await expect.poll(() => completed(id), poll).toBe(true);
		expect(received).toEqual(
			timely
				? { name: "test.event", payload: { key: "approval" }, cursor: expect.any(String) }
				: null,
		);
	});
}

for (const kind of ["event", "timer"] as const) {
	test(`absolute ${kind} deadline does not drift earlier across polling replays`, async () => {
		let began = 0;
		let ended = 0;
		let attempts = 0;
		const runtime = await setup([
			workflow("deadline", async (_input, ctx) => {
				attempts++;
				began = await ctx.step("began", () => Date.now());
				const at = began + 600;
				if (kind === "event")
					expect(await ctx.waitForEvent("wait", { event: "test.event", deadline: at })).toBeNull();
				else
					expect(await ctx.waitForAny("wait", { timeout: { kind: "timer", at } })).toEqual({
						key: "timeout",
						kind: "timer",
					});
				ended = Date.now();
				return null;
			}),
		]);
		const id = await runtime.invoke("deadline", null, { metadata });
		await expect.poll(() => completed(id), poll).toBe(true);
		expect(attempts).toBeGreaterThan(2);
		expect(ended - began).toBeGreaterThanOrEqual(590);
	});
}

test("dispatchOwner wins concurrent owner/observer emissions on the same workflow, including observer-first insertion", async () => {
	const seen: { input: Json; metadata: InvocationMetadata }[] = [];
	const withThread: InvocationMetadata = {
		...metadata,
		replyTo: { kind: "thread", provider: "test", threadId: "thread" },
	};
	const definitions = [
		workflow(
			"mixed",
			async (input, ctx) => {
				seen.push({ input, metadata: ctx.metadata });
				return null;
			},
			{
				triggers: [
					{ event: "test.event", role: "owner" },
					{ event: "test.other", role: "observer" },
				],
			},
		),
	];
	const first = await setup(definitions);
	for (let i = 0; i < 12; i++) {
		const options = {
			metadata: withThread,
			id: `delivery-${i}`,
			dispatchId: `ingress-${i}`,
			dispatchOwner: { workflow: "mixed", event: "test.event" },
		};
		await Promise.all([
			first.emit("test.other", { key: `observer-${i}` }, options),
			first.emit("test.event", { key: `owner-${i}` }, { ...options, owner: "mixed" }),
		]);
	}
	// Force the old bug: observer routing completes before the owner even emits.
	const options = {
		metadata: withThread,
		id: "delayed",
		dispatchId: "delayed",
		dispatchOwner: { workflow: "mixed", event: "test.event" },
	};
	await first.emit("test.other", { key: "observer-delayed" }, options);
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ done: boolean }[]
			>`select completed_at is not null as done from pgconductor._private_executions
			where task_key = 'fabrial.route:mixed' and payload -> 'payload' ->> 'key' = 'observer-delayed'`;
			return row?.done;
		}, poll)
		.toBe(true);
	await first.emit("test.event", { key: "owner-delayed" }, { ...options, owner: "mixed" });
	await expect.poll(() => seen.length, poll).toBe(13);
	expect(seen.every((entry) => (entry.input as { key: string }).key.startsWith("owner-"))).toBe(
		true,
	);
	for (const entry of seen) expect(entry.metadata).toEqual(withThread);
	// Handler bodies are replayable: wait for native settlement before restarting.
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_executions
			where task_key = 'mixed' and completed_at is not null`;
			return row?.count;
		}, poll)
		.toBe(13);
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	const second = await setup(definitions);
	await second.emit("test.other", { key: "observer-redelivery" }, { ...options, id: "again" });
	await second.emit(
		"test.event",
		{ key: "owner-redelivery" },
		{ ...options, id: "again", owner: "mixed" },
	);
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_executions where task_key = 'fabrial.route:mixed' and completed_at is not null`;
			return row?.count;
		}, poll)
		.toBe(28);
	expect(seen).toHaveLength(13);
});

test("owner routing, observers, overlapping subscriptions, clean payload and concurrent emit dedupe", async () => {
	const seen: { workflow: string; input: Json; metadata: InvocationMetadata }[] = [];
	const handler: RuntimeWorkflow["handler"] = async (input, ctx) => {
		seen.push({ workflow: ctx.workflow, input, metadata: ctx.metadata });
		return null;
	};
	const runtime = await setup([
		workflow("owner", handler, {
			triggers: [
				{ event: "test.event", role: "owner" },
				{ event: "test.event", role: "owner", filter: { key: ["yes"] } },
			],
		}),
		workflow("wrong-owner", handler, { triggers: [{ event: "test.event", role: "owner" }] }),
		workflow("observer", handler, { triggers: [{ event: "test.event", role: "observer" }] }),
	]);
	await Promise.all(
		Array.from({ length: 8 }, () =>
			runtime.emit("test.event", { key: "yes" }, { metadata, owner: "owner", id: "delivery" }),
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
	await runtime.emit("test.event", { key: "yes" }, { metadata, owner: "owner", id: "delivery" });
	expect(seen.find((entry) => entry.workflow === "owner")).toEqual({
		workflow: "owner",
		input: { key: "yes" },
		metadata,
	});
	expect(seen.find((entry) => entry.workflow === "observer")?.metadata).toEqual({
		...metadata,
		ownsThread: false,
	});
	await runtime.emit("test.event", { key: "yes" }, { metadata, id: "no-owner" });
	await expect.poll(() => seen.length, poll).toBe(3);
	expect(seen.at(-1)?.workflow).toBe("observer");
});

test("invoke/start/emit inherit metadata; overrides affect only the child; starts and client invocations dedupe after completion", async () => {
	const seen: InvocationMetadata[] = [];
	let starts = 0;
	let invokes = 0;
	let childId: string | undefined;
	const emitted: InvocationMetadata[] = [];
	const runtime = await setup([
		workflow("child", async (input, ctx) => {
			seen.push(ctx.metadata);
			starts++;
			return input;
		}),
		workflow("parent", async (_input, ctx) => {
			invokes++;
			const output = await ctx.invoke("call", "child", "output", {
				metadata: { interactionId: "override" },
			});
			expect(output).toBe("output");
			childId = await ctx.start("start", "child", { data: true });
			await ctx.start(
				"override-start",
				"child",
				{},
				{ metadata: { interactionId: "start-override" } },
			);
			await ctx.emit("test.other", { key: "metadata" }, { id: `${ctx.executionId}:emit` });
			await ctx.emit(
				"test.other",
				{ key: "override" },
				{ id: `${ctx.executionId}:override-emit`, metadata: { interactionId: "emit-override" } },
			);
			await ctx.sleep("replay", 30);
			return null;
		}),
		workflow(
			"observer",
			async (_input, ctx) => {
				emitted.push(ctx.metadata);
				return null;
			},
			{ triggers: [{ event: "test.other", role: "observer" }] },
		),
	]);
	const ids = await Promise.all(
		Array.from({ length: 5 }, () =>
			runtime.invoke("parent", {}, { metadata, dedupeKey: "parent" }),
		),
	);
	expect(new Set(ids).size).toBe(1);
	await expect.poll(() => completed(ids[0]!), poll).toBe(true);
	await expect.poll(() => starts, poll).toBe(3);
	expect(invokes).toBeGreaterThan(1);
	expect(seen).toContainEqual({ ...metadata, interactionId: "override" });
	expect(seen).toContainEqual(metadata);
	expect(seen).toContainEqual({ ...metadata, interactionId: "start-override" });
	await expect.poll(() => emitted.length, poll).toBe(2);
	expect(emitted).toContainEqual({ ...metadata, ownsThread: false });
	expect(emitted).toContainEqual({
		...metadata,
		interactionId: "emit-override",
		ownsThread: false,
	});
	expect(await runtime.invoke("parent", {}, { metadata, dedupeKey: "parent" })).toBe(ids[0]);
	expect(await completed(childId!)).toBe(true);
});

for (const winner of ["first", "second", "child", "timer"] as const) {
	test(`waitForAny: ${winner} wins, caches winner and removes losing subscriptions`, async () => {
		let result: Json | undefined;
		let childId: string | undefined;
		const runtime = await setup([
			workflow("child", async (_input, ctx) => {
				await ctx.waitForEvent("done", { event: "test.other", filter: { key: ["child"] } });
				return 42;
			}),
			workflow("race", async (_input, ctx) => {
				childId = await ctx.start("child", "child", {});
				const branches: Record<string, WaitBranch> = {
					first: { kind: "event", event: "test.event", filter: { key: ["first"] } },
					second: { kind: "event", event: "test.event", filter: { key: ["second"] } },
					child: { kind: "execution", executionId: childId },
					timer: {
						kind: "timer",
						at: await ctx.step(
							"deadline",
							() => Date.now() + (winner === "timer" ? 1_000 : 10_000),
						),
					},
				};
				result = await ctx.waitForAny("race", branches);
				await ctx.sleep("replay-winner", 30);
				return result;
			}),
		]);
		const id = await runtime.invoke("race", {}, { metadata });
		await waiting(id, 2);
		if (winner === "child") {
			await waiting(childId!);
			await runtime.emit("test.other", { key: "child" }, { metadata });
		} else if (winner !== "timer") await runtime.emit("test.event", { key: winner }, { metadata });
		await expect.poll(() => completed(id), poll).toBe(true);
		expect(result).toEqual(
			winner === "child"
				? { key: "child", kind: "execution", result: { status: "completed", output: 42 } }
				: winner === "timer"
					? { key: "timer", kind: "timer" }
					: {
							key: winner,
							kind: "event",
							event: { name: "test.event", payload: { key: winner } },
							cursor: expect.any(String),
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
				await ctx.waitForEvent("wait", { event: "test.event" });
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
	const runtime = await setup([
		workflow("suspended", async (_input, ctx) => {
			await ctx.waitForEvent("wait", { event: "test.event" });
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
			await ctx.cancel(child, "deliberate");
			return ctx.waitForAny("result", { child: { kind: "execution", executionId: child } });
		}),
		workflow("failure-call", async (_input, ctx) => {
			try {
				await ctx.invoke("child", "failure", {});
			} catch (error) {
				caught = error;
			}
			return null;
		}),
		workflow("timeout-call", async (_input, ctx) => {
			try {
				await ctx.invoke("child", "suspended", {}, { timeoutMs: 100 });
			} catch (error) {
				timeoutError = (error as Error).message;
			}
			return null;
		}),
		workflow("starter", async (_input, ctx) => {
			await ctx.start("child", "suspended", {}, { dedupeKey: "independent" });
			await ctx.waitForEvent("wait", { event: "test.other" });
			return null;
		}),
	]);
	const cancelled = await runtime.invoke("cancel-call", {}, { metadata });
	await expect.poll(() => completed(cancelled), poll).toBe(true);
	const [receipt] = await sql<
		{ result: { result: { result: { status: string; reason: string } } } }[]
	>`select result from pgconductor._private_steps where execution_id = ${cancelled}::uuid and key = '__fabrial:output'`;
	expect(receipt?.result.result.result).toEqual({ status: "cancelled", reason: "deliberate" });
	const failed = await runtime.invoke("failure-call", {}, { metadata });
	await expect.poll(() => completed(failed), poll).toBe(true);
	expect(caught).toBeInstanceOf(ExecutionError);
	expect((caught as ExecutionError).result).toEqual({ status: "failed", error: "broken" });
	const timeout = await runtime.invoke("timeout-call", {}, { metadata });
	await expect.poll(() => completed(timeout), poll).toBe(true);
	expect(timeoutError).toBe("Child execution timed out after 100ms");
	const parent = await runtime.invoke("starter", {}, { metadata });
	await waiting(parent);
	const [independent] = await sql<
		{ id: string }[]
	>`select id from pgconductor._private_executions where dedupe_key = 'independent'`;
	await waiting(independent!.id);
	await runtime.cancel(parent);
	expect(await terminal(independent!.id)).toEqual({ failed: false, cancelled: false });
});

test("soft mutex group excludes concurrent invocations on one worker", async () => {
	let active = 0;
	let maximum = 0;
	let finished = 0;
	const runtime = await setup([
		workflow(
			"locked",
			async (_input, ctx) => {
				await ctx.step("critical", async () => {
					active++;
					maximum = Math.max(maximum, active);
					try {
						await sql`select pg_sleep(0.03)`;
					} finally {
						active--;
					}
					finished++;
				});
				return null;
			},
			{ mutex: () => "session" },
		),
	]);
	const ids = await Promise.all(
		Array.from({ length: 10 }, () => runtime.invoke("locked", {}, { metadata })),
	);
	await expect.poll(() => finished, poll).toBe(10);
	await expect
		.poll(async () => (await Promise.all(ids.map(completed))).every(Boolean), poll)
		.toBe(true);
	expect(maximum).toBe(1);
});

test("restart preserves native wait subscriptions and memoized effects", async () => {
	let effects = 0;
	let result: Json | undefined;
	const definition = workflow("restart", async (_input, ctx) => {
		await ctx.step("once", () => {
			effects++;
		});
		result = await ctx.waitForEvent("wait", { event: "test.event", filter: { key: ["restart"] } });
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
		cursor: expect.any(String),
	});
});

test("retry exhaustion and suspended cancellation emit terminal notifications the core cannot see", async () => {
	const seen: Json[] = [];
	let attempts = 0;
	const runtime = await setup([
		workflow(
			"failure",
			async () => {
				attempts++;
				throw new Error("exhausted");
			},
			{ retries: { maxAttempts: 2 } },
		),
		workflow("suspended", async (_input, ctx) => {
			await ctx.sleep("long", 60_000);
			return null;
		}),
		workflow(
			"settled",
			async (input) => {
				seen.push(input);
				return null;
			},
			{ triggers: [{ event: EXECUTION_SETTLED_EVENT, role: "observer" }] },
		),
	]);
	const failed = await runtime.invoke("failure", {}, { metadata });
	await expect.poll(() => attempts, poll).toBe(1);
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ unlocked: boolean }[]
			>`select locked_by is null and attempts = 1 as unlocked from pgconductor._private_executions where id = ${failed}::uuid`;
			return row?.unlocked;
		}, poll)
		.toBe(true);
	// Conductor's first retry is 15 seconds. Advance only its due time, not the retry implementation.
	await sql`update pgconductor._private_executions set run_at = now() where id = ${failed}::uuid`;
	await expect.poll(() => terminal(failed), poll).toEqual({ failed: true, cancelled: false });
	expect(attempts).toBe(2);
	const cancelled = await runtime.invoke("suspended", {}, { metadata });
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_steps where execution_id = ${cancelled}::uuid and key = 'long'`;
			return row?.count;
		}, poll)
		.toBe(1);
	await runtime.cancel(cancelled, "cancelled while asleep");
	await expect.poll(() => seen.length, poll).toBe(2);
	expect(seen).toContainEqual({
		executionId: failed,
		result: { status: "failed", error: "exhausted" },
	});
	expect(seen).toContainEqual({
		executionId: cancelled,
		result: { status: "cancelled", reason: "cancelled while asleep" },
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
	>`select id from pgconductor._private_executions where task_key = 'fabrial.route:cron' and cron_expression = '* * * * *'`;
	expect(rows.length).toBeGreaterThan(0);
	await sql`update pgconductor._private_executions set run_at = now() where id = ${rows[0]!.id}::uuid`;
	await expect.poll(() => seen, poll).toBeDefined();
	expect(seen?.input).toEqual({ name: "tick" });
	expect(seen?.metadata).toMatchObject({
		ownsThread: false,
		replyTo: null,
		origin: null,
		requestedBy: null,
	});
});

test("an observer match on the named owner does not gain thread ownership when its owner filter misses", async () => {
	const seen: InvocationMetadata[] = [];
	const runtime = await setup([
		workflow(
			"mixed",
			async (_input, ctx) => {
				seen.push(ctx.metadata);
				return null;
			},
			{
				triggers: [
					{ event: "test.event", role: "owner", filter: { key: ["owner"] } },
					{ event: "test.event", role: "observer" },
				],
			},
		),
	]);
	const withThread: InvocationMetadata = {
		...metadata,
		replyTo: { kind: "thread", provider: "test", threadId: "thread" },
	};
	await runtime.emit("test.event", { key: "observer" }, { metadata: withThread, owner: "mixed" });
	await expect.poll(() => seen.length, poll).toBe(1);
	expect(seen[0]).toEqual({ ...withThread, ownsThread: false, replyTo: null });
	await runtime.emit("test.event", { key: "owner" }, { metadata: withThread, owner: "mixed" });
	await expect.poll(() => seen.length, poll).toBe(2);
	expect(seen[1]).toEqual(withThread);
});

test("waitForAny races concurrent events, replays a single winner, and excludes pre-wait events", async () => {
	let releaseId: string | undefined;
	const results: Json[] = [];
	const runtime = await setup([
		workflow("race", async (_input, ctx) => {
			releaseId = ctx.executionId;
			await ctx.waitForEvent("gate", { event: "test.other" });
			const result = await ctx.waitForAny("race", {
				first: {
					kind: "event",
					event: "test.event",
					filter: { key: [{ prefix: "first" }], amount: [{ numeric: [">=", 2, "<", 5] }] },
				},
				second: {
					kind: "event",
					event: "test.event",
					filter: { key: [{ "anything-but": "first" }], amount: [{ exists: false }] },
				},
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

test("waitForAny resumes across restart and can wait on the execution-settled event", async () => {
	let result: Json | undefined;
	const definition = workflow("restart-race", async (input, ctx) => {
		result = await ctx.waitForAny("race", {
			settled: {
				kind: "event",
				event: EXECUTION_SETTLED_EVENT,
				filter: { executionId: [input as string] },
			},
			timeout: { kind: "timer", at: await ctx.step("deadline", () => Date.now() + 10_000) },
		});
		return result;
	});
	const first = await setup([definition]);
	const id = await first.invoke("restart-race", crypto.randomUUID(), { metadata });
	await waiting(id);
	const [row] = await sql<
		{ payload: { input: string } }[]
	>`select payload from pgconductor._private_executions where id = ${id}::uuid`;
	await first.stop();
	runtimes = runtimes.filter((runtime) => runtime !== first);
	const second = await setup([definition]);
	await second.emit(
		EXECUTION_SETTLED_EVENT,
		{ executionId: row!.payload.input, result: { status: "completed", output: "done" } },
		{ metadata },
	);
	await expect.poll(() => completed(id), poll).toBe(true);
	expect(result).toEqual({
		key: "settled",
		kind: "event",
		cursor: expect.any(String),
		event: {
			name: EXECUTION_SETTLED_EVENT,
			payload: { executionId: row!.payload.input, result: { status: "completed", output: "done" } },
		},
	});
});

test("structured cancellation is recursive, removes waits, and reports cancelled execution branches", async () => {
	let childId: string | undefined;
	let grandchildId: string | undefined;
	let result: Json | undefined;
	const runtime = await setup([
		workflow("grandchild", async (_input, ctx) => {
			grandchildId = ctx.executionId;
			await ctx.waitForEvent("wait", { event: "test.event" });
			return null;
		}),
		workflow("child", async (_input, ctx) => {
			childId = ctx.executionId;
			return ctx.invoke("grandchild", "grandchild", {});
		}),
		workflow("parent", async (_input, ctx) => ctx.invoke("child", "child", {})),
		workflow("watch", async (input, ctx) => {
			result = await ctx.waitForAny("child", {
				child: { kind: "execution", executionId: input as string },
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

test("detached invoke timeout leaves the child running", async () => {
	let childId: string | undefined;
	let timedOut = false;
	const runtime = await setup([
		workflow("child", async (_input, ctx) => {
			childId = ctx.executionId;
			await ctx.waitForEvent("wait", { event: "test.event" });
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

test("terminal hooks survive restart, retry errors, and are never called for suspension", async () => {
	const seen: { id: string; metadata: InvocationMetadata; result: Json }[] = [];
	let rejectHook = true;
	const onSettled: RuntimeWorkflow["onSettled"] = async (id, metadata, result) => {
		if (rejectHook) throw new Error("temporarily unavailable");
		seen.push({ id, metadata, result });
	};
	const definitions = [
		workflow(
			"success",
			async (_input, ctx) => {
				await ctx.waitForEvent("wait", { event: "test.event" });
				return 9;
			},
			{ onSettled },
		),
		workflow(
			"failure",
			async () => {
				throw new Error("failed");
			},
			{ retries: { maxAttempts: 1 }, onSettled },
		),
	];
	const first = await setup(definitions);
	const success = await first.invoke("success", {}, { metadata });
	await waiting(success);
	expect(seen).toEqual([]);
	await first.emit("test.event", {}, { metadata });
	await expect.poll(() => completed(success), poll).toBe(true);
	const failure = await first.invoke("failure", {}, { metadata });
	await expect.poll(() => terminal(failure), poll).toEqual({ failed: true, cancelled: false });
	await first.stop();
	runtimes = runtimes.filter((runtime) => runtime !== first);
	rejectHook = false;
	await setup(definitions);
	await expect.poll(() => seen.length, poll).toBe(2);
	expect(seen).toContainEqual({
		id: success,
		metadata,
		result: { status: "completed", output: 9 },
	});
	expect(seen).toContainEqual({
		id: failure,
		metadata,
		result: { status: "failed", error: "failed" },
	});
	await expect
		.poll(async () => {
			const [row] = await sql<
				{ count: number }[]
			>`select count(*)::int as count from pgconductor._private_steps where key = '__fabrial:settled'`;
			return row?.count;
		}, poll)
		.toBe(2);
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
				triggers: [
					{ event: "test.event", role: "observer" },
					{ event: "test.other", role: "observer" },
				],
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
		{ metadata: { ...metadata, interactionId: "different" }, id: "same" },
	);
	const rows = await sql<
		{ payload: { payload: { key: string; __fabrial: { metadata: InvocationMetadata } } } }[]
	>`
		select payload from pgconductor._private_executions where task_key = 'pgconductor.event-dispatch' order by created_at
	`;
	expect(rows).toHaveLength(2);
	expect(rows[0]?.payload.payload.key).toBe("first");
	expect(rows[0]?.payload.payload.__fabrial.metadata).toEqual(metadata);
});

test("start/invoke mutex overrides share the per-task group", async () => {
	let active = 0;
	let maximum = 0;
	let finished = 0;
	const runtime = await setup([
		workflow(
			"locked",
			async (_input, ctx) => {
				await ctx.step("effect", async () => {
					active++;
					maximum = Math.max(maximum, active);
					try {
						await sql`select pg_sleep(0.02)`;
					} finally {
						active--;
					}
					finished++;
				});
				return true;
			},
			{ mutex: () => "default-key" },
		),
		workflow("parent", async (_input, ctx) => {
			for (let i = 0; i < 6; i++)
				await ctx.start(`start:${i}`, "locked", {}, { mutex: "override-key" });
			return ctx.invoke("last", "locked", {}, { mutex: "override-key" });
		}),
	]);
	const parent = await runtime.invoke("parent", {}, { metadata });
	await expect.poll(() => finished, poll).toBe(7);
	await expect.poll(() => completed(parent), poll).toBe(true);
	expect(maximum).toBe(1);
	const groups = await sql<
		{ group: string }[]
	>`select "group" from pgconductor._private_executions where task_key = 'locked'`;
	expect(groups).toHaveLength(7);
	expect(groups.every((row) => row.group === "override-key")).toBe(true);
});

test("restart repairs a partially cancelled child tree even if a settlement hook keeps failing", async () => {
	let childId: string | undefined;
	const definitions = [
		workflow("child", async (_input, ctx) => {
			childId = ctx.executionId;
			await ctx.waitForEvent("wait", { event: "test.event" });
			return null;
		}),
		workflow("parent", async (_input, ctx) => ctx.invoke("call", "child", {}), {
			onSettled: async () => {
				throw new Error("unavailable");
			},
		}),
	];
	const first = await setup(definitions);
	const parent = await first.invoke("parent", {}, { metadata });
	await expect.poll(() => childId, poll).toBeDefined();
	await waiting(childId!);
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	// Simulate process loss after the parent cancellation commits but before child propagation.
	await sql.begin(async (transaction) => {
		await transaction`select pgconductor.cancel_execution(${parent}::uuid, 'interrupted cancellation')`;
		await transaction`update pgconductor._private_executions set cancelled = true where id = ${parent}::uuid`;
	});
	expect(await terminal(childId!)).toEqual({ failed: false, cancelled: false });
	await setup(definitions);
	await expect.poll(() => terminal(childId!), poll).toEqual({ failed: true, cancelled: true });
});

test("dispatchId dedupes a workflow across event categories and restart without merging event receipts", async () => {
	const seen: { workflow: string; input: Json }[] = [];
	const definitions = ["observer", "other-observer"].map((name) =>
		workflow(
			name,
			async (input, ctx) => {
				seen.push({ workflow: ctx.workflow, input });
				return null;
			},
			{
				triggers: [
					{ event: "test.event", role: "observer" },
					{ event: "test.other", role: "observer" },
				],
			},
		),
	);
	const first = await setup(definitions);
	await Promise.all([
		first.emit("test.event", { key: "one" }, { metadata, id: "delivery", dispatchId: "ingress" }),
		first.emit("test.other", { key: "two" }, { metadata, id: "delivery", dispatchId: "ingress" }),
	]);
	await expect.poll(() => seen.length, poll).toBe(2);
	await expect
		.poll(async () => {
			const [row] = await sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key like 'fabrial.route:%' and completed_at is not null
		`;
			return row?.count;
		}, poll)
		.toBe(4);
	await expect
		.poll(async () => {
			const [row] = await sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = any(${definitions.map((definition) => definition.name)}) and completed_at is not null
		`;
			return row?.count;
		}, poll)
		.toBe(2);
	const original = [...seen];
	await first.stop();
	runtimes = runtimes.filter((entry) => entry !== first);
	const second = await setup(definitions);
	// A distinct receipt in the same ingress still must not start either workflow again.
	await second.emit(
		"test.event",
		{ key: "three" },
		{ metadata, id: "another", dispatchId: "ingress" },
	);
	await expect
		.poll(async () => {
			const [row] = await sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key like 'fabrial.route:%' and completed_at is not null
		`;
			return row?.count;
		}, poll)
		.toBe(6);
	expect(seen).toEqual(original);
	const receipts = await sql<{ id: string }[]>`
		select id from pgconductor._private_executions where task_key = 'pgconductor.event-dispatch'
	`;
	expect(receipts).toHaveLength(3);
	await second.emit("test.other", {}, { metadata, dispatchId: "next-ingress" });
	await expect.poll(() => seen.length, poll).toBe(4);
});

test("failed settlement hooks cannot starve healthy deliveries beyond the first monitor batch", async () => {
	let healthyId: string | undefined;
	const calls = new Map<string, number>();
	const delivered: string[] = [];
	const runtime = await setup([
		workflow("settle", async () => null, {
			onSettled: async (id) => {
				calls.set(id, (calls.get(id) ?? 0) + 1);
				if (id !== healthyId) throw new Error("poison receipt");
				delivered.push(id);
			},
		}),
	]);
	const ids = await Promise.all(
		Array.from({ length: 105 }, () => runtime.invoke("settle", {}, { metadata })),
	);
	await expect
		.poll(async () => (await Promise.all(ids.map(completed))).every(Boolean), poll)
		.toBe(true);
	healthyId = [...ids].sort().at(-1);
	await expect.poll(() => delivered, poll).toEqual([healthyId]);
	await expect
		.poll(
			() => Math.min(...ids.filter((id) => id !== healthyId).map((id) => calls.get(id) ?? 0)),
			poll,
		)
		.toBeGreaterThanOrEqual(2);
	const receipts = await sql<{ execution_id: string }[]>`
		select execution_id from pgconductor._private_steps where key = '__fabrial:settled'
	`;
	expect(receipts.map((row) => row.execution_id)).toEqual([healthyId]);
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
				child: { kind: "execution", executionId: input as string },
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
			pollIntervalMs: 50,
		},
	);
	runtimes.push(runtime);
	const chat = new FakeChat();
	const surface = { kind: "thread", provider: "test", threadId: "channel:general:thread" } as const;
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
					expect(info.metadata.interactionId).toBe("test.begin:delivery");
					return next(ctx);
				},
			],
		},
	});
	const child = defineWorkflow({
		name: "core-child",
		input: z.string(),
		run: async (input, ctx) => {
			expect(ctx.metadata.ownsThread).toBe(false);
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
			expect(ctx.metadata.ownsThread).toBe(true);
			await ctx.step("effect", () => {
				effects++;
			});
			await ctx.thread!.post("ack", "working");
			const output = await ctx.invoke("child", child, input.key);
			// Core exposes native runtime operations through its documented raw escape hatch.
			const execution = ctx.raw.execution as ConductorExecution;
			expect(execution.raw).toBeDefined();
			const event = await execution.waitForEvent("reply", {
				event: "test.reply",
				filter: { key: ["reply"] },
				deadline: await ctx.step("deadline", () => Date.now() + 5_000),
			});
			return { output, event };
		},
	});
	const app = fabrial({ runtime, chat, plugins: [plugin], workflows: [parent, child] });
	await app.start();
	await app.emit("test.begin", { key: "go" }, { id: "delivery", replyTo: surface });
	await expect.poll(() => parentId, poll).toBeDefined();
	await waiting(parentId!);
	const thread = await chat.thread(surface);
	expect((await thread.getState())?.handlerExecutionId).toBe(parentId);
	expect(thread.posts).toHaveLength(1);
	// With no after cursor this wait intentionally starts at registration.
	await app.emit("test.reply", { key: "wrong" });
	await app.emit("test.reply", { key: "reply" }, { id: "reply" });
	await expect.poll(() => completed(parentId!), poll).toBe(true);
	await expect.poll(() => thread.getState(), poll).toBeNull();
	expect(effects).toBe(1);
	expect(childEffects).toBe(1);
	expect(attempts.get(parent.name)).toBeGreaterThanOrEqual(2);
	expect(attempts.get(child.name)).toBe(1);
	const [row] = await sql<{ result: { result: Json } }[]>`
		select result from pgconductor._private_steps where execution_id = ${parentId!}::uuid and key = '__fabrial:output'
	`;
	expect(row?.result.result).toEqual({
		output: "go:child",
		event: { name: "test.reply", payload: { key: "reply" }, cursor: expect.any(String) },
	});
	await expect
		.poll(async () => {
			const settlements = await sql<{ payload: { payload: { executionId: string } } }[]>`
			select payload from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = ${EXECUTION_SETTLED_EVENT}
		`;
			return settlements.map((row) => row.payload.payload.executionId);
		}, poll)
		.toContain(parentId);
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
			await ctx.waitForEvent("gate", { event: "test.other" });
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
	expect(children).toHaveLength(2);
	expect(children[1]).toBe(children[0]);
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
			pollIntervalMs: 50,
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
				const approvalId = action.id.slice("fabrial.approval.approve:".length);
				const [row] = await sql<
					{ count: number }[]
				>`select count(*)::int as count from pgconductor._private_custom_event_subscriptions
					where execution_id = ${approvalId.split(":")[0]!}::uuid and event_key = 'fabrial.approval.decided'`;
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
	const app = fabrial({ runtime, chat, plugins: [plugin], workflows: [parent], identity: [alice] });
	await app.start();
	await app.emit("test.begin", { key: "go" }, { id: "approval" });
	await expect.poll(() => parentId, poll).toBeDefined();
	await expect.poll(() => completed(parentId!), poll).toBe(true);
	expect(decision).toBe("approved");
	expect(clicks).toBe(1);
	expect(dm.posts).toHaveLength(1);
	expect(dm.posts[0]!.content).toMatchObject({ card: { text: "Approved by Alice", actions: [] } });
	await app.stop();
	runtimes = runtimes.filter((entry) => entry !== runtime);
});
