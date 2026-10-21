import { expect, test } from "vitest";
import type { InvocationMetadata, Json, RuntimeWorkflow } from "../../internals.ts";
import { until, useConductor } from "../../conductor/helper.ts";

const db = useConductor();
const metadata: InvocationMetadata = {
	requestId: "interaction",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
const HOUR = 60 * 60 * 1000;
const signal = (id: string, payload: Json = id) => ({ name: "input", id, payload });

/** Collects "input" signals until one carries "stop"; returns what this attempt received. */
function session(received: Json[], extra: Partial<RuntimeWorkflow> = {}): RuntimeWorkflow {
	return {
		name: "session",
		triggers: [],
		handler: async (_input, ctx) => {
			const attempt: Json[] = [];
			await new Promise<void>((resolve, reject) => {
				// Shutdown releases an attempt that stops on its abort signal.
				ctx.signal.addEventListener("abort", () => reject(ctx.signal.reason as Error));
				ctx.onSignal("input", (payload) => {
					received.push(payload);
					attempt.push(payload);
					if (payload === "stop") resolve();
				});
			});
			return attempt;
		},
		...extra,
	};
}

test("a running handler receives signals from outside; stored signals arrive first, in order, deduped by id", async () => {
	const received: Json[] = [];
	// One slot, held by another key, keeps the first execution pending while its signals are stored.
	const runtime = await db.start([session(received, { concurrency: 1 })]);
	const start = { workflow: "session", metadata };
	const blocker = db.execution(
		await runtime.signalWithStart({ ...start, key: "blocker", signal: signal("0", "held") }),
	);
	await blocker.running();
	const id = await runtime.signalWithStart({ ...start, key: "thread-1", signal: signal("1", "a") });
	expect(await runtime.signal(id, signal("2", "b"))).toBe(true);
	expect(
		await runtime.signalWithStart({ ...start, key: "thread-1", signal: signal("1", "dup") }),
	).toBe(id);
	const execution = db.execution(id);
	expect(await execution.info()).toMatchObject({ status: "pending", attempts: 0 });

	expect(await runtime.signal(blocker.id, signal("stop"))).toBe(true);
	expect(await blocker.output()).toEqual(["held", "stop"]);
	await execution.running();
	await until("stored signals", async () => (received.length === 4 ? true : undefined));
	expect(received).toEqual(["held", "stop", "a", "b"]);
	expect(await runtime.signal(id, signal("2", "dup"))).toBe(true);
	expect(await runtime.signal(id, signal("3", "stop"))).toBe(true);
	expect(await execution.output()).toEqual(["a", "b", "stop"]);
	expect(received).toEqual(["held", "stop", "a", "b", "stop"]);
	expect(await runtime.signal(id, signal("4"))).toBe(false);
	expect(await runtime.signal(crypto.randomUUID(), signal("4"))).toBe(false);
});

test("concurrent signalWithStart calls start exactly one execution per key; settlement releases the key", async () => {
	const runtime = await db.start([session([])]);
	const send = (id: string, key = "thread-1") =>
		runtime.signalWithStart({ workflow: "session", key, signal: signal(id), metadata });
	const ids = await Promise.all(Array.from({ length: 10 }, (_, i) => send(`s${i}`)));
	expect(new Set(ids).size).toBe(1);
	const other = await send("o1", "thread-2");
	expect(other).not.toBe(ids[0]);
	expect(await send("stop", "thread-1")).toBe(ids[0]);
	expect(await send("stop", "thread-2")).toBe(other);
	const first = await db.execution(ids[0]!).output();
	expect(first).toHaveLength(11);
	expect(new Set(first as Json[])).toEqual(new Set([...ids.map((_, i) => `s${i}`), "stop"]));
	expect(await db.execution(other).output()).toEqual(["o1", "stop"]);
	const next = await send("n1");
	expect(next).not.toBe(ids[0]);
	await send("stop");
	expect(await db.execution(next).output()).toEqual(["n1", "stop"]);
	await expect(
		runtime.signalWithStart({ workflow: "missing", key: "k", signal: signal("x"), metadata }),
	).rejects.toThrow("Unknown workflow");
});

test("ctx.signalWithStart is memoized per id and passes merged metadata to the execution it starts", async () => {
	const received: Json[] = [];
	const seen: InvocationMetadata[] = [];
	const recorders: string[] = [];
	const runtime = await db.start([
		{
			name: "recorder",
			triggers: [],
			handler: async (_input, ctx) => {
				recorders.push(ctx.executionId);
				seen.push(ctx.metadata);
				ctx.onSignal("input", (payload) => void received.push(payload));
			},
		},
		{
			name: "caller",
			triggers: [],
			handler: async (_input, ctx) => {
				const options = { workflow: "recorder", key: "k", metadata: { ownerWorkflow: "recorder" } };
				const first = await ctx.signalWithStart("send", { ...options, signal: signal("a") });
				await ctx.sleep("pause", HOUR);
				const again = await ctx.signalWithStart("send", { ...options, signal: signal("b") });
				const stop = await ctx.signalWithStart("stop", { ...options, signal: signal("stop") });
				return [first, again, stop];
			},
		},
	]);
	const caller = db.execution(await runtime.invoke("caller", null, { metadata }));
	await caller.suspended("pause");
	const first = await until("the first recorder", async () => recorders[0]);
	await db.execution(first).output();
	// The first keyed execution settled during the sleep and released its key.
	await db.clock.advance(HOUR);
	const [memoized, again, stop] = (await caller.output()) as string[];
	expect(memoized).toBe(first);
	expect(again).toBe(first);
	expect(stop).not.toBe(first);
	await db.execution(stop!).output();
	expect(received).toEqual(["a", "stop"]);
	expect(seen[0]).toMatchObject({ ...metadata, ownerWorkflow: "recorder" });
});

test("signals arriving while the handler returns re-queue the execution without counting an attempt", async () => {
	let attempts = 0;
	const received: Json[] = [];
	const runtime = await db.start([
		{
			name: "returning",
			triggers: [],
			retries: { maxAttempts: 1 },
			handler: async (_input, ctx) => {
				attempts++;
				ctx.onSignal("input", async (payload) => {
					received.push(payload);
					if (payload !== "first") return;
					// By now the workflow handler has returned and this attempt reads no more signals.
					await runtime.signal(ctx.executionId, signal("second"));
				});
				ctx.onSignal("other", () => {});
				return attempts;
			},
		},
	]);
	const id = await runtime.signalWithStart({
		workflow: "returning",
		key: "k",
		signal: signal("first"),
		metadata,
	});
	const execution = db.execution(id);
	expect(await execution.settled()).toEqual({ status: "completed", output: 2 });
	expect(received).toEqual(["first", "second"]);
	expect(await execution.info()).toMatchObject({ attempts: 1 });
	expect(await runtime.signal(id, signal("x"))).toBe(false);
});

test("a throwing signal handler fails the attempt; the retry receives the signal again", async () => {
	let attempts = 0;
	const runtime = await db.start([
		{
			name: "failing",
			triggers: [],
			retries: { maxAttempts: 2 },
			handler: async (_input, ctx) => {
				attempts++;
				const delivered = new Promise<Json>((resolve) =>
					ctx.onSignal("late", (payload) => {
						if (attempts === 1) throw new Error("boom");
						resolve(payload);
					}),
				);
				await ctx.step("after", () => null);
				return delivered;
			},
		},
		{
			name: "fatal",
			triggers: [],
			retries: { maxAttempts: 1 },
			handler: async (_input, ctx) => {
				ctx.onSignal("input", () => {
					throw new Error("boom");
				});
				await ctx.step("after", () => null);
			},
		},
	]);
	const execution = db.execution(
		await runtime.signalWithStart({
			workflow: "failing",
			key: "k",
			signal: { name: "late", id: "1", payload: "ok" },
			metadata,
		}),
	);
	// The retry is scheduled 15s after the failed attempt.
	await until("the failed attempt", async () => {
		const info = await execution.info();
		return info?.status === "pending" && info.attempts === 1 ? info : undefined;
	});
	await db.clock.advance("15s");
	expect(await execution.settled()).toEqual({ status: "completed", output: "ok" });
	expect(attempts).toBe(2);

	const fatal = await runtime.signalWithStart({
		workflow: "fatal",
		key: "k",
		signal: signal("1"),
		metadata,
	});
	expect(await db.execution(fatal).settled()).toEqual({
		status: "failed",
		error: 'Signal handler for "input" failed: boom',
	});
});

test("a restarted runtime delivers the signals the stopped one had not, and none twice", async () => {
	const received: Json[] = [];
	const runtime = await db.start([session(received)]);
	const send = (id: string, payload: Json, to = runtime) =>
		to.signalWithStart({
			workflow: "session",
			key: "thread-1",
			signal: signal(id, payload),
			metadata,
		});
	const id = await send("1", "a");
	const execution = db.execution(id);
	await until("the first signal", async () => (received.length === 1 ? true : undefined));
	await execution.running();

	const restarted = await runtime.restart();
	expect(await send("2", "b", restarted)).toBe(id);
	expect(await send("1", "dup", restarted)).toBe(id);
	expect(await send("3", "stop", restarted)).toBe(id);
	expect(await execution.output()).toEqual(["b", "stop"]);
	expect(received).toEqual(["a", "b", "stop"]);
});

test("cancelling a session drops its signals and releases its key", async () => {
	const received: Json[] = [];
	const runtime = await db.start([session(received)]);
	const send = (id: string) =>
		runtime.signalWithStart({ workflow: "session", key: "thread-1", signal: signal(id), metadata });
	const execution = db.execution(await send("1"));
	await until("the first signal", async () => (received.length === 1 ? true : undefined));

	expect(await runtime.cancel(execution.id, "closed")).toBe(true);
	expect(await execution.settled()).toEqual({ status: "cancelled", reason: "closed" });
	expect(await runtime.signal(execution.id, signal("2"))).toBe(false);
	expect(await runtime.cancel(execution.id)).toBe(false);

	const next = await send("3");
	expect(next).not.toBe(execution.id);
	await send("stop");
	expect(await db.execution(next).output()).toEqual(["3", "stop"]);
	expect(received).toEqual(["1", "3", "stop"]);
});
