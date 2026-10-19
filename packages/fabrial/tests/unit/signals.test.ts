import { expect, test } from "vitest";
import type { InvocationMetadata, Json, RuntimeWorkflow } from "../../src/index.ts";
import { MemoryRuntime } from "../../src/testing.ts";

const metadata: InvocationMetadata = {
	interactionId: "interaction",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
const signal = (id: string, payload: Json = id) => ({ name: "input", id, payload });

async function setup(workflows: RuntimeWorkflow[]): Promise<MemoryRuntime> {
	const runtime = new MemoryRuntime();
	runtime.register({
		workflows: workflows.map((workflow) => ({ ...workflow, triggers: [] })),
		events: [],
	});
	await runtime.start();
	return runtime;
}
/** Collects "input" signals until one carries "stop". */
function session(received: Json[]): Omit<RuntimeWorkflow, "triggers"> & { triggers: [] } {
	return {
		name: "session",
		triggers: [],
		handler: async (_input, ctx) => {
			await new Promise<void>((resolve) =>
				ctx.onSignal("input", (payload) => {
					received.push(payload);
					if (payload === "stop") resolve();
				}),
			);
			return received;
		},
	};
}

test("a running handler receives signals from outside; stored signals arrive first, in order, deduped by id", async () => {
	const received: Json[] = [];
	const runtime = await setup([session(received)]);
	const start = { workflow: "session", key: "thread-1", metadata };
	const id = await runtime.signalWithStart({ ...start, signal: signal("1", "a") });
	expect(await runtime.signal(id, signal("2", "b"))).toBe(true);
	expect(await runtime.signalWithStart({ ...start, signal: signal("1", "dup") })).toBe(id);
	const flushing = runtime.flush();
	await expect.poll(() => received).toEqual(["a", "b"]);
	expect(runtime.executions("session")[0]!.status).toBe("running");
	expect(await runtime.signal(id, signal("2", "dup"))).toBe(true);
	expect(await runtime.signal(id, signal("3", "stop"))).toBe(true);
	await flushing;
	expect(runtime.result(id)).toEqual({ status: "completed", output: ["a", "b", "stop"] });
	expect(runtime.signals(id).map((s) => [s.id, s.delivered])).toEqual([
		["1", true],
		["2", true],
		["3", true],
	]);
	expect(await runtime.signal(id, signal("4"))).toBe(false);
	expect(await runtime.signal("unknown", signal("4"))).toBe(false);
});

test("concurrent signalWithStart calls start exactly one execution per key; settlement releases the key", async () => {
	const received: Json[] = [];
	const runtime = await setup([session(received)]);
	const send = (id: string, key = "thread-1") =>
		runtime.signalWithStart({ workflow: "session", key, signal: signal(id), metadata });
	const ids = await Promise.all(Array.from({ length: 10 }, (_, i) => send(`s${i}`)));
	expect(new Set(ids).size).toBe(1);
	const other = await send("o1", "thread-2");
	expect(other).not.toBe(ids[0]);
	expect(runtime.executions("session")).toHaveLength(2);
	await send("stop", "thread-1");
	await send("stop", "thread-2");
	await runtime.flush();
	expect(runtime.executions("session").map((e) => e.status)).toEqual(["settled", "settled"]);
	const next = await send("n1");
	expect(next).not.toBe(ids[0]);
	expect(runtime.executions("session")).toHaveLength(3);
	await expect(
		runtime.signalWithStart({ workflow: "missing", key: "k", signal: signal("x"), metadata }),
	).rejects.toThrow("Unknown workflow");
});

test("ctx.signalWithStart is memoized per id and passes merged metadata to the execution it starts", async () => {
	const received: Json[] = [];
	const seen: InvocationMetadata[] = [];
	let replays = 0;
	const runtime = await setup([
		{
			name: "recorder",
			triggers: [],
			handler: async (_input, ctx) => {
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
				replays++;
				if (replays === 1) await ctx.sleep("pause", 10);
				const again = await ctx.signalWithStart("send", { ...options, signal: signal("b") });
				const stop = await ctx.signalWithStart("stop", { ...options, signal: signal("stop") });
				return [first, again, stop];
			},
		},
	]);
	const caller = await runtime.invoke("caller", null, { metadata });
	await runtime.flush();
	await runtime.advanceBy(10);
	const output = runtime.result(caller);
	expect(output?.status).toBe("completed");
	const [first, again, stop] = (output as { output: string[] }).output;
	expect(again).toBe(first);
	// The first keyed execution settled during the sleep and released its key.
	expect(stop).not.toBe(first);
	expect(received).toEqual(["a", "stop"]);
	expect(seen[0]).toMatchObject(metadata);
});

test("signals arriving while the handler returns re-queue the execution without counting an attempt", async () => {
	let attempts = 0;
	const received: Json[] = [];
	const runtime = await setup([
		{
			name: "returning",
			triggers: [],
			retries: { maxAttempts: 1 },
			handler: async (_input, ctx) => {
				attempts++;
				ctx.onSignal("input", async (payload) => {
					received.push(payload);
					if (payload !== "first") return;
					// By now the workflow handler has returned and delivery stopped for this attempt.
					await new Promise((resolve) => setTimeout(resolve, 0));
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
	await runtime.flush();
	expect(received).toEqual(["first", "second"]);
	expect(runtime.result(id)).toEqual({ status: "completed", output: 2 });
	expect(await runtime.signal(id, signal("x"))).toBe(false);
});

test("a throwing signal handler fails the attempt; the retry receives the signal again", async () => {
	let attempts = 0;
	const runtime = await setup([
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
	]);
	const id = await runtime.signalWithStart({
		workflow: "failing",
		key: "k",
		signal: { name: "late", id: "1", payload: "ok" },
		metadata,
	});
	await runtime.flush();
	expect(attempts).toBe(2);
	expect(runtime.result(id)).toEqual({ status: "completed", output: "ok" });

	const failing = await setup([
		{
			name: "failing",
			triggers: [],
			handler: async (_input, ctx) => {
				ctx.onSignal("input", () => {
					throw new Error("boom");
				});
				await ctx.step("after", () => null);
			},
		},
	]);
	const failed = await failing.signalWithStart({
		workflow: "failing",
		key: "k",
		signal: signal("1"),
		metadata,
	});
	await failing.flush();
	expect(failing.result(failed)).toEqual({
		status: "failed",
		error: 'Signal handler for "input" failed: boom',
	});
});
