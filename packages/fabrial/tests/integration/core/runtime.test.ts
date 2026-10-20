import { beforeEach, expect, it } from "vitest";
import type { InvocationMetadata, Json, RuntimeWorkflow } from "../../../src/index.ts";
import { until, useConductor } from "../../conductor/helper.ts";

// Runtime-port contracts the core relies on, on real Conductor.
const db = useConductor();
beforeEach(() => db.clock.set(new Date()));
// PHASE2: drop once conductor fixes the frozen test clock (event created_at == subscription created_at).
const realTime = () => db.clock.reset();
const metadata: InvocationMetadata = {
	interactionId: "test",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
const surface = { kind: "thread", provider: "chat", threadId: "channel:general:thread" } as const;
const events = ["ping", "observer", "owned"].map((name) => ({ name, filterable: [] }));
const waiter = (handler: RuntimeWorkflow["handler"]): RuntimeWorkflow => ({
	name: "waiter",
	triggers: [],
	// One attempt: the frozen test clock never reaches Conductor's retry backoff.
	retries: { maxAttempts: 1 },
	handler,
});

it("enforces workflow concurrency", async () => {
	let active = 0;
	let max = 0;
	const work = async () => {
		active++;
		max = Math.max(max, active);
		await new Promise((resolve) => setTimeout(resolve, 20));
		active--;
	};
	const runtime = await db.start([
		{
			name: "limited",
			triggers: [],
			concurrency: 1,
			handler: (_input, execution) => execution.step("work", work),
		},
	]);
	const ids = await Promise.all(
		Array.from({ length: 3 }, () => runtime.invoke("limited", {}, { metadata })),
	);
	for (const id of ids) await db.execution(id).output();
	expect(max).toBe(1);
});

it("redelivers failed settlement hooks without replaying terminal handlers", async () => {
	let handlers = 0;
	let deliveries = 0;
	const runtime = await db.start([
		{
			name: "owner",
			triggers: [],
			handler: async () => {
				handlers++;
			},
			onSettled: async () => {
				if (++deliveries === 1) throw new Error("network");
			},
		},
	]);
	await db.execution(await runtime.invoke("owner", {}, { metadata })).output();
	await until("the redelivered hook", async () => (deliveries >= 2 ? true : undefined));
	expect(handlers).toBe(1);
	expect(deliveries).toBe(2);
});

it("rejects unknown child waits", async () => {
	const runtime = await db.start([
		waiter(async (_i, execution) => {
			await execution.waitForAny("wait", {
				child: { kind: "execution", executionId: crypto.randomUUID() },
			});
		}),
	]);
	const id = await runtime.invoke("waiter", {}, { metadata });
	expect(await db.execution(id).settled()).toMatchObject({
		status: "failed",
		error: expect.stringContaining("Unknown execution"),
	});
});

it.each([false, true])(
	"only explicit after sees events emitted before registration (after=%s)",
	async (explicit) => {
		await realTime();
		const runtime = await db.start(
			[
				waiter(async (_i, execution) => {
					const after = await execution.cursor("fabrial:cursor");
					await execution.step("fabrial:emit", () => execution.emit("ping", { value: "early" }));
					const deadline = await execution.step("deadline", () => runtime.now!() + 60_000);
					const value = await execution.waitForEvent("wait", {
						event: "ping",
						...(explicit ? { after } : {}),
						deadline,
					});
					return value?.payload ?? null;
				}),
			],
			{ events },
		);
		const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
		if (explicit) expect(await execution.output()).toEqual({ value: "early" });
		else {
			await execution.suspended("wait");
			await runtime.emit("ping", { value: "late" }, { metadata });
			expect(await execution.output()).toEqual({ value: "late" });
		}
	},
);

it("memoizes a cursor across replay and continues after the consumed event cursor", async () => {
	const runtime = await db.start(
		[
			waiter(async (_i, execution) => {
				const cursor = await execution.cursor("fabrial:cursor");
				await execution.sleep("delay", 10);
				const first = await execution.waitForEvent("first", { event: "ping", after: cursor });
				await execution.step("fabrial:between", () => execution.emit("ping", { value: "second" }));
				const second = await execution.waitForEvent("second", {
					event: "ping",
					after: first!.cursor,
				});
				return [first!.payload.value!, second!.payload.value!];
			}),
		],
		{ events },
	);
	const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
	await execution.suspended("delay");
	await runtime.emit("ping", { value: "first" }, { metadata });
	await db.clock.advance(10);
	expect(await execution.output()).toEqual(["first", "second"]);
});

it("does not let a late event beat an absolute expired deadline", async () => {
	const runtime = await db.start(
		[
			waiter(async (_i, execution) => {
				const deadline = await execution.step("deadline", () => runtime.now!() + 100);
				const after = await execution.cursor("cursor");
				await execution.sleep("delay", 200);
				return (await execution.waitForEvent("wait", {
					event: "ping",
					after,
					deadline,
				})) as unknown as Json;
			}),
		],
		{ events },
	);
	const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
	await execution.suspended("delay");
	await db.clock.advance(150);
	await runtime.emit("ping", {}, { metadata });
	await db.clock.advance(50);
	expect(await execution.output()).toBeNull();
});

it("suppresses a canonical owner's competing observer emission, even when it arrives first", async () => {
	const runs: InvocationMetadata[] = [];
	const runtime = await db.start(
		[
			{
				name: "owner",
				triggers: [
					{ event: "observer", role: "observer" },
					{ event: "owned", role: "owner" },
				],
				handler: async (_input, execution) => {
					runs.push(execution.metadata);
					return "ok";
				},
			},
		],
		{ events },
	);
	const options = {
		metadata: { ...metadata, replyTo: surface as InvocationMetadata["replyTo"] },
		dispatchId: "dispatch",
		dispatchOwner: { workflow: "owner", event: "owned" },
	};
	await runtime.emit("observer", {}, options);
	await runtime.emit("owned", {}, { ...options, owner: "owner" });
	await until("the owner", async () => runs[0]);
	expect(runs).toHaveLength(1);
	expect(runs[0]!.replyTo).toEqual(surface);
});
