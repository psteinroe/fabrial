import { beforeEach, expect, it } from "vitest";
import type { InvocationMetadata, RuntimeWorkflow } from "../../internals.ts";
import { useConductor } from "../../conductor/helper.ts";

// Runtime-port contracts the core relies on, on real Conductor.
const db = useConductor();
beforeEach(() => db.clock.set(new Date()));
const metadata: InvocationMetadata = {
	interactionId: "test",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
const waiter = (handler: RuntimeWorkflow["handler"]): RuntimeWorkflow => ({
	name: "waiter",
	triggers: [],
	// One attempt: the test clock reaches Conductor's retry backoff only when a test moves it.
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

it("rejects unknown child waits", async () => {
	const runtime = await db.start([
		waiter(async (_i, execution) => {
			await execution.waitForAny("wait", {
				child: { execution: crypto.randomUUID() },
			});
		}),
	]);
	const id = await runtime.invoke("waiter", {}, { metadata });
	expect(await db.execution(id).settled()).toMatchObject({
		status: "failed",
		error: expect.stringContaining("not found"),
	});
});
