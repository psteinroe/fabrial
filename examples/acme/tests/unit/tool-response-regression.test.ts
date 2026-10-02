import { defineWorkflow, fabrial, trigger, type InvocationMetadata } from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import { expect, it } from "vitest";

// Expected failure documents the metadata produced by @fabrial/pi/src/context.ts:start.
// The full integration tests exercise the example-local workaround with real Pi.
it.fails("a tool child must retain its response thread despite inherited trigger ownership", async () => {
	const runtime = new MemoryRuntime();
	const chat = new FakeChat();
	const child = defineWorkflow({
		name: "tool-child",
		async run(_input, ctx) {
			return { hasThread: !!ctx.thread };
		},
	});
	const owner = defineWorkflow({
		name: "original-owner",
		triggers: [trigger({ event: "example.mention" })],
		async run() {
			return null;
		},
	});
	const app = fabrial({ runtime, chat, plugins: [], workflows: [owner, child] });
	const metadata: InvocationMetadata = {
		interactionId: "interaction",
		origin: null,
		replyTo: { kind: "thread", provider: "slack", threadId: "slack:C_SUPPORT:root" },
		requestedBy: null,
		ownsThread: false,
		triggerEvent: "example.mention",
		ownerWorkflow: owner.name,
	};
	try {
		await app.start();
		const id = await runtime.invoke(child.name, null, { metadata });
		await runtime.flush();
		expect(runtime.result(id)).toEqual({ status: "completed", output: { hasThread: true } });
	} finally {
		await app.stop();
	}
});
