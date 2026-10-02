import { defineWorkflow, fabrial, trigger, type InvocationMetadata } from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import { expect, it } from "vitest";

// Pi clears the driver's trigger markers for tool children. The real-Pi integration
// scenarios also assert this metadata and the requester's card in the origin thread.
it("a tool child retains its response thread without inheriting trigger ownership", async () => {
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
		triggerEvent: null,
		ownerWorkflow: null,
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
