// oxlint-disable typescript/unbound-method -- Referenced adapter methods are Vitest mocks.
import { createTestMessage } from "@chat-adapter/tests";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createFabrial, trigger, type WorkflowContext } from "../../../src/index.ts";
import { z } from "zod";
import { MemoryRuntime, testApp } from "../../../src/testing.ts";
import { expect, it, vi } from "vitest";
import { chat, chatTrigger } from "../../../src/chat/index.ts";
import { setup } from "../../chat/helper.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

it("dispatches a mention to its owner and all matching observers without duplicate runs", async () => {
	const env = setup();
	const runtime = new MemoryRuntime();
	const owner = vi.fn(async () => {});
	const observer = vi.fn(async () => {});
	const app = testApp(createFabrial({ plugins: env.host.plugins }), {
		runtime,
		chat: chat({ state: createMemoryState() }),
		workflows: [
			defineWorkflow({ name: "owner", triggers: [chatTrigger("slack", "mentioned")], run: owner }),
			defineWorkflow({
				name: "observer",
				triggers: [
					chatTrigger("slack", "message", { observe: true }),
					chatTrigger("slack", "mentioned", { observe: true }),
				],
				run: observer,
			}),
		],
	});
	await app.start();
	try {
		await env.sdk().processMessage(
			env.adapter,
			"slack:C1:root",
			createTestMessage("root", "hello", {
				isMention: true,
				author: { userId: "U1", userName: "alice", fullName: "Alice", isMe: false, isBot: false },
			}),
		);
		await runtime.flush();
		expect(owner).toHaveBeenCalledOnce();
		expect(observer).toHaveBeenCalledOnce();
		expect(
			runtime.emitted
				.filter((event) => event.name.startsWith("slack."))
				.map((event) => event.name)
				.sort(),
		).toEqual(["slack.mentioned", "slack.message"]);
	} finally {
		await app.stop();
	}
});

it("rebinds a lazy channel from the durable first-post receipt on replay and child invocation", async () => {
	const env = setup();
	const runtime = new MemoryRuntime();
	const child = defineWorkflow({
		name: "child",
		input: z.null(),
		run: async (_input, ctx) => {
			expect(ctx.metadata.replyTo).toEqual({
				kind: "thread",
				provider: "slack",
				threadId: "slack:C1:root-1",
			});
			return null;
		},
	});
	const refs: string[] = [];
	const app = testApp(createFabrial({ plugins: env.host.plugins }), {
		runtime,
		chat: chat({ state: createMemoryState() }),
		workflows: [
			child,
			defineWorkflow({
				name: "owner",
				triggers: [trigger({ event: "demo.work" })],
				run: async (_input, ctx: WorkflowContext) => {
					const receipt = await ctx.thread!.post("root", "hello");
					refs.push(ctx.thread!.ref.threadId);
					expect(ctx.thread!.ref.threadId).toBe(receipt.threadId);
					await ctx.sleep("pause", 1000);
					await ctx.thread!.post("reply", "continued");
					await ctx.invoke("child", child, null);
				},
			}),
		],
	});
	await app.start();
	try {
		await app.emit(
			"demo.work",
			{},
			{ replyTo: { kind: "channel", provider: "slack", channelId: "C1" } },
		);
		await runtime.flush();
		await runtime.advanceBy(1000);
		expect(runtime.executions("owner")[0]?.result?.status).toBe("completed");
		expect(refs).toEqual(["slack:C1:root-1", "slack:C1:root-1", "slack:C1:root-1"]);
		expect(env.adapter.postChannelMessage).toHaveBeenCalledOnce();
		expect(env.adapter.postMessage).toHaveBeenCalledExactlyOnceWith("slack:C1:root-1", "continued");
	} finally {
		await app.stop();
	}
});
