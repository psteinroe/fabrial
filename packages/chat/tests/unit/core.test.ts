// oxlint-disable typescript/unbound-method -- Referenced adapter methods are Vitest mocks.
import { createTestMessage } from "@chat-adapter/tests";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createFabrial, trigger, type WorkflowContext } from "fabrial";
import { z } from "zod";
import { MemoryRuntime } from "fabrial/testing";
import { expect, it, vi } from "vitest";
import { chat, chatTrigger } from "../../src/index.ts";
import { setup } from "../helper.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

it("dispatches a mention to its owner and all matching observers without duplicate runs", async () => {
	const env = setup();
	const runtime = new MemoryRuntime();
	const owner = vi.fn(async () => {});
	const observer = vi.fn(async () => {});
	const app = createFabrial({ plugins: env.host.plugins }).app({
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
	const app = createFabrial({ plugins: env.host.plugins }).app({
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

it("authorizes native stop actors through core rather than cancelling the handler directly", async () => {
	const env = setup();
	const state = createMemoryState();
	const runtime = new MemoryRuntime();
	const app = createFabrial({ plugins: env.host.plugins }).app({
		runtime,
		chat: chat({ state }),
		workflows: [
			defineWorkflow({
				name: "owner",
				triggers: [chatTrigger("slack", "message")],
				run: async (_input, ctx) => {
					await ctx.sleep("wait", 1000);
				},
			}),
		],
	});
	await app.start();
	try {
		const threadId = "slack:C1:root";
		await env.sdk().processMessage(
			env.adapter,
			threadId,
			createTestMessage("root", "hello", {
				author: { userId: "U1", userName: "alice", fullName: "Alice", isMe: false, isBot: false },
			}),
		);
		await runtime.flush();
		const stop = async (userId: string) => {
			const tasks: Promise<unknown>[] = [];
			env.sdk().processAgentSessionStopped(
				{
					adapter: env.adapter,
					threadId,
					channelId: "C1",
					threadTs: "root",
					streamingMessageTs: ["stream"],
					userId,
				},
				{
					waitUntil: (task) => {
						tasks.push(task);
					},
				},
			);
			await Promise.all(tasks);
			await runtime.flush();
		};
		await stop("outsider");
		expect(runtime.executions("owner")[0]?.status).toBe("suspended");
		expect(env.adapter.postEphemeral).toHaveBeenCalled();
		await stop("U1");
		expect(runtime.executions("owner")[0]?.result?.status).toBe("cancelled");
	} finally {
		await app.stop();
	}
});
