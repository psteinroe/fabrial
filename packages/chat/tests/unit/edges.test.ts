// oxlint-disable typescript/unbound-method -- All referenced methods here are Vitest mocks.
import { createMockAdapter, createTestMessage } from "@chat-adapter/tests";
import type { ChatInstance } from "chat";
import { afterEach, expect, it, vi } from "vitest";
import { chat, chatCapability, chatEvents } from "../../src/index.ts";
import { routingState, setup } from "../helper.ts";

const ref = { kind: "thread" as const, provider: "slack", threadId: "slack:C1:root" };
const author = { userId: "U1", userName: "alice", fullName: "Alice", isMe: false, isBot: false };
afterEach(() => vi.useRealTimers());

it("initializes ONE SDK bot for all providers and exposes only chat webhook routes", async () => {
	const env = setup();
	let githubBot: ChatInstance | undefined;
	const github = createMockAdapter("github", {
		initialize: vi.fn(async (sdk) => {
			githubBot = sdk;
		}),
	});
	const connection = chat({ state: env.state }).connect({
		...env.host,
		plugins: [
			...env.host.plugins,
			{
				id: "github",
				events: chatEvents(),
				chat: chatCapability({ adapter: () => github, installationId: "org" }),
			},
			{ id: "non-chat" },
		],
	});
	await connection.port.thread(ref);
	expect(githubBot).toBe(env.sdk());
	expect(Object.keys(connection.routes)).toEqual(["POST /slack/events", "POST /github/events"]);
	expect(github.initialize).toHaveBeenCalledOnce();
	expect(env.adapter.initialize).toHaveBeenCalledOnce();
});

it("does not coalesce concurrent arrivals", async () => {
	const env = setup();
	await env.connection.port.thread(ref);
	await Promise.all(
		Array.from({ length: 20 }, (_, index) =>
			env
				.sdk()
				.processMessage(
					env.adapter,
					ref.threadId,
					createTestMessage(`message-${index}`, "hello", { author }),
				),
		),
	);
	expect(env.host.receiveMessage).toHaveBeenCalledTimes(20);
});

it("allows failed webhook ingress to be retried with the same platform id", async () => {
	const env = setup();
	vi.mocked(env.host.receiveMessage)
		.mockRejectedValueOnce(new Error("transient"))
		.mockResolvedValue("new");
	vi.mocked(env.adapter.handleWebhook).mockImplementation(async (_request, options) => {
		await env
			.sdk()
			.processMessage(
				env.adapter,
				ref.threadId,
				createTestMessage("same-id", "hello", { author }),
				options,
			);
		return new Response("ok");
	});
	const request = () => new Request("https://app/slack/events", { method: "POST" });
	const route = env.connection.routes["POST /slack/events"]!;
	await expect(route(request())).rejects.toThrow("transient");
	await expect(route(request())).resolves.toHaveProperty("status", 200);
	expect(env.host.receiveMessage).toHaveBeenCalledTimes(2);
	expect(vi.mocked(env.host.receiveMessage).mock.calls[0]?.[1]).toEqual(
		vi.mocked(env.host.receiveMessage).mock.calls[1]?.[1],
	);
});

it("expires routing bindings after 30 days", async () => {
	vi.useFakeTimers();
	const env = setup();
	const thread = await env.connection.port.thread(ref);
	await thread.setState(routingState);
	vi.advanceTimersByTime(30 * 24 * 60 * 60 * 1000 + 1);
	expect(await thread.getState()).toBeNull();
});

it("maps the native platform stop control to actor-aware cancellation ingress", async () => {
	const env = setup();
	const thread = await env.connection.port.thread(ref);
	await thread.setState(routingState);
	const tasks: Promise<unknown>[] = [];
	env.sdk().processAgentSessionStopped(
		{
			adapter: env.adapter,
			threadId: ref.threadId,
			channelId: "C1",
			threadTs: "root",
			streamingMessageTs: [],
			userId: "U2",
		},
		{
			waitUntil: (task) => {
				tasks.push(task);
			},
		},
	);
	await Promise.all(tasks);
	expect(env.host.receiveCancellation).toHaveBeenCalledExactlyOnceWith({
		actor: { provider: "slack", installationId: "acme", subjectId: "U2" },
		thread: ref,
		dedupeId: JSON.stringify(["slack", "agent-session-stopped", ref.threadId, "U2", []]),
	});
	expect(env.host.runtime.cancel).not.toHaveBeenCalled();
});

it("creates one fallback status message and edits it", async () => {
	vi.useFakeTimers();
	const env = setup();
	vi.mocked(env.adapter.startTyping).mockRejectedValue(new Error("unsupported"));
	const thread = await env.connection.port.thread(ref);
	await thread.setState(routingState);
	await thread.setStatus("Thinking");
	vi.advanceTimersByTime(1001);
	await thread.setStatus("Searching");
	expect(env.adapter.postMessage).toHaveBeenCalledOnce();
	expect(env.adapter.editMessage).toHaveBeenCalledWith(ref.threadId, "msg-1", {
		markdown: "Searching",
	});
	expect((await thread.getState())?.statusMessageId).toBe("msg-1");
});
