// oxlint-disable typescript/unbound-method -- All referenced methods here are Vitest mocks.
import { createMockAdapter, createTestMessage } from "@chat-adapter/tests";
import type { ChatInstance } from "chat";
import { afterEach, expect, it, vi } from "vitest";
import {
	chatCapability,
	chatEvents,
	connectChat,
	routingState,
	updateRoutingState,
} from "../../../src/chat/index.ts";
import { routingFixture, setup } from "../../chat/helper.ts";

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
	const connection = connectChat(
		{ state: env.state },
		{
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
		},
	);
	await connection.thread(ref);
	expect(githubBot).toBe(env.sdk());
	expect(Object.keys(connection.routes)).toEqual(["POST /slack/events", "POST /github/events"]);
	expect(github.initialize).toHaveBeenCalledOnce();
	expect(env.adapter.initialize).toHaveBeenCalledOnce();
});

it("does not coalesce concurrent arrivals", async () => {
	const env = setup();
	await env.connection.thread(ref);
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
		.mockResolvedValue("accepted");
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
	const thread = await env.connection.thread(ref);
	await updateRoutingState(env.connection, thread, () => routingFixture);
	vi.advanceTimersByTime(30 * 24 * 60 * 60 * 1000 + 1);
	expect(await routingState(thread)).toBeNull();
});

it("maps the native platform stop control to actor-aware cancellation ingress", async () => {
	const env = setup();
	const thread = await env.connection.thread(ref);
	await updateRoutingState(env.connection, thread, () => routingFixture);
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
	expect(env.host.conductor.cancel).not.toHaveBeenCalled();
});

it("creates one fallback status message and edits it", async () => {
	vi.useFakeTimers();
	const env = setup();
	vi.mocked(env.adapter.startTyping).mockRejectedValue(new Error("unsupported"));
	const thread = await env.connection.thread(ref);
	await updateRoutingState(env.connection, thread, () => routingFixture);
	await env.connection.setStatus(thread, "Thinking");
	vi.advanceTimersByTime(1001);
	await env.connection.setStatus(thread, "Searching");
	expect(env.adapter.postMessage).toHaveBeenCalledOnce();
	expect(env.adapter.editMessage).toHaveBeenCalledWith(ref.threadId, "msg-1", {
		markdown: "Searching",
	});
	expect((await routingState(thread))?.statusMessageId).toBe("msg-1");
});

it("serializes independent Chat instances and releases the state lock after updater/write failures", async () => {
	const first = setup();
	const second = setup(first.state);
	const a = await first.connection.thread(ref);
	const b = await second.connection.thread(ref);
	await updateRoutingState(first.connection, a, () => routingFixture);
	await expect(
		updateRoutingState(first.connection, a, () => {
			throw new Error("updater failed");
		}),
	).rejects.toThrow("updater failed");
	const set = vi.spyOn(first.state, "set").mockRejectedValueOnce(new Error("write failed"));
	await expect(updateRoutingState(first.connection, a, () => routingFixture)).rejects.toThrow(
		"write failed",
	);
	set.mockRestore();
	await Promise.all(
		Array.from({ length: 20 }, (_, i) =>
			updateRoutingState(i % 2 ? first.connection : second.connection, i % 2 ? a : b, (state) => ({
				...state!,
				follows: [...(state?.follows ?? []), { session: `user-${i}`, agent: "a" }],
			})),
		),
	);
	const state = await routingState(a);
	expect(state?.follows).toHaveLength(21);
	expect(new Set(state?.follows?.map((f) => f.session)).size).toBe(21);
	// Caller mutation must not change persisted state in memory adapters either.
	state?.follows?.push({ session: "outside-mutation", agent: "a" });
	expect((await routingState(a))?.follows).toHaveLength(21);
});

it("bounds lock contention retries without force-releasing another updater's lock", async () => {
	vi.useFakeTimers();
	const env = setup();
	const thread = await env.connection.thread(ref);
	const lock = await env.state.acquireLock(`fabrial:routing:${ref.threadId}`, 60_000);
	const result = expect(
		updateRoutingState(env.connection, thread, () => routingFixture),
	).rejects.toThrow("Timed out");
	await vi.advanceTimersByTimeAsync(10_001);
	await result;
	expect(await env.state.acquireLock(`fabrial:routing:${ref.threadId}`, 60_000)).toBeNull();
	await env.state.releaseLock(lock!);
});

it("retries a pure updater after losing its lease and preserves unrelated fields in fallback status writes", async () => {
	const env = setup();
	const replica = setup(env.state);
	const thread = await env.connection.thread(ref);
	const other = await replica.connection.thread(ref);
	await updateRoutingState(env.connection, thread, () => routingFixture);
	const extend = vi.spyOn(env.state, "extendLock").mockResolvedValueOnce(false);
	const updater = vi.fn((state: typeof routingFixture | null) => state);
	await updateRoutingState(env.connection, thread, updater);
	expect(updater).toHaveBeenCalledTimes(2);
	extend.mockRestore();
	vi.mocked(env.adapter.startTyping).mockRejectedValue(new Error("unsupported"));
	vi.mocked(env.adapter.postMessage).mockImplementationOnce(async () => {
		await updateRoutingState(replica.connection, other, (current) => ({
			...current!,
			follows: [{ session: "slack:C1:root", agent: "carol" }],
		}));
		return { id: "status-post", threadId: ref.threadId, raw: {} };
	});
	await env.connection.setStatus(thread, "Thinking");
	expect(await routingState(thread)).toMatchObject({
		follows: [{ session: "slack:C1:root", agent: "carol" }],
		statusMessageId: "status-post",
	});
});
