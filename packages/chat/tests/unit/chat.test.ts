// oxlint-disable typescript/unbound-method -- All referenced methods here are Vitest mocks.
import { createTestMessage } from "@chat-adapter/tests";
import { Chat, type Author } from "chat";
import { afterEach, describe, expect, it, vi } from "vitest";
import { actionDedupeId, chat, chatEvents, chatTrigger, renderMessage } from "../../src/index.ts";
import { routingState, setup } from "../helper.ts";

const ref = { kind: "thread" as const, provider: "slack", threadId: "slack:C1:root-1" };
const actor = { provider: "slack", installationId: "acme", subjectId: "U1" };
const author: Author = {
	userId: "U1",
	userName: "alice",
	fullName: "Alice",
	isBot: false,
	isMe: false,
};
const message = (id: string, text: string, isMention = false) =>
	createTestMessage(id, text, {
		threadId: ref.threadId,
		author,
		isMention,
		metadata: { dateSent: new Date("2026-01-01T00:00:00Z"), edited: false },
	});

afterEach(() => vi.useRealTimers());

describe("inbound Chat SDK dispatch", () => {
	it.each([
		{
			name: "mention",
			thread: ref.threadId,
			mention: true,
			isDM: false,
			events: ["message", "mentioned"],
		},
		{
			name: "channel message",
			thread: ref.threadId,
			mention: false,
			isDM: false,
			events: ["message"],
		},
		{ name: "DM", thread: "slack:D1:root-1", mention: false, isDM: true, events: ["dm"] },
		{
			name: "DM mention",
			thread: "slack:D1:root-1",
			mention: true,
			isDM: true,
			events: ["dm", "mentioned"],
		},
	])("normalizes $name and subscribes new work", async ({ thread, mention, isDM, events }) => {
		const env = setup();
		await env.connection.port.thread({ ...ref, threadId: thread });
		await env.sdk().processMessage(env.adapter, thread, message("root-1", "hello", mention));
		expect(env.host.receiveMessage).toHaveBeenCalledExactlyOnceWith(
			{
				provider: "slack",
				threadId: thread,
				channelId: isDM ? "D1" : "C1",
				messageId: "root-1",
				text: "hello",
				author: { identity: actor, name: "Alice", isBot: false },
				isDM,
				isMention: mention,
				isNewThread: true,
				sentAt: "2026-01-01T00:00:00.000Z",
			},
			{
				events: events.map((event) => `slack.${event}`),
				dedupeId: JSON.stringify(["slack", isDM ? "D1" : "C1", "root-1"]),
			},
		);
		expect(await env.state.isSubscribed(thread)).toBe(true);
	});

	it("routes subscribed replies and subscribed mentions once, retaining classification", async () => {
		const env = setup();
		await env.connection.port.thread(ref);
		await env.state.subscribe(ref.threadId);
		vi.mocked(env.host.receiveMessage).mockResolvedValue("reply");
		await env.sdk().processMessage(env.adapter, ref.threadId, message("reply-1", "followup"));
		await env.sdk().processMessage(env.adapter, ref.threadId, message("reply-2", "mention", true));
		expect(env.host.receiveMessage).toHaveBeenCalledTimes(2);
		expect(
			vi.mocked(env.host.receiveMessage).mock.calls.map(([, options]) => options.events),
		).toEqual([["slack.message"], ["slack.message", "slack.mentioned"]]);
	});

	it("dedupes repeated messages and ignores the bot's own messages", async () => {
		const env = setup();
		await env.connection.port.thread(ref);
		await env.sdk().processMessage(env.adapter, ref.threadId, message("one", "hello"));
		await env.sdk().processMessage(env.adapter, ref.threadId, message("one", "hello", true));
		await env
			.sdk()
			.processMessage(
				env.adapter,
				ref.threadId,
				createTestMessage("self", "hello", { author: { ...author, isMe: true, isBot: true } }),
			);
		expect(env.host.receiveMessage).toHaveBeenCalledTimes(1);
	});

	it("does not subscribe ignored messages", async () => {
		const env = setup();
		await env.connection.port.thread(ref);
		vi.mocked(env.host.receiveMessage).mockResolvedValue("ignored");
		await env.sdk().processMessage(env.adapter, ref.threadId, message("one", "hello"));
		expect(await env.state.isSubscribed(ref.threadId)).toBe(false);
	});

	it("forwards actions with scoped actor and stable receipt dedupe", async () => {
		const env = setup();
		await env.connection.port.thread(ref);
		const action = {
			adapter: env.adapter,
			actionId: "approve",
			value: "request-1",
			user: author,
			threadId: ref.threadId,
			messageId: "card-1",
			raw: { actions: [{ action_ts: "123.456" }] },
		};
		await env.sdk().processAction(action, undefined);
		expect(env.host.receiveAction).toHaveBeenCalledExactlyOnceWith({
			actionId: "approve",
			value: "request-1",
			actor,
			thread: ref,
			messageId: "card-1",
			dedupeId: actionDedupeId("slack", action),
		});
		expect(
			actionDedupeId("slack", { ...action, raw: { actions: [{ action_ts: "123.457" }] } }),
		).not.toBe(actionDedupeId("slack", action));
	});

	it("mounts one webhook route per chat plugin and waits for local ingress", async () => {
		const env = setup();
		vi.mocked(env.adapter.handleWebhook).mockImplementation(async (_request, options) => {
			await env.sdk().processMessage(env.adapter, ref.threadId, message("one", "hello"), options);
			return new Response("accepted");
		});
		const route = env.connection.routes["POST /slack/events"]!;
		expect(
			await (await route(new Request("https://app/slack/events", { method: "POST" }))).text(),
		).toBe("accepted");
		expect(env.host.receiveMessage).toHaveBeenCalledOnce();
		expect(env.adapter.initialize).toHaveBeenCalledOnce();
	});
});

describe("ChatPort", () => {
	it("posts text/markdown/cards, updates by receipt and rejects foreign receipts", async () => {
		const env = setup();
		const thread = await env.connection.port.thread(ref);
		const receipt = await thread.post("hello");
		expect(receipt).toEqual({ provider: "slack", threadId: ref.threadId, messageId: "msg-1" });
		await thread.update(receipt, { markdown: "**edited**" });
		expect(env.adapter.editMessage).toHaveBeenCalledWith(ref.threadId, "msg-1", {
			markdown: "**edited**",
		});
		await thread.post({ markdown: "**hello**" });
		await thread.post({
			card: {
				title: "Approve?",
				text: "Details",
				fields: [{ label: "SQL", value: "select 1" }],
				code: { language: "sql", content: "select 1" },
				actions: [{ id: "approve", label: "Approve", style: "primary", value: "request-1" }],
			},
		});
		expect(vi.mocked(env.adapter.postMessage).mock.calls.at(-1)?.[1]).toMatchObject({
			type: "card",
			title: "Approve?",
			children: [
				{ type: "text", content: "Details" },
				{ type: "fields", children: [{ label: "SQL", value: "select 1" }] },
				{ type: "text", content: "```sql\nselect 1\n```" },
				{ type: "actions", children: [{ id: "approve", value: "request-1", style: "primary" }] },
			],
		});
		await expect(thread.update({ ...receipt, provider: "other" }, "bad")).rejects.toThrow(
			"does not belong",
		);
	});

	it("round-trips routing state under fabrial, preserving other state with 30-day TTL", async () => {
		const env = setup();
		const set = vi.spyOn(env.state, "set");
		const thread = await env.connection.port.thread(ref);
		await env.sdk().getState().set(`thread-state:${ref.threadId}`, { other: "kept" });
		expect(await thread.getState()).toBeNull();
		await thread.updateState(() => routingState);
		expect(await thread.getState()).toEqual(routingState);
		expect(set).toHaveBeenLastCalledWith(
			`thread-state:${ref.threadId}`,
			{ other: "kept", fabrial: routingState },
			30 * 24 * 60 * 60 * 1000,
		);
		await thread.updateState(() => null);
		expect(await thread.getState()).toBeNull();
		expect(await env.state.get(`thread-state:${ref.threadId}`)).toEqual({
			other: "kept",
			fabrial: null,
		});
	});

	it("throttles typing/status and always clears it", async () => {
		vi.useFakeTimers();
		const env = setup();
		const thread = await env.connection.port.thread(ref);
		await thread.setStatus("Thinking");
		await thread.setStatus("Looking up");
		expect(env.adapter.startTyping).toHaveBeenCalledTimes(1);
		vi.advanceTimersByTime(1001);
		await thread.setStatus("Looking up");
		await thread.setStatus(null);
		expect(env.adapter.startTyping).toHaveBeenLastCalledWith(ref.threadId, "");
		expect(env.adapter.startTyping).toHaveBeenCalledTimes(3);
	});

	it("falls back to a single edited status post, reattaches, clears, and is best effort", async () => {
		vi.useFakeTimers();
		const env = setup();
		vi.mocked(env.adapter.startTyping).mockRejectedValue(new Error("unsupported"));
		const thread = await env.connection.port.thread(ref);
		await thread.updateState(() => ({ ...routingState, statusMessageId: "old-status" }));
		await thread.setStatus("Thinking");
		expect(env.adapter.postMessage).not.toHaveBeenCalled();
		expect(env.adapter.editMessage).toHaveBeenLastCalledWith(ref.threadId, "old-status", {
			markdown: "Thinking",
		});
		vi.advanceTimersByTime(1001);
		await thread.setStatus("Looking up");
		await thread.setStatus(null);
		expect(env.adapter.deleteMessage).toHaveBeenCalledWith(ref.threadId, "old-status");
		expect((await thread.getState())?.statusMessageId).toBeNull();
		vi.mocked(env.adapter.postMessage).mockRejectedValue(new Error("offline"));
		vi.advanceTimersByTime(1001);
		await expect(thread.setStatus("Thinking")).resolves.toBeUndefined();
	});

	it("loads bounded attributed history and applies the last own-bot-reply boundary", async () => {
		const env = setup();
		vi.mocked(env.adapter.fetchMessages).mockResolvedValue({
			messages: [
				message("old", "old"),
				createTestMessage("bot", "answer", { author: { ...author, isBot: true, isMe: true } }),
				message("new", "new"),
			],
		});
		const thread = await env.connection.port.thread(ref);
		const history = await thread.history({ limit: 3, sinceLastBotReply: true });
		expect(history.map((m) => [m.text, m.author.name, m.author.identity])).toEqual([
			["new", "Alice", actor],
		]);
		expect((await thread.history({ limit: 1 })).map((m) => m.messageId)).toEqual(["new"]);
		expect(await thread.history({ limit: 0 })).toEqual([]);
	});

	it("opens provider-scoped DMs and posts ephemeral messages", async () => {
		const env = setup();
		const dm = await env.connection.port.openDM(actor);
		expect(dm.isDM).toBe(true);
		expect(dm.ref.threadId).toBe("slack:DU1:");
		await env.connection.port.postEphemeral(ref, actor, "not authorized");
		expect(env.adapter.postEphemeral).toHaveBeenCalledWith(ref.threadId, "U1", "not authorized", {
			fallbackToDM: true,
		});
		await expect(env.connection.port.openDM({ ...actor, installationId: "other" })).rejects.toThrow(
			"another installation",
		);
	});

	it("creates a channel thread only on first post and then reuses it", async () => {
		const env = setup();
		const thread = await env.connection.port.resolve({
			kind: "channel",
			provider: "slack",
			channelId: "C1",
		});
		expect(thread.ref.threadId).toBe("provisional:slack:C1");
		await expect(env.connection.port.thread(thread.ref)).rejects.toThrow("another provider");
		await expect(thread.updateState(() => routingState)).rejects.toThrow("provisional");
		expect(await thread.getState()).toBeNull();
		await thread.setStatus("Thinking");
		expect(await thread.history({})).toEqual([]);
		expect(env.adapter.postChannelMessage).not.toHaveBeenCalled();
		const root = await thread.post("root");
		expect(root.threadId).toBe(ref.threadId);
		expect(thread.ref).toEqual(ref);
		await thread.updateState(() => routingState);
		expect(await thread.getState()).toEqual(routingState);
		await thread.post("reply");
		expect(env.adapter.postChannelMessage).toHaveBeenCalledOnce();
		expect(env.adapter.postMessage).toHaveBeenCalledWith(ref.threadId, "reply");
	});
});

it("exports standard events and channel/thread specificity for other plugins", async () => {
	const events = chatEvents();
	expect(events.message.filterable).toContain("channelId");
	expect(
		chatTrigger("github", "mentioned", { channel: "acme/repo", thread: "github:acme:repo:1" })
			.specificity,
	).toBe(2);
	expect((await events.message.payload["~standard"].validate({})).issues).toBeDefined();
	expect(renderMessage({ card: { title: "Empty" } })).toMatchObject({
		type: "card",
		title: "Empty",
	});
});

it("rejects a chat plugin lacking configured installation identity", () => {
	const env = setup();
	expect(() =>
		chat({ state: env.state }).connect({
			...env.host,
			plugins: [{ id: "broken", chat: { adapter: () => env.adapter, installationId: "" } }],
		}),
	).toThrow("installationId");
});

it("maps application signal aborts with authenticated reasons, never impersonating the message author", async () => {
	const registration = vi.spyOn(Chat.prototype, "onNewMessage");
	try {
		const env = setup();
		await env.connection.start!();
		const receive = registration.mock.calls[0]![1];
		for (const authenticated of [false, true]) {
			const controller = new AbortController();
			const thread = (env.sdk() as Chat).thread(ref.threadId);
			Object.defineProperty(thread, "signal", { value: controller.signal });
			vi.mocked(env.host.receiveMessage).mockImplementation(async () => {
				controller.abort(authenticated ? { actor, dedupeId: "application-stop" } : undefined);
				return "new";
			});
			await receive(thread, message(`abort-${authenticated}`, "hello"));
			expect(env.host.receiveCancellation).toHaveBeenCalledTimes(authenticated ? 1 : 0);
		}
		expect(env.host.receiveCancellation).toHaveBeenCalledWith({
			actor,
			thread: ref,
			dedupeId: "application-stop",
		});
	} finally {
		registration.mockRestore();
	}
});

it("starts and shuts down the SDK through the connection lifecycle", async () => {
	const env = setup();
	const disconnect = vi.spyOn(env.state, "disconnect");
	const shutdown = vi.spyOn(env.adapter, "disconnect");
	expect(env.adapter.initialize).not.toHaveBeenCalled();
	await env.connection.start!();
	await env.connection.start!();
	expect(env.adapter.initialize).toHaveBeenCalledOnce();
	await env.connection.stop!();
	expect(disconnect).toHaveBeenCalledOnce();
	expect(shutdown).toHaveBeenCalledOnce();
});
