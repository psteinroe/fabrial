// oxlint-disable typescript/unbound-method -- Host methods are Vitest mocks.
import { createHmac } from "node:crypto";
import { SlackAdapter } from "@chat-adapter/slack";
import { pendingCard, finalCard } from "../../../fabrial/src/approval-card.ts";
import { connectChat, editMessage, postMessage } from "../../../fabrial/src/chat/index.ts";
import type { WebClient } from "@slack/web-api";
import { afterEach, expect, it, vi } from "vitest";
import { setup } from "../../../fabrial/tests/chat/helper.ts";
import { slack } from "../../src/index.ts";

afterEach(() => vi.restoreAllMocks());

function mockSlackAPI(adapter: SlackAdapter) {
	// SDK internals use a different WebClient than the public token-bound getter.
	// Access that protected client only in this SDK regression test; never call Slack.
	const internal = Reflect.get(adapter, "_client") as WebClient;
	vi.spyOn(internal.users, "info").mockResolvedValue({
		ok: true,
		user: { id: "U1", name: "alice" },
	});
	vi.spyOn(internal.auth, "test").mockResolvedValue({ ok: true, team_id: "T1", user_id: "BOT" });
	return vi
		.spyOn(adapter.client.auth, "test")
		.mockResolvedValue({ ok: true, team_id: "T1", user_id: "BOT" });
}

function signed(payload: unknown, retry = false, interactive = false) {
	const body = interactive
		? new URLSearchParams({ payload: JSON.stringify(payload) }).toString()
		: JSON.stringify(payload);
	const timestamp = Math.floor(Date.now() / 1000).toString();
	return new Request("https://app/slack/events", {
		method: "POST",
		body,
		headers: {
			"content-type": interactive ? "application/x-www-form-urlencoded" : "application/json",
			"x-slack-request-timestamp": timestamp,
			"x-slack-signature": `v0=${createHmac("sha256", "secret").update(`v0:${timestamp}:${body}`).digest("hex")}`,
			...(retry ? { "x-slack-retry-num": "1" } : {}),
		},
	});
}

function environment() {
	const plugin = slack({
		botToken: "xoxb-test",
		signingSecret: "secret",
		workspace: "acme",
		teamId: "T1",
	});
	const adapter = plugin.chat!.adapter() as SlackAdapter;
	mockSlackAPI(adapter);
	vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
		Object.assign(adapter, { chat: sdk });
	});
	const env = setup();
	// Keep this exact adapter instance in the plugin capability.
	const connection = connectChat(
		{ state: env.state },
		{
			...env.host,
			plugins: [{ ...plugin, chat: { ...plugin.chat!, adapter: () => adapter } }],
		},
	);
	return { ...env, adapter, connection, route: connection.routes["POST /slack/events"]! };
}

const message = {
	type: "event_callback",
	team_id: "T1",
	event_id: "Ev1",
	event: { type: "message", channel: "C1", user: "U1", ts: "1700000000.000001", text: "hello" },
};

it("processes Slack's same-event-id retry after failed durable ingress, ignoring old pre-ingress markers", async () => {
	const env = environment();
	vi.mocked(env.host.receiveMessage).mockRejectedValueOnce(
		new Error("durable ingress unavailable"),
	);
	await expect(env.route(signed(message))).rejects.toThrow("durable ingress unavailable");
	expect(await env.state.get("slack:event-delivered:Ev1")).toBeNull();
	// Old deployments may have already marked the failed delivery. Do not trust those markers.
	await env.state.set("slack:event-delivered:Ev1", true);
	expect((await env.route(signed(message, true))).status).toBe(200);
	expect(env.host.receiveMessage).toHaveBeenCalledTimes(2);
	expect(vi.mocked(env.host.receiveMessage).mock.calls[0]?.[1]).toEqual(
		vi.mocked(env.host.receiveMessage).mock.calls[1]?.[1],
	);
});

it.each([false, true])(
	"rejects signed foreign-workspace ingress (interactive=%s)",
	async (interactive) => {
		const env = environment();
		const payload = interactive
			? {
					type: "block_actions",
					team: { id: "T_OTHER" },
					user: { id: "U1" },
					channel: { id: "C1" },
					message: { ts: "1700000000.000001" },
					actions: [{ type: "button", action_id: "approve", value: "yes" }],
				}
			: { ...message, team_id: "T_OTHER" };
		expect((await env.route(signed(payload, false, interactive))).status).toBe(403);
		expect(env.host.receiveMessage).not.toHaveBeenCalled();
		expect(env.host.receiveAction).not.toHaveBeenCalled();
	},
);

it("resolves the native team ID with auth.test when no teamId option is given", async () => {
	const adapter = slack({
		botToken: "xoxb-test",
		signingSecret: "secret",
		workspace: "acme",
	}).chat!.adapter() as SlackAdapter;
	const auth = mockSlackAPI(adapter);
	await adapter.initialize({} as Parameters<SlackAdapter["initialize"]>[0]);
	expect(auth).toHaveBeenCalled();
	expect((await adapter.handleWebhook(signed({ ...message, team_id: "T_OTHER" }))).status).toBe(
		403,
	);
});

it("allows signed same-workspace action retries after failed durable ingress", async () => {
	const env = environment();
	const payload = {
		type: "block_actions",
		team: { id: "T1" },
		user: { id: "U1", username: "alice" },
		channel: { id: "C1" },
		message: { ts: "1700000000.000001" },
		trigger_id: "same-receipt",
		actions: [
			{ type: "button", action_id: "approve", value: "yes", action_ts: "1700000001.000001" },
		],
	};
	vi.mocked(env.host.receiveAction).mockRejectedValueOnce(new Error("action ingress unavailable"));
	await expect(env.route(signed(payload, false, true))).rejects.toThrow(
		"action ingress unavailable",
	);
	expect((await env.route(signed(payload, true, true))).status).toBe(200);
	expect(env.host.receiveAction).toHaveBeenCalledTimes(2);
	expect(vi.mocked(env.host.receiveAction).mock.calls[0]?.[0]).toEqual(
		vi.mocked(env.host.receiveAction).mock.calls[1]?.[0],
	);
});

it.each([false, true])(
	"still rejects invalid Slack signatures (interactive=%s)",
	async (interactive) => {
		const env = environment();
		const request = signed(
			interactive ? { type: "block_actions", team: { id: "T1" } } : message,
			false,
			interactive,
		);
		request.headers.set("x-slack-signature", "v0=invalid");
		expect((await env.route(request)).status).toBe(401);
		expect(env.host.receiveMessage).not.toHaveBeenCalled();
		expect(env.host.receiveAction).not.toHaveBeenCalled();
	},
);

it("posts and updates approval cards literally, with buttons that still route", async () => {
	const env = environment();
	const internal = Reflect.get(env.adapter, "_client") as WebClient;
	const post = vi
		.spyOn(internal.chat, "postMessage")
		.mockResolvedValue({ ok: true, ts: "1700000002.000001" });
	const update = vi.spyOn(internal.chat, "update").mockResolvedValue({ ok: true, ts: "x" });
	const hostile = "rm **/secrets/** ``` <!channel> <@U123> :smile: &lt; *bold* _it_ ~s~";
	const details = `run:\n\`\`\`\n${hostile}\n\`\`\`\nafter the fence: <!here>`;
	const proposal = {
		title: "Deploy :rocket: <!channel>",
		details,
		requester: { id: "alice", name: "*Admin* <@U123> :smile:", identities: [], known: true },
		source: "https://github.com/acme/app/pull/1 (acme/app#1)",
		thread: null,
		deadline: Date.parse("2026-10-21T10:00:00Z"),
		content: { text: hostile, fields: [{ label: "Glob", value: "**/secrets/**" }] },
	};
	const actions = [
		{ id: "fabrial.approve", label: "Approve", style: "primary" as const, value: "req-1" },
		{ id: "fabrial.reject", label: "Reject", style: "danger" as const, value: "req-1" },
	];
	const thread = await env.connection.thread({
		kind: "thread",
		provider: "slack",
		threadId: "slack:C1:1700000000.000001",
	});
	const ref = await postMessage(thread, {
		card: pendingCard(proposal, "Waiting for approval", actions),
	});
	expect(ref.messageId).toBe("1700000002.000001");

	const sent = post.mock.calls[0]![0] as unknown as {
		text: string;
		blocks: Record<string, unknown>[];
	};
	// Notifications are mrkdwn: the title cannot mention anyone there.
	expect(sent.text).toBe("Deploy :rocket: &lt;!channel&gt;");
	expect(JSON.stringify(sent.blocks)).not.toContain("mrkdwn");
	const plain = (text: string) => ({ type: "plain_text", text, emoji: false });
	expect(sent.blocks[0]).toEqual({ type: "header", text: plain("Deploy :rocket: <!channel>") });
	expect(sent.blocks).toContainEqual({
		type: "section",
		fields: [
			plain("Requested by\n*Admin* <@U123> :smile: (alice)"),
			plain("Source\nhttps://github.com/acme/app/pull/1 (acme/app#1)"),
			plain("Expires\n2026-10-21T10:00:00Z"),
		],
	});
	// The details exactly as proposed, fences and all.
	expect(sent.blocks).toContainEqual({
		type: "rich_text",
		elements: [{ type: "rich_text_preformatted", elements: [{ type: "text", text: details }] }],
	});
	expect(sent.blocks).toContainEqual({ type: "section", text: plain(hostile) });
	expect(sent.blocks).toContainEqual({ type: "section", fields: [plain("Glob\n**/secrets/**")] });
	// The SDK's own button format.
	expect(sent.blocks.at(-1)).toEqual({
		type: "actions",
		elements: [
			{
				type: "button",
				text: { type: "plain_text", text: "Approve", emoji: true },
				action_id: "fabrial.approve",
				value: "req-1",
				style: "primary",
			},
			{
				type: "button",
				text: { type: "plain_text", text: "Reject", emoji: true },
				action_id: "fabrial.reject",
				value: "req-1",
				style: "danger",
			},
		],
	});

	// A click on that button reaches Fabrial as the action it posted.
	const click = {
		type: "block_actions",
		team: { id: "T1" },
		user: { id: "U1", username: "alice" },
		channel: { id: "C1" },
		message: { ts: "1700000002.000001", thread_ts: "1700000000.000001" },
		trigger_id: "t1",
		actions: [
			{ type: "button", action_id: "fabrial.approve", value: "req-1", action_ts: "1700000003.1" },
		],
	};
	expect((await env.route(signed(click, false, true))).status).toBe(200);
	expect(env.host.receiveAction).toHaveBeenCalledWith(
		expect.objectContaining({
			actionId: "fabrial.approve",
			value: "req-1",
			messageId: "1700000002.000001",
		}),
	);

	await editMessage(env.connection, ref, {
		card: finalCard(proposal, { label: "Expired", at: "2026-10-21T10:00:00.000Z" }),
	});
	const edited = update.mock.calls[0]![0] as unknown as {
		ts: string;
		blocks: Record<string, unknown>[];
	};
	expect(edited.ts).toBe("1700000002.000001");
	expect(JSON.stringify(edited.blocks)).not.toContain("mrkdwn");
	expect(edited.blocks).toContainEqual({
		type: "rich_text",
		elements: [{ type: "rich_text_preformatted", elements: [{ type: "text", text: details }] }],
	});
	expect(edited.blocks.some((block) => block.type === "actions")).toBe(false);
});
