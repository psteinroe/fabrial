import { createHmac, randomUUID } from "node:crypto";
import { createMockLogger } from "@chat-adapter/tests";
import {
	createIntegrationApp,
	reply,
	scriptedModels,
	type ScriptedMessage,
	type ScriptedModel,
} from "fabrial/testing";
import { WebClient } from "@slack/web-api";
import { slack } from "@fabrial/slack";
import type { Adapter } from "chat";
import postgres, { type Sql } from "postgres";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { agents, workflows } from "../../src/app.ts";
import { routingModel } from "../../src/workflows/general-assistant.ts";
import { f } from "../../src/fabrial.ts";
import { bob, engineeringTriage, weeklyRotationForTest } from "./rotation.ts";
import { supportModel } from "../../src/agents/support.ts";

const poll = { timeout: 20_000, interval: 25 };
const repair =
	"UPDATE acme_accounts SET balance = balance + 10 WHERE organisation_id = current_setting('acme.organisation_id')";
interface SlackCall {
	method: string;
	args: Record<string, unknown>;
	ts: string;
}
let applicationSql: Sql;
let sql: Sql;
const url = process.env.DATABASE_URL!;
let app: ReturnType<typeof f.app>;
let calls: SlackCall[];
let faux: ScriptedModel;
let models: ReturnType<typeof scriptedModels>;
let logger: ReturnType<typeof createMockLogger>;
let nextMessage: number;
let toolResult: string;
let approvalDelivery: (() => Promise<void>) | undefined;

beforeEach(async () => {
	sql = postgres(url, { max: 20, onnotice: () => {} });
	applicationSql = postgres(process.env.APP_DATABASE_URL!, { onnotice: () => {} });
	// Reuse the definition-time URLs, but isolate every scenario's durable and business state.
	await sql`DROP SCHEMA IF EXISTS pgconductor CASCADE`;
	await sql`DROP SCHEMA IF EXISTS fabrial_pi CASCADE`;
	await sql`DROP SCHEMA IF EXISTS fabrial CASCADE`;
	await sql`DROP SCHEMA public CASCADE`;
	await sql`CREATE SCHEMA public`;
	await applicationSql`DROP SCHEMA public CASCADE`;
	await applicationSql`CREATE SCHEMA public`;
	await applicationSql`CREATE TABLE acme_accounts (organisation_id text PRIMARY KEY, balance integer NOT NULL)`;
	await applicationSql`INSERT INTO acme_accounts VALUES ('org-acme', 0), ('org-other', 100)`;
	await applicationSql`CREATE TABLE acme_changelog (repo text, number integer, title text, PRIMARY KEY (repo, number))`;
	calls = [];
	nextMessage = 1;
	toolResult = "";
	approvalDelivery = undefined;
	logger = createMockLogger();
	// Pin the app's on-call clock to the first week, independent of the wall-clock test date.
	vi.spyOn(engineeringTriage, "resolve").mockImplementation(() => [weeklyRotationForTest()]);
	const sdkSlack = slack({
		workspace: process.env.SLACK_WORKSPACE!,
		teamId: process.env.SLACK_TEAM_ID,
		botToken: process.env.SLACK_BOT_TOKEN!,
		signingSecret: process.env.SLACK_SIGNING_SECRET!,
	}).chat!.adapter() as Adapter & { webClient: WebClient };
	const prototype = Object.getPrototypeOf(sdkSlack.webClient) as WebClient;
	const apiCall = vi.fn<WebClient["apiCall"]>(async (method, options) => {
		const args = { ...options } as Record<string, unknown>;
		const ts = `1700000000.${String(nextMessage++).padStart(6, "0")}`;
		calls.push({ method, args, ts });
		switch (method) {
			case "auth.test":
				return {
					ok: true,
					team_id: process.env.SLACK_TEAM_ID,
					user_id: "U_BOT",
					bot_id: "B_BOT",
					user: "fabrial",
				};
			case "users.info":
				return {
					ok: true,
					user: {
						id: args.user,
						name: args.user,
						is_bot: false,
						profile: { display_name: args.user },
					},
				};
			case "conversations.info":
				return {
					ok: true,
					channel: { id: args.channel, is_im: String(args.channel).startsWith("D_") },
				};
			case "conversations.open":
				return { ok: true, channel: { id: `D_${String(args.users)}` } };
			case "conversations.replies":
			case "conversations.history":
				return { ok: true, messages: [], has_more: false };
			case "chat.postMessage":
				if (args.channel === "D_U_BOB" && button({ method, args, ts }, "Approve"))
					await approvalDelivery?.();
				return {
					ok: true,
					ts,
					channel: args.channel,
					message: { ts, text: args.text, blocks: args.blocks },
				};
			case "chat.update":
				return { ok: true, ts: args.ts, channel: args.channel, text: args.text };
			case "chat.postEphemeral":
				return { ok: true, message_ts: ts };
			case "assistant.threads.setStatus":
				return { ok: true };
			default:
				throw new Error(`Unexpected Slack API call: ${method}`);
		}
	});
	vi.spyOn(prototype, "apiCall").mockImplementation(apiCall);
	vi.spyOn(WebClient.prototype, "apiCall").mockImplementation(apiCall);
	models = scriptedModels();
	faux = models.model(supportModel);
	models.classifier(routingModel, () => ({ bug: { probability: 0.01 } }));
	app = newApp();
	// No Linear API calls are needed in these Slack/GitHub tests. Skip only Linear's startup profile lookup.
	await app.start();
});
afterEach(async () => {
	await app?.stop();
	await sql?.end();
	await applicationSql?.end();
	vi.restoreAllMocks();
});

function newApp() {
	const instance = createIntegrationApp(f, { workflows, agents, models, logger });
	const adapter = instance.host.plugins.find((p) => p.id === "linear")!.chat!.adapter() as Adapter;
	vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
		Object.assign(adapter, {
			chat: sdk,
			defaultBotUserId: "lin_bot",
			defaultOrganizationId: process.env.LINEAR_ORGANIZATION_ID,
		});
	});
	return instance;
}
function scriptRepair() {
	faux.respond([
		reply.toolCall("read_only_query", {
			organisationId: "org-acme",
			sql: "SELECT balance FROM acme_accounts WHERE organisation_id = current_setting('acme.organisation_id')",
		}),
		(context) => {
			// The prior read-only tool committed Findings and Pi rendered it into this turn.
			expect(JSON.stringify(context.messages)).toContain("Queried organisation org-acme");
			return reply.toolCall("run_sql", {
				organisationId: "org-acme",
				sql: repair,
				reason: "Repair the customer's missing credit",
			});
		},
		(context) => {
			const result = context.messages
				.filter((message) => message.role === "toolResult" && message.toolName === "run_sql")
				.at(-1);
			toolResult = JSON.stringify(result);
			return reply.text(
				toolResult.includes("declined")
					? "The request was declined; no SQL executed."
					: "Repair applied to your organisation.",
			);
		},
	]);
}
async function slackRequest(payload: unknown, action = false) {
	const timestamp = String(Math.floor(Date.now() / 1000));
	const body = action
		? new URLSearchParams({ payload: JSON.stringify(payload) }).toString()
		: JSON.stringify(payload);
	const signature = createHmac("sha256", process.env.SLACK_SIGNING_SECRET!)
		.update(`v0:${timestamp}:${body}`)
		.digest("hex");
	return app.fetch(
		new Request("https://acme.test/slack/events", {
			method: "POST",
			body,
			headers: {
				"content-type": action ? "application/x-www-form-urlencoded" : "application/json",
				"x-slack-request-timestamp": timestamp,
				"x-slack-signature": `v0=${signature}`,
			},
		}),
	);
}
async function mention(channel = "C_SUPPORT", ts = "1700000000.000001", user = "U_ALICE") {
	const response = await slackRequest({
		type: "event_callback",
		team_id: process.env.SLACK_TEAM_ID,
		event_id: randomUUID(),
		event: {
			type: "app_mention",
			user,
			channel,
			text: "<@U_BOT> Please repair organisation org-acme's missing credit",
			ts,
			event_ts: ts,
		},
	});
	expect(response.status).toBe(200);
}
function button(call: SlackCall, label: string) {
	const blocks = call.args.blocks as
		| { elements?: { action_id?: string; value?: string; text?: { text?: string } }[] }[]
		| undefined;
	return blocks?.flatMap((b) => b.elements ?? []).find((e) => e.text?.text === label);
}
async function approval() {
	await expect
		.poll(
			() =>
				calls.find(
					(c) =>
						c.method === "chat.postMessage" && c.args.channel === "D_U_BOB" && button(c, "Approve"),
				),
			poll,
		)
		.toBeDefined();
	const card = calls.find(
		(c) => c.method === "chat.postMessage" && c.args.channel === "D_U_BOB" && button(c, "Approve"),
	)!;
	// Click as soon as the card exists: the durable cursor retains decisions even
	// when delivery precedes wait registration (also after an unauthorized click).
	return card;
}
async function click(card: SlackCall, label: string, user = "U_BOB", receipt = randomUUID()) {
	const control = button(card, label)!;
	expect(control).toBeDefined();
	const response = await slackRequest(
		{
			type: "block_actions",
			team: { id: process.env.SLACK_TEAM_ID },
			user: { id: user, username: user },
			trigger_id: receipt,
			channel: { id: card.args.channel },
			message: { ts: card.ts, thread_ts: card.args.thread_ts, blocks: card.args.blocks },
			actions: [
				{
					action_id: control.action_id,
					value: control.value,
					action_ts: String(Date.now() / 1000),
				},
			],
		},
		true,
	);
	expect(response.status).toBe(200);
}
async function balances() {
	return applicationSql`SELECT balance FROM acme_accounts ORDER BY organisation_id`;
}
async function finalReply(text: string) {
	await expect
		.poll(
			() =>
				calls.filter(
					(c) =>
						c.method === "chat.postMessage" &&
						c.args.channel === "C_SUPPORT" &&
						String(c.args.text).includes(text),
				).length,
			poll,
		)
		.toBe(1);
	const reply = calls.find(
		(c) => c.method === "chat.postMessage" && String(c.args.text).includes(text),
	)!;
	expect(reply.args.thread_ts).toBe("1700000000.000001");
	const thread = await app.host
		.chat()!
		.thread({ kind: "thread", provider: "slack", threadId: "slack:C_SUPPORT:1700000000.000001" });
	// The support agent follows the thread; every routed message was delivered.
	await expect
		.poll(async () => {
			const state = await thread.getState();
			return {
				follows: state?.follows?.map((each) => each.agent),
				pending: state?.ingestedDedupeIds?.some((receipt) => receipt.pending) ?? false,
			};
		}, poll)
		.toEqual({ follows: ["acme-support"], pending: false });
	expect(calls.some((c) => c.method === "assistant.threads.setStatus" && c.args.status)).toBe(true);
	// Results, including findings and tool receipts, really reached Pi's Postgres log.
	expect((await sql`SELECT 1 FROM fabrial_pi.commits`).length).toBeGreaterThan(0);
}

it("Slack → Pi tools → triage DM approval → SQL → original thread, including duplicate approval", async () => {
	scriptRepair();
	await mention();
	const card = await approval();
	expect(bob.identities[0]?.subjectId).toBe("U_BOB");
	expect(JSON.stringify(card.args)).toContain("missing credit");
	const receipt = randomUUID();
	await click(card, "Approve", "U_BOB", receipt);
	await click(card, "Approve", "U_BOB", receipt);
	await finalReply("Repair applied");
	// Redelivery of the same Slack message after settlement must not start a new interaction.
	await mention();
	const owners = await sql`SELECT id FROM pgconductor._private_executions
		WHERE task_key = 'general-assistant'`;
	expect(owners).toHaveLength(1);
	expect(await balances()).toEqual([{ balance: 10 }, { balance: 100 }]);
	expect(toolResult).toContain("executed");
	expect(faux.calls).toBe(3);
	expect(
		calls.some(
			(c) => c.method === "chat.update" && JSON.stringify(c.args).includes("Approved by Bob"),
		),
	).toBe(true);
});
it("rejection declines the workflow tool without writing SQL", async () => {
	scriptRepair();
	await mention();
	await click(await approval(), "Reject");
	await finalReply("declined");
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
	expect(toolResult).toContain("rejected");
});
it("requester Cancel request updates the card to Cancelled without writing SQL", async () => {
	scriptRepair();
	await mention();
	await approval();
	// The requester's status card and the approver's card are separate posts, in no fixed order.
	const statusCard = () =>
		calls.find(
			(c) =>
				c.method === "chat.postMessage" &&
				c.args.channel === "C_SUPPORT" &&
				button(c, "Cancel request"),
		);
	await expect.poll(statusCard, poll).toBeDefined();
	const status = statusCard()!;
	expect(status.args.thread_ts).toBe("1700000000.000001");
	const [child] = await sql`SELECT payload -> '__fabrial' -> 'metadata' AS metadata
		FROM pgconductor._private_executions WHERE task_key = 'run-sql'`;
	expect(child?.metadata).toMatchObject({
		requestedBy: { id: "alice" },
		origin: { provider: "slack" },
		replyTo: { kind: "thread", provider: "slack", threadId: "slack:C_SUPPORT:1700000000.000001" },
		triggerEvent: null,
		ownerWorkflow: null,
	});
	await click(status, "Cancel request", "U_ALICE");
	await finalReply("declined");
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
	expect(toolResult).toContain("cancelled");
	expect(
		calls.some((c) => c.method === "chat.update" && JSON.stringify(c.args).includes("Cancelled")),
	).toBe(true);
});
it("a non-approver gets an ephemeral notice; the real approver can still approve", async () => {
	scriptRepair();
	await mention();
	const card = await approval();
	await click(card, "Approve", "U_ALICE");
	await expect
		.poll(
			() => calls.some((c) => c.method === "chat.postEphemeral" && c.args.user === "U_ALICE"),
			poll,
		)
		.toBe(true);
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
	expect(
		calls.some((c) => c.method === "chat.update" && JSON.stringify(c.args).includes("Approved by")),
	).toBe(false);
	await approval();
	await click(card, "Approve");
	await finalReply("Repair applied");
	expect(await balances()).toEqual([{ balance: 10 }, { balance: 100 }]);
});
it("restarts Conductor, Pi and Chat during approval and completes exactly once", async () => {
	scriptRepair();
	await mention();
	const card = await approval();
	// This scenario restarts suspended work, not a still-running card delivery.
	await expect
		.poll(async () => {
			const [row] = await sql`SELECT count(*)::int AS count
				FROM pgconductor._private_custom_event_subscriptions s
				JOIN pgconductor._private_executions e ON e.id = s.execution_id
				WHERE e.task_key = 'run-sql' AND e.locked_by IS NULL`;
			return row?.count;
		}, poll)
		.toBe(1);
	await app.stop();
	app = newApp();
	await app.start();
	await click(card, "Approve");
	await finalReply("Repair applied");
	expect(await balances()).toEqual([{ balance: 10 }, { balance: 100 }]);
	expect(faux.calls).toBe(3);
	expect(
		calls.filter(
			(c) =>
				c.method === "chat.postMessage" && c.args.channel === "D_U_BOB" && button(c, "Approve"),
		),
	).toHaveLength(1);
	const children =
		await sql`SELECT id FROM pgconductor._private_executions WHERE task_key = 'run-sql'`;
	expect(children).toHaveLength(1);
});
it("stops promptly once an in-flight approval-card delivery finishes", async () => {
	let reached!: () => void;
	const delivering = new Promise<void>((resolve) => {
		reached = resolve;
	});
	let release!: () => void;
	const delivered = new Promise<void>((resolve) => {
		release = resolve;
	});
	approvalDelivery = () => {
		reached();
		return delivered;
	};
	scriptRepair();
	try {
		await mention();
		await delivering;
		// Conductor's stop() waits for the running step instead of abandoning it mid-delivery.
		const stopped = app.stop().then(() => "stopped");
		await expect(
			Promise.race([
				stopped,
				new Promise<string>((resolve) => setTimeout(() => resolve("waiting"), 300)),
			]),
		).resolves.toBe("waiting");
		release();
		await expect(
			Promise.race([
				stopped,
				new Promise<string>((resolve) => setTimeout(() => resolve("hung"), 2000)),
			]),
		).resolves.toBe("stopped");
		expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
	} finally {
		release();
	}
});
it("signed GitHub PR opened webhook has one PR-thread owner plus a changelog observer", async () => {
	vi.spyOn(app.host.clients().github, "request").mockResolvedValue({
		data: { id: 1, name: "Alice", login: "alice" },
	} as never);
	const comments = vi
		.spyOn(app.host.clients().github.rest.issues, "createComment")
		.mockResolvedValue({
			data: {
				id: 71,
				body: "Review queued",
				html_url: "https://github.com/acme/app/pull/7#issuecomment-71",
			},
		} as never);
	const payload = {
		action: "opened",
		sender: { id: 1, login: "alice" },
		installation: { id: Number(process.env.GITHUB_INSTALLATION_ID) },
		repository: { full_name: "acme/app", owner: { login: process.env.GITHUB_OWNER } },
		pull_request: {
			id: 7,
			number: 7,
			title: "Fix missing credits",
			body: "Regression tests included",
			html_url: "https://github.com/acme/app/pull/7",
			user: { id: 1, login: "alice" },
		},
	};
	const body = JSON.stringify(payload);
	const request = () =>
		new Request("https://acme.test/github/webhook", {
			method: "POST",
			body,
			headers: {
				"x-github-event": "pull_request",
				"x-github-delivery": "pr-7",
				"x-hub-signature-256": `sha256=${createHmac("sha256", process.env.GITHUB_WEBHOOK_SECRET!).update(body).digest("hex")}`,
			},
		});
	expect((await app.fetch(request())).status).toBe(200);
	expect((await app.fetch(request())).status).toBe(200);
	await expect.poll(() => comments.mock.calls.length, poll).toBe(1);
	expect(comments.mock.calls[0]?.[0]).toMatchObject({
		owner: "acme",
		repo: "app",
		issue_number: 7,
		body: expect.stringContaining("Fix missing credits"),
	});
	await expect
		.poll(async () => (await applicationSql`SELECT * FROM acme_changelog`).length, poll)
		.toBe(1);
	const thread = await app.host
		.chat()!
		.thread({ kind: "thread", provider: "github", threadId: "github:acme/app:7" });
	// Lifecycle ingress emits directly; unlike chat messages it creates no routing tombstone.
	await expect.poll(() => thread.getState(), poll).toBeNull();
	await expect
		.poll(async () => {
			const [row] =
				await sql`SELECT count(*)::int AS count FROM pgconductor._private_executions WHERE task_key IN ('pr-review', 'changelog') AND completed_at IS NOT NULL`;
			return row?.count;
		}, poll)
		.toBe(2);
	const runs = await sql`SELECT task_key,
		payload -> '__fabrial' -> 'metadata' -> 'replyTo' ->> 'provider' AS reply_provider
		FROM pgconductor._private_executions WHERE task_key IN ('pr-review', 'changelog') ORDER BY task_key`;
	expect(runs).toEqual([
		{ task_key: "changelog", reply_provider: null },
		{ task_key: "pr-review", reply_provider: "github" },
	]);
});

it("read-only query rejects writes at the database, not by a model instruction", async () => {
	await expect(app.host.clients().database.query("org-acme", repair)).rejects.toMatchObject({
		code: "25006",
	});
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
});
it("#bugs: the router investigates inline with ctx.agent and posts the analysis; nothing follows the thread", async () => {
	faux.respond([reply.text("Investigated the bug; engineering should add a regression test.")]);
	await mention("C_BUGS");
	await expect
		.poll(
			() =>
				calls.filter(
					(c) =>
						c.method === "chat.postMessage" &&
						c.args.channel === "C_BUGS" &&
						String(c.args.text).includes("Investigated the bug"),
				).length,
			poll,
		)
		.toBe(1);
	const thread = await app.host
		.chat()!
		.thread({ kind: "thread", provider: "slack", threadId: "slack:C_BUGS:1700000000.000001" });
	// The investigation ran in the call's own Session: the thread has no follower.
	expect((await thread.getState())?.follows).toBeUndefined();
	expect(calls.some((c) => String(c.args.text).includes("I'll investigate this bug report"))).toBe(
		true,
	);
});
it("mentions outside #help: two threads get independent support conversations", async () => {
	const step = (context: { messages: readonly { role: string; content: unknown }[] }) => {
		const asks = (JSON.stringify(context.messages).match(/<message /g) ?? []).length;
		return reply.text(`Answer ${asks}`);
	};
	faux.respond([step, step]);
	await mention("C_SUPPORT", "1700000300.000001");
	const replies = () =>
		calls.filter(
			(c) =>
				c.method === "chat.postMessage" &&
				c.args.channel === "C_SUPPORT" &&
				String(c.args.text).includes("Answer"),
		);
	await expect.poll(() => replies().length, poll).toBe(1);
	await mention("C_SUPPORT", "1700000400.000001", "U_BOB");
	await expect.poll(() => replies().length, poll).toBe(2);
	// Each thread's conversation saw only its own message.
	expect(replies().map((c) => [c.args.thread_ts, c.args.text])).toEqual([
		["1700000300.000001", "Answer 1"],
		["1700000400.000001", "Answer 1"],
	]);
	const sessions =
		await sql`SELECT key FROM fabrial_pi.sessions WHERE key LIKE '%C_SUPPORT%' ORDER BY key`;
	expect(sessions.map((row) => row.key)).toEqual([
		"slack:slack:C_SUPPORT:1700000300.000001",
		"slack:slack:C_SUPPORT:1700000400.000001",
	]);
});
it("signed Linear issue created webhook posts a triage comment", async () => {
	const client = app.host.clients().linear;
	vi.spyOn(client, "user").mockResolvedValue({ id: "lin_alice", displayName: "Alice" } as never);
	const comments = vi.spyOn(client, "createComment").mockImplementation(
		async (input) =>
			({
				success: true,
				comment: Promise.resolve({
					id: "comment-1",
					body: input.body,
					createdAt: new Date(),
					updatedAt: new Date(),
					url: "https://linear.app/acme/issue/ENG-1",
				}),
			}) as never,
	);
	const body = JSON.stringify({
		type: "Issue",
		action: "create",
		organizationId: process.env.LINEAR_ORGANIZATION_ID,
		webhookTimestamp: Date.now(),
		actor: { id: "lin_alice" },
		data: {
			id: "issue-1",
			identifier: "ENG-1",
			title: "Missing credit",
			description: "Investigate",
			url: "https://linear.app/acme/issue/ENG-1",
			team: { key: "ENG" },
			state: { type: "triage" },
			creator: { id: "lin_alice" },
		},
	});
	const response = await app.fetch(
		new Request("https://acme.test/linear/webhook", {
			method: "POST",
			body,
			headers: {
				"linear-delivery": "issue-1",
				"linear-signature": createHmac("sha256", process.env.LINEAR_WEBHOOK_SECRET!)
					.update(body)
					.digest("hex"),
			},
		}),
	);
	expect(response.status).toBe(200);
	await expect.poll(() => comments.mock.calls.length, poll).toBe(1);
	expect(comments.mock.calls[0]?.[0]).toMatchObject({
		issueId: "issue-1",
		body: expect.stringContaining("ENG-1"),
	});
	const thread = await app.host
		.chat()!
		.thread({ kind: "thread", provider: "linear", threadId: "linear:issue-1" });
	await expect.poll(() => thread.getState(), poll).toBeNull();
});
it("read-only query cannot escape its transaction with multiple statements", async () => {
	await expect(
		app.host.clients().database.query("org-acme", `COMMIT; ${repair}`),
	).rejects.toMatchObject({ code: "42601" });
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
});

it("support channel: two threads share one conversation, the final reply posts to the asker, and a restart mid-run resumes", async () => {
	const step = (context: { messages: readonly { role: string; content: unknown }[] }) => {
		const asks = (JSON.stringify(context.messages).match(/<message /g) ?? []).length;
		return reply.text(`Answer ${asks}`);
	};
	faux.respond(Array.from({ length: 8 }, () => step));
	const post = async (user: string, ts: string, text: string, thread?: string) =>
		expect(
			(
				await slackRequest({
					type: "event_callback",
					team_id: process.env.SLACK_TEAM_ID,
					event_id: randomUUID(),
					event: {
						type: "message",
						user,
						channel: "C_HELP",
						text,
						ts,
						event_ts: ts,
						...(thread ? { thread_ts: thread } : {}),
					},
				})
			).status,
		).toBe(200);
	await post("U_ALICE", "1700000100.000001", "Is billing down?");
	// Restart right after durable ingress; the subscription and its driver resume after it.
	await app.stop();
	app = newApp();
	await app.start();
	const replies = () =>
		calls.filter((c) => c.method === "chat.postMessage" && c.args.channel === "C_HELP");
	await expect.poll(() => replies().length, poll).toBe(1);
	expect(replies()[0]!.args.thread_ts).toBe("1700000100.000001");
	await post("U_BOB", "1700000200.000001", "Same for me");
	await expect.poll(() => replies().length, poll).toBe(2);
	expect(replies()[1]!.args.thread_ts).toBe("1700000200.000001");
	expect(String(replies()[1]!.args.text)).toContain("Answer 2");
	expect(calls.some((c) => String(c.args.text).includes("never posted"))).toBe(false);
	const sessions = await sql`SELECT key FROM fabrial_pi.sessions WHERE key LIKE '%C_HELP%'`;
	expect(sessions.map((row) => row.key)).toEqual(["slack:channel:C_HELP"]);
});

it("dev thread: the agent opens a PR and follows it; a requested change is answered on GitHub and in the Slack home; it unfollows after the merge", async () => {
	const github = () => app.host.clients().github;
	const mockGitHub = () => {
		vi.spyOn(github(), "request").mockResolvedValue({
			data: { id: 1, name: "Alice", login: "alice" },
		} as never);
		vi.spyOn(github().rest.pulls, "getReview").mockResolvedValue({
			data: {
				user: { login: "alice" },
				html_url: "https://github.com/acme/app/pull/42#pullrequestreview-7",
			},
		} as never);
		return vi.spyOn(github().rest.issues, "createComment").mockResolvedValue({
			data: { html_url: "https://github.com/acme/app/pull/42#issuecomment-1" },
		} as never);
	};
	mockGitHub();
	const create = vi.spyOn(github().rest.pulls, "create").mockResolvedValue({
		data: { number: 42, html_url: "https://github.com/acme/app/pull/42" },
	} as never);
	let notification = "";
	faux.respond([
		reply.toolCall("open_pull_request", { title: "Fix flaky test", head: "fix-flaky" }),
		reply.text("Opened acme/app#42."),
		(context) => {
			notification = JSON.stringify(context.messages);
			return reply.toolCall("github_reply_to_review", {
				pr: "acme/app#42",
				review: 7,
				body: "Added a regression test.",
			});
		},
		reply.text("Addressed the review on #42."),
		reply.toolCall("github_unfollow_pull_request", { pr: "acme/app#42" }),
		reply.text("#42 is merged; I stopped following it."),
	]);
	const ts = "1700000300.000001";
	const response = await slackRequest({
		type: "event_callback",
		team_id: process.env.SLACK_TEAM_ID,
		event_id: randomUUID(),
		event: {
			type: "message",
			user: "U_ALICE",
			channel: "C_DEV",
			text: "Please fix the flaky test",
			ts,
			event_ts: ts,
		},
	});
	expect(response.status).toBe(200);
	const replies = () =>
		calls.filter((c) => c.method === "chat.postMessage" && c.args.channel === "C_DEV");
	await expect.poll(() => replies().length, poll).toBe(1);
	expect(create).toHaveBeenCalledOnce();
	// The follow is a record in the PR thread's Chat SDK state; notifications reply to the Slack home.
	const follows = async () =>
		(
			await (
				await app.host
					.chat()!
					.thread({ kind: "thread", provider: "github", threadId: "github:acme/app:42" })
			).getState()
		)?.resourceFollows ?? [];
	await expect.poll(follows, poll).toEqual([
		{
			session: `slack:slack:C_DEV:${ts}`,
			agent: "acme-developer",
			resource: "github:acme/app#42",
			replyTo: { kind: "thread", provider: "slack", threadId: `slack:C_DEV:${ts}` },
		},
	]);
	await expect.poll(() => faux.calls, poll).toBe(2);

	// Restart between the follow and the review: the record lives in Postgres-backed thread state.
	await app.stop();
	app = newApp();
	await app.start();
	const comment = mockGitHub();

	const pr = {
		id: 42,
		number: 42,
		title: "Fix flaky test",
		body: null,
		html_url: "https://github.com/acme/app/pull/42",
		user: { id: 99, login: "fabrial[bot]", type: "Bot" },
	};
	const repository = { full_name: "acme/app", owner: { login: process.env.GITHUB_OWNER } };
	const installation = { id: Number(process.env.GITHUB_INSTALLATION_ID) };
	const webhook = async (event: string, delivery: string, payload: Record<string, unknown>) => {
		const body = JSON.stringify({ installation, repository, pull_request: pr, ...payload });
		const signature = createHmac("sha256", process.env.GITHUB_WEBHOOK_SECRET!)
			.update(body)
			.digest("hex");
		const result = await app.fetch(
			new Request("https://acme.test/github/webhook", {
				method: "POST",
				body,
				headers: {
					"x-github-event": event,
					"x-github-delivery": delivery,
					"x-hub-signature-256": `sha256=${signature}`,
				},
			}),
		);
		expect(result.status).toBe(200);
	};
	const alice = { id: 1, login: "alice", type: "User" };
	const review = {
		action: "submitted",
		sender: alice,
		review: {
			id: 7,
			state: "CHANGES_REQUESTED",
			body: "Please add a regression test",
			user: alice,
			author_association: "MEMBER",
		},
	};
	await webhook("pull_request_review", "review-42", review);
	await webhook("pull_request_review", "review-42", review);
	// The agent answers the reviewer on GitHub, and its final reply posts in the Slack home.
	await expect.poll(() => replies().length, poll).toBe(2);
	expect(replies()[1]!.args).toMatchObject({
		thread_ts: ts,
		text: expect.stringContaining("Addressed the review on #42."),
	});
	expect(comment).toHaveBeenCalledOnce();
	expect(comment.mock.calls[0]![0]).toEqual({
		owner: "acme",
		repo: "app",
		issue_number: 42,
		body: "> In reply to @alice's [review](https://github.com/acme/app/pull/42#pullrequestreview-7)\n\nAdded a regression test.",
	});
	expect(notification).toContain('kind=\\"review\\" priority=\\"urgent\\" pr=\\"acme/app#42\\"');
	expect(notification).toContain('review=\\"7\\"');
	expect(notification).toContain("Please add a regression test");

	// The merge is delivered like any event; the agent unfollows, and its reply goes home too.
	await webhook("pull_request", "merge-42", {
		action: "closed",
		sender: alice,
		pull_request: { ...pr, merged: true },
	});
	await expect.poll(() => replies().length, poll).toBe(3);
	expect(replies()[2]!.args).toMatchObject({
		thread_ts: ts,
		text: expect.stringContaining("#42 is merged; I stopped following it."),
	});
	expect(await follows()).toEqual([]);
	expect(faux.calls).toBe(6);
});

it("dev thread: the developer asks its researcher in the foreground and the background, and replies with both", async () => {
	const search = vi.spyOn(app.host.clients().github.rest.search, "code").mockResolvedValue({
		data: { items: [{ path: "tests/flaky.test.ts" }] },
	} as never);
	const last = (context: { messages: readonly ScriptedMessage[] }) => {
		const message = context.messages.findLast((each) => each.role !== "system")!;
		return typeof message.content === "string"
			? message.content
			: (message.content as readonly { text?: string }[]).map((block) => block.text ?? "").join("");
	};
	faux.respond(
		Array.from({ length: 12 }, () => (context: { messages: readonly ScriptedMessage[] }) => {
			const text = last(context);
			// The researcher.
			if (text === "Find the flaky test") return reply.toolCall("search_code", { query: "flaky" });
			if (text === "tests/flaky.test.ts") return reply.text("It is tests/flaky.test.ts.");
			if (text === "Check its CI history") return reply.text("It failed 3 of the last 10 runs.");
			// The developer.
			if (text.includes("Where is the flaky test?"))
				return reply.toolCall("acme_researcher", { task: "Find the flaky test" });
			if (text.includes("It is tests/flaky.test.ts."))
				return reply.toolCall("acme_researcher", {
					task: "Check its CI history",
					background: true,
				});
			if (text.startsWith("Started")) return reply.text("It is tests/flaky.test.ts; checking CI.");
			if (text.includes("3 of the last 10"))
				return reply.text("CI: it failed 3 of the last 10 runs.");
			return reply.text("done");
		}),
	);
	const ts = "1700000400.000001";
	const response = await slackRequest({
		type: "event_callback",
		team_id: process.env.SLACK_TEAM_ID,
		event_id: randomUUID(),
		event: {
			type: "message",
			user: "U_ALICE",
			channel: "C_DEV",
			text: "Where is the flaky test?",
			ts,
			event_ts: ts,
		},
	});
	expect(response.status).toBe(200);
	const replies = () =>
		calls
			.filter((c) => c.method === "chat.postMessage" && c.args.channel === "C_DEV")
			.map((c) => [c.args.thread_ts, c.args.text]);
	await expect.poll(replies, poll).toEqual([
		[ts, "It is tests/flaky.test.ts; checking CI."],
		[ts, "CI: it failed 3 of the last 10 runs."],
	]);
	expect(search).toHaveBeenCalledOnce();
});
