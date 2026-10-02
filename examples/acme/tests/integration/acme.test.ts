import { createHmac, randomUUID } from "node:crypto";
import { createPostgresState } from "@chat-adapter/state-pg";
import { createMockLogger } from "@chat-adapter/tests";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { WebClient } from "@slack/web-api";
import { slack } from "@fabrial/slack";
import type { Adapter } from "chat";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import { createApp } from "../../src/app.ts";
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
let container: StartedPostgreSqlContainer;
let admin: Sql;
let sql: Sql;
let url: string;
let app: ReturnType<typeof createApp>;
let calls: SlackCall[];
let states: ReturnType<typeof createPostgresState>[];
let faux: ReturnType<typeof fauxProvider>;
let models: ReturnType<typeof createModels>;
let logger: ReturnType<typeof createMockLogger>;
let nextMessage: number;
let toolResult: string;
let approvalDelivery: (() => Promise<void>) | undefined;

beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	admin = postgres(container.getConnectionUri());
});
afterAll(async () => {
	await admin?.end();
	await container?.stop();
});
beforeEach(async () => {
	const name = `acme_${randomUUID().replaceAll("-", "")}`;
	await admin.unsafe(`CREATE DATABASE ${name}`);
	const address = new URL(container.getConnectionUri());
	address.pathname = name;
	url = address.toString();
	sql = postgres(url, { max: 20 });
	await sql`CREATE TABLE acme_accounts (organisation_id text PRIMARY KEY, balance integer NOT NULL)`;
	await sql`INSERT INTO acme_accounts VALUES ('org-acme', 0), ('org-other', 100)`;
	await sql`CREATE TABLE acme_changelog (repo text, number integer, title text, PRIMARY KEY (repo, number))`;
	calls = [];
	states = [];
	nextMessage = 1;
	toolResult = "";
	approvalDelivery = undefined;
	logger = createMockLogger();
	// Pin the app's on-call clock to the first week, independent of the wall-clock test date.
	vi.spyOn(engineeringTriage, "resolve").mockImplementation(() => [weeklyRotationForTest()]);
	const sdkSlack = slack({
		workspace: "acme",
		teamId: "T_ACME",
		botToken: "xoxb-test",
		signingSecret: "slack-secret",
	}).chat!.adapter() as Adapter & { webClient: WebClient };
	const prototype = Object.getPrototypeOf(sdkSlack.webClient) as WebClient;
	const apiCall = vi.fn<WebClient["apiCall"]>(async (method, options) => {
		const args = { ...options } as Record<string, unknown>;
		const ts = `1700000000.${String(nextMessage++).padStart(6, "0")}`;
		calls.push({ method, args, ts });
		switch (method) {
			case "auth.test":
				return { ok: true, team_id: "T_ACME", user_id: "U_BOT", bot_id: "B_BOT", user: "fabrial" };
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
	faux = fauxProvider({ provider: supportModel.provider, models: [{ id: supportModel.modelId }] });
	models = createModels();
	models.setProvider(faux.provider);
	const classifier = fauxProvider({ provider: "jev" });
	models.setProvider({
		...classifier.provider,
		getAllModels: () => [{ ...classifier.getModel(), id: "route", type: "classifier" as const }],
		classify: async () => ({
			api: classifier.api,
			provider: "jev",
			model: "route",
			answers: { bug: { type: "bool", probability: 0.01 } },
			stopReason: "stop",
			timestamp: Date.now(),
		}),
	});
	app = newApp();
	// No Linear API calls are needed in these Slack/GitHub tests. Skip only Linear's startup profile lookup.
	await app.start();
});
afterEach(async () => {
	await app?.stop();
	for (const state of states ?? []) await state.disconnect();
	await sql?.end();
	vi.restoreAllMocks();
});

function newApp() {
	const state = createPostgresState({ url });
	states.push(state);
	const instance = createApp({
		sql,
		state,
		models,
		piSettings: { retry: { enabled: false } },
		logger,
		slack: {
			workspace: "acme",
			teamId: "T_ACME",
			botToken: "xoxb-test",
			signingSecret: "slack-secret",
		},
		github: {
			token: "github-test",
			owner: "acme",
			webhookSecret: "github-secret",
			installationId: 42,
			botUserId: 99,
			userName: "fabrial",
			logger,
		},
		linear: {
			apiKey: "linear-test",
			webhookSecret: "linear-secret",
			organizationId: "acme",
			userName: "fabrial",
			logger,
		},
		runtimeOptions: {
			logger,
			worker: { concurrency: 8, pollIntervalMs: 20, flushIntervalMs: 20, fetchBatchSize: 10 },
			pollIntervalMs: 40,
		},
	});
	const adapter = instance.host.plugins.find((p) => p.id === "linear")!.chat!.adapter() as Adapter;
	vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
		Object.assign(adapter, {
			chat: sdk,
			defaultBotUserId: "lin_bot",
			defaultOrganizationId: "acme",
		});
	});
	return instance;
}
function scriptRepair() {
	faux.setResponses([
		fauxAssistantMessage(
			fauxToolCall("read_only_query", {
				organisationId: "org-acme",
				sql: "SELECT balance FROM acme_accounts WHERE organisation_id = current_setting('acme.organisation_id')",
			}),
			{ stopReason: "toolUse" },
		),
		(context) => {
			// The prior read-only tool committed Findings and Pi rendered it into this turn.
			expect(JSON.stringify(context.messages)).toContain("Queried organisation org-acme");
			return fauxAssistantMessage(
				fauxToolCall("run_sql", {
					organisationId: "org-acme",
					sql: repair,
					reason: "Repair the customer's missing credit",
				}),
				{ stopReason: "toolUse" },
			);
		},
		(context) => {
			const result = context.messages
				.filter((message) => message.role === "toolResult" && message.toolName === "run_sql")
				.at(-1);
			toolResult = JSON.stringify(result);
			return fauxAssistantMessage(
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
	const signature = createHmac("sha256", "slack-secret")
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
async function mention(channel = "C_SUPPORT") {
	const response = await slackRequest({
		type: "event_callback",
		team_id: "T_ACME",
		event_id: randomUUID(),
		event: {
			type: "app_mention",
			user: "U_ALICE",
			channel,
			text: "<@U_BOT> Please repair organisation org-acme's missing credit",
			ts: "1700000000.000001",
			event_ts: "1700000000.000001",
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
			team: { id: "T_ACME" },
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
	return sql`SELECT balance FROM acme_accounts ORDER BY organisation_id`;
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
	await expect
		.poll(() => thread.getState(), poll)
		.toMatchObject({ interactionId: null, handlerExecutionId: null, agentActive: false });
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
	expect(faux.state.callCount).toBe(3);
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
	const status = calls.find(
		(c) =>
			c.method === "chat.postMessage" &&
			c.args.channel === "C_SUPPORT" &&
			button(c, "Cancel request"),
	)!;
	expect(status).toBeDefined();
	expect(status.args.thread_ts).toBe("1700000000.000001");
	const [child] = await sql`SELECT payload -> '__fabrial' -> 'metadata' AS metadata
		FROM pgconductor._private_executions WHERE task_key = 'run-sql'`;
	expect(child?.metadata).toMatchObject({
		requestedBy: { id: "alice" },
		origin: { provider: "slack" },
		replyTo: { kind: "thread", provider: "slack", threadId: "slack:C_SUPPORT:1700000000.000001" },
		ownsThread: false,
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
	expect(faux.state.callCount).toBe(3);
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
it("stops promptly while approval-card delivery is still in flight", async () => {
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
		await expect(
			Promise.race([
				app.stop().then(() => "stopped"),
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
		installation: { id: 42 },
		repository: { full_name: "acme/app", owner: { login: "acme" } },
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
				"x-hub-signature-256": `sha256=${createHmac("sha256", "github-secret").update(body).digest("hex")}`,
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
	await expect.poll(async () => (await sql`SELECT * FROM acme_changelog`).length, poll).toBe(1);
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
		payload -> '__fabrial' -> 'metadata' -> 'ownsThread' AS owns_thread,
		payload -> '__fabrial' -> 'metadata' -> 'replyTo' ->> 'provider' AS reply_provider
		FROM pgconductor._private_executions WHERE task_key IN ('pr-review', 'changelog') ORDER BY task_key`;
	expect(runs).toEqual([
		{ task_key: "changelog", owns_thread: false, reply_provider: null },
		{ task_key: "pr-review", owns_thread: true, reply_provider: "github" },
	]);
});

it("read-only query rejects writes at the database, not by a model instruction", async () => {
	await expect(app.host.clients().database.query("org-acme", repair)).rejects.toMatchObject({
		code: "25006",
	});
	expect(await balances()).toEqual([{ balance: 0 }, { balance: 100 }]);
});
it("#bugs hands off to bugIntake rather than answering in the router", async () => {
	faux.setResponses([
		fauxAssistantMessage("Investigated the bug; engineering should add a regression test."),
	]);
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
	await expect
		.poll(() => thread.getState(), poll)
		.toMatchObject({ interactionId: null, handlerExecutionId: null, agentActive: false });
	expect(calls.some((c) => String(c.args.text).includes("I'll investigate this bug report"))).toBe(
		true,
	);
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
		organizationId: "acme",
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
				"linear-signature": createHmac("sha256", "linear-secret").update(body).digest("hex"),
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
