import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { createModels } from "@earendil-works/pi-ai/models";
import type { Message } from "@earendil-works/pi-ai";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
	createSession,
	type ConversationRetryPolicy,
	type StorageWrite,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	definePlugin,
	trigger,
	type AgentRef,
	type ChatMessage,
	type ExternalIdentity,
	type JsonObject,
} from "../../../src/index.ts";
import { Inbox, pi, PostgresStorage } from "../../../src/agents/index.ts";
import { Conversations, Invocation, Runs } from "../../../src/agents/documents.ts";
import { WAITING_STATUS } from "../../../src/agents/errors.ts";
import type { Logger } from "../../../src/app.ts";
import { FakeChat, MemoryRuntime, testApp } from "../../../src/testing.ts";

let sql: Sql;
let container: Awaited<ReturnType<PostgreSqlContainer["start"]>>;
beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	sql = postgres(container.getConnectionUri(), { onnotice: () => {} });
	await PostgresStorage.migrate(sql);
});
afterAll(async () => {
	await sql?.end();
	await container?.stop();
});

const plugin = definePlugin({
	id: "chat",
	events: {
		message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
	},
});
const f = createFabrial({ plugins: [plugin] });
const stops: (() => Promise<void>)[] = [];
afterEach(async () => {
	for (const stop of stops.splice(0)) await stop();
});

const text = (messages: readonly Message[]) => JSON.stringify(messages);
/** Request ids Pi accepted in a Session, in order, read from its commit log without taking the lease. */
async function submitted(session: string): Promise<string[]> {
	const rows =
		await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${session} ORDER BY seq`;
	const ids = new Set<string>();
	for (const row of rows)
		for (const write of JSON.parse(String(row.writes)) as StorageWrite[])
			if (write.type === "submission" && write.value.requestId) ids.add(write.value.requestId);
	return [...ids];
}
type Response = Parameters<ReturnType<typeof fauxProvider>["setResponses"]>[0][number];

/** One app on a fresh channel: a channel-wide subscription and a notify workflow on a custom key. */
async function setup(
	tools: (
		channel: string,
		self: () => AgentRef,
	) => Parameters<typeof f.defineAgent>[0]["tools"] = () => [],
	options: { retry?: Partial<ConversationRetryPolicy>; logger?: Logger; researcher?: boolean } = {},
) {
	const channel = `c${randomUUID().slice(0, 8)}`;
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const researcher = options.researcher
		? f.defineAgent({ name: `researcher-${channel}`, description: "Researches.", model })
		: undefined;
	const agent = f.defineAgent({
		name: `support-${channel}`,
		description: "Answers test questions.",
		model,
		tools: tools(channel, (): AgentRef => agent),
		...(researcher ? { subagents: [researcher] } : {}),
	});
	const subscription = f.subscribe({
		name: `channel-${channel}`,
		source: trigger({ event: "chat.message", filter: { channelId: [channel] } }),
		agent,
		session: "channel",
	});
	const notify = f.defineWorkflow({
		name: `notify-${channel}`,
		input: z.object({ text: z.string(), priority: z.enum(["urgent", "normal", "low"]) }),
		async run(input, ctx) {
			await ctx.send("notify", agent, {
				session: `ops-${channel}`,
				input: input.text,
				kind: "notification",
				priority: input.priority,
			});
		},
	});
	// Another process on the same database: its driver waits for the Session lease.
	const launch = async () => {
		const runtime = new MemoryRuntime({ idPrefix: randomUUID() });
		const chat = new FakeChat(() => runtime.now());
		const app = testApp(f, {
			workflows: [notify],
			subscriptions: [subscription],
			runtime,
			chat,
			agents: pi({ models, sql, settings: { retry: options.retry ?? { enabled: false } } }),
			...(options.logger ? { logger: options.logger } : {}),
		});
		await app.start();
		stops.push(() => app.stop());
		const settle = async () => {
			await expect
				.poll(
					async () => {
						await runtime.flush();
						return runtime.executions().every((e) => e.status === "settled");
					},
					{ timeout: 10_000 },
				)
				.toBe(true);
		};
		return { runtime, chat, app, settle };
	};
	const { runtime, chat, app, settle } = await launch();
	const message = (id: string, thread: string, body = id): ChatMessage => ({
		provider: "chat",
		threadId: `chat:${channel}:${thread}`,
		channelId: channel,
		messageId: id,
		text: body,
		author: {
			identity: { provider: "chat", installationId: "w", subjectId: id } as ExternalIdentity &
				JsonObject,
			name: `Author ${id}`,
			isBot: false,
		},
		isMention: false,
		isDM: false,
		sentAt: new Date().toISOString(),
	});
	const posts = async (thread: string) =>
		(await chat.thread({ kind: "thread", provider: "chat", threadId: `chat:${channel}:${thread}` }))
			.posts;
	const read = async <T>(
		session: string,
		fn: (s: ReturnType<typeof createSession>, storage: PostgresStorage) => Promise<T>,
	) => {
		const storage = await PostgresStorage.open(sql, session);
		const handle = createSession(storage);
		try {
			return await fn(handle, storage);
		} finally {
			await handle.close(BACKGROUND_CONTEXT);
		}
	};
	const respond = (responses: Response[]) => faux.setResponses(responses);
	return {
		channel,
		faux,
		runtime,
		chat,
		app,
		message,
		settle,
		posts,
		read,
		respond,
		notify,
		launch,
	};
}

it("lands messages from different threads in one channel conversation; reply targets a request and the final answer is not posted", async () => {
	const t = await setup();
	let second = "";
	t.respond([
		fauxAssistantMessage(fauxToolCall("reply", { text: "Answer one" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("final answer one"),
		(context) => {
			second = text(context.messages);
			const first = /request=\\"([^"\\]+)\\"/.exec(second)![1]!;
			return fauxAssistantMessage(fauxToolCall("reply", { text: "Back to one", request: first }), {
				stopReason: "toolUse",
			});
		},
		fauxAssistantMessage("final answer two"),
	]);
	expect(await t.chat.receive(t.message("m1", "t1", "first <question>"))).toBe("new");
	await t.settle();
	expect(await t.chat.receive(t.message("m2", "t2", "second"))).toBe("new");
	await t.settle();
	// One conversation saw both messages, attributed and escaped.
	expect(second).toContain("first &lt;question&gt;");
	expect(second).toContain('from=\\"Author m2\\"');
	expect(t.faux.state.callCount).toBe(4);
	const session = `chat:channel:${t.channel}`;
	const conversations = await t.read(session, (s) => s.snapshot(Conversations, BACKGROUND_CONTEXT));
	expect(Object.keys(conversations!.ids)).toHaveLength(1);
	// Both replies went to thread t1 (the default latest request, then the targeted one); t2 got none.
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one", "Back to one"]);
	expect(await t.posts("t2")).toEqual([]);
	// The model saw short request ids, and the sent run showed progress on the reply thread.
	expect(second).toMatch(/request=\\"[0-9a-f]{8}\\"/);
	const statuses = (
		await t.chat.thread({ kind: "thread", provider: "chat", threadId: `chat:${t.channel}:t1` })
	).statuses;
	expect(statuses).toContain("Thinking…");
	expect(statuses.at(-1)).toBeNull();
	const requests = await submitted(session);
	expect(requests).toHaveLength(2);
	// Admitted input is pruned from the inbox.
	expect(await sql`SELECT 1 FROM fabrial.inbox WHERE session_key = ${session}`).toHaveLength(0);
	const invocation = await t.read(session, (s) =>
		s.snapshot(Invocation, requests[1]!, BACKGROUND_CONTEXT),
	);
	expect(invocation).toMatchObject({
		replyTo: { kind: "thread", threadId: `chat:${t.channel}:t2` },
		actor: { id: "chat:w:m2" },
		origin: { provider: "chat", messageId: "m2" },
		send: { kind: "message", priority: "normal" },
	});
});

it("steers a busy run with a second message and drops a redelivered event", async () => {
	const t = await setup();
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	let busy!: () => void;
	const started = new Promise<void>((resolve) => (busy = resolve));
	let steered = "";
	t.respond([
		async () => {
			busy();
			await gate;
			return fauxAssistantMessage(fauxToolCall("reply", { text: "Working on it" }), {
				stopReason: "toolUse",
			});
		},
		(context) => {
			steered = text(context.messages);
			return fauxAssistantMessage("done");
		},
	]);
	const other = await t.launch();
	await t.chat.receive(t.message("m1", "t1"));
	const flushing = t.runtime.flush();
	try {
		await started;
		expect(await other.chat.receive(t.message("m2", "t2", "also this"))).toBe("new");
		expect(await other.chat.receive(t.message("m2", "t2", "also this"))).toBe("ignored");
		void other.runtime.flush();
		await expect
			.poll(async () => (await submitted(`chat:channel:${t.channel}`)).length, { timeout: 10_000 })
			.toBe(2);
	} finally {
		release();
	}
	await flushing;
	await t.settle();
	await other.settle();
	expect(steered).toContain("also this");
	expect(t.faux.state.callCount).toBe(2);
	const session = `chat:channel:${t.channel}`;
	// Both inputs were answered by the same run (one answer entry), and each was submitted once.
	const answers = await t.read(session, async (s, storage) => {
		const runs = await s.snapshot(Runs, BACKGROUND_CONTEXT);
		const requests = Object.values(runs!.runs).flatMap((run) => Object.entries(run.requests ?? {}));
		const unique = new Map(requests);
		expect(unique.size).toBe(2);
		return Promise.all(
			[...unique.values()].map(async (request) => {
				const record = await storage.submission(request.submissionId as never, BACKGROUND_CONTEXT);
				return record?.status === "done" ? record.answer : record?.status;
			}),
		);
	});
	expect(new Set(answers).size).toBe(1);
});

it("maps notification priorities to write, followUp, and steer; written entries reach the next run", async () => {
	const t = await setup();
	const seen: string[] = [];
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	let busy!: () => void;
	const started = new Promise<void>((resolve) => (busy = resolve));
	t.respond([
		async (context) => {
			seen.push(text(context.messages));
			busy();
			await gate;
			return fauxAssistantMessage(fauxToolCall("reply", { text: "ack" }), {
				stopReason: "toolUse",
			});
		},
		(context) => {
			seen.push(text(context.messages));
			return fauxAssistantMessage("after urgent");
		},
		(context) => {
			seen.push(text(context.messages));
			return fauxAssistantMessage("after normal");
		},
	]);
	const notify = (
		input: { text: string; priority: "urgent" | "normal" | "low" },
		runtime = t.runtime,
	) =>
		runtime.invoke(t.notify.name, input, {
			metadata: {
				interactionId: randomUUID(),
				origin: { provider: "github" },
				replyTo: null,
				requestedBy: null,
				ownsThread: false,
			},
		});
	await notify({ text: "low: a label was added", priority: "low" });
	await t.settle();
	expect(t.faux.state.callCount).toBe(0);
	const other = await t.launch();
	await notify({ text: "normal: CI passed", priority: "normal" });
	const flushing = t.runtime.flush();
	try {
		await started;
		await notify({ text: "normal: new commit", priority: "normal" }, other.runtime);
		await notify({ text: "urgent: changes requested", priority: "urgent" }, other.runtime);
		void other.runtime.flush();
		await expect
			.poll(async () => (await submitted(`app:ops-${t.channel}`)).length, { timeout: 10_000 })
			.toBe(4);
	} finally {
		release();
	}
	await flushing;
	await t.settle();
	await other.settle();
	expect(seen).toHaveLength(3);
	// The low-priority write did not wake the agent but is in the context of the next run.
	expect(seen[0]).toContain("low: a label was added");
	expect(seen[0]).toContain('priority=\\"low\\"');
	expect(seen[0]).toContain("normal: CI passed");
	// Urgent steers into the running work; normal waits for the next run.
	expect(seen[1]).toContain("urgent: changes requested");
	expect(seen[1]).not.toContain("normal: new commit");
	expect(seen[2]).toContain("normal: new commit");
	const deliveries = await t.read(`app:ops-${t.channel}`, async (s) => {
		const runs = await s.snapshot(Runs, BACKGROUND_CONTEXT);
		return Object.values(runs!.runs)
			.flatMap((run) => Object.values(run.requests ?? {}))
			.map((request) => request.delivery ?? "")
			.sort((a, b) => a.localeCompare(b));
	});
	expect(deliveries).toEqual(["followUp", "followUp", "steer", "write"]);
});

it("inbox notifies watchers of a Session and prunes admitted input", async () => {
	const inbox = new Inbox(sql);
	await inbox.start();
	const session = `app:inbox-${randomUUID()}`;
	const watcher = inbox.watch(session);
	try {
		// A new watcher reads once; then only a notification (or the slow fallback) makes it due.
		expect(watcher.due()).toBe(true);
		expect(watcher.due()).toBe(false);
		const row = {
			request_id: randomUUID(),
			session_key: session,
			agent: "a",
			content: "hi",
			delivery: "steer" as const,
			metadata: {} as never,
		};
		await inbox.enqueue(row);
		await expect.poll(() => watcher.due(), { timeout: 1_000, interval: 10 }).toBe(true);
		await inbox.enqueue(row);
		expect(await inbox.pending(session, "a")).toHaveLength(1);
		await inbox.remove([row.request_id]);
		expect(await inbox.pending(session, "a")).toEqual([]);
	} finally {
		watcher.close();
		await inbox.stop();
	}
});

it("a tool sends to another conversation through the same send path", async () => {
	const t = await setup((channel, self) => [
		f.defineTool({
			name: "escalate",
			description: "Escalate to ops",
			input: z.object({ summary: z.string() }),
			async run(input, ctx) {
				const receipt = await ctx.send("escalate", self(), {
					session: `ops-${channel}`,
					input: input.summary,
					kind: "notification",
				});
				return receipt.session;
			},
		}),
	]);
	t.respond([
		fauxAssistantMessage(fauxToolCall("escalate", { summary: "billing is down" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("escalated"),
		(context) => {
			expect(text(context.messages)).toContain("billing is down");
			return fauxAssistantMessage("noted");
		},
	]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	expect(t.faux.state.callCount).toBe(3);
	expect(await submitted(`app:ops-${t.channel}`)).toHaveLength(1);
});

it.each(["notification", "message"] as const)(
	"a %s steered while the final answer completes is answered by the next run",
	async (kind) => {
		const t = await setup();
		const other = await t.launch();
		const session = kind === "message" ? `chat:channel:${t.channel}` : `app:ops-${t.channel}`;
		const seen: string[] = [];
		t.respond([
			fauxAssistantMessage(fauxToolCall("reply", { text: "first" }), { stopReason: "toolUse" }),
			async () => {
				// Steer while this final answer is in flight; answer only once the driver admitted it.
				if (kind === "message")
					expect(await other.chat.receive(t.message("m2", "t2", "late steer"))).toBe("new");
				else
					await other.runtime.invoke(
						t.notify.name,
						{ text: "late steer", priority: "urgent" },
						{
							metadata: {
								interactionId: randomUUID(),
								origin: { provider: "github" },
								replyTo: null,
								requestedBy: null,
								ownsThread: false,
							},
						},
					);
				void other.runtime.flush();
				await expect
					.poll(async () => (await submitted(session)).length, { timeout: 10_000 })
					.toBe(2);
				return fauxAssistantMessage("done");
			},
			(context) => {
				seen.push(text(context.messages));
				return fauxAssistantMessage(fauxToolCall("reply", { text: "second" }), {
					stopReason: "toolUse",
				});
			},
			fauxAssistantMessage("done"),
		]);
		if (kind === "message") await t.chat.receive(t.message("m1", "t1"));
		else
			await t.runtime.invoke(
				t.notify.name,
				{ text: "first", priority: "normal" },
				{
					metadata: {
						interactionId: randomUUID(),
						origin: { provider: "github" },
						replyTo: null,
						requestedBy: null,
						ownsThread: false,
					},
				},
			);
		await t.settle();
		await other.settle();
		await t.settle();
		expect(seen).toHaveLength(1);
		expect(seen[0]).toContain("late steer");
		expect(t.faux.state.callCount).toBe(4);
		expect(await sql`SELECT 1 FROM fabrial.inbox WHERE session_key = ${session}`).toHaveLength(0);
	},
);

it.each(["notification", "message"] as const)(
	"a %s queued behind a failed run is still answered",
	async (kind) => {
		const t = await setup();
		const other = await t.launch();
		const session = kind === "message" ? `chat:channel:${t.channel}` : `app:ops-${t.channel}`;
		const seen: string[] = [];
		t.respond([
			async () => {
				// The steer queues behind this generation, which then fails: Pi leaves it in its inbox.
				if (kind === "message")
					expect(await other.chat.receive(t.message("m2", "t2", "queued steer"))).toBe("new");
				else
					await other.runtime.invoke(
						t.notify.name,
						{ text: "queued steer", priority: "urgent" },
						{
							metadata: {
								interactionId: randomUUID(),
								origin: { provider: "github" },
								replyTo: null,
								requestedBy: null,
								ownsThread: false,
							},
						},
					);
				void other.runtime.flush();
				await expect
					.poll(async () => (await submitted(session)).length, { timeout: 10_000 })
					.toBe(2);
				return fauxAssistantMessage("", { stopReason: "error", errorMessage: "overloaded" });
			},
			(context) => {
				seen.push(text(context.messages));
				return fauxAssistantMessage("answered");
			},
		]);
		if (kind === "message") await t.chat.receive(t.message("m1", "t1"));
		else
			await t.runtime.invoke(
				t.notify.name,
				{ text: "first", priority: "normal" },
				{
					metadata: {
						interactionId: randomUUID(),
						origin: { provider: "github" },
						replyTo: null,
						requestedBy: null,
						ownsThread: false,
					},
				},
			);
		await t.settle();
		await other.settle();
		expect(seen).toHaveLength(1);
		expect(seen[0]).toContain("queued steer");
		expect(t.faux.state.callCount).toBe(2);
		expect(await sql`SELECT 1 FROM fabrial.inbox WHERE session_key = ${session}`).toHaveLength(0);
	},
);

it("an urgent notification during a tool call steers the running run and is answered in it", async () => {
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	let busy!: () => void;
	const started = new Promise<void>((resolve) => (busy = resolve));
	const t = await setup(() => [
		f.defineTool({
			name: "deploy",
			description: "Deploy",
			input: z.object({}),
			async run() {
				busy();
				await gate;
				return "deployed";
			},
		}),
	]);
	const other = await t.launch();
	let steered = "";
	t.respond([
		fauxAssistantMessage(fauxToolCall("deploy", {}), { stopReason: "toolUse" }),
		(context) => {
			steered = text(context.messages);
			return fauxAssistantMessage(fauxToolCall("reply", { text: "Rolled back" }), {
				stopReason: "toolUse",
			});
		},
		fauxAssistantMessage("done"),
	]);
	const notify = (text: string, priority: "urgent" | "normal", runtime = t.runtime) =>
		runtime.invoke(
			t.notify.name,
			{ text, priority },
			{
				metadata: {
					interactionId: randomUUID(),
					origin: { provider: "github" },
					replyTo: { kind: "thread", provider: "chat", threadId: `chat:${t.channel}:t1` },
					requestedBy: null,
					ownsThread: false,
				},
			},
		);
	await notify("deploy please", "normal");
	const flushing = t.runtime.flush();
	const session = `app:ops-${t.channel}`;
	try {
		await started;
		await notify("error rate is up", "urgent", other.runtime);
		void other.runtime.flush();
		await expect.poll(async () => (await submitted(session)).length, { timeout: 10_000 }).toBe(2);
	} finally {
		release();
	}
	await flushing;
	await t.settle();
	await other.settle();
	expect(steered).toContain("deployed");
	expect(steered).toContain("error rate is up");
	expect(steered).toContain('priority=\\"urgent\\"');
	expect(t.faux.state.callCount).toBe(3);
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Rolled back"]);
	// Both inputs settled with the same answer: one run.
	const answers = await t.read(session, async (s, storage) => {
		const runs = await s.snapshot(Runs, BACKGROUND_CONTEXT);
		const requests = Object.values(runs!.runs).flatMap((run) => Object.values(run.requests ?? {}));
		return Promise.all(
			requests.map(async (request) => {
				const record = await storage.submission(request.submissionId as never, BACKGROUND_CONTEXT);
				return record?.status === "done" ? record.answer : record?.status;
			}),
		);
	});
	expect(answers).toHaveLength(2);
	expect(new Set(answers).size).toBe(1);
});

const UNREACHABLE =
	/^I couldn't reach the model provider while handling your request\. Please try again in a few minutes\.\n\n_Error id: `[0-9a-f]{12}`_$/;
const FAILED =
	/^I hit an error while handling your request\. Please try again, rephrase, or reach out if it keeps failing\.\n\n_Error id: `[0-9a-f]{12}`_$/;
const statuses = async (t: Awaited<ReturnType<typeof setup>>, thread: string) =>
	(
		await t.chat.thread({
			kind: "thread",
			provider: "chat",
			threadId: `chat:${t.channel}:${thread}`,
		})
	).statuses;

it("a non-retryable model error posts one error reply with an error id; the conversation answers the next message", async () => {
	const errors: Record<string, unknown>[] = [];
	const logger: Logger = {
		debug() {},
		info() {},
		warn() {},
		error: (_m, data) => errors.push(data!),
	};
	const t = await setup(undefined, { logger });
	t.respond([
		fauxAssistantMessage("", {
			stopReason: "error",
			errorMessage: "invalid_request_error: tools.0.input_schema is invalid",
		}),
		fauxAssistantMessage(fauxToolCall("reply", { text: "Answer two" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	const [failure] = (await t.posts("t1")).map((post) => post.content as string);
	expect(failure).toMatch(FAILED);
	// The provider's text is logged under the error id, not posted.
	expect(failure).not.toContain("invalid_request_error");
	expect(errors).toEqual([
		expect.objectContaining({
			reason: "model_error",
			error: "invalid_request_error: tools.0.input_schema is invalid",
			errorId: /`([0-9a-f]{12})`/.exec(failure!)![1],
		}),
	]);
	expect((await statuses(t, "t1")).at(-1)).toBeNull();
	await t.chat.receive(t.message("m2", "t1", "again"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual([failure, "Answer two"]);
	expect(t.faux.state.callCount).toBe(3);
});

it("a transient model error retried durably shows the waiting status and posts no error reply", async () => {
	const t = await setup(undefined, {
		retry: { enabled: true, maxRetries: 2, baseDelayMs: 300, maxAgentDelayMs: 300 },
	});
	t.respond([
		fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 overloaded" }),
		fauxAssistantMessage(fauxToolCall("reply", { text: "Answer one" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one"]);
	const shown = await statuses(t, "t1");
	expect(shown).toContain(WAITING_STATUS);
	expect(shown.slice(shown.indexOf(WAITING_STATUS))).toContain("Thinking…");
	expect(shown.at(-1)).toBeNull();
	expect(t.faux.state.callCount).toBe(3);
});

it("a transient model error that exhausts its retries posts the couldn't-reach reply", async () => {
	const t = await setup(undefined, {
		retry: { enabled: true, maxRetries: 1, baseDelayMs: 5, maxAgentDelayMs: 5 },
	});
	const overloaded = () =>
		fauxAssistantMessage("", { stopReason: "error", errorMessage: "overloaded_error: Overloaded" });
	t.respond([overloaded(), overloaded()]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	const posts = (await t.posts("t1")).map((post) => post.content as string);
	expect(posts).toHaveLength(1);
	expect(posts[0]).toMatch(UNREACHABLE);
	expect(t.faux.state.callCount).toBe(2);
});

it("an aborted sent run posts no reply and the conversation stays usable", async () => {
	let started!: () => void;
	const blocking = new Promise<void>((resolve) => (started = resolve));
	const t = await setup(() => [
		f.defineTool({
			name: "slow",
			description: "Slow work",
			input: z.object({}),
			run: (_input, ctx) => {
				started();
				return new Promise<never>((_resolve, reject) =>
					ctx.signal.addEventListener("abort", () => reject(ctx.signal.reason), { once: true }),
				);
			},
		}),
	]);
	t.respond([
		fauxAssistantMessage(fauxToolCall("slow", {}), { stopReason: "toolUse" }),
		fauxAssistantMessage(fauxToolCall("reply", { text: "Answer two" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	await t.chat.receive(t.message("m1", "t1"));
	void t.runtime.flush();
	await blocking;
	for (const driver of t.runtime.executions("fabrial.pi.run"))
		if (driver.status !== "settled") await t.runtime.cancel(driver.executionId, "Stopped");
	await t.settle();
	expect(await t.posts("t1")).toEqual([]);
	await t.chat.receive(t.message("m2", "t1", "again"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer two"]);
});

it("a driver redelivered after starting the error reply does not post it twice", async () => {
	let crashed = false;
	const logger: Logger = {
		debug() {},
		info() {},
		warn() {},
		error: (message) => {
			// Crash once right after the error reply's durable intent started.
			if (message === "Sent input failed" && !crashed) {
				crashed = true;
				throw new Error("driver crashed");
			}
		},
	};
	const t = await setup(undefined, { logger });
	const driver = t.runtime.workflows.get("fabrial.pi.run")!;
	driver.retries = { maxAttempts: 2 };
	t.respond([fauxAssistantMessage("", { stopReason: "error", errorMessage: "no such model" })]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	expect(crashed).toBe(true);
	const posts = (await t.posts("t1")).map((post) => post.content as string);
	expect(posts).toHaveLength(1);
	expect(posts[0]).toMatch(FAILED);
	expect(t.runtime.executions("fabrial.pi.run").map((e) => e.result?.status)).toEqual([
		"completed",
	]);
});

it("a sent run's background child reports as a follow-up and the lead replies again", async () => {
	const t = await setup(undefined, { researcher: true });
	const tool = `researcher_${t.channel}`;
	const last = (messages: readonly Message[]) => {
		const message = messages.findLast((each) => (each.role as string) !== "system")!;
		return typeof message.content === "string" ? message.content : text([message]);
	};
	t.respond(
		Array.from({ length: 12 }, () => (context: { messages: Message[] }) => {
			const latest = last(context.messages);
			if (latest === "Find the cause") return fauxAssistantMessage("It is DNS");
			if (latest.includes("subagent name=") && latest.includes("It is DNS"))
				return fauxAssistantMessage(fauxToolCall("reply", { text: "Found: DNS" }), {
					stopReason: "toolUse",
				});
			if (latest.includes("Started"))
				return fauxAssistantMessage(fauxToolCall("reply", { text: "Looking into it" }), {
					stopReason: "toolUse",
				});
			if (latest.includes("Posted.")) return fauxAssistantMessage("done");
			return fauxAssistantMessage(
				fauxToolCall(tool, { task: "Find the cause", background: true }),
				{ stopReason: "toolUse" },
			);
		}),
	);
	await t.chat.receive(t.message("m1", "t1", "Why is it down?"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual([
		"Looking into it",
		"Found: DNS",
	]);
	expect(
		(await submitted(`chat:channel:${t.channel}`)).filter((id) =>
			id.startsWith("fabrial.subagent-report:"),
		),
	).toHaveLength(1);
});
