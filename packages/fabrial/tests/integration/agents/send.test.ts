import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { createModels } from "@earendil-works/pi-ai/models";
import type { Message } from "@earendil-works/pi-ai";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { createSession } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	definePlugin,
	trigger,
	type ChatMessage,
	type ExternalIdentity,
	type JsonObject,
} from "../../../src/index.ts";
import { pi, PostgresStorage } from "../../../src/agents/index.ts";
import { Conversations, Invocation, Runs } from "../../../src/agents/documents.ts";
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
type Response = Parameters<ReturnType<typeof fauxProvider>["setResponses"]>[0][number];

/** One app on a fresh channel: a channel-wide subscription and a notify workflow on a custom key. */
async function setup() {
	const channel = `c${randomUUID().slice(0, 8)}`;
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const agent = f.defineAgent({
		name: `support-${channel}`,
		model: `${faux.provider.id}/${faux.getModel().id}`,
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
			agents: pi({ models, sql, settings: { retry: { enabled: false } } }),
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
	const rows =
		await sql`SELECT request_id, metadata FROM fabrial_pi.inbox WHERE session_key = ${session} ORDER BY seq`;
	expect(rows).toHaveLength(2);
	const invocation = await t.read(session, (s) =>
		s.snapshot(Invocation, String(rows[1]!.request_id), BACKGROUND_CONTEXT),
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
			.poll(
				async () =>
					(
						await sql`SELECT 1 FROM fabrial_pi.inbox WHERE session_key = ${`chat:channel:${t.channel}`} AND admitted`
					).length,
				{ timeout: 10_000 },
			)
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
			.poll(
				async () =>
					(
						await sql`SELECT 1 FROM fabrial_pi.inbox WHERE session_key = ${`app:ops-${t.channel}`} AND admitted`
					).length,
				{ timeout: 10_000 },
			)
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
