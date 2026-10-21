import type { FakeChat } from "../../../src/testing.ts";
import { randomUUID } from "node:crypto";
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
import { expect, it, vi } from "vitest";
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
} from "../../internals.ts";
import { pi, PostgresStorage } from "../../../src/agents/index.ts";
import { Conversations, Invocation, Obligations } from "../../../src/agents/documents.ts";
import { WAITING_STATUS } from "../../../src/agents/errors.ts";
import type { Logger } from "../../../src/app.ts";
import { until } from "../../conductor/helper.ts";
import { useAgents } from "./setup.ts";

const { db, sql, launch: start, idle, open } = useAgents();

const plugin = definePlugin({
	id: "chat",
	events: {
		message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
	},
});
const f = createFabrial({ plugins: [plugin] });

const text = (messages: readonly Message[]) => JSON.stringify(messages);
/** Request ids Pi accepted in a Session, in order, read from its commit log without taking the lease. */
async function submitted(session: string): Promise<string[]> {
	const rows =
		await sql()`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${session} ORDER BY seq`;
	const ids = new Set<string>();
	for (const row of rows)
		for (const write of JSON.parse(String(row.writes)) as StorageWrite[])
			if (write.type === "submission" && write.value.requestId) ids.add(write.value.requestId);
	return [...ids];
}
/** A tool that does nothing, for a tool round the next input can steer. */
const look = f.defineTool({
	name: "look",
	description: "Looking around",
	input: z.object({}),
	run: async () => "nothing",
});
const UNREACHABLE =
	/^I couldn't reach the model provider while handling your request\. Please try again in a few minutes\.\n\n_Error id: `[0-9a-f]{12}`_$/;
const FAILED =
	/^I hit an error while handling your request\. Please try again, rephrase, or reach out if it keeps failing\.\n\n_Error id: `[0-9a-f]{12}`_$/;
type Response = Parameters<ReturnType<typeof fauxProvider>["setResponses"]>[0][number];

/** One app on a fresh channel: a channel-wide subscription and a notify workflow on a custom key. */
async function setup(
	tools: (
		channel: string,
		self: () => AgentRef,
	) => Parameters<typeof f.defineAgent>[0]["tools"] = () => [],
	options: {
		retry?: Partial<ConversationRetryPolicy>;
		logger?: Logger;
		researcher?: boolean;
		/** Attempts of the Session workflow; Conductor retries a failed attempt after 15 seconds. */
		sessionAttempts?: number;
		/** Attempts of the effect workflow that posts replies. */
		effectAttempts?: number;
	} = {},
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
		triggers: [trigger({ event: "chat.message", filter: { channelId: [channel] } })],
		session: "channel",
	});
	const notify = f.defineWorkflow({
		name: `notify-${channel}`,
		input: z.object({ text: z.string(), priority: z.enum(["urgent", "normal", "low"]) }),
		async run(input, ctx) {
			await ctx.send("notify", agent, {
				session: `ops-${channel}`,
				input: input.text,
				priority: input.priority,
			});
		},
	});
	const ask = f.defineWorkflow({
		name: `ask-${channel}`,
		input: z.object({ question: z.string() }),
		run: (input, ctx) => ctx.agent("ask", agent, { input: input.question }),
	});
	// Another process on the same database: its driver waits for the Session lease.
	/** Another instance: by default another deployment (its own queue and chat); `peer` shares both. */
	const launch = async (peer?: { queue: string; chat: FakeChat }) => {
		const { runtime, chat, app } = await start(
			f,
			{
				...peer,
				workflows: [notify, ask],
				routedAgents: [agent],
				agents: pi({
					models,
					sql: sql(),
					settings: { retry: options.retry ?? { enabled: false } },
				}),
				...(options.logger ? { logger: options.logger } : {}),
			},
			{
				...(options.sessionAttempts
					? { "fabrial.session": { maxAttempts: options.sessionAttempts } }
					: {}),
				...(options.effectAttempts
					? { "fabrial.pi.effect": { maxAttempts: options.effectAttempts } }
					: {}),
			},
		);
		const settle = (timeoutMs?: number) => idle(runtime.queue, timeoutMs);
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
		const storage = await PostgresStorage.open(sql(), session);
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
		ask,
		launch,
	};
}

it("lands messages from different threads in one channel conversation; each final answer posts to its input's thread", async () => {
	const t = await setup();
	let second = "";
	t.respond([
		fauxAssistantMessage("Answer one"),
		(context) => {
			second = text(context.messages);
			return fauxAssistantMessage("Answer two");
		},
	]);
	expect(await t.chat.receive(t.message("m1", "t1", "first <question>"))).toBe("accepted");
	await t.settle();
	expect(await t.chat.receive(t.message("m2", "t2", "second"))).toBe("accepted");
	await t.settle();
	// One conversation saw both messages, attributed and escaped.
	expect(second).toContain("first &lt;question&gt;");
	expect(second).toContain('from=\\"Author m2\\"');
	expect(t.faux.state.callCount).toBe(2);
	const session = `chat:channel:${t.channel}`;
	const conversations = await t.read(session, (s) => s.snapshot(Conversations, BACKGROUND_CONTEXT));
	expect(Object.keys(conversations!.ids)).toHaveLength(1);
	// Each answer went to the thread of the input it answers.
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one"]);
	expect((await t.posts("t2")).map((post) => post.content)).toEqual(["Answer two"]);
	// Inputs render no request ids, and the sent run showed progress on the reply thread.
	expect(second).not.toContain("request=");
	const statuses = (
		await t.chat.thread({ kind: "thread", provider: "chat", threadId: `chat:${t.channel}:t1` })
	).statuses;
	expect(statuses).toContain("Thinking…");
	expect(statuses.at(-1)).toBeNull();
	const requests = await submitted(session);
	expect(requests).toHaveLength(2);
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

it("steers a busy run with a second message, replies in the newest input's thread, and drops a redelivered event", async () => {
	const t = await setup(() => [look]);
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	let busy!: () => void;
	const started = new Promise<void>((resolve) => (busy = resolve));
	let steered = "";
	t.respond([
		async () => {
			busy();
			await gate;
			return fauxAssistantMessage(fauxToolCall("look", {}), { stopReason: "toolUse" });
		},
		(context) => {
			steered = text(context.messages);
			return fauxAssistantMessage("done");
		},
	]);
	await t.chat.receive(t.message("m1", "t1"));
	try {
		await started;
		// The second message reaches the busy Session's execution as a signal.
		expect(await t.chat.receive(t.message("m2", "t2", "also this"))).toBe("accepted");
		// A redelivery is accepted again; the ingress emit dedupes it.
		expect(await t.chat.receive(t.message("m2", "t2", "also this"))).toBe("accepted");
		await expect
			.poll(async () => (await submitted(`chat:channel:${t.channel}`)).length, { timeout: 10_000 })
			.toBe(2);
	} finally {
		release();
	}
	await t.settle();
	expect(steered).toContain("also this");
	expect(t.faux.state.callCount).toBe(2);
	const session = `chat:channel:${t.channel}`;
	// Both inputs were answered by the same run (one answer entry), and each was submitted once.
	const answers = await t.read(session, async (s, storage) => {
		const unique = new Map(await requestsOf(s));
		expect(unique.size).toBe(2);
		return Promise.all(
			[...unique].map(async ([requestId, conversationId]) => {
				const record = await storage.submissionByRequest(
					conversationId as never,
					requestId,
					BACKGROUND_CONTEXT,
				);
				return record?.status === "done" ? record.answer : record?.status;
			}),
		);
	});
	expect(new Set(answers).size).toBe(1);
	// The one answer posts once, to the newest input's thread.
	expect(await t.posts("t1")).toEqual([]);
	expect((await t.posts("t2")).map((post) => post.content)).toEqual(["done"]);
});

it("maps notification priorities to write, followUp, and steer; written entries reach the next run", async () => {
	const t = await setup(() => [look]);
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
			return fauxAssistantMessage(fauxToolCall("look", {}), { stopReason: "toolUse" });
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
	const notify = (input: { text: string; priority: "urgent" | "normal" | "low" }) =>
		t.runtime.invoke(t.notify.name, input, {
			metadata: {
				requestId: randomUUID(),
				origin: { provider: "github" },
				replyTo: null,
				requestedBy: null,
			},
		});
	await notify({ text: "low: a label was added", priority: "low" });
	await t.settle();
	expect(t.faux.state.callCount).toBe(0);
	await notify({ text: "normal: CI passed", priority: "normal" });
	try {
		await started;
		await notify({ text: "normal: new commit", priority: "normal" });
		await notify({ text: "urgent: changes requested", priority: "urgent" });
		await expect
			.poll(async () => (await submitted(`app:ops-${t.channel}`)).length, { timeout: 10_000 })
			.toBe(4);
	} finally {
		release();
	}
	await t.settle();
	expect(seen).toHaveLength(3);
	// The low-priority write did not wake the agent but is in the context of the next run.
	expect(seen[0]).toContain("low: a label was added");
	expect(seen[0]).toContain('priority=\\"low\\"');
	expect(seen[0]).toContain("normal: CI passed");
	// Urgent steers into the running work; normal waits for the next run.
	expect(seen[1]).toContain("urgent: changes requested");
	expect(seen[1]).not.toContain("normal: new commit");
	expect(seen[2]).toContain("normal: new commit");
	// Pi keeps how each was submitted: the low-priority one as a passive write, the others as input.
	const types = await t.read(`app:ops-${t.channel}`, async (s, storage) =>
		Promise.all(
			(await requestsOf(s)).map(
				async ([requestId, conversationId]) =>
					(
						await storage.submissionByRequest(
							conversationId as never,
							requestId,
							BACKGROUND_CONTEXT,
						)
					)?.type ?? "",
			),
		),
	);
	expect(types.sort((a, b) => a.localeCompare(b))).toEqual(["input", "input", "input", "write"]);
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
				});
				return receipt.conversation.session;
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
			async () => {
				// Steer while this final answer is in flight; answer only once the driver admitted it.
				if (kind === "message")
					expect(await other.chat.receive(t.message("m2", "t2", "late steer"))).toBe("accepted");
				else
					await other.runtime.invoke(
						t.notify.name,
						{ text: "late steer", priority: "urgent" },
						{
							metadata: {
								requestId: randomUUID(),
								origin: { provider: "github" },
								replyTo: null,
								requestedBy: null,
							},
						},
					);
				await expect
					.poll(async () => (await submitted(session)).length, { timeout: 10_000 })
					.toBe(2);
				return fauxAssistantMessage("first");
			},
			(context) => {
				seen.push(text(context.messages));
				return fauxAssistantMessage("second");
			},
		]);
		if (kind === "message") await t.chat.receive(t.message("m1", "t1"));
		else
			await t.runtime.invoke(
				t.notify.name,
				{ text: "first", priority: "normal" },
				{
					metadata: {
						requestId: randomUUID(),
						origin: { provider: "github" },
						replyTo: null,
						requestedBy: null,
					},
				},
			);
		await t.settle();
		await other.settle();
		await t.settle();
		expect(seen).toHaveLength(1);
		expect(seen[0]).toContain("late steer");
		expect(t.faux.state.callCount).toBe(2);
		// Messages answer in their own threads; notifications without a reply thread post nothing.
		const posted = async (thread: string) =>
			(await other.chat.thread({
				kind: "thread",
				provider: "chat",
				threadId: `chat:${t.channel}:${thread}`,
			}))!.posts.length + (await t.posts(thread)).length;
		expect(await posted("t1")).toBe(kind === "message" ? 1 : 0);
		expect(await posted("t2")).toBe(kind === "message" ? 1 : 0);
	},
);

const queuedBehindFailedRun = async (kind: "notification" | "message") => {
	const t = await setup();
	// Another instance of the same app: on another queue, its Session execution would be a second one,
	// waiting for the lease, and its effects would post through its own chat.
	const other = await t.launch({ queue: t.runtime.queue, chat: t.chat });
	const session = kind === "message" ? `chat:channel:${t.channel}` : `app:ops-${t.channel}`;
	const seen: string[] = [];
	// Asserted after settling: a throw inside the response would only fail the generation, which
	// this test expects anyway, so a missed steer could still end in a passing run.
	let steerQueued = false;
	t.respond([
		async () => {
			// The steer queues behind this generation, which then fails: Pi leaves it in its inbox.
			if (kind === "message")
				expect(await other.chat.receive(t.message("m2", "t2", "queued steer"))).toBe("accepted");
			else
				await other.runtime.invoke(
					t.notify.name,
					{ text: "queued steer", priority: "urgent" },
					{
						metadata: {
							requestId: randomUUID(),
							origin: { provider: "github" },
							replyTo: null,
							requestedBy: null,
						},
					},
				);
			steerQueued = await expect
				.poll(async () => (await submitted(session)).length, { timeout: 10_000 })
				.toBe(2)
				.then(
					() => true,
					() => false,
				);
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
					requestId: randomUUID(),
					origin: { provider: "github" },
					replyTo: null,
					requestedBy: null,
				},
			},
		);
	await t.settle();
	await other.settle();
	expect(steerQueued, "the steer was not submitted while the failing generation ran").toBe(true);
	expect(seen).toHaveLength(1);
	expect(seen[0]).toContain("queued steer");
	expect(t.faux.state.callCount).toBe(2);
	if (kind === "message") {
		// The failed input got the error reply in its thread; the queued one its answer in its own.
		// "overloaded" is transient and tests retry nothing, so the retry budget ran out.
		const [failure] = (await t.posts("t1")).map((post) => post.content as string);
		expect(failure).toMatch(UNREACHABLE);
		expect((await t.posts("t2")).map((post) => post.content)).toEqual(["answered"]);
	}
};
it("a message queued behind a failed run is still answered", () =>
	queuedBehindFailedRun("message"));
it("a notification queued behind a failed run is still answered", () =>
	queuedBehindFailedRun("notification"));

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
	let steered = "";
	t.respond([
		fauxAssistantMessage(fauxToolCall("deploy", {}), { stopReason: "toolUse" }),
		(context) => {
			steered = text(context.messages);
			return fauxAssistantMessage("Rolled back");
		},
	]);
	const notify = (text: string, priority: "urgent" | "normal") =>
		t.runtime.invoke(
			t.notify.name,
			{ text, priority },
			{
				metadata: {
					requestId: randomUUID(),
					origin: { provider: "github" },
					replyTo: { kind: "thread", provider: "chat", threadId: `chat:${t.channel}:t1` },
					requestedBy: null,
				},
			},
		);
	await notify("deploy please", "normal");
	const session = `app:ops-${t.channel}`;
	try {
		await started;
		await notify("error rate is up", "urgent");
		await expect.poll(async () => (await submitted(session)).length, { timeout: 10_000 }).toBe(2);
	} finally {
		release();
	}
	await t.settle();
	expect(steered).toContain("deployed");
	expect(steered).toContain("error rate is up");
	expect(steered).toContain('priority=\\"urgent\\"');
	expect(t.faux.state.callCount).toBe(2);
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Rolled back"]);
	// Both inputs settled with the same answer: one run.
	const answers = await t.read(session, async (s, storage) => {
		return Promise.all(
			(await requestsOf(s)).map(async ([requestId, conversationId]) => {
				const record = await storage.submissionByRequest(
					conversationId as never,
					requestId,
					BACKGROUND_CONTEXT,
				);
				return record?.status === "done" ? record.answer : record?.status;
			}),
		);
	});
	expect(answers).toHaveLength(2);
	expect(new Set(answers).size).toBe(1);
});

/** Conductor retries a failed attempt after 15 seconds of real time. */
const RETRY_MS = 45_000;
/** Outcomes of the Session executions of the app on `queue`. */
async function sessions(queue: string) {
	const rows = await sql()<{ id: string }[]>`
		select id from pgconductor._private_executions
		where queue = ${queue} and task_key = 'fabrial.session' order by created_at`;
	return Promise.all(
		rows.map(async (row) => {
			const info = await db.execution(row.id).info();
			return { status: info?.status, attempts: info?.attempts };
		}),
	);
}

/** Replace a thread's posts: `post` gets the content and the real post. */
async function intercept(
	t: Awaited<ReturnType<typeof setup>>,
	thread: string,
	post: (content: ChatMessage["text"] | object, actual: () => Promise<unknown>) => Promise<unknown>,
) {
	const io = await t.chat.thread({
		kind: "thread",
		provider: "chat",
		threadId: `chat:${t.channel}:${thread}`,
	});
	const actual = io.post.bind(io);
	io.post = ((content: Parameters<typeof actual>[0]) =>
		post(content, () => actual(content))) as typeof io.post;
}
/** The requests the Session admitted, with their conversation. */
const requestsOf = async (s: Pick<ReturnType<typeof createSession>, "snapshot">) =>
	Object.entries((await s.snapshot(Obligations, BACKGROUND_CONTEXT))!.requests).flatMap(
		([requestId, obligation]) =>
			obligation.owes === "task" ? [] : [[requestId, obligation.conversationId] as const],
	);
/** The posts of the replies the Session owed, by intent key. */
const deliveries = async (t: Awaited<ReturnType<typeof setup>>) => {
	const obligations = await t.read(`chat:channel:${t.channel}`, (s) =>
		s.snapshot(Obligations, BACKGROUND_CONTEXT),
	);
	return Object.values(obligations?.requests ?? {}).flatMap((obligation) =>
		Object.values(obligation.deliveries ?? {}),
	);
};

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
		fauxAssistantMessage("Answer two"),
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
	expect(t.faux.state.callCount).toBe(2);
});

it("a transient model error retried durably shows the waiting status and posts no error reply", async () => {
	// The chat thread shows a different status at most once a second: the error and the retry come later.
	const t = await setup(undefined, {
		retry: { enabled: true, maxRetries: 2, baseDelayMs: 1_200, maxAgentDelayMs: 1_200 },
	});
	t.respond([
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 1_200));
			return fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 overloaded" });
		},
		// Slow too: a "Thinking…" inside the window shows when it ends, unless the reply's clear comes first.
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 1_200));
			return fauxAssistantMessage("Answer one");
		},
	]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one"]);
	const shown = await statuses(t, "t1");
	expect(shown).toContain(WAITING_STATUS);
	expect(shown.slice(shown.indexOf(WAITING_STATUS))).toContain("Thinking…");
	expect(shown.at(-1)).toBeNull();
	expect(t.faux.state.callCount).toBe(2);
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
		fauxAssistantMessage("Answer two"),
	]);
	await t.chat.receive(t.message("m1", "t1"));
	await blocking;
	for (const driver of await open(t.runtime.queue, "fabrial.session"))
		await t.runtime.cancel(driver, "Stopped");
	await t.settle();
	expect(await t.posts("t1")).toEqual([]);
	await t.chat.receive(t.message("m2", "t1", "again"));
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer two"]);
});

it(
	"a driver redelivered after starting the error reply does not post it twice",
	async () => {
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
		const t = await setup(undefined, { logger, sessionAttempts: 2 });
		t.respond([fauxAssistantMessage("", { stopReason: "error", errorMessage: "no such model" })]);
		await t.chat.receive(t.message("m1", "t1"));
		// The crashed attempt is retried after Conductor's backoff.
		await t.settle(RETRY_MS);
		expect(crashed).toBe(true);
		const posts = (await t.posts("t1")).map((post) => post.content as string);
		expect(posts).toHaveLength(1);
		expect(posts[0]).toMatch(FAILED);
		expect(await sessions(t.runtime.queue)).toEqual([{ status: "completed", attempts: 2 }]);
	},
	2 * RETRY_MS,
);

/** Pi submission ids per request id in a Session, from its commit log. */
async function submissionIds(session: string): Promise<Map<string, Set<number>>> {
	const rows =
		await sql()`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${session} ORDER BY seq`;
	const ids = new Map<string, Set<number>>();
	for (const row of rows)
		for (const write of JSON.parse(String(row.writes)) as StorageWrite[])
			if (write.type === "submission" && write.value.requestId) {
				const each = ids.get(write.value.requestId) ?? new Set();
				each.add(write.value.id);
				ids.set(write.value.requestId, each);
			}
	return ids;
}

it(
	"a crash after an input's obligation committed and before its submission answers and posts it once",
	async () => {
		let crashed = false;
		const logger: Logger = {
			debug: (message) => {
				if (message === "Submitting input" && !crashed) {
					crashed = true;
					throw new Error("driver crashed");
				}
			},
			info() {},
			warn() {},
			error() {},
		};
		const t = await setup(undefined, { logger, sessionAttempts: 2 });
		t.respond([fauxAssistantMessage("Answer one")]);
		await t.chat.receive(t.message("m1", "t1"));
		await t.settle(RETRY_MS);
		expect(crashed).toBe(true);
		const session = `chat:channel:${t.channel}`;
		expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one"]);
		expect([...(await submissionIds(session)).values()].map((ids) => ids.size)).toEqual([1]);
		expect(t.faux.state.callCount).toBe(1);
		const obligations = await t.read(session, (s) => s.snapshot(Obligations, BACKGROUND_CONTEXT));
		expect(Object.values(obligations!.requests)).toEqual([
			expect.objectContaining({ owes: "reply", fulfilled: true }),
		]);
		expect(await sessions(t.runtime.queue)).toEqual([{ status: "completed", attempts: 2 }]);
	},
	2 * RETRY_MS,
);

it(
	"a crash after a steering input's submission and before its signal is acknowledged answers it once",
	async () => {
		let submissions = 0;
		let crashed!: () => void;
		const crash = new Promise<void>((resolve) => (crashed = resolve));
		const logger: Logger = {
			debug: (message) => {
				// The second message is submitted while its signal is handled: fail the handler right after.
				if (message === "Submitted input" && ++submissions === 2) {
					crashed();
					throw new Error("driver crashed");
				}
			},
			info() {},
			warn() {},
			error() {},
		};
		const t = await setup(() => [look], { logger, sessionAttempts: 2 });
		let release!: () => void;
		const gate = new Promise<void>((resolve) => (release = resolve));
		let busy!: () => void;
		const started = new Promise<void>((resolve) => (busy = resolve));
		t.respond([
			async () => {
				busy();
				await gate;
				return fauxAssistantMessage(fauxToolCall("look", {}), { stopReason: "toolUse" });
			},
			fauxAssistantMessage("done"),
			fauxAssistantMessage("done"),
		]);
		await t.chat.receive(t.message("m1", "t1"));
		try {
			await started;
			expect(await t.chat.receive(t.message("m2", "t2", "also this"))).toBe("accepted");
			await crash;
		} finally {
			release();
		}
		await t.settle(RETRY_MS);
		const session = `chat:channel:${t.channel}`;
		const ids = await submissionIds(session);
		expect([...ids.values()].map((each) => each.size)).toEqual([1, 1]);
		// One answer for both, posted once to the newest input's thread.
		expect(await t.posts("t1")).toEqual([]);
		expect((await t.posts("t2")).map((post) => post.content)).toEqual(["done"]);
		const obligations = await t.read(session, (s) => s.snapshot(Obligations, BACKGROUND_CONTEXT));
		expect(Object.values(obligations!.requests).map((each) => each.fulfilled)).toEqual([
			true,
			true,
		]);
	},
	2 * RETRY_MS,
);

it("a sent run's background child reports as a follow-up; the lead's answer posts to the starting input's thread", async () => {
	const t = await setup(undefined, { researcher: true });
	const tool = `researcher_${t.channel}`;
	const seen: string[] = [];
	const last = (messages: readonly Message[]) => {
		const message = messages.findLast((each) => (each.role as string) !== "system")!;
		return typeof message.content === "string" ? message.content : text([message]);
	};
	t.respond(
		Array.from({ length: 12 }, () => (context: { messages: Message[] }) => {
			const latest = last(context.messages);
			seen.push(latest);
			if (latest === "Find the cause") return fauxAssistantMessage("It is DNS");
			if (latest.includes("subagent name=") && latest.includes("It is DNS"))
				return fauxAssistantMessage("Found: DNS");
			if (latest.includes("Started")) return fauxAssistantMessage("Looking into it");
			return fauxAssistantMessage(
				fauxToolCall(tool, { task: "Find the cause", background: true }),
				{ stopReason: "toolUse" },
			);
		}),
	);
	await t.chat.receive(t.message("m1", "t1", "Why is it down?"));
	// The child reports after the lead's run returned: wait for the answer to its report.
	await until("the report's answer", async () =>
		(await t.posts("t1")).some((post) => post.content === "Found: DNS") ? true : undefined,
	).catch(async (error: Error) => {
		const posts = (await t.posts("t1")).map((post) => post.content);
		throw new Error(
			`${error.message}; model saw ${JSON.stringify(seen)}; posts ${JSON.stringify(posts)}; sessions ${JSON.stringify(await sessions(t.runtime.queue))}`,
		);
	});
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

it("no_reply ends a run without posting and keeps its reason in the transcript", async () => {
	const t = await setup();
	let next = "";
	t.respond([
		fauxAssistantMessage(fauxToolCall("no_reply", { reason: "just chatter" }), {
			stopReason: "toolUse",
		}),
		(context) => {
			next = text(context.messages);
			return fauxAssistantMessage("Answer two");
		},
	]);
	await t.chat.receive(t.message("m1", "t1", "lol"));
	await t.settle();
	// The tool ended the run: no second model call, nothing posted, no status left.
	expect(t.faux.state.callCount).toBe(1);
	expect(await t.posts("t1")).toEqual([]);
	expect((await statuses(t, "t1")).at(-1)).toBeNull();
	// Nothing to deliver: the Session completed without waiting on a post.
	expect(await deliveries(t)).toEqual([]);
	expect(await sessions(t.runtime.queue)).toEqual([{ status: "completed", attempts: 1 }]);
	await t.chat.receive(t.message("m2", "t1", "a real question"));
	await t.settle();
	expect(next).toContain("just chatter");
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer two"]);
});

it(
	"posts a final reply at least once across a restart, a redelivery, and a post repeated after the provider accepted it",
	async () => {
		let crashed = false;
		const logger: Logger = {
			debug: (message) => {
				// Crash once right after the final reply's durable intent started, while its post is in flight.
				if (message === "Posting final reply" && !crashed) {
					crashed = true;
					throw new Error("driver crashed");
				}
			},
			info() {},
			warn() {},
			error() {},
		};
		const t = await setup(undefined, { logger, sessionAttempts: 2 });
		// The provider accepts the first post, but its receipt is lost: the effect's retry posts again.
		let attempts = 0;
		await intercept(t, "t1", async (_content, actual) => {
			const posted = await actual();
			if (++attempts === 1) throw new Error("connection reset after the post");
			return posted;
		});
		const signals = vi.spyOn(t.runtime, "signalWithStart");
		t.respond([fauxAssistantMessage("The answer"), fauxAssistantMessage("unexpected")]);
		await t.chat.receive(t.message("m1", "t1"));
		// The crashed attempt and the failed post are retried after Conductor's backoff.
		await t.settle(RETRY_MS);
		expect(crashed).toBe(true);
		expect(await t.chat.receive(t.message("m1", "t1"))).toBe("accepted");
		// The same input signalled again, as a redelivered Conductor signal would be.
		const [request] = await submitted(`chat:channel:${t.channel}`);
		const input = signals.mock.calls.find(([start]) => start.signal.id === request)![0].signal
			.payload;
		await t.runtime.signalWithStart({
			workflow: "fabrial.session",
			key: `chat:channel:${t.channel}`,
			signal: { name: "input", id: `${request}:again`, payload: input },
			input: { session: `chat:channel:${t.channel}` },
			metadata: {
				requestId: request!,
				origin: null,
				replyTo: null,
				requestedBy: null,
			},
		});
		await t.settle();
		// At least once: the restarted Session reconnected to the same post, which repeated only the accepted one.
		expect((await t.posts("t1")).map((post) => post.content)).toEqual(["The answer", "The answer"]);
		expect(attempts).toBe(2);
		expect(t.faux.state.callCount).toBe(1);
		expect(await deliveries(t)).toEqual([expect.objectContaining({ status: "posted" })]);
		expect((await sessions(t.runtime.queue)).at(0)).toEqual({ status: "completed", attempts: 2 });
		// The crashed attempt cleared its progress; the reopened one, with only the post pending, shows none.
		expect(await statuses(t, "t1")).toEqual(["Thinking…", null]);
	},
	2 * RETRY_MS,
);

it("a run waits for its final post; input arriving meanwhile is still answered", async () => {
	const t = await setup();
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	let held = false;
	await intercept(t, "t1", async (_content, actual) => {
		held = true;
		await gate;
		return actual();
	});
	t.respond([fauxAssistantMessage("Answer one"), fauxAssistantMessage("Answer two")]);
	await t.chat.receive(t.message("m1", "t1"));
	await until("the held post", async () => (held ? true : undefined));
	// The answer exists, but the run waits for its post: the Session does not complete.
	await new Promise((resolve) => setTimeout(resolve, 500));
	expect((await sessions(t.runtime.queue)).map((session) => session.status)).not.toContain(
		"completed",
	);
	expect(await deliveries(t)).toEqual([expect.objectContaining({ status: "pending" })]);
	// Another thread's message is handled while the post is pending.
	await t.chat.receive(t.message("m2", "t2", "second"));
	await until("the second answer", async () => ((await t.posts("t2")).length ? true : undefined));
	expect((await t.posts("t2")).map((post) => post.content)).toEqual(["Answer two"]);
	expect(await t.posts("t1")).toEqual([]);
	expect((await sessions(t.runtime.queue)).map((session) => session.status)).not.toContain(
		"completed",
	);
	release();
	await t.settle();
	expect((await t.posts("t1")).map((post) => post.content)).toEqual(["Answer one"]);
	expect((await deliveries(t)).map((delivery) => delivery.status)).toEqual(["posted", "posted"]);
	expect(await sessions(t.runtime.queue)).toEqual([{ status: "completed", attempts: 1 }]);
	expect(t.faux.state.callCount).toBe(2);
});

it("a final post that fails for good is logged and recorded, without rerunning the model or an error reply", async () => {
	const errors: { message: string; data?: Record<string, unknown> }[] = [];
	const logger: Logger = {
		debug() {},
		info() {},
		warn() {},
		error: (message, data) => errors.push({ message, data }),
	};
	const t = await setup(undefined, { logger, effectAttempts: 1 });
	const attempted: unknown[] = [];
	await intercept(t, "t1", async (content) => {
		attempted.push(content);
		throw new Error("chat is down");
	});
	t.respond([fauxAssistantMessage("The answer"), fauxAssistantMessage("unexpected")]);
	await t.chat.receive(t.message("m1", "t1"));
	await t.settle();
	// One post attempted, no error reply for it, and the model ran once.
	expect(attempted).toEqual(["The answer"]);
	expect(await t.posts("t1")).toEqual([]);
	expect(t.faux.state.callCount).toBe(1);
	expect(await deliveries(t)).toEqual([
		expect.objectContaining({ status: "failed", error: "chat is down" }),
	]);
	expect(errors).toEqual([
		expect.objectContaining({
			message: "Reply post failed",
			data: expect.objectContaining({ error: "chat is down" }),
		}),
	]);
	expect(await sessions(t.runtime.queue)).toEqual([{ status: "completed", attempts: 1 }]);
	// The conversation stays usable.
	const posted: unknown[] = [];
	await intercept(t, "t2", async (content, actual) => {
		posted.push(content);
		return actual();
	});
	t.respond([fauxAssistantMessage("Answer two")]);
	await t.chat.receive(t.message("m2", "t2"));
	await t.settle();
	expect(posted).toEqual(["Answer two"]);
});

it("ctx.agent returns the answer and posts nothing, without offering no_reply", async () => {
	const t = await setup();
	let after = "";
	t.respond([
		fauxAssistantMessage(fauxToolCall("no_reply", {}), { stopReason: "toolUse" }),
		(context) => {
			after = text(context.messages);
			return fauxAssistantMessage("42");
		},
	]);
	const thread = { kind: "thread" as const, provider: "chat", threadId: `chat:${t.channel}:t3` };
	const id = await t.runtime.invoke(
		t.ask.name,
		{ question: "What is the answer?" },
		{
			metadata: {
				requestId: randomUUID(),
				origin: null,
				replyTo: thread,
				requestedBy: null,
			},
		},
	);
	expect(await db.execution(id).settled()).toMatchObject({
		status: "completed",
		output: "42",
	});
	// no_reply is not offered: the call failed as an unknown tool and the run went on.
	expect(t.faux.state.callCount).toBe(2);
	expect(after).toContain("no_reply");
	expect(await t.posts("t3")).toEqual([]);
});
