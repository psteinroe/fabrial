import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
	createRegistry,
	createSession,
	Harness,
	LiveDoc,
	defineExtension,
	hook,
	GenerationTask,
} from "@earendil-works/pi-durable";
import { Type } from "typebox";
import {
	defineState,
	defineWorkflow,
	type DurableExecution,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeWorkflow,
} from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { ChildIntents, Invocation, Runs } from "../../src/documents.ts";
import { z } from "zod";
import {
	abortOrphans,
	checkOrphans,
	defineAgent,
	defineTool,
	pi,
	PostgresStorage,
	section,
	sessionKey,
} from "../../src/index.ts";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";

let sql: Sql;
let container: Awaited<ReturnType<PostgreSqlContainer["start"]>>;
beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	sql = postgres(container.getConnectionUri());
	await PostgresStorage.migrate(sql);
});
afterAll(async () => {
	await sql?.end();
	await container?.stop();
});

function fixture(plugins: FabrialHost["plugins"] = []) {
	const runtime = new MemoryRuntime();
	const chat = new FakeChat();
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		interactionId: randomUUID(),
		origin: null,
		replyTo: { kind: "thread", provider: "slack-test", threadId: randomUUID() },
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownsThread: true,
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const host = {
		plugins,
		workflows: [],
		runtime,
		clients: () => ({ example: "client" }),
		chat: () => chat,
		logger: { warn() {}, error() {}, debug() {}, info() {} },
		directory: { isMember: async () => true },
		resolvePrincipal: async (identity: { subjectId: string }) => ({
			id: identity.subjectId,
			known: true,
			identities: [],
			name: identity.subjectId,
		}),
	} as unknown as FabrialHost;
	const integration = pi({ models, sql, settings: { retry: { enabled: false } } }).connect(host);
	return { runtime, chat, faux, integration, metadata, host };
}
async function execute(f: ReturnType<typeof fixture>, workflow: RuntimeWorkflow) {
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	await f.runtime.flush();
	return { id, result: f.runtime.result(id) };
}
function runtimeRegister(
	f: ReturnType<typeof fixture>,
	workflow: RuntimeWorkflow,
	others: RuntimeWorkflow[] = [],
) {
	f.runtime.register({ workflows: [workflow, ...f.integration.workflows, ...others], events: [] });
}

it("runs durable structured answers, memoizes submissions, and continues one agent conversation", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage('{"answer":42}'), fauxAssistantMessage("continued")]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			await execution.step("answer:output", () => "user receipt");
			const first = await f.integration.agents.run(execution, "answer", agent, {
				input: "Question",
				output: z.object({ answer: z.number() }),
			});
			const again = await f.integration.agents.run(execution, "again", agent, {
				input: "Continue",
			});
			return { first, again };
		},
	};
	const { result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { first: { answer: 42 }, again: "continued" },
	});
	expect(f.faux.state.callCount).toBe(2);
	const commits =
		await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${sessionKey(f.metadata)} ORDER BY seq`;
	const conversations = commits
		.flatMap((row) => JSON.parse(String(row.writes)) as { type: string }[])
		.filter((write) => write.type === "conversation");
	expect(conversations).toHaveLength(1);
	await f.integration.stop();
});

it("binds adapter identity, clients, sections and durable posts; commits tool state with a result receipt", async () => {
	const f = fixture();
	const state = defineState({
		name: randomUUID(),
		scope: "thread",
		schema: z.object({ notes: z.array(z.string()) }),
		initial: () => ({ notes: [] as string[] }),
		render: (value) => value.notes.join(","),
	});
	let actor: string | undefined;
	const tool = defineTool({
		name: "record_note",
		description: "Record a note",
		parameters: Type.Object({ text: Type.String() }),
		execute: async ({ text }, ctx) => {
			actor = ctx.actor?.id;
			expect(ctx.clients).toEqual({ example: "client" });
			await ctx.state(state).update((draft) => {
				draft.notes.push(text);
			});
			await ctx.thread!.post("post", "Recorded");
			return { content: [{ type: "text", text: "done" }] };
		},
	});
	const extension = defineExtension({
		name: randomUUID(),
		tools: [tool],
		sections: [section("requester", (_input, ctx) => `Requester ${ctx.actor?.name}`)],
	});
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
		extensions: [extension],
		state: [state],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("record_note", { text: "hello" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("finished"),
	]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			await f.integration.state.update(execution, "seed", state, (draft) => {
				draft.notes.push("seed");
			});
			const answer = await f.integration.agents.run(execution, "agent", agent, { input: "Do it" });
			return { answer, state: await f.integration.state.get(execution, "read", state) };
		},
	};
	const { result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { answer: "finished", state: { notes: ["seed", "hello"] } },
	});
	expect(actor).toBe("alice");
	const io = await f.chat.resolve(f.metadata.replyTo!);
	const recorded = await f.chat.thread(io.ref);
	expect(recorded.posts).toHaveLength(1);
	expect(recorded.statuses).toContain("Thinking…");
	expect(recorded.statuses.at(-1)).toBeNull();
	const rows = await sql`SELECT writes FROM fabrial_pi.commits`;
	expect(
		rows.some((row) => {
			const writes = JSON.parse(String(row.writes)) as {
				type: string;
				record?: { kind: string };
			}[];
			return (
				writes.some((write) => write.record?.kind === "fabrial.tool-result") &&
				writes.some((write) => write.type === "document.change")
			);
		}),
	).toBe(true);
	await f.integration.stop();
});

it("releases the Session while a workflow tool waits and reconnects with a stable child id", async () => {
	const f = fixture();
	f.metadata.triggerEvent = "slack.mentioned";
	f.metadata.ownerWorkflow = "router";
	const child = defineWorkflow({
		name: randomUUID(),
		input: z.object({ value: z.string() }),
		run: async () => "unused",
	});
	(f.host.workflows as unknown as (typeof child)[]).push(child);
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
		tools: [child.asTool()],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall(child.asTool().name, { value: "ok" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("child finished"),
	]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Run child" }),
	};
	runtimeRegister(f, workflow, [
		{
			name: child.name,
			triggers: [],
			handler: async (_input, execution) => {
				expect(execution.metadata).toMatchObject({
					...f.metadata,
					ownsThread: false,
					triggerEvent: null,
					ownerWorkflow: null,
				});
				const storage = await PostgresStorage.open(sql, sessionKey(f.metadata));
				await storage.close(BACKGROUND_CONTEXT);
				return "ok";
			},
		},
	]);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	await f.runtime.flush();
	expect(f.runtime.result(id)).toEqual({ status: "completed", output: "child finished" });
	expect(f.runtime.executions(child.name)).toHaveLength(1);
	await f.integration.stop();
});

it("rejects call-time configure and invalid structured answers", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage('{"answer":"not a number"}')]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, {
				input: "Answer",
				output: z.object({ answer: z.number() }),
			}),
	};
	const { result } = await execute(f, workflow);
	expect(result).toMatchObject({
		status: "failed",
		error: expect.stringContaining("Invalid agent"),
	});
	await f.integration.stop();
});

it("detects missing live task kinds and explicitly aborts their ownership tree", async () => {
	const storage = await PostgresStorage.open(sql, randomUUID());
	const id = await storage.mintId();
	await storage.commit(
		[
			{ type: "conversation", value: { id: 1 as never } },
			{
				type: "task",
				value: {
					id: id as never,
					conversationId: 1 as never,
					kind: "removed.extension",
					version: 1,
					input: {},
					background: false,
					abortRequested: false,
					state: { status: "pending", checkpoint: { phase: "run" } },
				},
			},
		],
		BACKGROUND_CONTEXT,
	);
	await storage.close(BACKGROUND_CONTEXT);
	await expect(checkOrphans(sql, ["pi.generation", "pi.tool", "pi.compaction"])).rejects.toThrow(
		"abortOrphans",
	);
	expect(await abortOrphans(sql, ["removed.extension"])).toBe(1);
	await expect(
		checkOrphans(sql, ["pi.generation", "pi.tool", "pi.compaction"]),
	).resolves.toBeUndefined();
});

it.each([
	[false, false],
	[true, false],
	[false, true],
])(
	"honors agent detached=%s and tool detached=%s when the agent caller is cancelled",
	async (detached, toolDetached) => {
		const f = fixture();
		const child = defineWorkflow({ name: randomUUID(), run: async () => null });
		(f.host.workflows as unknown as (typeof child)[]).push(child);
		const agent = defineAgent({
			name: randomUUID(),
			model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
			tools: [child.asTool({ detached: toolDetached })],
		});
		f.faux.setResponses([
			fauxAssistantMessage(fauxToolCall(child.asTool().name, {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("approved"),
		]);
		const workflow: RuntimeWorkflow = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, execution) =>
				f.integration.agents.run(execution, "agent", agent, { input: "Wait", detached }),
		};
		runtimeRegister(f, workflow, [
			{
				name: child.name,
				triggers: [],
				handler: async (_input, execution) => {
					await execution.sleep("approval", 86_400_000);
					return "approved";
				},
			},
		]);
		await f.integration.start();
		await f.runtime.start();
		const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
		await f.runtime.flush();
		expect(f.runtime.executions(child.name)[0]?.status).toBe("suspended");
		await f.runtime.cancel(id, "User stopped");
		// Cancellation queues onSettled; flush delivers the bridge's persisted-child cleanup.
		await f.runtime.flush();
		expect(f.runtime.result(id)?.status).toBe("cancelled");
		if (detached) {
			expect(f.runtime.executions(child.name)[0]?.status).toBe("suspended");
			expect(f.runtime.executions("fabrial.pi.run")[0]?.status).toBe("suspended");
			await f.runtime.advanceBy(86_400_000);
			expect(f.runtime.executions("fabrial.pi.run")[0]?.result).toEqual({
				status: "completed",
				output: "approved",
			});
		} else {
			if (toolDetached) expect(f.runtime.executions(child.name)[0]?.status).toBe("suspended");
			else expect(f.runtime.executions(child.name)[0]?.result?.status).toBe("cancelled");
			expect(f.runtime.executions("fabrial.pi.run")[0]?.result?.status).toBe("cancelled");
		}
		await f.integration.stop();
	},
);

it("installs plugin hooks globally but does not select unrequested plugin extensions", async () => {
	let requests = 0;
	const f = fixture([
		{
			id: "unselected",
			extension: () =>
				defineExtension({
					name: "unselected",
					sections: [
						section("hidden", () => {
							throw new Error("Unselected extension rendered");
						}),
					],
				}),
			hooks: {
				agent: [
					hook(GenerationTask, {
						beforeRequest: () => {
							requests++;
						},
					}),
				],
			},
		},
	]);
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage("ok")]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Hello" }),
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(requests).toBe(1);
	await f.integration.stop();
});

it("consumes buffered steering replies with stable request ids", async () => {
	const f = fixture();
	const io = await f.chat.resolve(f.metadata.replyTo!);
	await io.updateState(() => ({
		interactionId: f.metadata.interactionId,
		handlerExecutionId: null,
		agentActive: false,
		statusMessageId: null,
		bufferedReplies: [
			{
				provider: "slack-test",
				threadId: io.ref.threadId,
				channelId: io.channelId,
				messageId: "steer",
				text: "Use the new account",
				author: {
					identity: { provider: "slack-test", installationId: "test", subjectId: "bob" },
					name: "Bob",
					isBot: false,
				},
				isMention: false,
				isDM: false,
				sentAt: new Date().toISOString(),
			},
		],
	}));
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	const routing = await io.getState();
	// Admit steering only after the original input finishes. The driver must still drain it.
	vi.spyOn(io, "getState").mockImplementationOnce(async () => {
		await expect
			.poll(async () => {
				const rows =
					await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${sessionKey(f.metadata)}`;
				return rows.some((row) =>
					(JSON.parse(String(row.writes)) as { type: string; value?: { status?: string } }[]).some(
						(write) => write.type === "submission" && write.value?.status === "done",
					),
				);
			})
			.toBe(true);
		return routing;
	});
	f.faux.setResponses([
		fauxAssistantMessage("ok"),
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 100));
			return fauxAssistantMessage("steered");
		},
	]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Hello" }),
	});
	expect(result).toEqual({ status: "completed", output: "steered" });
	expect(f.faux.state.callCount).toBe(2);
	const storage = await PostgresStorage.open(sql, sessionKey(f.metadata));
	const session = createSession(storage);
	try {
		const requestId = JSON.stringify([
			"fabrial.pi.reply",
			f.metadata.interactionId,
			"slack-test",
			"steer",
		]);
		expect(await session.snapshot(Invocation, requestId, BACKGROUND_CONTEXT)).toMatchObject({
			requestedBy: { id: "bob" },
			actor: { id: "bob" },
			interactionId: f.metadata.interactionId,
			replyTo: f.metadata.replyTo,
			origin: { provider: "slack-test", threadId: io.ref.threadId, messageId: "steer" },
		});
		const runs = await session.snapshot(Runs, BACKGROUND_CONTEXT);
		const requests = Object.values(runs!.runs).flatMap((run) => Object.values(run.requests ?? {}));
		expect(requests).toHaveLength(2);
		for (const request of requests)
			expect(
				(await storage.submission(request.submissionId as never, BACKGROUND_CONTEXT))?.status,
			).toBe("done");
	} finally {
		await session.close(BACKGROUND_CONTEXT);
	}
	expect((await io.getState())?.bufferedReplies).toHaveLength(0);
	expect((await io.getState())?.agentActive).toBe(false);
	await f.integration.stop();
});

it("does not promote an unposted channel locator to a Pi Session key", async () => {
	const f = fixture();
	f.metadata.replyTo = { kind: "channel", provider: "slack-test", channelId: "C1" };
	const state = defineState({
		name: randomUUID(),
		scope: "thread",
		schema: z.object({ value: z.string() }),
		initial: () => ({ value: "initial" }),
	});
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			await expect(f.integration.state.get(execution, "state", state)).rejects.toThrow(
				"ctx.thread.post",
			);
			return f.integration.agents.run(
				execution,
				"agent",
				{ kind: "fabrial.agent", name: "unused" },
				{ input: "Hello" },
			);
		},
	});
	expect(result).toMatchObject({
		status: "failed",
		error: expect.stringContaining("ctx.thread.post"),
	});
	expect(f.runtime.executions("fabrial.pi.run")).toHaveLength(0);
	expect(f.chat.threads.size).toBe(0);
	await f.integration.stop();
});

it("replays a workflow state update after a lost runtime receipt without applying twice", async () => {
	const f = fixture();
	const state = defineState({
		name: randomUUID(),
		scope: "thread",
		schema: z.object({ count: z.number() }),
		initial: () => ({ count: 0 }),
	});
	const execution = {
		executionId: randomUUID(),
		metadata: f.metadata,
		signal: new AbortController().signal,
		step: async (_id: string, fn: () => Promise<unknown>) => fn(),
	} as DurableExecution;
	const change = vi.fn((draft: { count: number }) => {
		draft.count++;
	});
	const step = vi.spyOn(execution, "step");
	step.mockImplementationOnce(async (_id, fn) => {
		await fn();
		throw new Error("runtime receipt lost");
	});
	await expect(f.integration.state.update(execution, "increment", state, change)).rejects.toThrow(
		"runtime receipt lost",
	);
	await f.integration.state.update(execution, "other", state, (draft) => {
		draft.count += 10;
	});
	expect(await f.integration.state.update(execution, "increment", state, change)).toEqual({
		count: 1,
	});
	expect(change).toHaveBeenCalledTimes(1);
	expect(await f.integration.state.get(execution, "read", state)).toEqual({ count: 11 });
});

it("delayed cancellation reconciles lost start receipts and leaves a newer conversation run intact", async () => {
	const f = fixture();
	const childWorkflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async () => null,
	};
	f.runtime.register({ workflows: [childWorkflow], events: [] });
	const key = "fabrial:pi:lost-start";
	const dedupeKey = JSON.stringify([sessionKey(f.metadata), key]);
	const child = await f.runtime.invoke(childWorkflow.name, null, {
		metadata: f.metadata,
		dedupeKey,
	});
	const storage = await PostgresStorage.open(sql, sessionKey(f.metadata));
	const harness = await Harness.open(
		storage,
		{ models: createModels(), registry: createRegistry() },
		BACKGROUND_CONTEXT,
	);
	const conversation = await harness.createConversation(
		{ ownership: { kind: "ownerless" } },
		BACKGROUND_CONTEXT,
	);
	const oldRequest = "old-request";
	const current = await harness.commit(async (tx) => {
		const live = await tx.doc(LiveDoc, conversation.id);
		const runs = await tx.doc(Runs);
		await tx.doc(ChildIntents, key, {
			workflow: childWorkflow.name,
			input: null,
			metadata: f.metadata,
			dedupeKey,
			detached: false,
			executionId: null,
		});
		const old = await tx.createSubmission({
			conversationId: conversation.id,
			requestId: oldRequest,
			type: "input",
			status: "queued",
		});
		const entry = await tx.appendEntry(conversation.id, {
			kind: "pi.user",
			model: [{ role: "user", content: "new request", timestamp: Date.now() }],
		});
		const submission = await tx.createSubmission({
			conversationId: conversation.id,
			requestId: "new-request",
			type: "input",
			status: "placed",
			entry: entry.id,
		});
		const task = await tx.createTask(
			GenerationTask,
			{},
			{ conversationId: conversation.id, ownership: { kind: "conversation" } },
		);
		live.run = { taskId: task, inputs: [submission.id] };
		runs.runs.old = {
			conversationId: conversation.id,
			children: {},
			intents: [key],
			requests: { [oldRequest]: { content: "old", submissionId: old.id } },
		};
		return { task, submission: submission.id, old: old.id };
	}, BACKGROUND_CONTEXT);
	await harness.close(BACKGROUND_CONTEXT);
	await f.integration.workflows.find((workflow) => workflow.name === "fabrial.pi.run")!.onSettled!(
		"old",
		f.metadata,
		{ status: "cancelled", reason: "late cancellation" },
	);
	expect(f.runtime.executions(childWorkflow.name)).toHaveLength(1);
	expect(f.runtime.result(child)).toMatchObject({ status: "cancelled" });
	const reopened = await PostgresStorage.open(sql, sessionKey(f.metadata));
	const session = createSession(reopened);
	try {
		expect((await reopened.task(current.task, BACKGROUND_CONTEXT))?.abortRequested).toBe(false);
		expect((await reopened.submission(current.submission, BACKGROUND_CONTEXT))?.status).toBe(
			"placed",
		);
		expect((await reopened.submission(current.old, BACKGROUND_CONTEXT))?.status).toBe("unanswered");
		expect(
			(await session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT))?.run?.taskId,
		).toBe(current.task);
	} finally {
		await session.close(BACKGROUND_CONTEXT);
	}
});

it("stop joins driver routing cleanup after the harness has closed", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage("done")]);
	const io = await f.chat.thread(f.metadata.replyTo as Parameters<typeof f.chat.thread>[0]);
	let reached!: () => void;
	const cleaning = new Promise<void>((resolve) => {
		reached = resolve;
	});
	let release!: () => void;
	const cleanup = new Promise<void>((resolve) => {
		release = resolve;
	});
	const update = io.updateState.bind(io);
	vi.spyOn(io, "updateState").mockImplementation(async (change) => {
		const next = change(await io.getState());
		if (next?.agentActive === false) {
			reached();
			await cleanup;
		}
		return update(change);
	});
	await io.updateState(() => ({
		interactionId: f.metadata.interactionId,
		handlerExecutionId: null,
		statusMessageId: null,
		agentActive: true,
		bufferedReplies: [],
	}));
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Answer" }),
	};
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.runtime.start();
	await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	const driving = f.runtime.flush();
	let stopped = false;
	try {
		await cleaning;
		const stopping = f.integration.stop().then(() => {
			stopped = true;
		});
		await new Promise((resolve) => setTimeout(resolve, 50));
		expect(stopped).toBe(false);
		release();
		await stopping;
		expect(stopped).toBe(true);
	} finally {
		release();
		await driving;
		await f.integration.stop();
	}
});
