import { metadataOf } from "../../../src/execution.ts";
import { connectChat } from "../../../src/chat/index.ts";
import { randomUUID } from "node:crypto";
import type { Message, ToolResultMessage, UserMessage } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
	createFabrial,
	type FabrialHost,
	type InvocationMetadata,
	type TaskEvent,
	type WorkflowTask,
	type ToolCall,
} from "../../internals.ts";
import { FakeChat } from "../../../src/testing.ts";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import {
	callSession,
	connectAgents,
	PostgresStorage,
	sessionKey,
} from "../../../src/agents/index.ts";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { until } from "../../conductor/helper.ts";
import { useAgents } from "./setup.ts";

const definitions = createFabrial({ plugins: [] });
const { defineWorkflow } = definitions;
const { defineAgent, defineTool, defineToolkit, defineState } = definitions;

/**
 * The Pi integration connected to a bare host on real Conductor, in real time: tests register workflows
 * with Conductor directly and wait for executions through `db.execution`.
 */
const { db, sql, idle, open, all } = useAgents();

function fixture(
	plugins: FabrialHost["plugins"] = [],
	definitionScope: object = definitions,
	stateTimeoutMs?: number,
	stateConflicts?: number,
) {
	const tasks = db.tasks();
	const client = tasks.client;
	const chat = new FakeChat({ installationId: "w" });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		requestId: randomUUID(),
		origin: null,
		replyTo: {
			kind: "thread",
			provider: "slack-test",
			threadId: `slack-test:general:${randomUUID()}`,
		},
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const host = {
		definitionScope,
		plugins,
		workflows: [],
		get conductor() {
			return tasks.conductor;
		},
		queue: tasks.queue,
		now: () => tasks.now(),
		clients: () => ({ example: "client" }),
		chat: () => chat.connection(),
		logger: { warn() {}, error() {}, debug() {}, info() {} },
		directory: { isMember: async () => true },
		resolvePrincipal: async (identity: { subjectId: string }) => ({
			id: identity.subjectId,
			known: true,
			identities: [],
			name: identity.subjectId,
		}),
	} as unknown as FabrialHost;
	// The fake platform behind `slack-test`, the provider of the tests' threads.
	// Not a spread: the host reads `conductor` lazily, once the tasks started.
	connectChat(
		chat,
		Object.assign(Object.create(host) as FabrialHost, {
			plugins: [...host.plugins, { id: "slack-test" }],
		}),
	);
	const integration = connectAgents(host, {
		models,
		sql: sql(),
		settings: { retry: { enabled: false } },
		...(stateTimeoutMs ? { stateTimeoutMs } : {}),
		...(stateConflicts ? { stateConflicts } : {}),
	});
	return { client, tasks, chat, faux, integration, metadata, host };
}
async function execute(f: ReturnType<typeof fixture>, workflow: WorkflowTask) {
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.tasks.start();
	const id = await f.client.invoke(workflow.name, null, { metadata: f.metadata });
	return { id, result: await db.execution(id).settled() };
}
function runtimeRegister(
	f: ReturnType<typeof fixture>,
	workflow: WorkflowTask,
	others: WorkflowTask[] = [],
	events: TaskEvent[] = [],
) {
	f.tasks.register({
		workflows: [workflow, ...f.integration.workflows, ...others],
		events: [...events, ...(f.integration.events ?? [])],
	});
}

it("runs durable structured answers and memoizes submissions; each call has a Session of its own", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
	});
	let asked = "";
	f.faux.setResponses([
		(context) => {
			const content = context.messages.findLast(
				(message): message is UserMessage => message.role === "user",
			)!.content;
			asked =
				typeof content === "string"
					? content
					: content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
			return fauxAssistantMessage('{"answer":42}');
		},
		fauxAssistantMessage("continued"),
	]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			await ctx.step("answer:output", () => "user receipt");
			const first = await f.integration.run(ctx, "answer", agent, {
				input: "Question",
				output: z.object({ answer: z.number() }),
			});
			const again = await f.integration.run(ctx, "again", agent, {
				input: "Continue",
			});
			return { first, again };
		},
	};
	const { id, result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { first: { answer: 42 }, again: "continued" },
	});
	expect(f.faux.state.callCount).toBe(2);
	// The model is told the answer's shape: the output schema's JSON Schema.
	const shape = JSON.stringify({
		type: "object",
		properties: { answer: { type: "number" } },
		required: ["answer"],
	});
	expect(asked).toBe(
		`Question\n\nReturn your final answer as a single JSON value matching this JSON Schema, without Markdown fences or surrounding prose:\n${shape}`,
	);
	for (const call of ["answer", "again"]) {
		const commits =
			await sql()`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${callSession(id, call)} ORDER BY seq`;
		const conversations = commits
			.flatMap((row) => JSON.parse(String(row.writes)) as { type: string }[])
			.filter((write) => write.type === "conversation");
		expect(conversations).toHaveLength(1);
	}
	// Neither call wrote to the thread's Session.
	const [thread] =
		await sql()`SELECT 1 FROM fabrial_pi.sessions WHERE key = ${`slack-test:${(f.metadata.replyTo as { threadId: string }).threadId}`}`;
	expect(thread).toBeUndefined();
	await f.integration.stop();
});

it("binds adapter identity, clients, sections and durable posts; commits tool state to the call's Session with a result receipt", async () => {
	const f = fixture();
	const state = defineState({
		name: randomUUID(),
		scope: "session",
		schema: z.object({ notes: z.array(z.string()) }),
		initial: () => ({ notes: [] as string[] }),
		render: (value) => value.notes.join(","),
	});
	let actor: string | undefined;
	const tool = defineTool({
		name: "record_note",
		description: "Record a note",
		input: z.object({ text: z.string() }),
		idempotent: true,
		run: async ({ text }, ctx) => {
			actor = ctx.actor?.id;
			expect(ctx.clients).toEqual({ example: "client" });
			await ctx.state(state).update((draft) => {
				draft.notes.push(text);
			});
			await ctx.thread!.post("post", "Recorded");
			return "done";
		},
	});
	const extension = defineToolkit({
		name: randomUUID(),
		tools: [tool],
		instructions: (ctx) => `Requester ${ctx.actor?.name}`,
	});
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		toolkits: [extension],
		state: [state],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("record_note", { text: "hello" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("finished"),
	]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			await f.integration.state.update(ctx, "seed", state, (draft) => {
				draft.notes.push("seed");
			});
			const answer = await f.integration.run(ctx, "agent", agent, { input: "Do it" });
			return { answer, state: await f.integration.state.get(ctx, "read", state) };
		},
	};
	const { result } = await execute(f, workflow);
	// Thread state belongs to the thread's Session; the call's tools write the call Session's state.
	expect(result).toEqual({
		status: "completed",
		output: { answer: "finished", state: { notes: ["seed"] } },
	});
	expect(actor).toBe("alice");
	const recorded = await f.chat.thread(
		f.metadata.replyTo as import("../../internals.ts").ThreadRef,
	);
	expect(recorded.posts).toHaveLength(1);
	expect(recorded.statuses).toContain("Thinking…");
	expect(recorded.statuses.at(-1)).toBeNull();
	const rows = await sql()`SELECT writes FROM fabrial_pi.commits`;
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

it("suspends the Session while a workflow tool waits and resumes it with a stable child id", async () => {
	const f = fixture();
	f.metadata.triggerEvent = "slack.mentioned";
	f.metadata.ownerWorkflow = "router";
	const child = defineWorkflow({
		name: randomUUID(),
		input: z.object({ value: z.string() }),
		run: async () => ({}),
	});
	(f.host.workflows as unknown as (typeof child)[]).push(child);
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [child],
	});
	f.faux.setResponses([
		fauxAssistantMessage(
			fauxToolCall(child.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"), { value: "ok" }),
			{
				stopReason: "toolUse",
			},
		),
		fauxAssistantMessage("child finished"),
	]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Run child" }),
	};
	runtimeRegister(f, workflow, [
		{
			name: child.name,
			triggers: [],
			handler: async (_input, ctx) => {
				expect(metadataOf(ctx)).toMatchObject({
					...f.metadata,
					triggerEvent: null,
					ownerWorkflow: null,
				});
				const storage = await PostgresStorage.open(sql(), metadataOf(ctx).piSession as string);
				await storage.close(BACKGROUND_CONTEXT);
				return "ok";
			},
		},
	]);
	await f.integration.start();
	await f.tasks.start();
	const id = await f.client.invoke(workflow.name, null, { metadata: f.metadata });
	expect(await db.execution(id).settled()).toEqual({
		status: "completed",
		output: "child finished",
	});
	expect(await all(f.client.queue, child.name)).toHaveLength(1);
	await f.integration.stop();
});

it("rejects invalid structured answers", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
	});
	f.faux.setResponses([fauxAssistantMessage('{"answer":"not a number"}')]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 1 },
		handler: (_input, ctx) =>
			f.integration.run(ctx, "agent", agent, {
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

it("aborts live task kinds that are no longer installed at start and logs them", async () => {
	const session = randomUUID();
	const storage = await PostgresStorage.open(sql(), session);
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
	const f = fixture();
	const warn = vi.spyOn(f.host.logger, "warn");
	await f.integration.start();
	expect(warn).toHaveBeenCalledWith("Aborted orphaned agent work", {
		session,
		kind: "removed.extension",
		count: 1,
	});
	await f.integration.stop();
	const again = fixture();
	const warnAgain = vi.spyOn(again.host.logger, "warn");
	await again.integration.start();
	expect(warnAgain).not.toHaveBeenCalledWith(
		"Aborted orphaned agent work",
		expect.objectContaining({ session }),
	);
	await again.integration.stop();
});

it("starts when an unrelated commit holds a lone surrogate", async () => {
	// Storage persists JSON text: Pi strings may hold lone surrogates and NUL, which Postgres JSON types reject.
	const unrelated = await PostgresStorage.open(sql(), randomUUID());
	await unrelated.commit(
		[
			{
				type: "conversation",
				value: { id: 1 as never, title: "broken \ud800 \u0000 \\ud800 text" } as never,
			},
		],
		BACKGROUND_CONTEXT,
	);
	await unrelated.close(BACKGROUND_CONTEXT);
	const session = randomUUID();
	const storage = await PostgresStorage.open(sql(), session);
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
					input: { text: "\udc00" },
					background: false,
					abortRequested: false,
					state: { status: "pending", checkpoint: { phase: "run" } },
				},
			},
		],
		BACKGROUND_CONTEXT,
	);
	await storage.close(BACKGROUND_CONTEXT);
	const f = fixture();
	const warn = vi.spyOn(f.host.logger, "warn");
	await f.integration.start();
	expect(warn).toHaveBeenCalledWith("Aborted orphaned agent work", {
		session,
		kind: "removed.extension",
		count: 1,
	});
	await f.integration.stop();
});

it.each([[false], [true]])(
	"honors agent detached=%s and cancels workflow tools with the agent",
	async (detached) => {
		const f = fixture();
		const child = defineWorkflow({ name: randomUUID(), run: async () => null });
		// The approval the workflow tool waits for; the test sends it.
		const approved = { name: `approved-${randomUUID()}`, filterable: [] };
		(f.host.workflows as unknown as (typeof child)[]).push(child);
		const agent = defineAgent({
			name: randomUUID(),
			description: "Answers test questions.",
			model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
			tools: [child],
		});
		f.faux.setResponses([
			fauxAssistantMessage(fauxToolCall(child.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"), {}), {
				stopReason: "toolUse",
			}),
			fauxAssistantMessage("approved"),
		]);
		const workflow: WorkflowTask = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Wait", detached }),
		};
		runtimeRegister(
			f,
			workflow,
			[
				{
					name: child.name,
					triggers: [],
					handler: async (_input, ctx) => {
						await ctx.waitForEvent("approval", { event: { name: approved.name } } as never);
						return "approved";
					},
				},
			],
			[approved],
		);
		await f.integration.start();
		await f.tasks.start();
		const id = await f.client.invoke(workflow.name, null, { metadata: f.metadata });
		const tool = db.execution(
			await until(
				"the workflow tool to start",
				async () => (await all(f.client.queue, child.name))[0],
			),
		);
		await tool.suspended("approval");
		await f.client.cancel(id, "User stopped");
		// The call's Session execution is the caller's child: cancelled with it unless detached.
		expect((await db.execution(id).settled()).status).toBe("cancelled");
		if (detached) {
			// The run outlives its caller: the workflow tool finishes and the agent answers.
			// The Session's execution suspends on its tool with the lease free.
			await until("the Session to suspend while its tool waits", async () => {
				const [lease] =
					await sql()`SELECT owner FROM fabrial_pi.sessions WHERE key = ${callSession(id, "agent")}`;
				return lease && lease.owner === null ? true : undefined;
			});
			expect(await open(f.client.queue, "fabrial.session")).toHaveLength(1);
			expect(["pending", "running"]).toContain((await tool.info())?.status);
			await f.client.emit(approved.name, {}, { metadata: f.metadata });
			await until("the agent to answer", async () =>
				f.faux.state.callCount === 2 ? true : undefined,
			);
			// The detached call's Session execution still settles with the answer.
			const [call] = await all(f.client.queue, "fabrial.session");
			expect(await db.execution(call!).settled()).toEqual({
				status: "completed",
				output: { status: "completed", output: "approved" },
			});
		} else {
			expect((await tool.settled()).status).toBe("cancelled");
		}
		await idle(f.client.queue);
		if (!detached) expect(f.faux.state.callCount).toBe(1);
		const [lease] =
			await sql()`SELECT owner FROM fabrial_pi.sessions WHERE key = ${callSession(id, "agent")}`;
		expect(lease?.owner).toBeNull();
		await f.integration.stop();
	},
);

it("a timed-out agent call cancels its attached Session execution and the call's workflow tools", async () => {
	// A permanently failed caller cancels it the same way: the call is an attached child (cancelWithParent).
	const f = fixture();
	const child = defineWorkflow({ name: randomUUID(), run: async () => null });
	(f.host.workflows as unknown as (typeof child)[]).push(child);
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [child],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall(child.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"), {}), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("never"),
	]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 1 },
		handler: (_input, ctx) =>
			f.integration.run(ctx, "agent", agent, { input: "Wait", timeout: "3s" }),
	};
	runtimeRegister(
		f,
		workflow,
		[
			{
				name: child.name,
				triggers: [],
				handler: async (_input, ctx) => {
					await ctx.waitForEvent("approval", { event: { name: `never-${child.name}` } } as never);
					return "approved";
				},
			},
		],
		[{ name: `never-${child.name}`, filterable: [] }],
	);
	await f.integration.start();
	await f.tasks.start();
	const id = await f.client.invoke(workflow.name, null, { metadata: f.metadata });
	const tool = db.execution(
		await until(
			"the workflow tool to start",
			async () => (await all(f.client.queue, child.name))[0],
		),
	);
	await tool.suspended("approval");
	expect(await db.execution(id).settled()).toMatchObject({
		status: "failed",
		error: expect.stringMatching(/timed out/),
	});
	// The call's Session execution and its tool's are cancelled with the caller; the agent never resumes.
	const [call] = await all(f.client.queue, "fabrial.session");
	expect((await db.execution(call!).settled()).status).toBe("cancelled");
	expect((await tool.settled()).status).toBe("cancelled");
	await idle(f.client.queue);
	expect(f.faux.state.callCount).toBe(1);
	const [lease] =
		await sql()`SELECT owner FROM fabrial_pi.sessions WHERE key = ${callSession(id, "agent")}`;
	expect(lease?.owner).toBeNull();
	await f.integration.stop();
});

it("installs plugin observers globally but does not select unrequested plugin toolkits", async () => {
	let requests = 0;
	const f = fixture([
		{
			id: "unselected",
			toolkit: () => ({
				tools: [],
				instructions: () => {
					throw new Error("Unselected toolkit rendered");
				},
			}),
			hooks: {
				agent: {
					onModelResponse: () => {
						requests++;
					},
				},
			},
		},
	]);
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
	});
	f.faux.setResponses([fauxAssistantMessage("ok")]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Hello" }),
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(requests).toBe(1);
	await f.integration.stop();
});

it("observes each Session's agent on its own: two Sessions at once", async () => {
	const seen: { agent: string; text: string }[] = [];
	const f = fixture([
		{
			id: "records",
			hooks: { agent: { onModelResponse: ({ agent, text }) => void seen.push({ agent, text }) } },
		},
	]);
	const model = `${f.faux.provider.id}/${f.faux.getModel().id}`;
	const [a, b] = [randomUUID(), randomUUID()].map((name) =>
		defineAgent({ name, description: "Answers test questions.", model }),
	);
	// Each answer echoes the question its own Session asked, whichever Session asks first.
	const echo = (context: { messages: readonly Message[] }) => {
		const user = context.messages.findLast((message) => message.role === "user")!;
		return fauxAssistantMessage(
			`echo ${typeof user.content === "string" ? user.content : JSON.stringify(user.content)}`,
		);
	};
	f.faux.setResponses([echo, echo]);
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: async (input, ctx) => {
			const agent = input === "a" ? a! : b!;
			return f.integration.run(ctx, "agent", agent, {
				input: `from ${String(input as string)}`,
			});
		},
	};
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.tasks.start();
	const ids = await Promise.all(
		["a", "b"].map((input) => f.client.invoke(workflow.name, input, { metadata: f.metadata })),
	);
	const results = await Promise.all(ids.map((id) => db.execution(id).settled()));
	expect(results).toEqual([
		{ status: "completed", output: "echo from a" },
		{ status: "completed", output: "echo from b" },
	]);
	expect(new Set(ids.map((id) => callSession(id, "agent"))).size).toBe(2);
	expect(seen.toSorted((x, y) => x.text.localeCompare(y.text))).toEqual([
		{ agent: a!.name, text: "echo from a" },
		{ agent: b!.name, text: "echo from b" },
	]);
	await f.integration.stop();
});

/** The tool results the model sees on its next request. */
function toolResults(messages: readonly Message[]): ToolResultMessage[] {
	return messages.filter((message): message is ToolResultMessage => message.role === "toolResult");
}

it("notifies onToolCall after a tool settles and logs observers that throw", async () => {
	const calls: ToolCall[] = [];
	const f = fixture([
		{ id: "throws", hooks: { agent: { onToolCall: () => Promise.reject(new Error("boom")) } } },
		{ id: "records", hooks: { agent: { onToolCall: (call) => void calls.push(call) } } },
	]);
	const warn = vi.spyOn(f.host.logger, "warn");
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [
			defineTool({
				name: "lookup",
				description: "Look up a key",
				input: z.object({ key: z.string() }),
				run: async ({ key }) => `value of ${key}`,
			}),
		],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("lookup", { key: "a" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Look up a" }),
	});
	expect(result).toEqual({ status: "completed", output: "done" });
	expect(calls).toEqual([
		{
			agent: agent.name,
			callId: expect.any(String),
			name: "lookup",
			arguments: { key: "a" },
			result: "value of a",
			isError: false,
		},
	]);
	expect(warn).toHaveBeenCalledWith("Agent observer failed", { error: new Error("boom") });
	await f.integration.stop();
});

it("attributes observer calls to each Session's own agent while two Sessions interleave", async () => {
	const calls: ToolCall[] = [];
	const f = fixture([
		{ id: "records", hooks: { agent: { onToolCall: (call) => void calls.push(call) } } },
	]);
	const model = `${f.faux.provider.id}/${f.faux.getModel().id}`;
	let entered!: () => void;
	const inside = new Promise<void>((resolve) => (entered = resolve));
	let release!: () => void;
	const gate = new Promise<void>((resolve) => (release = resolve));
	const waiting = defineAgent({
		name: randomUUID(),
		description: "Waits.",
		model,
		tools: [
			defineTool({
				name: "wait",
				description: "Wait",
				input: z.object({}),
				run: async () => {
					entered();
					await gate;
					return "waited";
				},
			}),
		],
	});
	const other = defineAgent({
		name: randomUUID(),
		description: "Looks up.",
		model,
		tools: [
			defineTool({
				name: "lookup",
				description: "Look up a key",
				input: z.object({ key: z.string() }),
				run: async ({ key }) => `value of ${key}`,
			}),
		],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("wait", {}), { stopReason: "toolUse" }),
		fauxAssistantMessage(fauxToolCall("lookup", { key: "b" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("b done"),
		fauxAssistantMessage("a done"),
	]);
	const run = (agent: typeof waiting, input: string): WorkflowTask => ({
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input }),
	});
	const [a, b] = [run(waiting, "Wait"), run(other, "Look up b")];
	runtimeRegister(f, a, [b]);
	await f.integration.start();
	await f.tasks.start();
	const first = await f.client.invoke(a.name, null, { metadata: f.metadata });
	await inside;
	// Each Session numbers its conversations from the start, so both leads share a conversation id.
	const second = await f.client.invoke(b.name, null, { metadata: f.metadata });
	expect(await db.execution(second).settled()).toEqual({ status: "completed", output: "b done" });
	release();
	expect(await db.execution(first).settled()).toEqual({ status: "completed", output: "a done" });
	expect(calls.map((call) => [call.name, call.agent])).toEqual([
		["lookup", other.name],
		["wait", waiting.name],
	]);
	await f.integration.stop();
});

it("returns input that fails the tool schema to the model without running the tool", async () => {
	const f = fixture();
	let runs = 0;
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [
			defineTool({
				name: "deploy",
				description: "Deploy a version",
				// The JSON Schema the model sees cannot express the refinement; only the Standard Schema checks it.
				input: z.object({
					version: z.string().refine((value) => /^v\d+$/.test(value), "Expected vN"),
				}),
				run: async () => {
					runs++;
					return "deployed";
				},
			}),
		],
	});
	let seen: ReturnType<typeof toolResults> = [];
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("deploy", { version: "latest" }), { stopReason: "toolUse" }),
		(context) => {
			seen = toolResults(context.messages);
			return fauxAssistantMessage("fixed nothing");
		},
	]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Deploy" }),
	});
	expect(result).toEqual({ status: "completed", output: "fixed nothing" });
	expect(runs).toBe(0);
	expect(seen).toMatchObject([
		{
			toolName: "deploy",
			isError: true,
			content: [{ type: "text", text: "Invalid input for deploy: Expected vN" }],
		},
	]);
	await f.integration.stop();
});

it("sends a non-string tool result to the model as serialized JSON", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [
			defineTool({
				name: "query",
				description: "Run a query",
				input: z.object({ sql: z.string() }),
				run: async () => ({ rows: [{ id: 1, name: "a" }], truncated: false }),
			}),
		],
	});
	let seen: ReturnType<typeof toolResults> = [];
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("query", { sql: "select 1" }), { stopReason: "toolUse" }),
		(context) => {
			seen = toolResults(context.messages);
			return fauxAssistantMessage("one row");
		},
	]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Query" }),
	});
	expect(result).toEqual({ status: "completed", output: "one row" });
	expect(seen).toMatchObject([
		{
			toolName: "query",
			isError: false,
			content: [{ type: "text", text: '{"rows":[{"id":1,"name":"a"}],"truncated":false}' }],
		},
	]);
	await f.integration.stop();
});

it("workflow state needs a posted thread; an unposted channel locator is not a Session key", async () => {
	const f = fixture();
	f.metadata.replyTo = { kind: "channel", provider: "slack-test", channelId: "C1" };
	const state = defineState({
		name: randomUUID(),
		scope: "session",
		schema: z.object({ value: z.string() }),
		initial: () => ({ value: "initial" }),
	});
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			await expect(f.integration.state.get(ctx, "state", state)).rejects.toThrow("ctx.thread.post");
			return "ok";
		},
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(await all(f.client.queue, "fabrial.session")).toHaveLength(0);
	expect(f.chat.threads.size).toBe(0);
	await f.integration.stop();
});

const counter = () =>
	defineState({
		name: randomUUID(),
		scope: "session",
		schema: z.object({ count: z.number().max(100) }),
		initial: () => ({ count: 0 }),
	});

it("workflow state resolves by name on this instance: an unknown definition fails clearly", async () => {
	const f = fixture();
	const stranger = createFabrial({ plugins: [] }).defineState({
		name: randomUUID(),
		scope: "session",
		schema: z.object({ count: z.number() }),
		initial: () => ({ count: 0 }),
	});
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			await expect(f.integration.state.get(ctx, "read", stranger)).rejects.toThrow(
				"is not defined on this Fabrial instance",
			);
			return "ok";
		},
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(await all(f.client.queue, "fabrial.session")).toHaveLength(0);
	await f.integration.stop();
});

it("a workflow update that fails its schema fails the workflow and changes nothing", async () => {
	const f = fixture();
	const state = counter();
	const invalid: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 1 },
		handler: async (_input, ctx) => {
			await f.integration.state.update(ctx, "seed", state, (draft) => {
				draft.count = 5;
			});
			return f.integration.state.update(ctx, "invalid", state, (draft) => {
				draft.count = 1_000;
			});
		},
	};
	const reader: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.state.get(ctx, "read", state),
	};
	runtimeRegister(f, invalid, [reader]);
	await f.integration.start();
	await f.tasks.start();
	const failed = await f.client.invoke(invalid.name, null, { metadata: f.metadata });
	expect(await db.execution(failed).settled()).toMatchObject({
		status: "failed",
		error: expect.stringContaining(`state ${state.name}`),
	});
	const read = await f.client.invoke(reader.name, null, { metadata: f.metadata });
	expect(await db.execution(read).settled()).toEqual({ status: "completed", output: { count: 5 } });
	await f.integration.stop();
});

it("a lost state response is asked for again with the same operation: the update applies once", async () => {
	const f = fixture([], definitions, 1_000);
	const state = counter();
	const change = vi.fn((draft: { count: number }) => {
		draft.count++;
	});
	// The Session's first write response never arrives.
	const session = f.integration.workflows.find((workflow) => workflow.name === "fabrial.session")!;
	const handler = session.handler;
	let dropped = 0;
	session.handler = (input, ctx) =>
		handler(
			input,
			intercept(ctx, {
				emit: async (emit, event: string, payload: { operation?: string }, options: unknown) => {
					if (payload.operation?.includes(":write") && !dropped++) return;
					return emit(event, payload, options);
				},
			}),
		);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			const updated = await f.integration.state.update(ctx, "increment", state, change);
			return { updated, read: await f.integration.state.get(ctx, "read", state) };
		},
	});
	expect(dropped).toBe(2);
	expect(result).toEqual({
		status: "completed",
		output: { updated: { count: 1 }, read: { count: 1 } },
	});
	expect(change).toHaveBeenCalledTimes(1);
	await f.integration.stop();
});

it("a workflow restarted after sending its update waits for the same answer; nothing applies twice", async () => {
	const f = fixture();
	const state = counter();
	const change = vi.fn((draft: { count: number }) => {
		draft.count++;
	});
	let crashed = false;
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 3 },
		handler: async (_input, ctx) => {
			const crashing = intercept(ctx, {
				signalWithStart: async (signalWithStart, id: string, options: unknown) => {
					const sent = await signalWithStart(id, options);
					if (id.endsWith(":write:request") && !crashed) {
						crashed = true;
						throw new Error("crashed after sending");
					}
					return sent;
				},
			});
			const updated = await f.integration.state.update(crashing, "increment", state, change);
			return { updated, read: await f.integration.state.get(ctx, "read", state) };
		},
	};
	const { result } = await execute(f, workflow);
	expect(crashed).toBe(true);
	expect(result).toEqual({
		status: "completed",
		output: { updated: { count: 1 }, read: { count: 1 } },
	});
	expect(change).toHaveBeenCalledTimes(1);
	await f.integration.stop();
});

/**
 * `ctx` with some of Conductor's methods replaced where Fabrial's internals call them (its prototype); each replacement
 * receives the original first.
 */
function intercept<T extends object>(
	ctx: T,
	methods: Record<string, (original: (...args: any[]) => Promise<any>, ...args: any[]) => unknown>,
): T {
	const proto = Object.getPrototypeOf(ctx) as Record<
		string,
		(...args: unknown[]) => Promise<unknown>
	>;
	const replaced = Object.create(proto) as Record<string, unknown>;
	for (const [name, method] of Object.entries(methods))
		replaced[name] = (...args: unknown[]) => method(proto[name]!.bind(ctx), ...args);
	return new Proxy(ctx, {
		getPrototypeOf: () => replaced,
		get: (target, property) => {
			const value = Reflect.get(target, property) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
}

/** `ctx` with `before` run ahead of each state write request it sends; `before` gets the write's number. */
function beforeWrites<T extends object>(ctx: T, before: (n: number) => Promise<void>): T {
	let writes = 0;
	return intercept(ctx, {
		signalWithStart: async (signalWithStart, id: string, options: unknown) => {
			if (id.endsWith(":write:request")) await before(writes++);
			return signalWithStart(id, options);
		},
	});
}

it("a tool that changes the state between a workflow's read and write makes the workflow read again", async () => {
	const f = fixture();
	const state = counter();
	const bumped = Promise.withResolvers<void>();
	const bump = defineTool({
		name: "bump",
		description: "Bumps the counter",
		input: z.object({}),
		run: async (_input, ctx) => {
			await ctx.state(state).update((draft) => {
				draft.count += 10;
			});
			bumped.resolve();
			return "bumped";
		},
	});
	const agent = defineAgent({
		name: randomUUID(),
		description: "Bumps the counter.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [bump],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("bump", {}), { stopReason: "toolUse" }),
		fauxAssistantMessage("Bumped."),
	]);
	const seen: number[] = [];
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			// The first write waits until the agent's tool changed the state it read.
			const forced = beforeWrites(ctx, async (n) => {
				if (n) return;
				await f.integration.deliver(agent, {
					session: sessionKey(f.metadata),
					requestId: randomUUID(),
					content: "Bump it",
					delivery: "steer",
					metadata: f.metadata,
					follow: null,
				});
				await bumped.promise;
			});
			const updated = await f.integration.state.update(forced, "increment", state, (draft) => {
				seen.push(draft.count);
				draft.count++;
			});
			return { updated, read: await f.integration.state.get(ctx, "read", state) };
		},
	});
	expect(result).toEqual({
		status: "completed",
		output: { updated: { count: 11 }, read: { count: 11 } },
	});
	expect(seen).toEqual([0, 10]);
	await f.integration.stop();
});

it("an update that keeps conflicting fails clearly after its conflicts run out", async () => {
	const f = fixture([], definitions, undefined, 3);
	const state = counter();
	const change = vi.fn((draft: { count: number }) => {
		draft.count++;
	});
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 1 },
		handler: async (_input, ctx) => {
			// Another update lands between every read and write.
			const contended = beforeWrites(ctx, async (n) => {
				await f.integration.state.update(ctx, `other:${n}`, state, (draft) => {
					draft.count++;
				});
			});
			return f.integration.state.update(contended, "increment", state, change);
		},
	});
	expect(result).toMatchObject({
		status: "failed",
		error: expect.stringContaining("conflicted 3 times"),
	});
	expect(change).toHaveBeenCalledTimes(3);
	await f.integration.stop();
});

it("a duplicated state write gets the same answer again and applies once", async () => {
	const f = fixture();
	const state = counter();
	// The Session's responses to writes, as it emits them.
	const answers: unknown[] = [];
	const session = f.integration.workflows.find((workflow) => workflow.name === "fabrial.session")!;
	const handler = session.handler;
	session.handler = (input, ctx) =>
		handler(
			input,
			intercept(ctx, {
				emit: async (emit, event: string, payload: { operation?: string }, options: unknown) => {
					if (payload.operation?.includes(":write")) answers.push(payload);
					return emit(event, payload, options);
				},
			}),
		);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			// Every write request is delivered twice, under two signal ids: the second arrives once the first applied.
			const duplicating = intercept(ctx, {
				signalWithStart: async (
					signalWithStart,
					id: string,
					options: { signal: { id: string } },
				) => {
					const sent = await signalWithStart(id, options);
					if (id.endsWith(":write:request"))
						await signalWithStart(`${id}:again`, {
							...options,
							signal: { ...options.signal, id: `${options.signal.id}:again` },
						});
					return sent;
				},
			});
			const updated = await f.integration.state.update(duplicating, "increment", state, (draft) => {
				draft.count++;
			});
			await until("both answers", async () => (answers.length === 2 ? true : undefined));
			return { updated, read: await f.integration.state.get(ctx, "read", state) };
		},
	});
	expect(result).toEqual({
		status: "completed",
		output: { updated: { count: 1 }, read: { count: 1 } },
	});
	expect(answers).toHaveLength(2);
	expect(answers[1]).toEqual(answers[0]);
	expect(answers[0]).toMatchObject({ value: { count: 1 }, revision: 1 });
	await f.integration.stop();
});

it("concurrent workflow updates of one Session's state lose nothing: a stale write reads again", async () => {
	const f = fixture();
	const state = counter();
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, ctx) => {
			for (let n = 0; n < 4; n++)
				await f.integration.state.update(ctx, `increment:${n}`, state, (draft) => {
					draft.count++;
				});
			return null;
		},
	};
	const reader: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.state.get(ctx, "read", state),
	};
	runtimeRegister(f, workflow, [reader]);
	await f.integration.start();
	await f.tasks.start();
	const ids = await Promise.all(
		[0, 1, 2].map(() => f.client.invoke(workflow.name, null, { metadata: f.metadata })),
	);
	for (const id of ids)
		expect(await db.execution(id).settled()).toEqual({ status: "completed", output: null });
	const read = await f.client.invoke(reader.name, null, { metadata: f.metadata });
	expect(await db.execution(read).settled()).toEqual({
		status: "completed",
		output: { count: 12 },
	});
	await f.integration.stop();
});

it("stop joins driver routing cleanup after the harness has closed", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
	});
	f.faux.setResponses([fauxAssistantMessage("done")]);
	const connection = f.chat.connection();
	let reached!: () => void;
	const cleaning = new Promise<void>((resolve) => {
		reached = resolve;
	});
	let release!: () => void;
	const cleanup = new Promise<void>((resolve) => {
		release = resolve;
	});
	const setStatus = connection.setStatus.bind(connection);
	vi.spyOn(connection, "setStatus").mockImplementation(async (thread, status) => {
		if (status === null) {
			reached();
			await cleanup;
		}
		return setStatus(thread, status);
	});
	const workflow: WorkflowTask = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, ctx) => f.integration.run(ctx, "agent", agent, { input: "Answer" }),
	};
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.tasks.start();
	await f.client.invoke(workflow.name, null, { metadata: f.metadata });
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
		await f.integration.stop();
	}
});

it("does not install agents from an unrelated Fabrial catalog", async () => {
	const unrelated = createFabrial({
		plugins: [{ id: "unrelated", toolkit: () => ({ tools: [] }) }],
	});
	unrelated.defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		toolkits: ["unrelated"],
	});
	const local = createFabrial({ plugins: [] });
	const f = fixture([], local);
	// Installing the unrelated agent would fail: this host has no unrelated plugin toolkit.
	await f.integration.start();
	await f.integration.stop();
});
