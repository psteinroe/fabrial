import { randomUUID } from "node:crypto";
import type { Message, ToolResultMessage } from "@earendil-works/pi-ai";
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
	GenerationTask,
} from "@earendil-works/pi-durable";
import {
	createFabrial,
	type DurableExecution,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeEvent,
	type RuntimeWorkflow,
	type ToolCall,
} from "../../../src/index.ts";
import { FakeChat } from "../../../src/testing.ts";
import { expect, it, vi } from "vitest";
import { ChildIntents, Runs } from "../../../src/agents/documents.ts";
import { z } from "zod";
import { callSession, pi, PostgresStorage } from "../../../src/agents/index.ts";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { until } from "../../conductor/helper.ts";
import { useAgents } from "./setup.ts";
import { AGENT_SETTLED_EVENT } from "../../../src/runtime.ts";

/** Events core registers for its own waits; these fixtures register with the runtime directly. */
const internalEvents: RuntimeEvent[] = [{ name: AGENT_SETTLED_EVENT, filterable: ["requestId"] }];
const definitions = createFabrial({ plugins: [] });
const { defineWorkflow } = definitions;
const { defineAgent, defineTool, defineToolkit, defineState } = definitions;

/**
 * The Pi integration connected to a bare host on real Conductor, in real time: tests register workflows
 * with the runtime directly and wait for executions through `db.execution`.
 */
const { db, sql, idle, open, all } = useAgents();

function fixture(plugins: FabrialHost["plugins"] = [], definitionScope: object = definitions) {
	const runtime = db.runtime();
	const chat = new FakeChat();
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		interactionId: randomUUID(),
		origin: null,
		replyTo: { kind: "thread", provider: "slack-test", threadId: randomUUID() },
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const host = {
		definitionScope,
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
	const integration = pi({ models, sql: sql(), settings: { retry: { enabled: false } } }).connect(
		host,
	);
	return { runtime, chat, faux, integration, metadata, host };
}
async function execute(f: ReturnType<typeof fixture>, workflow: RuntimeWorkflow) {
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	return { id, result: await db.execution(id).settled() };
}
function runtimeRegister(
	f: ReturnType<typeof fixture>,
	workflow: RuntimeWorkflow,
	others: RuntimeWorkflow[] = [],
	events: RuntimeEvent[] = [],
) {
	f.runtime.register({
		workflows: hooked(f.integration, [workflow, ...f.integration.workflows, ...others]),
		events: [...events, ...internalEvents],
	});
}

it("runs durable structured answers and memoizes submissions; each call has a Session of its own", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
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
	const { id, result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { first: { answer: 42 }, again: "continued" },
	});
	expect(f.faux.state.callCount).toBe(2);
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
		scope: "thread",
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
	// Thread state belongs to the thread's Session; the call's tools write the call Session's state.
	expect(result).toEqual({
		status: "completed",
		output: { answer: "finished", state: { notes: ["seed"] } },
	});
	expect(actor).toBe("alice");
	const io = await f.chat.resolve(f.metadata.replyTo!);
	const recorded = await f.chat.thread(io.ref);
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
					triggerEvent: null,
					ownerWorkflow: null,
				});
				const storage = await PostgresStorage.open(sql(), execution.metadata.piChild as string);
				await storage.close(BACKGROUND_CONTEXT);
				return "ok";
			},
		},
	]);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	expect(await db.execution(id).settled()).toEqual({
		status: "completed",
		output: "child finished",
	});
	expect(await all(f.runtime.queue, child.name)).toHaveLength(1);
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
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		retries: { maxAttempts: 1 },
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
		const workflow: RuntimeWorkflow = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, execution) =>
				f.integration.agents.run(execution, "agent", agent, { input: "Wait", detached }),
		};
		runtimeRegister(
			f,
			workflow,
			[
				{
					name: child.name,
					triggers: [],
					handler: async (_input, execution) => {
						await execution.waitForEvent("approval", { event: approved.name });
						return "approved";
					},
				},
			],
			[approved],
		);
		await f.integration.start();
		await f.runtime.start();
		const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
		const tool = db.execution(
			await until(
				"the workflow tool to start",
				async () => (await all(f.runtime.queue, child.name))[0],
			),
		);
		await tool.suspended("approval");
		await f.runtime.cancel(id, "User stopped");
		// The cancelled caller signals `cancel` to the Session its call reached.
		expect((await db.execution(id).settled()).status).toBe("cancelled");
		if (detached) {
			// The run outlives its caller: the workflow tool finishes and the agent answers.
			await until("the Session to release while its tool waits", async () =>
				(await open(f.runtime.queue, "fabrial.session")).length ? undefined : true,
			);
			expect(["pending", "running"]).toContain((await tool.info())?.status);
			await f.runtime.emit(approved.name, {}, { metadata: f.metadata });
			await until("the agent to answer", async () =>
				f.faux.state.callCount === 2 ? true : undefined,
			);
		} else {
			expect((await tool.settled()).status).toBe("cancelled");
		}
		await idle(f.runtime.queue);
		if (!detached) expect(f.faux.state.callCount).toBe(1);
		const [lease] =
			await sql()`SELECT owner FROM fabrial_pi.sessions WHERE key = ${callSession(id, "agent")}`;
		expect(lease?.owner).toBeNull();
		await f.integration.stop();
	},
);

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
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Hello" }),
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(requests).toBe(1);
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
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Look up a" }),
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
	const run = (agent: typeof waiting, input: string): RuntimeWorkflow => ({
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) => f.integration.agents.run(execution, "agent", agent, { input }),
	});
	const [a, b] = [run(waiting, "Wait"), run(other, "Look up b")];
	runtimeRegister(f, a, [b]);
	await f.integration.start();
	await f.runtime.start();
	const first = await f.runtime.invoke(a.name, null, { metadata: f.metadata });
	await inside;
	// Each Session numbers its conversations from the start, so both leads share a conversation id.
	const second = await f.runtime.invoke(b.name, null, { metadata: f.metadata });
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
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Deploy" }),
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
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Query" }),
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
			return "ok";
		},
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(await all(f.runtime.queue, "fabrial.session")).toHaveLength(0);
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
	// Runs until the Session's cancellation reaches it.
	const childWorkflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			new Promise((_resolve, reject) =>
				execution.signal.addEventListener("abort", () => reject(execution.signal.reason), {
					once: true,
				}),
			),
	};
	f.runtime.register({
		workflows: hooked(f.integration, [childWorkflow, ...f.integration.workflows]),
		events: internalEvents,
	});
	await f.integration.start();
	await f.runtime.start();
	const key = "fabrial:pi:lost-start";
	const dedupeKey = JSON.stringify([callSession("old", "agent"), key]);
	const child = await f.runtime.invoke(childWorkflow.name, null, {
		metadata: f.metadata,
		dedupeKey,
	});
	const storage = await PostgresStorage.open(sql(), callSession("old", "agent"));
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
			call: { requestId: oldRequest, caller: "old", detached: false },
			intents: [key],
			requests: { [oldRequest]: { content: "old", submissionId: old.id } },
		};
		return { task, submission: submission.id, old: old.id };
	}, BACKGROUND_CONTEXT);
	await harness.close(BACKGROUND_CONTEXT);
	// The caller "old" settled cancelled: the Session receives a `cancel` signal for its call.
	await f.integration.agents.settled!("old", f.metadata, {
		status: "cancelled",
		reason: "late cancellation",
	});
	expect(await db.execution(child).settled()).toMatchObject({ status: "cancelled" });
	await idle(f.runtime.queue);
	expect(await all(f.runtime.queue, childWorkflow.name)).toHaveLength(1);
	const reopened = await PostgresStorage.open(sql(), callSession("old", "agent"));
	const session = createSession(reopened);
	try {
		// The Session went on to run the newer request (it has no model here), but never aborted it.
		expect((await reopened.task(current.task, BACKGROUND_CONTEXT))?.abortRequested).toBe(false);
		expect(await reopened.submission(current.submission, BACKGROUND_CONTEXT)).not.toMatchObject({
			reason: expect.stringMatching(/abort/i),
		});
		expect((await reopened.submission(current.old, BACKGROUND_CONTEXT))?.status).toBe("unanswered");
		expect(await session.snapshot(Runs, BACKGROUND_CONTEXT)).toMatchObject({
			runs: { old: { done: true } },
		});
	} finally {
		await session.close(BACKGROUND_CONTEXT);
	}
});

it("stop joins driver routing cleanup after the harness has closed", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
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
	const setStatus = io.setStatus.bind(io);
	vi.spyOn(io, "setStatus").mockImplementation(async (status) => {
		if (status === null) {
			reached();
			await cleanup;
		}
		return setStatus(status);
	});
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

/** Core calls the agent port on every settlement; these fixtures register workflows with the runtime directly. */
function hooked(integration: { agents: { settled?: AgentSettled } }, workflows: RuntimeWorkflow[]) {
	return workflows.map((workflow) => ({
		...workflow,
		onSettled: async (...args: Parameters<AgentSettled>) => {
			await workflow.onSettled?.(...args);
			await integration.agents.settled?.(...args);
		},
	}));
}
type AgentSettled = NonNullable<import("../../../src/runtime.ts").AgentPort["settled"]>;
