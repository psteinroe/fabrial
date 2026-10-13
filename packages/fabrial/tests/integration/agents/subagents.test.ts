import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { Context as TranscriptContext } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
	type FauxResponseStep,
} from "@earendil-works/pi-ai/providers/faux";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	type Agent,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeWorkflow,
} from "../../../src/index.ts";
import { toolName } from "../../../src/agent.ts";
import { pi, PostgresStorage, sessionKey } from "../../../src/agents/index.ts";
import { FakeChat, MemoryRuntime } from "../../../src/testing.ts";

const definitions = createFabrial({ plugins: [] });
const { defineAgent, defineTool, defineWorkflow } = definitions;

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

function fixture() {
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
	const logger = { warn: vi.fn(), error: vi.fn(), debug() {}, info() {} };
	const host = {
		definitionScope: definitions,
		plugins: [],
		workflows: [],
		runtime,
		clients: () => ({}),
		chat: () => chat,
		logger,
		directory: { isMember: async () => true },
		resolvePrincipal: async () => null,
	} as unknown as FabrialHost;
	const integration = pi({ models, sql, settings: { retry: { enabled: false } } }).connect(host);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	/** Scripted responses that record the context each request saw. */
	const contexts: TranscriptContext[] = [];
	const respond = (steps: FauxResponseStep[]) =>
		faux.setResponses(
			steps.map((step) => (context, ...rest) => {
				contexts.push(structuredClone(context));
				return typeof step === "function" ? step(context, ...rest) : step;
			}),
		);
	/** Runs `agent` from a workflow call with `input`; the runtime settles everything it started. */
	const call = async (agent: Agent, input: string, others: RuntimeWorkflow[] = []) => {
		const workflow: RuntimeWorkflow = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, execution) => integration.agents.run(execution, "agent", agent, { input }),
		};
		runtime.register({ workflows: [workflow, ...integration.workflows, ...others], events: [] });
		await integration.start();
		await runtime.start();
		const id = await runtime.invoke(workflow.name, null, { metadata });
		return { id, settled: runtime.flush().then(() => runtime.result(id)) };
	};
	const writes = async () =>
		(
			await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${sessionKey(metadata)} ORDER BY seq`
		).flatMap((row) => JSON.parse(String(row.writes)) as StorageWrite[]);
	return {
		runtime,
		chat,
		faux,
		integration,
		host,
		metadata,
		logger,
		model,
		contexts,
		respond,
		call,
		writes,
	};
}
type Fixture = ReturnType<typeof fixture>;

const short = () => randomUUID().slice(0, 8);
/** Pi sends the prompt and tools as positional system messages, only what changed. */
type System = {
	role: "system";
	sections?: Record<string, string>;
	toolsAdded?: { name: string; description: string; parameters: { properties: object } }[];
	toolsRemoved?: (string | { name: string })[];
};
const systems = (context: TranscriptContext) =>
	(context.messages as unknown as System[]).filter((message) => message.role === "system");
const toolsOf = (context: TranscriptContext) => {
	const tools = new Map<string, NonNullable<System["toolsAdded"]>[number]>();
	for (const message of systems(context)) {
		for (const tool of message.toolsAdded ?? []) tools.set(tool.name, tool);
		for (const tool of message.toolsRemoved ?? [])
			tools.delete(typeof tool === "string" ? tool : tool.name);
	}
	return [...tools.values()];
};
const toolNames = (context: TranscriptContext) => toolsOf(context).map((tool) => tool.name);
const prompt = (context: TranscriptContext) =>
	JSON.stringify(Object.assign({}, ...systems(context).map((message) => message.sections ?? {})));
const toolResults = (context: TranscriptContext) =>
	context.messages.flatMap((message) =>
		message.role === "toolResult"
			? [message.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("")]
			: [],
	);
const userTexts = (context: TranscriptContext) =>
	context.messages.flatMap((message) =>
		message.role === "user"
			? [typeof message.content === "string" ? message.content : JSON.stringify(message.content)]
			: [],
	);

function researcherOf(f: Fixture, tools: Parameters<typeof defineAgent>[0]["tools"] = []) {
	return defineAgent({
		name: `researcher-${short()}`,
		description: "Investigates a question and returns findings with sources.",
		model: f.model,
		instructions: "You research.",
		tools,
	});
}

it("a fresh foreground child answers its task and the lead uses the escaped result", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	f.respond([
		fauxAssistantMessage(fauxToolCall(tool, { task: "Find the root cause" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("It is <b>DNS</b> & </subagent>"),
		fauxAssistantMessage("The researcher says it is DNS."),
	]);
	const { settled } = await f.call(lead, "Why is the site down?");
	expect(await settled).toEqual({ status: "completed", output: "The researcher says it is DNS." });
	const [first, child, last] = f.contexts;
	const offered = toolsOf(first!).find((each) => each.name === tool)!;
	expect(offered.description).toBe(researcher.definition.description);
	expect(Object.keys(offered.parameters.properties)).toEqual(["task", "name", "background"]);
	// The child sees only its task, with its own instructions and tools: no reply, no subagents.
	expect(userTexts(child!)).toEqual(["Find the root cause"]);
	expect(prompt(child!)).toContain("You research.");
	expect(toolNames(child!)).toEqual([]);
	expect(toolResults(last!)).toEqual([
		`<subagent name="${tool}-1" status="done">It is &lt;b&gt;DNS&lt;/b&gt; &amp; &lt;/subagent&gt;</subagent>`,
	]);
	// Owned by the lead's tool call, in the lead's Session.
	const conversations = (await f.writes()).filter((write) => write.type === "conversation");
	expect(conversations).toHaveLength(2);
	expect(conversations[1]!.value).toMatchObject({ owner: { taskId: expect.any(Number) } });
	// Child activity shows on the lead's progress status.
	const statuses = (await f.chat.thread(f.metadata.replyTo as Parameters<typeof f.chat.thread>[0]))
		.statuses;
	expect(statuses).toContain(`${researcher.name}: Thinking…`);
	await f.integration.stop();
});

it("a forked self copy sees the lead's transcript and gets no subagent tools at depth 1", async () => {
	const f = fixture();
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		instructions: "You lead.",
		subagents: [{ agent: "self", context: "fork", background: "never" }],
	});
	const tool = toolName(lead.name);
	f.respond([
		fauxAssistantMessage(fauxToolCall(tool, { task: "What was the code word?" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("pineapple"),
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "The code word is pineapple.");
	expect(await settled).toMatchObject({ status: "completed", output: "done" });
	const [first, child, last] = f.contexts;
	expect(
		Object.keys(toolsOf(first!).find((each) => each.name === tool)!.parameters.properties),
	).toEqual(["task", "name"]);
	expect(userTexts(child!)).toEqual(["The code word is pineapple.", "What was the code word?"]);
	// The fork ends before the response that delegated, so it holds no dangling call.
	expect(child!.messages.filter((message) => message.role === "assistant")).toEqual([]);
	expect(prompt(child!)).toContain("You lead.");
	expect(toolNames(child!)).toEqual([]);
	expect(toolResults(last!)[0]).toBe(
		`<subagent name="${tool}-1" status="done">pineapple</subagent>`,
	);
	await f.integration.stop();
});

it("depth 2 passes one level down: a self copy gets the subagent tool, its child does not", async () => {
	const f = fixture();
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Delegates recursively.",
		model: f.model,
		subagents: ["self"],
		limits: { depth: 2 },
	});
	const tool = toolName(lead.name);
	f.respond([
		fauxAssistantMessage(fauxToolCall(tool, { task: "level one" }), { stopReason: "toolUse" }),
		fauxAssistantMessage(fauxToolCall(tool, { task: "level two" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("deepest"),
		fauxAssistantMessage("middle"),
		fauxAssistantMessage("top"),
	]);
	const { settled } = await f.call(lead, "Go deep");
	expect(await settled).toMatchObject({ status: "completed", output: "top" });
	expect(f.contexts.map(toolNames)).toEqual([[tool], [tool], [], [tool], [tool]]);
	expect(toolResults(f.contexts[3]!)).toEqual([
		`<subagent name="${tool}-1" status="done">deepest</subagent>`,
	]);
	await f.integration.stop();
});

it("continuing a named child keeps its history; unknown names and background calls are tool errors", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	f.respond([
		fauxAssistantMessage(fauxToolCall(tool, { task: "Remember 7" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("Remembered."),
		fauxAssistantMessage(
			[
				fauxToolCall(tool, { task: "What number?", name: `${tool}-1` }),
				fauxToolCall(tool, { task: "x", name: "nobody" }),
				fauxToolCall(tool, { task: "x", background: true }),
			],
			{ stopReason: "toolUse" },
		),
		fauxAssistantMessage("7"),
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "Go");
	await settled;
	expect(await settled).toMatchObject({ status: "completed", output: "done" });
	const continued = f.contexts[3]!;
	expect(userTexts(continued)).toEqual(["Remember 7", "What number?"]);
	expect(toolResults(f.contexts[4]!)).toEqual([
		`<subagent name="${tool}-1" status="done">Remembered.</subagent>`,
		`<subagent name="${tool}-1" status="done">7</subagent>`,
		`There is no ${tool} child called nobody. Call ${tool} without a name to start one.`,
		"background subagents are not available yet",
	]);
	await f.integration.stop();
});

it("limits.concurrent turns away calls beyond the limit with an error the model can act on", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
		limits: { concurrent: 1 },
	});
	const tool = toolName(researcher.name);
	f.respond([
		fauxAssistantMessage(
			[fauxToolCall(tool, { task: "one" }), fauxToolCall(tool, { task: "two" })],
			{ stopReason: "toolUse" },
		),
		fauxAssistantMessage("answer"),
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "Go");
	expect(await settled).toMatchObject({ status: "completed", output: "done" });
	const results = toolResults(f.contexts[2]!);
	expect(results).toHaveLength(2);
	expect(results.filter((text) => text.includes('status="done">answer'))).toHaveLength(1);
	expect(results.filter((text) => text.includes("the limit for this conversation"))).toHaveLength(
		1,
	);
	await f.integration.stop();
});

it("aborting the lead aborts the child through Pi ownership", async () => {
	const f = fixture();
	let started!: () => void;
	const blocking = new Promise<void>((resolve) => (started = resolve));
	let aborted = false;
	const slow = defineTool({
		name: "slow",
		description: "Slow work",
		input: z.object({}),
		run: (_input, ctx) => {
			started();
			return new Promise<never>((_resolve, reject) =>
				ctx.signal.addEventListener(
					"abort",
					() => {
						aborted = true;
						reject(ctx.signal.reason);
					},
					{ once: true },
				),
			);
		},
	});
	const researcher = researcherOf(f, [slow]);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
	});
	f.respond([
		fauxAssistantMessage(fauxToolCall(toolName(researcher.name), { task: "Work slowly" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage(fauxToolCall("slow", {}), { stopReason: "toolUse" }),
	]);
	const { id, settled } = await f.call(lead, "Go");
	await blocking;
	await f.runtime.cancel(id, "Stopped");
	expect(await settled).toMatchObject({ status: "cancelled" });
	expect(aborted).toBe(true);
	// Cancelling marks the lead's run for abort; Pi's cascade marks the child's tasks through ownership.
	const live = [
		...new Map(
			(await f.writes()).flatMap((write) =>
				write.type === "task" ? [[write.value.id, write.value] as const] : [],
			),
		).values(),
	].filter((task) => task.state.status !== "terminal");
	expect(live.map((task) => task.kind)).toEqual(
		expect.arrayContaining(["pi.generation", "pi.tool"]),
	);
	expect(new Set(live.map((task) => task.conversationId)).size).toBe(2);
	expect(live.every((task) => task.abortRequested)).toBe(true);
	await f.integration.stop();
});

it("a child's workflow tool acts for the lead's request; the Session is released and resumed without resubmitting", async () => {
	const f = fixture();
	const seen: InvocationMetadata[] = [];
	const approve = defineWorkflow({
		name: `approve-${short()}`,
		description: "Run SQL after approval",
		input: z.object({ sql: z.string() }),
		run: async () => "unused",
	});
	const researcher = researcherOf(f, [approve]);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
	});
	f.respond([
		fauxAssistantMessage(fauxToolCall(toolName(researcher.name), { task: "Fix the row" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage(fauxToolCall(toolName(approve.name), { sql: "update x" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("fixed"),
		fauxAssistantMessage("all done"),
	]);
	(f.host.workflows as unknown as (typeof approve)[]).push(approve);
	const { settled } = await f.call(lead, "Please fix it", [
		{
			name: approve.name,
			triggers: [],
			handler: async (_input, execution) => {
				seen.push(execution.metadata);
				// The driver released the Session while the child waits on this workflow.
				const storage = await PostgresStorage.open(sql, sessionKey(f.metadata));
				await storage.close(BACKGROUND_CONTEXT);
				return "approved and ran";
			},
		},
	]);
	expect(await settled).toEqual({ status: "completed", output: "all done" });
	expect(seen).toHaveLength(1);
	expect(seen[0]).toMatchObject({
		interactionId: f.metadata.interactionId,
		replyTo: f.metadata.replyTo,
		requestedBy: f.metadata.requestedBy,
		ownsThread: false,
	});
	expect(toolResults(f.contexts[2]!)).toEqual(['"approved and ran"']);
	// The driver waited for the workflow with the Session released, then reran the lead's call: the same
	// child and the same submission, so the child model ran once per turn.
	expect(f.faux.state.callCount).toBe(4);
	const writes = await f.writes();
	expect(writes.filter((write) => write.type === "conversation")).toHaveLength(2);
	const subagentRequests = writes.flatMap((write) =>
		write.type === "submission" && write.value.requestId?.startsWith("fabrial.subagent:")
			? [write.value.id]
			: [],
	);
	expect(new Set(subagentRequests).size).toBe(1);
	await f.integration.stop();
});

it("a child that fails returns a generic failed result and logs the provider error with an id", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	f.respond([
		fauxAssistantMessage(fauxToolCall(tool, { task: "Try" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("", { stopReason: "error", errorMessage: "invalid x-api-key secret" }),
		fauxAssistantMessage("I'll answer myself."),
	]);
	const { settled } = await f.call(lead, "Go");
	expect(await settled).toMatchObject({ status: "completed", output: "I'll answer myself." });
	const [result] = toolResults(f.contexts[2]!);
	expect(result).toMatch(
		new RegExp(
			`^<subagent name="${tool}-1" status="failed">The subagent stopped without an answer \\(error id [0-9a-f]{12}\\)`,
		),
	);
	expect(result).not.toContain("x-api-key");
	const errorId = /error id ([0-9a-f]{12})/.exec(result!)![1];
	expect(f.logger.error).toHaveBeenCalledWith(
		"Subagent failed",
		expect.objectContaining({ errorId, reason: "model_error" }),
	);
	await f.integration.stop();
});
