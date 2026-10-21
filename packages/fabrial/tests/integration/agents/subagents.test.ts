import { metadataOf } from "../../../src/context.ts";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import type { Context as TranscriptContext } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
	type FauxResponseStep,
} from "@earendil-works/pi-ai/providers/faux";
import { createSession, type StorageWrite } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineUser,
	type Agent,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeEvent,
	type RuntimeWorkflow,
} from "../../internals.ts";
import { toolName } from "../../../src/agent.ts";
import { callSession, pi, PostgresStorage } from "../../../src/agents/index.ts";
import { SubagentWork } from "../../../src/agents/documents.ts";
import { FakeChat } from "../../../src/testing.ts";
import { until } from "../../conductor/helper.ts";
import { useAgents } from "./setup.ts";

const definitions = createFabrial({ plugins: [] });
const { defineAgent, defineTool, defineWorkflow } = definitions;

/**
 * The Pi integration connected to a bare host on real Conductor, in real time; apps built with `launch`
 * for the tests that route through core.
 */
const { db, sql, launch, idle, open, all } = useAgents();

function fixture() {
	const runtime = db.runtime();
	const chat = new FakeChat();
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		requestId: randomUUID(),
		origin: null,
		replyTo: { kind: "thread", provider: "slack-test", threadId: randomUUID() },
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const logger = {
		warn: vi.fn(),
		error: vi.fn(),
		debug: (_message: string, _fields?: object) => {},
		info() {},
	};
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
	const integration = pi({ models, sql: sql(), settings: { retry: { enabled: false } } }).connect(
		host,
	);
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
	/** The Session of the latest call: a `ctx.agent` call runs in its own. */
	let calling = "";
	const key = () => callSession(calling, "agent");
	/**
	 * Runs `agent` from a workflow call with `input`. `settled()` resolves with the call's result once
	 * everything it started settled.
	 */
	const call = async (
		agent: Agent,
		input: string,
		others: RuntimeWorkflow[] = [],
		events: RuntimeEvent[] = [],
	) => {
		const workflow: RuntimeWorkflow = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, execution) => integration.agents.run(execution, "agent", agent, { input }),
		};
		runtime.register({
			workflows: [workflow, ...integration.workflows, ...others],
			events,
		});
		await integration.start();
		await runtime.start();
		const id = await runtime.invoke(workflow.name, null, { metadata });
		calling = id;
		const settled = async () => {
			const result = await db.execution(id).settled();
			await idle(runtime.queue);
			return result;
		};
		return { id, settled };
	};
	const writes = async () =>
		(
			await sql()`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${key()} ORDER BY seq`
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
		key,
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
	expect(await settled()).toEqual({
		status: "completed",
		output: "The researcher says it is DNS.",
	});
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
	expect(await settled()).toMatchObject({ status: "completed", output: "done" });
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

it("forks at the delegating response behind a long history", async () => {
	const f = fixture();
	const note = defineTool({
		name: "note",
		description: "Takes a note",
		input: z.object({ n: z.number() }),
		run: ({ n }) => `noted ${n}`,
	});
	const lead = defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		tools: [note],
		subagents: [{ agent: "self", context: "fork", background: "never" }],
	});
	const tool = toolName(lead.name);
	const notes = (from: number, count: number) =>
		Array.from({ length: count }, (_, i) => fauxToolCall("note", { n: from + i }));
	// Far more than a page of entries before the delegating response, and more after it within the same response.
	f.respond([
		fauxAssistantMessage(notes(0, 120), { stopReason: "toolUse" }),
		fauxAssistantMessage([...notes(120, 5), fauxToolCall(tool, { task: "Count the notes." })], {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("120"),
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "Take 120 notes.");
	expect(await settled()).toMatchObject({ status: "completed", output: "done" });
	const child = f.contexts[2]!;
	expect(userTexts(child)).toEqual(["Take 120 notes.", "Count the notes."]);
	expect(toolResults(child)).toEqual(Array.from({ length: 120 }, (_, n) => `noted ${n}`));
	// The fork ends before the response that delegated, so it holds no dangling call.
	const calls = child.messages.flatMap((message) =>
		message.role === "assistant"
			? message.content.filter((block) => block.type === "toolCall")
			: [],
	);
	expect(calls).toHaveLength(120);
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
	expect(await settled()).toMatchObject({ status: "completed", output: "top" });
	const offered = [tool, "subagent_stop"];
	expect(f.contexts.map(toolNames)).toEqual([offered, offered, [], offered, offered]);
	expect(toolResults(f.contexts[3]!)).toEqual([
		`<subagent name="${tool}-1" status="done">deepest</subagent>`,
	]);
	await f.integration.stop();
});

it("continuing a named child keeps its history; unknown names are tool errors", async () => {
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
			],
			{ stopReason: "toolUse" },
		),
		fauxAssistantMessage("7"),
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "Go");
	await settled();
	expect(await settled()).toMatchObject({ status: "completed", output: "done" });
	const continued = f.contexts[3]!;
	expect(userTexts(continued)).toEqual(["Remember 7", "What number?"]);
	expect(toolResults(f.contexts[4]!)).toEqual([
		`<subagent name="${tool}-1" status="done">Remembered.</subagent>`,
		`<subagent name="${tool}-1" status="done">7</subagent>`,
		`There is no ${tool} child called nobody. Call ${tool} without a name to start one.`,
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
	expect(await settled()).toMatchObject({ status: "completed", output: "done" });
	const results = toolResults(f.contexts[2]!);
	expect(results).toHaveLength(2);
	expect(results.filter((text) => text.includes('status="done">answer'))).toHaveLength(1);
	expect(results.filter((text) => text.includes("the limit for this conversation"))).toHaveLength(
		1,
	);
	await f.integration.stop();
});

it("cancelling the lead's caller aborts the child's tool; no further model calls", async () => {
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
	expect(await settled()).toMatchObject({ status: "cancelled" });
	// The call's Session execution is cancelled with its caller; the child's running tool is aborted in process.
	await cancelledCall(f);
	expect(aborted).toBe(true);
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
				await leaseFreed(f.key());
				return "approved and ran";
			},
		},
	]);
	expect(await settled()).toEqual({ status: "completed", output: "all done" });
	expect(seen).toHaveLength(1);
	expect(seen[0]).toMatchObject({
		requestId: f.metadata.requestId,
		replyTo: f.metadata.replyTo,
		requestedBy: f.metadata.requestedBy,
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
	expect(await settled()).toMatchObject({ status: "completed", output: "I'll answer myself." });
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

/** The text of the newest message a request answers: a user input or a tool result. */
const lastText = (context: TranscriptContext) => {
	const last = context.messages.findLast((message) => (message.role as string) !== "system")!;
	const content =
		typeof last.content === "string" ? [{ type: "text", text: last.content }] : last.content;
	return content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
};
type Reply = ReturnType<typeof fauxAssistantMessage>;
/** Answers every request from its newest message: lead and children run concurrently, so order is not fixed. */
const route = (f: Fixture, answer: (text: string) => Reply | Promise<Reply>) =>
	f.respond(
		Array.from({ length: 40 }, () => (context: TranscriptContext) => answer(lastText(context))),
	);
const use = (tool: string, args: object) =>
	fauxAssistantMessage(fauxToolCall(tool, args as never), { stopReason: "toolUse" });
const say = (text: string) => fauxAssistantMessage(text);
/** Request ids of submissions in the fixture's Session that start with `prefix`. */
const requests = async (f: Fixture, prefix: string) => [
	...new Set(
		(await f.writes()).flatMap((write) =>
			write.type === "submission" && write.value.requestId?.startsWith(prefix)
				? [write.value.requestId]
				: [],
		),
	),
];
function gate() {
	let open!: () => void;
	const opened = new Promise<void>((resolve) => (open = resolve));
	return { open, opened };
}
function leadOf(f: Fixture, researcher: Agent, limits?: { concurrent?: number }) {
	return defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
		...(limits ? { limits } : {}),
	});
}

it("a background child returns its name at once; a call settles after the lead answered its report", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	route(f, (text) => {
		if (text.includes("Why is the site down?"))
			return use(tool, { task: "Find the root cause", background: true });
		if (text.startsWith("Started")) return say("I asked a researcher.");
		if (text === "Find the root cause") return say("It is <DNS>");
		if (text.startsWith("<subagent")) return say("Final: DNS");
		return say(`unexpected: ${text}`);
	});
	const { settled } = await f.call(lead, "Why is the site down?");
	expect(await settled()).toEqual({ status: "completed", output: "Final: DNS" });
	const started = f.contexts.find((context) => lastText(context).startsWith("Started"))!;
	expect(lastText(started)).toBe(
		`Started ${tool}-1 in the background. Its answer arrives later as input <subagent name="${tool}-1">; continue meanwhile.`,
	);
	const report = f.contexts.find((context) => lastText(context).startsWith("<subagent"))!;
	expect(lastText(report)).toBe(
		`<subagent name="${tool}-1" status="done">It is &lt;DNS&gt;</subagent>`,
	);
	// The status section lists children from durable state, only once there are some.
	expect(prompt(f.contexts[0]!)).not.toContain("Your subagents");
	expect(prompt(report)).toMatch(
		new RegExp(`- ${tool}-1 \\(${researcher.name}, background\\): (working|idle)`),
	);
	expect(await requests(f, "fabrial.subagent-report:")).toHaveLength(1);
	const statuses = (await f.chat.thread(f.metadata.replyTo as Parameters<typeof f.chat.thread>[0]))
		.statuses;
	expect(statuses).toContain(`${researcher.name}: Thinking…`);
	await f.integration.stop();
});

it("a background report replayed after its submission is submitted and answered once", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	let replays = 0;
	// The driver loses the Session right after Pi admitted the report, before the reporter ends: another process takes
	// the lease while this one is blocked, so the reporter's next commit fails and its report phase replays on reopen.
	f.logger.debug = (message: string) => {
		if (message !== "Reported to the lead" || replays++) return;
		execFileSync(
			process.execPath,
			[
				"--input-type=module",
				"-e",
				`import postgres from "postgres";
				const sql = postgres(process.env.URL, { max: 1, onnotice: () => {} });
				await sql\`UPDATE fabrial_pi.sessions SET epoch = epoch + 1, owner = NULL, lease_until = NULL WHERE key = \${process.env.KEY}\`;
				await sql.end();`,
			],
			{ cwd: import.meta.dirname, env: { ...process.env, URL: db.url, KEY: f.key() } },
		);
	};
	route(f, (text) => {
		if (text.includes("Why is the site down?"))
			return use(tool, { task: "Find the root cause", background: true });
		if (text.startsWith("Started")) return say("I asked a researcher.");
		if (text === "Find the root cause") return say("It is DNS");
		if (text.startsWith("<subagent")) return say("Final: DNS");
		return say(`unexpected: ${text}`);
	});
	f.integration.workflows.find((workflow) => workflow.name === "fabrial.session")!.retries = {
		maxAttempts: 3,
	};
	const { settled } = await f.call(lead, "Why is the site down?");
	expect(await settled()).toEqual({ status: "completed", output: "Final: DNS" });
	expect(replays).toBe(2);
	const [report] = await requests(f, "fabrial.subagent-report:");
	const ids = new Set(
		(await f.writes()).flatMap((write) =>
			write.type === "submission" && write.value.requestId === report ? [write.value.id] : [],
		),
	);
	expect(ids.size).toBe(1);
	expect(f.contexts.filter((context) => lastText(context).startsWith("<subagent"))).toHaveLength(1);
	await f.integration.stop();
});

it("continuing a busy background child steers it; an idle one gets new input; each answer is reported once", async () => {
	const f = fixture();
	const held = gate();
	const wait = defineTool({
		name: "wait",
		description: "Waits",
		input: z.object({}),
		run: async () => {
			await held.opened;
			return "waited";
		},
	});
	const researcher = researcherOf(f, [wait]);
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	const name = `${tool}-1`;
	const childTexts: string[][] = [];
	f.respond(
		Array.from({ length: 40 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text.includes("Go")) return use(tool, { task: "Count to three", background: true });
			if (text.startsWith("Started")) return use(tool, { task: "Also count to four", name });
			if (text.startsWith("Sent the task")) {
				held.open();
				return say("Waiting.");
			}
			if (text === "Count to three") return use("wait", {});
			// The steer is placed after the tool result, so it is the newest message.
			if (text === "waited" || text === "Also count to four") {
				childTexts.push(userTexts(context));
				return say(userTexts(context).includes("Also count to four") ? "1 2 3 4" : "1 2 3");
			}
			if (text.includes("1 2 3 4</subagent>")) return use(tool, { task: "Now five", name });
			if (text === "Now five") {
				childTexts.push(userTexts(context));
				return say("5");
			}
			if (text.startsWith("<subagent") && text.includes(">5<")) return say("Final");
			return say("ok");
		}),
	);
	const { settled } = await f.call(lead, "Go");
	expect(await settled()).toEqual({ status: "completed", output: "Final" });
	const reports = f.contexts.map(lastText).filter((text) => text.startsWith("<subagent"));
	expect(reports).toEqual([
		`<subagent name="${name}" status="done">1 2 3 4</subagent>`,
		`<subagent name="${name}" status="done">5</subagent>`,
	]);
	// One conversation keeps the whole history: the steer reached it mid-run, the next task after it.
	expect(childTexts.at(-1)).toEqual(["Count to three", "Also count to four", "Now five"]);
	const conversations = (await f.writes()).filter((write) => write.type === "conversation");
	expect(new Set(conversations.map((write) => write.value.id)).size).toBe(2);
	await f.integration.stop();
});

it("subagent_stop aborts a background child's work; it reports nothing and the call settles", async () => {
	const f = fixture();
	const started = gate();
	let aborted = false;
	const slow = defineTool({
		name: "slow",
		description: "Slow work",
		input: z.object({}),
		run: (_input, ctx) => {
			started.open();
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
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	route(f, async (text) => {
		if (text.includes("Go")) return use(tool, { task: "Work slowly", background: true });
		if (text.startsWith("Started")) {
			await started.opened;
			return use("subagent_stop", { name: `${tool}-1` });
		}
		if (text.startsWith("Stopped")) return say("Stopped it.");
		if (text === "Work slowly") return use("slow", {});
		return say(`unexpected: ${text}`);
	});
	const { settled } = await f.call(lead, "Go");
	expect(await settled()).toEqual({ status: "completed", output: "Stopped it." });
	expect(aborted).toBe(true);
	expect(f.contexts.map(lastText)).toContain(`Stopped ${tool}-1. It can be continued by name.`);
	expect(await requests(f, "fabrial.subagent-report:")).toEqual([]);
	await f.integration.stop();
});

it("a background child's report survives the Session being released and the driver crashing, and arrives once", async () => {
	const f = fixture();
	const approve = defineWorkflow({
		name: `approve-${short()}`,
		description: "Run SQL after approval",
		input: z.object({ sql: z.string() }),
		run: async () => "unused",
	});
	let crashes = 0;
	const flaky = defineTool({
		name: "flaky",
		description: "Flaky",
		input: z.object({}),
		run: async () => {
			// The first run replaces the Session owner, so the driver loses its lease and is redelivered.
			if (!crashes++)
				await sql()`UPDATE fabrial_pi.sessions SET epoch = epoch + 1, owner = NULL, lease_until = NULL
					WHERE key = ${f.key()}`;
			return "flaked";
		},
	});
	const researcher = researcherOf(f, [approve, flaky]);
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	route(f, (text) => {
		if (text.includes("Go")) return use(tool, { task: "Fix the row", background: true });
		if (text.startsWith("Started")) return say("Working on it.");
		if (text === "Fix the row") return use(toolName(approve.name), { sql: "update x" });
		if (text === '"approved and ran"' || text.includes("flaky was interrupted"))
			return use("flaky", {});
		if (text === "flaked") return say("fixed");
		if (text.startsWith("<subagent")) return say("Final");
		return say(`unexpected: ${text}`);
	});
	(f.host.workflows as unknown as (typeof approve)[]).push(approve);
	f.integration.workflows.find((workflow) => workflow.name === "fabrial.session")!.retries = {
		maxAttempts: 3,
	};
	let released = false;
	const { settled } = await f.call(lead, "Go", [
		{
			name: approve.name,
			triggers: [],
			handler: async () => {
				// The driver released the Session while the background child waits on this workflow.
				await leaseFreed(f.key());
				released = true;
				return "approved and ran";
			},
		},
	]);
	expect(await settled()).toEqual({ status: "completed", output: "Final" });
	expect(released).toBe(true);
	expect(crashes).toBeGreaterThanOrEqual(1);
	expect(await requests(f, "fabrial.subagent-report:")).toHaveLength(1);
	expect(f.contexts.map(lastText).filter((text) => text.startsWith("<subagent"))).toEqual([
		`<subagent name="${tool}-1" status="done">fixed</subagent>`,
	]);
	await f.integration.stop();
});

it("limits.concurrent counts a running background child durably across driver attempts", async () => {
	const f = fixture();
	const held = gate();
	const crashed = gate();
	let runs = 0;
	const hold = defineTool({
		name: "hold",
		description: "Holds",
		input: z.object({}),
		run: async (_input, ctx) => {
			// The first run replaces the Session owner: the driver loses its lease and is redelivered.
			if (!runs++) {
				await sql()`UPDATE fabrial_pi.sessions SET epoch = epoch + 1, owner = NULL, lease_until = NULL
					WHERE key = ${f.key()}`;
				crashed.open();
			}
			await Promise.race([
				held.opened,
				new Promise((_resolve, reject) =>
					ctx.signal.addEventListener("abort", () => reject(ctx.signal.reason), { once: true }),
				),
			]);
			return "held";
		},
	});
	const researcher = researcherOf(f, [hold]);
	const lead = leadOf(f, researcher, { concurrent: 1 });
	const tool = toolName(researcher.name);
	route(f, async (text) => {
		if (text.includes("Go")) return use(tool, { task: "Hold on", background: true });
		if (text.startsWith("Started")) {
			// Ask for a second child only from the redelivered driver, which has no memory of the first.
			await crashed.opened;
			return use(tool, { task: "Another", background: true });
		}
		if (text.includes("already running")) {
			held.open();
			return say("Waiting for the first.");
		}
		if (text === "Hold on" || text.includes("hold was interrupted")) return use("hold", {});
		if (text === "held") return say("held");
		if (text.startsWith("<subagent")) return say("Final");
		return say(`unexpected: ${text}`);
	});
	f.integration.workflows.find((workflow) => workflow.name === "fabrial.session")!.retries = {
		maxAttempts: 3,
	};
	const { settled } = await f.call(lead, "Go");
	expect(await settled()).toEqual({ status: "completed", output: "Final" });
	expect(f.contexts.map(lastText)).toContain(
		`1 subagents are already running, the limit for this conversation. Wait for one of them to answer, then call ${tool} again.`,
	);
	expect(await requests(f, "fabrial.subagent-report:")).toHaveLength(1);
	await f.integration.stop();
});

it("cancelling the interaction aborts its background children", async () => {
	const f = fixture();
	const started = gate();
	let aborted = false;
	const slow = defineTool({
		name: "slow",
		description: "Slow work",
		input: z.object({}),
		run: (_input, ctx) => {
			started.open();
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
	const lead = leadOf(f, researcher);
	const tool = toolName(researcher.name);
	route(f, (text) => {
		if (text.includes("Go")) return use(tool, { task: "Work slowly", background: true });
		if (text.startsWith("Started")) return say("Waiting.");
		if (text === "Work slowly") return use("slow", {});
		return say(`unexpected: ${text}`);
	});
	const { id, settled } = await f.call(lead, "Go");
	await started.opened;
	await f.runtime.cancel(id, "Stopped");
	expect(await settled()).toMatchObject({ status: "cancelled" });
	// The background child dies with the call's Session execution: its tool is aborted, and it never reports.
	await cancelledCall(f);
	expect(aborted).toBe(true);
	await f.integration.stop();
});

it("a child's workflow tool waits for approval: the approver's click resumes it and the answer reaches the lead", async () => {
	const alice = defineUser({
		id: `alice-${short()}`,
		name: "Alice",
		identities: [{ provider: "slack-test", installationId: "w", subjectId: `A${short()}` }],
	});
	const g = createFabrial({ plugins: [], identity: [alice] });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const runSql = g.defineWorkflow({
		name: `run-sql-${short()}`,
		description: "Runs SQL after approval",
		input: z.object({ sql: z.string() }),
		async run(input, ctx) {
			const approval = await ctx.waitForApproval("approve", {
				title: "Run SQL?",
				details: input.sql,
				approvers: alice,
			});
			return approval.status === "approved" ? "ran" : "declined";
		},
	});
	const researcher = g.defineAgent({
		name: `researcher-${short()}`,
		description: "Fixes rows.",
		model,
		tools: [runSql],
	});
	const lead = g.defineAgent({
		name: `lead-${short()}`,
		description: "Leads.",
		model,
		subagents: [researcher],
	});
	const entry = g.defineWorkflow({
		name: `entry-${short()}`,
		run: (_input, ctx) => ctx.agent("lead", lead, { input: "Fix the row" }),
	});
	faux.setResponses(
		Array.from({ length: 10 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text === "Fix it") return use(toolName(runSql.name), { sql: "update x" });
			if (text.includes("Fix the row")) return use(toolName(researcher.name), { task: "Fix it" });
			if (text === '"ran"') return say("Fixed after approval.");
			if (text.startsWith("<subagent")) return say(`Lead: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat();
	const dm = await chat.openDM(alice.identities[0]!);
	const post = dm.post.bind(dm);
	let cards = 0;
	dm.post = async (content) => {
		const ref = await post(content);
		const action =
			typeof content === "object" && "card" in content
				? content.card.actions?.find((each) => each.id.startsWith("fabrial.approval.approve:"))
				: undefined;
		if (action) {
			cards++;
			await chat.click(ref, action.id, alice.identities[0]!);
		}
		return ref;
	};
	const { runtime } = await launch(g, {
		workflows: [runSql, entry],
		chat,
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	{
		const id = await runtime.invoke(entry.name, null, {
			metadata: {
				requestId: randomUUID(),
				origin: null,
				replyTo: null,
				requestedBy: { id: alice.id, known: true, identities: [], name: "Alice" },
				ownerWorkflow: null,
				triggerEvent: null,
			},
		});
		expect(await db.execution(id).settled()).toMatchObject({
			status: "completed",
			output: `Lead: <subagent name="${toolName(researcher.name)}-1" status="done">Fixed after approval.</subagent>`,
		});
		expect(cards).toBe(1);
	}
});

it.each([
	["the lead's", ["lead", "child"]],
	["the child's", ["child", "lead"]],
] as const)(
	"%s workflow completing first resumes the task waiting on it while the other still waits",
	async (_label, order) => {
		const f = fixture();
		const waiter = (who: string) => {
			const workflow = defineWorkflow({
				name: `wait-${who}-${short()}`,
				description: `Waits for ${who}`,
				input: z.object({}),
				run: async () => "unused",
			});
			(f.host.workflows as unknown as (typeof workflow)[]).push(workflow);
			const runtime: RuntimeWorkflow = {
				name: workflow.name,
				triggers: [],
				handler: async (_input, execution) => {
					const go = await execution.subscribe("go", { event: `go-${workflow.name}` });
					await execution.waitForAny("wait", { go });
					return `${who} went`;
				},
			};
			return {
				workflow,
				runtime,
				event: { name: `go-${workflow.name}`, filterable: [] },
				go: () => f.runtime.emit(`go-${workflow.name}`, {}, { metadata: f.metadata }),
			};
		};
		const lead = waiter("lead");
		const child = waiter("child");
		const researcher = researcherOf(f, [child.workflow]);
		const agent = defineAgent({
			name: `lead-${short()}`,
			description: "Answers support questions.",
			model: f.model,
			tools: [lead.workflow],
			subagents: [researcher],
		});
		const tool = toolName(researcher.name);
		const parked = { lead: gate(), child: gate() };
		route(f, (text) => {
			if (text.includes("Go")) return use(tool, { task: "Wait", background: true });
			if (text === "Wait") {
				parked.child.open();
				return use(toolName(child.workflow.name), {});
			}
			if (text.startsWith("Started")) {
				parked.lead.open();
				return use(toolName(lead.workflow.name), {});
			}
			if (text === '"lead went"') return say("Waiting for the researcher.");
			if (text === '"child went"') return say("child done");
			if (text.startsWith("<subagent")) return say(`Final: ${text}`);
			return say(`unexpected: ${text}`);
		});
		const { id, settled } = await f.call(
			agent,
			"Go",
			[lead.runtime, child.runtime],
			[lead.event, child.event],
		);
		await Promise.all([parked.lead.opened, parked.child.opened]);
		for (const waiter of [lead, child])
			await db
				.execution(
					await until(
						`${waiter.workflow.name} to start`,
						async () => (await open(f.runtime.queue, waiter.workflow.name))[0],
					),
				)
				.suspended("wait");
		expect((await db.execution(id).info())?.status).not.toMatch(/completed|failed|cancelled/);
		const waiters = { lead, child };
		await waiters[order[0]].go();
		await expect
			.poll(() => f.contexts.map(lastText), { timeout: 10_000 })
			.toContain(order[0] === "lead" ? '"lead went"' : '"child went"');
		await waiters[order[1]].go();
		expect(await settled()).toEqual({
			status: "completed",
			output: `Final: <subagent name="${tool}-1" status="done">child done</subagent>`,
		});
		await f.integration.stop();
	},
);

it("a stop after the lead answered cancels a background child's approval workflow: its card no longer approves", async () => {
	const alice = defineUser({
		id: `alice-${short()}`,
		name: "Alice",
		identities: [{ provider: "slack-test", installationId: "w", subjectId: `A${short()}` }],
	});
	const g = createFabrial({ plugins: [], identity: [alice] });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const runSql = g.defineWorkflow({
		name: `run-sql-${short()}`,
		description: "Runs SQL after approval",
		input: z.object({ sql: z.string() }),
		async run(input, ctx) {
			const approval = await ctx.waitForApproval("approve", {
				title: "Run SQL?",
				details: input.sql,
				approvers: alice,
			});
			return approval.status === "approved" ? "ran" : "declined";
		},
	});
	const researcher = g.defineAgent({
		name: `researcher-${short()}`,
		description: "Fixes rows.",
		model,
		tools: [runSql],
	});
	const lead = g.defineAgent({
		name: `lead-${short()}`,
		description: "Leads.",
		model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	const session = `ticket-${short()}`;
	const thread = { kind: "thread", provider: "slack-test", threadId: randomUUID() } as const;
	const ask = g.defineWorkflow({
		name: `ask-${short()}`,
		input: z.object({ text: z.string() }),
		run: async (input, ctx) => {
			await ctx.send("ask", lead, { session, input: input.text, home: thread });
		},
	});
	faux.setResponses(
		Array.from({ length: 20 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text === "Fix it") return use(toolName(runSql.name), { sql: "update x" });
			if (text === '"ran"') return say("Fixed after approval.");
			if (text.includes("Fix the row")) return use(tool, { task: "Fix it", background: true });
			if (text.startsWith("Started")) return say("On it.");
			if (text.includes("Status?")) return say("Still waiting for approval.");
			if (text.startsWith("<subagent")) return say(`Done: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat();
	const dm = await chat.openDM(alice.identities[0]!);
	const post = dm.post.bind(dm);
	let approve: (() => Promise<void>) | undefined;
	dm.post = async (content) => {
		const ref = await post(content);
		const action =
			typeof content === "object" && "card" in content
				? content.card.actions?.find((each) => each.id.startsWith("fabrial.approval.approve:"))
				: undefined;
		if (action) approve = () => chat.click(ref, action.id, alice.identities[0]!);
		return ref;
	};
	const { runtime } = await launch(g, {
		workflows: [runSql, ask],
		chat,
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	const send = (text: string) =>
		runtime.invoke(
			ask.name,
			{ text },
			{
				metadata: {
					requestId: randomUUID(),
					origin: null,
					replyTo: null,
					requestedBy: { id: alice.id, known: true, identities: [], name: "Alice" },
					ownerWorkflow: null,
					triggerEvent: null,
				},
			},
		);
	const posts = async () => {
		// Replies only; the approval status card is posted here too.
		return (await chat.thread(thread)).posts.flatMap((each) =>
			typeof each.content === "string" ? [each.content] : [],
		);
	};
	await send("Fix the row");
	await expect.poll(posts, { timeout: 30_000 }).toEqual(["On it."]);
	await until("the approval card", async () => (approve ? true : undefined));
	const db = sql();
	const approvalWorkflow = async () => {
		const rows = await db<{ cancelled: boolean; done: boolean }[]>`
			select cancelled, completed_at is not null as done
			from pgconductor._private_executions where task_key = ${runSql.name}`;
		return rows[0];
	};
	// The lead's reply is posted (its obligation fulfilled) while the worker's approval workflow still waits.
	await chat.cancel(thread, alice.identities[0]!);
	await expect
		.poll(approvalWorkflow, { timeout: 30_000 })
		.toMatchObject({ cancelled: true, done: false });
	await approve!();
	await idle(runtime.queue);
	expect(await approvalWorkflow()).toMatchObject({ cancelled: true, done: false });
	expect(await posts()).toEqual(["On it."]);
});

it("a background child waiting for approval suspends the Session: a new message is answered meanwhile, and the click resumes the child", async () => {
	const alice = defineUser({
		id: `alice-${short()}`,
		name: "Alice",
		identities: [{ provider: "slack-test", installationId: "w", subjectId: `A${short()}` }],
	});
	const g = createFabrial({ plugins: [], identity: [alice] });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const runSql = g.defineWorkflow({
		name: `run-sql-${short()}`,
		description: "Runs SQL after approval",
		input: z.object({ sql: z.string() }),
		async run(input, ctx) {
			const approval = await ctx.waitForApproval("approve", {
				title: "Run SQL?",
				details: input.sql,
				approvers: alice,
			});
			return approval.status === "approved" ? "ran" : "declined";
		},
	});
	const researcher = g.defineAgent({
		name: `researcher-${short()}`,
		description: "Fixes rows.",
		model,
		tools: [runSql],
	});
	const lead = g.defineAgent({
		name: `lead-${short()}`,
		description: "Leads.",
		model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	const session = `ticket-${short()}`;
	const thread = { kind: "thread", provider: "slack-test", threadId: randomUUID() } as const;
	const ask = g.defineWorkflow({
		name: `ask-${short()}`,
		input: z.object({ text: z.string() }),
		run: async (input, ctx) => {
			await ctx.send("ask", lead, { session, input: input.text, home: thread });
		},
	});
	faux.setResponses(
		Array.from({ length: 20 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text === "Fix it") return use(toolName(runSql.name), { sql: "update x" });
			if (text === '"ran"') return say("Fixed after approval.");
			if (text.includes("Fix the row")) return use(tool, { task: "Fix it", background: true });
			if (text.startsWith("Started")) return say("On it.");
			if (text.includes("Status?")) return say("Still waiting for approval.");
			if (text.startsWith("<subagent")) return say(`Done: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat();
	const dm = await chat.openDM(alice.identities[0]!);
	const post = dm.post.bind(dm);
	let approve: (() => Promise<void>) | undefined;
	dm.post = async (content) => {
		const ref = await post(content);
		const action =
			typeof content === "object" && "card" in content
				? content.card.actions?.find((each) => each.id.startsWith("fabrial.approval.approve:"))
				: undefined;
		if (action) approve = () => chat.click(ref, action.id, alice.identities[0]!);
		return ref;
	};
	const { runtime } = await launch(g, {
		workflows: [runSql, ask],
		chat,
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	const send = (text: string) =>
		runtime.invoke(
			ask.name,
			{ text },
			{
				metadata: {
					requestId: randomUUID(),
					origin: null,
					replyTo: null,
					requestedBy: { id: alice.id, known: true, identities: [], name: "Alice" },
					ownerWorkflow: null,
					triggerEvent: null,
				},
			},
		);
	const posts = async () => {
		// Replies only; the approval status card is posted here too.
		return (await chat.thread(thread)).posts.flatMap((each) =>
			typeof each.content === "string" ? [each.content] : [],
		);
	};
	{
		await send("Fix the row");
		await expect.poll(posts, { timeout: 30_000 }).toEqual(["On it."]);
		await until("the approval card", async () => (approve ? true : undefined));
		// Every live task waits on the approval: the Session's execution suspended on it and its lease is free.
		await expect
			.poll(
				async () => {
					if ((await open(runtime.queue, "fabrial.session")).length !== 1) return false;
					const storage = await PostgresStorage.open(sql(), session).catch(() => undefined);
					await storage?.close(BACKGROUND_CONTEXT);
					return !!storage;
				},
				{ timeout: 30_000 },
			)
			.toBe(true);
		await send("Status?");
		await expect
			.poll(posts, { timeout: 30_000 })
			.toEqual(["On it.", "Still waiting for approval."]);
		await approve!();
		// The report reaches the lead after its Session reopened; the run stays hosted until it is answered, so the
		// answer posts.
		await expect
			.poll(posts, { timeout: 30_000 })
			.toEqual([
				"On it.",
				"Still waiting for approval.",
				`Done: <subagent name="${tool}-1" status="done">Fixed after approval.</subagent>`,
			]);
		await idle(runtime.queue);
	}
});

it("a child's answer frees its concurrency slot as it settles, and its status line gives way to the lead's", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = leadOf(f, researcher, { concurrent: 1 });
	const tool = toolName(researcher.name);
	route(f, (text) => {
		if (text.includes("Go")) return use(tool, { task: "First" });
		if (text === "First") return say("one");
		if (text.startsWith(`<subagent name="${tool}-1"`))
			return use(tool, { task: "Second", background: true });
		if (text === "Second") return say("two");
		if (text.startsWith("Started")) return say("Waiting.");
		if (text.startsWith(`<subagent name="${tool}-2"`)) return say("Final");
		return say(`unexpected: ${text}`);
	});
	const { settled } = await f.call(lead, "Go");
	expect(await settled()).toEqual({ status: "completed", output: "Final" });
	const storage = await PostgresStorage.open(sql(), f.key());
	const session = createSession(storage);
	try {
		// Holders stay recorded, so a rerun is admitted, but none counts against the limit.
		const slots = Object.values((await session.snapshot(SubagentWork, BACKGROUND_CONTEXT))!.slots);
		const values = slots.flatMap((each) => Object.values(each));
		expect(values.length).toBeGreaterThan(0);
		expect(new Set(values)).toEqual(new Set([""]));
	} finally {
		await session.close(BACKGROUND_CONTEXT);
	}
	// Each child's activity is followed by the lead's own status, not left showing until the driver ends.
	const statuses = (await f.chat.thread(f.metadata.replyTo as Parameters<typeof f.chat.thread>[0]))
		.statuses;
	const child = statuses.flatMap((status, index) =>
		status?.startsWith(`${researcher.name}: `) ? [index] : [],
	);
	expect(child.length).toBeGreaterThan(0);
	for (const index of child)
		expect(
			// Before the status the driver clears as it ends.
			statuses.slice(index + 1, -1).find((status) => !status?.startsWith(`${researcher.name}: `)),
		).not.toBeUndefined();
	await f.integration.stop();
});

/**
 * The latest call's Session execution was cancelled with its caller: the lease is free, and no model request
 * follows (the count stays put for a while).
 */
async function cancelledCall(f: Fixture) {
	const [call] = await all(f.runtime.queue, "fabrial.session");
	expect((await db.execution(call!).settled()).status).toBe("cancelled");
	await leaseFreed(f.key());
	const requests = f.contexts.length;
	await new Promise((resolve) => setTimeout(resolve, 1_000));
	expect(f.contexts).toHaveLength(requests);
}

/** Waits until no execution hosts the Session: it closes once every live task waits on a workflow. */
async function leaseFreed(key: string) {
	await expect
		.poll(
			async () => (await sql()`SELECT owner FROM fabrial_pi.sessions WHERE key = ${key}`)[0]?.owner,
			{ timeout: 10_000, interval: 20 },
		)
		.toBeNull();
}

it("continuing a background child from another actor and thread runs its tools for the new request", async () => {
	const users = ["alice", "bob"].map((id) =>
		defineUser({
			id: `${id}-${short()}`,
			name: id,
			identities: [{ provider: "slack-test", installationId: "w", subjectId: `${id}${short()}` }],
		}),
	);
	const g = createFabrial({ plugins: [], identity: users });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const seen: string[] = [];
	const whoami = g.defineTool({
		name: "whoami",
		description: "Reports who asked",
		input: z.object({}),
		run: async (_input, ctx) => {
			const replyTo = metadataOf(ctx).replyTo;
			seen.push(`${ctx.actor?.id}@${replyTo?.kind === "thread" ? replyTo.threadId : ""}`);
			return "noted";
		},
	});
	const researcher = g.defineAgent({
		name: `researcher-${short()}`,
		description: "Notes who asked.",
		model,
		tools: [whoami],
	});
	const lead = g.defineAgent({
		name: `lead-${short()}`,
		description: "Leads.",
		model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	const session = `ticket-${short()}`;
	const ask = g.defineWorkflow({
		name: `ask-${short()}`,
		input: z.object({ text: z.string() }),
		run: async (input, ctx) => {
			await ctx.send("ask", lead, { session, input: input.text });
		},
	});
	faux.setResponses(
		Array.from({ length: 20 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text === "First" || text === "Second") return use("whoami", {});
			if (text === "noted") return say("Noted.");
			if (text.includes("Go")) return use(tool, { task: "First", background: true });
			if (text.includes("Again")) return use(tool, { task: "Second", name: `${tool}-1` });
			return say("ok");
		}),
	);
	const { runtime } = await launch(g, {
		workflows: [ask],
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	const threads = [randomUUID(), randomUUID()];
	const send = (text: string, index: number) =>
		runtime.invoke(
			ask.name,
			{ text },
			{
				metadata: {
					requestId: randomUUID(),
					origin: null,
					replyTo: { kind: "thread", provider: "slack-test", threadId: threads[index]! },
					requestedBy: { id: users[index]!.id, known: true, identities: [] },
					ownerWorkflow: null,
					triggerEvent: null,
				},
			},
		);
	await send("Go", 0);
	await expect.poll(() => seen.length, { timeout: 30_000 }).toBe(1);
	await send("Again", 1);
	await expect.poll(() => seen.length, { timeout: 30_000 }).toBe(2);
	expect(seen).toEqual([`${users[0]!.id}@${threads[0]}`, `${users[1]!.id}@${threads[1]}`]);
	await idle(runtime.queue);
});

it("channel-session agents share session state across threads with their workflow tools; a ctx.agent call's tools see their own", async () => {
	const g = createFabrial({ plugins: [] });
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const Counter = g.defineState({
		name: `counter-${short()}`,
		scope: "session",
		schema: z.object({ count: z.number() }),
		initial: () => ({ count: 0 }),
	});
	const seen: number[] = [];
	const bump = g.defineTool({
		name: "bump",
		description: "Bumps the counter",
		input: z.object({}),
		run: async (_input, ctx) => {
			const { count } = await ctx.state(Counter).update((draft) => {
				draft.count++;
			});
			seen.push(count);
			return `count=${count}`;
		},
	});
	// A workflow the agent calls as a tool uses the calling conversation's (channel) Session, not its thread's.
	const flow = g.defineWorkflow({
		name: `flow-${short()}`,
		description: "Bumps the counter from a workflow",
		input: z.object({}),
		run: async (_input, ctx) => {
			const { count } = await ctx.state(Counter).update("bump", (draft) => {
				draft.count++;
			});
			seen.push(count);
			return `count=${count}`;
		},
	});
	// An independent workflow on the thread uses the thread's Session.
	const read = g.defineWorkflow({
		name: `read-${short()}`,
		run: (_input, ctx) => ctx.state(Counter).get("read"),
	});
	const desk = g.defineAgent({
		name: `desk-${short()}`,
		description: "Bumps.",
		model,
		tools: [bump, flow],
	});
	const deliver = g.defineWorkflow({
		name: `deliver-${short()}`,
		input: z.string(),
		run: async (input, ctx) => {
			await ctx.send("ask", desk, { session: "channel", input });
		},
	});
	const call = g.defineWorkflow({
		name: `call-${short()}`,
		input: z.string(),
		run: (input, ctx) => ctx.agent("call", desk, { input }),
	});
	faux.setResponses(
		Array.from({ length: 20 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text.replaceAll('"', "").startsWith("count=")) return say(`Done ${text}.`);
			if (text.includes("Flow")) return use(toolName(flow.name), {});
			if (text.includes("Bump")) return use("bump", {});
			return say("ok");
		}),
	);
	const { runtime } = await launch(g, {
		workflows: [deliver, call, flow, read],
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	// The fake chat derives a thread's channel from its id (`<kind>:<channel>:<thread>`): both threads share one.
	const channel = `C-${short()}`;
	const metadata = (thread: string): InvocationMetadata => ({
		requestId: randomUUID(),
		origin: null,
		replyTo: { kind: "thread", provider: "slack-test", threadId: `thread:${channel}:${thread}` },
		requestedBy: { id: "alice", known: true, identities: [] },
		ownerWorkflow: null,
		triggerEvent: null,
	});
	await runtime.invoke(deliver.name, "Bump", { metadata: metadata("a") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1]);
	await runtime.invoke(deliver.name, "Bump", { metadata: metadata("b") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2]);
	const id = await runtime.invoke(call.name, "Bump", { metadata: metadata("a") });
	expect(await db.execution(id).settled()).toEqual({
		status: "completed",
		output: "Done count=1.",
	});
	expect(seen).toEqual([1, 2, 1]);
	await runtime.invoke(deliver.name, "Bump", { metadata: metadata("a") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2, 1, 3]);
	await runtime.invoke(deliver.name, "Flow", { metadata: metadata("b") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2, 1, 3, 4]);
	await runtime.invoke(deliver.name, "Bump", { metadata: metadata("a") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2, 1, 3, 4, 5]);
	const own = await runtime.invoke(read.name, null, { metadata: metadata("a") });
	expect(await db.execution(own).settled()).toEqual({
		status: "completed",
		output: { count: 0 },
	});
	await idle(runtime.queue);
});
