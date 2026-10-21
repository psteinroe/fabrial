import { connectChat } from "../../../src/chat/connection.ts";
import { metadataOf } from "../../../src/workflows/metadata.ts";
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
import type { StorageWrite } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	definePlugin,
	defineUser,
	type Agent,
	type FabrialHost,
	type InvocationMetadata,
	type TaskEvent,
	type WorkflowTask,
} from "../../internals.ts";
import { toolName } from "../../../src/agents/definition.ts";
import { callSession } from "../../../src/sessions/delivery.ts";
import { connectAgents } from "../../../src/sessions/host.ts";
import type { ExternalIdentity } from "../../../src/identity.ts";
import { FakeChat } from "../../../src/testing/chat.ts";
import { until } from "../../conductor/helper.ts";
import { PostgresStorage } from "../../../src/sessions/storage.ts";
import { useAgents } from "./setup.ts";

const definitions = createFabrial({ plugins: [] });
// The provider of the tests' threads and DMs: apps reach the fake platform through a plugin of that id.
const slackTest = definePlugin({ id: "slack-test" });
const { defineAgent, defineTool, defineWorkflow } = definitions;

/**
 * The Pi integration connected to a bare host on real Conductor, in real time; apps built with `launch`
 * for the tests that route through core.
 */
const { db, sql, launch, idle, open, all } = useAgents();

function fixture() {
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
		get conductor() {
			return tasks.conductor;
		},
		queue: tasks.queue,
		now: () => tasks.now(),
		clients: () => ({}),
		chat: () => chat.connection(),
		logger,
		directory: { isMember: async () => true },
		resolvePrincipal: async () => null,
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
	});
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
		others: WorkflowTask[] = [],
		events: TaskEvent[] = [],
	) => {
		const workflow: WorkflowTask = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, ctx) => integration.run(ctx, "agent", agent, { input }),
		};
		tasks.register({
			workflows: [workflow, ...integration.workflows, ...others],
			events,
		});
		await integration.start();
		await tasks.start();
		const id = await client.invoke(workflow.name, null, { metadata });
		calling = id;
		const settled = async () => {
			const result = await db.execution(id).settled();
			await idle(client.queue);
			return result;
		};
		return { id, settled };
	};
	const writes = async () =>
		(
			await sql()`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${key()} ORDER BY seq`
		).flatMap((row) => JSON.parse(String(row.writes)) as StorageWrite[]);
	return {
		client,
		tasks,
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

/**
 * A reply that takes a while: the chat thread shows a different status at most once a second, so a child's
 * status shows only if its turn outlasts that window.
 */
const slowly = <T>(reply: T) =>
	new Promise<T>((resolve) => setTimeout(() => resolve(reply), 1_200));

/** The approval cards in `identity`'s DM, each with a click on its approve action. */
async function approvalCards(chat: FakeChat, identity: ExternalIdentity) {
	return (await chat.openDM(identity)).posts.flatMap((post) => {
		const action =
			typeof post.content === "object" && "card" in post.content
				? post.content.card.actions?.find((each) => each.id.startsWith("fabrial.approval.approve:"))
				: undefined;
		return action ? [{ post, approve: () => chat.click(post.ref, action.id, identity) }] : [];
	});
}

/** Resolves with the first approval card posted to `identity`'s DM. */
const approvalCard = (chat: FakeChat, identity: ExternalIdentity) =>
	until("the approval card", async () => (await approvalCards(chat, identity))[0]);

function researcherOf(f: Fixture, tools: Parameters<typeof defineAgent>[0]["tools"] = []) {
	return defineAgent({
		name: `researcher-${short()}`,
		description: "Investigates a question and returns findings with sources.",
		model: f.model,
		instructions: "You research.",
		tools,
	});
}

function leadOf(f: Fixture, researcher: Agent) {
	return defineAgent({
		name: `lead-${short()}`,
		description: "Answers support questions.",
		model: f.model,
		subagents: [researcher],
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
		() => slowly(fauxAssistantMessage("It is <b>DNS</b> & </subagent>")),
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
		`<subagent name="${tool}" status="done">It is &lt;b&gt;DNS&lt;/b&gt; &amp; &lt;/subagent&gt;</subagent>`,
	]);
	// Owned by the lead's tool call, in the lead's Session.
	const conversations = (await f.writes()).filter((write) => write.type === "conversation");
	expect(conversations).toHaveLength(2);
	expect(conversations[1]!.value).toMatchObject({ owner: { taskId: expect.any(Number) } });
	// The child shows no status line of its own; the lead's shows the subagent tool like any tool.
	const statuses = (await f.chat.thread(f.metadata.replyTo as Parameters<typeof f.chat.thread>[0]))
		.statuses;
	expect(statuses.filter((status) => status?.startsWith(`${researcher.name}: `))).toEqual([]);
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
		`<subagent name="${tool}" status="done">deepest</subagent>`,
	]);
	await f.integration.stop();
});

// Foreground children are one-shot (Pi example 22): only background children have names to continue.
it("a name continues only a background child; unknown names are tool errors", async () => {
	const f = fixture();
	const researcher = researcherOf(f);
	const lead = leadOf(f, researcher);
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
		fauxAssistantMessage("done"),
	]);
	const { settled } = await f.call(lead, "Go");
	expect(await settled()).toMatchObject({ status: "completed", output: "done" });
	expect(toolResults(f.contexts.at(-1)!)).toEqual([
		`<subagent name="${tool}" status="done">Remembered.</subagent>`,
		`There is no ${tool} child called ${tool}-1. Call ${tool} without a name to start one.`,
		`There is no ${tool} child called nobody. Call ${tool} without a name to start one.`,
	]);
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
	await f.client.cancel(id, "Stopped");
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
		run: async () => ({}),
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
			handler: async (_input, ctx) => {
				seen.push(metadataOf(ctx));
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
			`^<subagent name="${tool}" status="failed">The subagent stopped without an answer \\(error id [0-9a-f]{12}\\)`,
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

// A call no longer waits for its background children: it returns the lead's answer, and the report arrives later as
// input to the lead's Session, which stays open until it was answered.
it("a background child returns its name at once; the call returns the lead's answer, the report arrives later", async () => {
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
	expect(await settled()).toEqual({ status: "completed", output: "I asked a researcher." });
	const started = f.contexts.find((context) => lastText(context).startsWith("Started"))!;
	expect(lastText(started)).toBe(
		`Started ${tool}-1 in the background. Its answer arrives later as input <subagent name="${tool}-1">; continue meanwhile.`,
	);
	const report = f.contexts.find((context) => lastText(context).startsWith("<subagent"))!;
	expect(lastText(report)).toBe(
		`<subagent name="${tool}-1" status="done">It is &lt;DNS&gt;</subagent>`,
	);
	expect(await requests(f, "fabrial.subagent-report:")).toHaveLength(1);
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
	expect(await settled()).toEqual({ status: "completed", output: "I asked a researcher." });
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
	expect(await settled()).toEqual({ status: "completed", output: "Waiting." });
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
		run: async () => ({}),
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
	expect(await settled()).toEqual({ status: "completed", output: "Working on it." });
	expect(released).toBe(true);
	expect(crashes).toBeGreaterThanOrEqual(1);
	expect(await requests(f, "fabrial.subagent-report:")).toHaveLength(1);
	expect(f.contexts.map(lastText).filter((text) => text.startsWith("<subagent"))).toEqual([
		`<subagent name="${tool}-1" status="done">fixed</subagent>`,
	]);
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
	await f.client.cancel(id, "Stopped");
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
	const g = createFabrial({ plugins: [slackTest], identity: [alice] });
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
			return { result: approval.status === "approved" ? "ran" : "declined" };
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
		run: async (_input, ctx) => ({
			answer: await ctx.agent("lead", lead, { input: "Fix the row" }),
		}),
	});
	faux.setResponses(
		Array.from({ length: 10 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text === "Fix it") return use(toolName(runSql.name), { sql: "update x" });
			if (text.includes("Fix the row")) return use(toolName(researcher.name), { task: "Fix it" });
			if (text === '{"result":"ran"}') return say("Fixed after approval.");
			if (text.startsWith("<subagent")) return say(`Lead: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat({ installationId: "w" });
	const { client } = await launch(g, {
		workflows: [runSql, entry],
		chat,
		agents: { models, sql: sql(), settings: { retry: { enabled: false } } },
	});
	{
		const id = await client.invoke(entry.name, null, {
			metadata: {
				requestId: randomUUID(),
				origin: null,
				replyTo: null,
				requestedBy: { id: alice.id, known: true, identities: [], name: "Alice" },
				ownerWorkflow: null,
				triggerEvent: null,
			},
		});
		await (await approvalCard(chat, alice.identities[0]!)).approve();
		expect(await db.execution(id).settled()).toMatchObject({
			status: "completed",
			output: {
				answer: `Lead: <subagent name="${toolName(researcher.name)}" status="done">Fixed after approval.</subagent>`,
			},
		});
		// One card, decided in place.
		expect((await chat.openDM(alice.identities[0]!)).posts).toHaveLength(1);
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
				run: async () => ({}),
			});
			(f.host.workflows as unknown as (typeof workflow)[]).push(workflow);
			const task: WorkflowTask = {
				name: workflow.name,
				triggers: [],
				handler: async (_input, ctx) => {
					await ctx.waitForEvent("wait", { event: { name: `go-${workflow.name}` } } as never);
					return `${who} went`;
				},
			};
			return {
				workflow,
				task,
				event: { name: `go-${workflow.name}`, filterable: [] },
				go: () => f.client.emit(`go-${workflow.name}`, {}, { metadata: f.metadata }),
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
			[lead.task, child.task],
			[lead.event, child.event],
		);
		await Promise.all([parked.lead.opened, parked.child.opened]);
		for (const waiter of [lead, child])
			await db
				.execution(
					await until(
						`${waiter.workflow.name} to start`,
						async () => (await open(f.client.queue, waiter.workflow.name))[0],
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
		// The call returns the lead's answer; the child's report arrives as input before the Session closes.
		expect(await settled()).toEqual({ status: "completed", output: "Waiting for the researcher." });
		expect(f.contexts.map(lastText)).toContain(
			`<subagent name="${tool}-1" status="done">child done</subagent>`,
		);
		await f.integration.stop();
	},
);

it("a stop after the lead answered cancels a background child's approval workflow: its card no longer approves", async () => {
	const alice = defineUser({
		id: `alice-${short()}`,
		name: "Alice",
		identities: [{ provider: "slack-test", installationId: "w", subjectId: `A${short()}` }],
	});
	const g = createFabrial({ plugins: [slackTest], identity: [alice] });
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
			return { result: approval.status === "approved" ? "ran" : "declined" };
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
	const thread = {
		kind: "thread",
		provider: "slack-test",
		threadId: `slack-test:general:${randomUUID()}`,
	} as const;
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
			if (text === '{"result":"ran"}') return say("Fixed after approval.");
			if (text.includes("Fix the row")) return use(tool, { task: "Fix it", background: true });
			if (text.startsWith("Started")) return say("On it.");
			if (text.includes("Status?")) return say("Still waiting for approval.");
			if (text.startsWith("<subagent")) return say(`Done: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat({ installationId: "w" });
	const { client } = await launch(g, {
		workflows: [runSql, ask],
		chat,
		agents: { models, sql: sql(), settings: { retry: { enabled: false } } },
	});
	const send = (text: string) =>
		client.invoke(
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
	const { approve } = await approvalCard(chat, alice.identities[0]!);
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
	await approve();
	await idle(client.queue);
	expect(await approvalWorkflow()).toMatchObject({ cancelled: true, done: false });
	expect(await posts()).toEqual(["On it."]);
});

it("a background child waiting for approval suspends the Session: a new message is answered meanwhile, and the click resumes the child", async () => {
	const alice = defineUser({
		id: `alice-${short()}`,
		name: "Alice",
		identities: [{ provider: "slack-test", installationId: "w", subjectId: `A${short()}` }],
	});
	const g = createFabrial({ plugins: [slackTest], identity: [alice] });
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
			return { result: approval.status === "approved" ? "ran" : "declined" };
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
	const thread = {
		kind: "thread",
		provider: "slack-test",
		threadId: `slack-test:general:${randomUUID()}`,
	} as const;
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
			if (text === '{"result":"ran"}') return say("Fixed after approval.");
			if (text.includes("Fix the row")) return use(tool, { task: "Fix it", background: true });
			if (text.startsWith("Started")) return say("On it.");
			if (text.includes("Status?")) return say("Still waiting for approval.");
			if (text.startsWith("<subagent")) return say(`Done: ${text}`);
			return say(`unexpected: ${text}`);
		}),
	);
	const chat = new FakeChat({ installationId: "w" });
	const { client } = await launch(g, {
		workflows: [runSql, ask],
		chat,
		agents: { models, sql: sql(), settings: { retry: { enabled: false } } },
	});
	const send = (text: string) =>
		client.invoke(
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
		const { approve } = await approvalCard(chat, alice.identities[0]!);
		// Every live task waits on the approval: the Session's execution suspended on it and its lease is free.
		await expect
			.poll(
				async () => {
					if ((await open(client.queue, "fabrial.session")).length !== 1) return false;
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
		await approve();
		// The report reaches the lead after its Session reopened; the run stays hosted until it is answered, so the
		// answer posts.
		await expect
			.poll(posts, { timeout: 30_000 })
			.toEqual([
				"On it.",
				"Still waiting for approval.",
				`Done: <subagent name="${tool}-1" status="done">Fixed after approval.</subagent>`,
			]);
		await idle(client.queue);
	}
});

/**
 * The latest call's Session execution was cancelled with its caller: the lease is free, and no model request
 * follows (the count stays put for a while).
 */
async function cancelledCall(f: Fixture) {
	const [call] = await all(f.client.queue, "fabrial.session");
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
	const g = createFabrial({ plugins: [slackTest], identity: users });
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
	const { client } = await launch(g, {
		workflows: [ask],
		agents: { models, sql: sql(), settings: { retry: { enabled: false } } },
	});
	const threads = [`slack-test:general:${randomUUID()}`, `slack-test:general:${randomUUID()}`];
	const send = (text: string, index: number) =>
		client.invoke(
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
	await idle(client.queue);
});

it("channel-session agents share session state across threads; a ctx.agent call's tools see their own", async () => {
	const g = createFabrial({ plugins: [slackTest] });
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
			return { result: `count=${count}` };
		},
	});
	const desk = g.defineAgent({
		name: `desk-${short()}`,
		description: "Bumps.",
		model,
		tools: [bump],
	});
	const deliver = g.defineWorkflow({
		name: `deliver-${short()}`,
		input: z.object({ text: z.string() }),
		run: async ({ text: input }, ctx) => {
			await ctx.send("ask", desk, { session: "channel", input });
		},
	});
	const call = g.defineWorkflow({
		name: `call-${short()}`,
		input: z.object({ text: z.string() }),
		run: async ({ text: input }, ctx) => ({ answer: await ctx.agent("call", desk, { input }) }),
	});
	faux.setResponses(
		Array.from({ length: 20 }, () => (context: TranscriptContext) => {
			const text = lastText(context);
			if (text.includes("count="))
				return say(`Done ${(JSON.parse(text) as { result: string }).result}.`);
			if (text.includes("Bump")) return use("bump", {});
			return say("ok");
		}),
	);
	const { client } = await launch(g, {
		workflows: [deliver, call],
		agents: { models, sql: sql(), settings: { retry: { enabled: false } } },
	});
	// The fake chat derives a thread's channel from its id (`slack-test:<channel>:<thread>`): both threads share one.
	const channel = `C-${short()}`;
	const metadata = (thread: string): InvocationMetadata => ({
		requestId: randomUUID(),
		origin: null,
		replyTo: {
			kind: "thread",
			provider: "slack-test",
			threadId: `slack-test:${channel}:${thread}`,
		},
		requestedBy: { id: "alice", known: true, identities: [] },
		ownerWorkflow: null,
		triggerEvent: null,
	});
	await client.invoke(deliver.name, { text: "Bump" }, { metadata: metadata("a") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1]);
	await client.invoke(deliver.name, { text: "Bump" }, { metadata: metadata("b") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2]);
	const id = await client.invoke(call.name, { text: "Bump" }, { metadata: metadata("a") });
	expect(await db.execution(id).settled()).toEqual({
		status: "completed",
		output: { answer: "Done count=1." },
	});
	expect(seen).toEqual([1, 2, 1]);
	await client.invoke(deliver.name, { text: "Bump" }, { metadata: metadata("a") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2, 1, 3]);
	await client.invoke(deliver.name, { text: "Bump" }, { metadata: metadata("b") });
	await expect.poll(() => seen, { timeout: 30_000 }).toEqual([1, 2, 1, 3, 4]);
	await idle(client.queue);
});
