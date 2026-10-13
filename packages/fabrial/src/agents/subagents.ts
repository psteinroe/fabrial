import type { Context } from "@earendil-works/chord";
import type { TSchema } from "@earendil-works/pi-ai";
import {
	configure,
	defineTool as piTool,
	watchEvents,
	type AgentChange,
	type ConversationId,
	type EntryId,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import { SUBAGENT_DEFAULTS, type Agent, type ResolvedSubagent } from "../agent.ts";
import type { Logger } from "../app.ts";
import { cloneJson, currentRequest, frame, type BridgeFrame } from "./context.ts";
import { Binding, Subagent, SubagentNames } from "./documents.ts";
import { newErrorId } from "./errors.ts";

export interface SubagentDeps {
	/** The Pi agent of a child running `agent` with `depth` subagent levels left below it. */
	childAgent(agent: Agent, depth: number): AgentChange;
	/** Records the agent a conversation runs, for observers. */
	named(conversationId: number, agent: string): void;
	logger: Logger;
	/** Admitted subagent calls per calling conversation, for `limits.concurrent`. */
	running: Map<string, Set<number>>;
	/** The call working on each named child. */
	busy: Map<string, number>;
}

export const BACKGROUND_UNAVAILABLE = "background subagents are not available yet";

const escape = (text: string) =>
	text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

/** A child's answer as data for the lead: escaped inside a `<subagent>` tag. */
export function subagentResult(name: string, status: "done" | "failed", text: string): string {
	return `<subagent name="${escape(name).replaceAll('"', "&quot;")}" status="${status}">${escape(text)}</subagent>`;
}

const error = (text: string): ToolExecutionResult => ({
	content: [{ type: "text", text }],
	isError: true,
});

/**
 * One subagent as a Pi tool (Pi example 22): the call owns its child conversation, so aborting the call aborts the
 * child, and a rerun after a restart finds the same child through the ownership index and the same submission by
 * request id. A named child is continued as a fork of its newest conversation, owned by the continuing call.
 */
export function subagentTool(
	lead: Agent,
	subagent: ResolvedSubagent,
	deps: SubagentDeps,
): ToolRegistration {
	const limits = { ...SUBAGENT_DEFAULTS, ...lead.definition.limits };
	const properties: Record<string, unknown> = {
		task: {
			type: "string",
			description: "The task, with everything the subagent needs to know to do it",
		},
		name: {
			type: "string",
			description: `Continue an earlier ${subagent.tool} child by its name (e.g. ${subagent.tool}-1), keeping its history`,
		},
	};
	if (subagent.background !== "never")
		properties.background = {
			type: "boolean",
			description: "Run in the background and get the answer later as input",
		};
	return piTool({
		name: subagent.tool,
		description: subagent.agent.definition.description,
		parameters: {
			type: "object",
			properties,
			required: ["task"],
			additionalProperties: false,
		} as unknown as TSchema,
		replay: "safe",
		execute: async (input, api, context) => {
			const bridge = frame(context);
			const args = input as { task: string; background?: boolean; name?: string };
			if (args.background && subagent.background === "never")
				return error(`${subagent.tool} cannot run in the background; call it without background.`);
			if (args.background || subagent.background === "always") return error(BACKGROUND_UNAVAILABLE);
			const depth =
				(await bridge.harness.snapshot(Subagent, api.conversationId, context))?.depth ??
				limits.depth;
			if (depth < 1) return error("This conversation cannot start subagents.");
			const scope = `${bridge.session}:${api.conversationId}`;
			const running = deps.running.get(scope) ?? new Set<number>();
			deps.running.set(scope, running);
			// Admission is durable, so a rerun after a restart is never turned away by the limit.
			const admitted = await api.memo<boolean>("fabrial:subagent:admitted", context);
			if (!admitted && running.size >= limits.concurrent)
				return error(
					`${running.size} subagents are already running, the limit for this conversation. Wait for one of them to answer, then call ${subagent.tool} again.`,
				);
			running.add(api.taskId);
			let named: string | undefined;
			try {
				if (!admitted) await api.memo("fabrial:subagent:admitted", true, context);
				let child = await api.commit(
					async (tx) => (await tx.scanConversations({ ownerTaskId: api.taskId }, 1)).items[0]?.id,
					context,
				);
				if (child === undefined) {
					const created = await createChild(bridge, api, subagent, args, depth - 1, deps, context);
					if (typeof created === "string") return error(created);
					child = created;
				}
				const name = (await bridge.harness.snapshot(Subagent, child, context))!.name!;
				const holder = deps.busy.get(`${scope}:${name}`);
				if (holder !== undefined && holder !== api.taskId)
					return error(`${name} is still working on another task; wait for its answer.`);
				named = `${scope}:${name}`;
				deps.busy.set(named, api.taskId);
				deps.named(child, subagent.agent.name);
				await api.details({ conversationId: child, name }, context);
				return await run(bridge, api, child, name, subagent, args.task, deps, context);
			} finally {
				running.delete(api.taskId);
				if (named && deps.busy.get(named) === api.taskId) deps.busy.delete(named);
			}
		},
	});
}

type Api = Parameters<ToolRegistration["execute"]>[1];

/** Creates the call's child in one commit; returns an error for the model instead when there is none to continue. */
async function createChild(
	bridge: BridgeFrame,
	api: Api,
	subagent: ResolvedSubagent,
	args: { name?: string },
	depth: number,
	deps: SubagentDeps,
	context: Context,
): Promise<ConversationId | string> {
	const requestId = await currentRequest(bridge, api.conversationId, context);
	const names = (await bridge.harness.snapshot(SubagentNames, context))?.leads[api.conversationId];
	let source: { conversationId: ConversationId; at: EntryId } | undefined;
	const previous = args.name ? names?.children[args.name] : undefined;
	if (args.name) {
		if (!previous || previous.agent !== subagent.agent.name)
			return `There is no ${subagent.tool} child called ${args.name}. Call ${subagent.tool} without a name to start one.`;
		const at = (await entries(bridge, previous.conversationId as ConversationId, context))[0];
		if (!at) return `${args.name} has no history to continue.`;
		source = { conversationId: previous.conversationId as ConversationId, at: at.id };
	} else if (subagent.context === "fork") {
		// The lead's transcript up to the response that made this call.
		const list = await entries(bridge, api.conversationId, context);
		const call = list.findIndex((entry) =>
			entry.model?.some(
				(message) =>
					message.role === "assistant" &&
					message.content.some((block) => block.type === "toolCall" && block.id === api.callId),
			),
		);
		const at = call < 0 ? undefined : list[call + 1];
		if (!at) return "Could not find this conversation's position to fork from.";
		source = { conversationId: api.conversationId, at: at.id };
	}
	return api.commit(async (tx) => {
		// Ownership records the child: a rerun of this call finds it instead of creating another.
		const existing = (await tx.scanConversations({ ownerTaskId: api.taskId }, 1)).items[0];
		if (existing) return existing.id;
		const ownership = { kind: "task", taskId: api.taskId } as const;
		const record = source
			? await tx.forkConversation(source.conversationId, source.at, { ownership })
			: await tx.createConversation({ ownership });
		await configure(tx, record.id, deps.childAgent(subagent.agent, depth));
		const registry = await tx.doc(SubagentNames);
		registry.leads[api.conversationId] ??= { next: {}, children: {} };
		const own = registry.leads[api.conversationId]!;
		const name =
			args.name ??
			`${subagent.tool}-${(own.next[subagent.tool] = (own.next[subagent.tool] ?? 0) + 1)}`;
		own.children[name] = { agent: subagent.agent.name, conversationId: record.id };
		// A continued child keeps the names of its own children.
		const inherited = previous ? registry.leads[previous.conversationId] : undefined;
		if (inherited) registry.leads[record.id] = cloneJson(inherited);
		Object.assign(await tx.doc(Subagent, record.id), { agent: subagent.agent.name, name, depth });
		// No chat binding: the child acts for the lead's current request.
		(await tx.doc(Binding, record.id)).requestId = requestId;
		return record.id;
	}, context);
}

async function entries(bridge: BridgeFrame, conversationId: ConversationId, context: Context) {
	const conversation = await bridge.harness.conversation(conversationId, context);
	return conversation ? (await conversation.entries({}, 100, undefined, context)).items : [];
}

/** Submits the task once, shows the child's activity on the lead's status, and returns its answer as data. */
async function run(
	bridge: BridgeFrame,
	api: Api,
	child: ConversationId,
	name: string,
	subagent: ResolvedSubagent,
	task: string,
	deps: SubagentDeps,
	context: Context,
): Promise<ToolExecutionResult> {
	const handle = (await api.conversation(child, context))!;
	// The request id makes a rerun get back the submission it made before the restart.
	const submission = await handle.submit(
		{ type: "input", content: task, requestId: `fabrial.subagent:${api.taskId}` },
		context,
	);
	const label = subagent.agent.name;
	const status = bridge.status;
	let events: Awaited<ReturnType<typeof watchEvents>> | undefined;
	if (status) {
		const tools = (await (await bridge.harness.conversation(child, context))!.agent(context)).tools;
		events = await watchEvents(bridge.harness, child, context);
		events.start(async (batch) => {
			if (batch.every((event) => event.type === "tool_execution_update")) return;
			const tool = batch.findLast((event) => event.type === "tool_execution_start");
			const text =
				tool?.type === "tool_execution_start"
					? (tools.find((each) => each.name === tool.toolName)?.description ?? "Using a tool…")
					: "Thinking…";
			await status(`${label}: ${text}`);
		});
		await status(`${label}: Thinking…`);
	}
	bridge.waiting.add(api.taskId);
	let settled: Awaited<ReturnType<typeof submission.wait>>;
	try {
		settled = await submission.wait(context);
	} finally {
		bridge.waiting.delete(api.taskId);
		await events?.stop();
	}
	const details = { conversationId: child, name };
	if (settled.status === "done" && settled.type === "input") {
		const entry = await api.commit((tx) => tx.entry(settled.answer), context);
		const text =
			entry?.model
				?.flatMap((message) =>
					message.role === "assistant"
						? message.content.flatMap((block) => (block.type === "text" ? [block.text] : []))
						: [],
				)
				.join("\n") ?? "";
		return { content: [{ type: "text", text: subagentResult(name, "done", text) }], details };
	}
	// Provider details stay in logs; the lead sees a generic message with an id to find them.
	const fresh = newErrorId();
	const errorId = await api.memo("fabrial:subagent:error", fresh, context);
	if (errorId === fresh)
		deps.logger.error("Subagent failed", {
			subagent: name,
			agent: subagent.agent.name,
			errorId,
			reason: settled.status === "unanswered" ? settled.reason : settled.status,
			error: settled.status === "unanswered" ? settled.detail : undefined,
		});
	return {
		content: [
			{
				type: "text",
				text: subagentResult(
					name,
					"failed",
					`The subagent stopped without an answer (error id ${errorId}). You may retry or continue without it.`,
				),
			},
		],
		isError: true,
		details,
	};
}
