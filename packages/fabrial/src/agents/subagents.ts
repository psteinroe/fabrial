import {
	type AgentChange,
	configure,
	type ConversationId,
	defineDoc,
	defineTask,
	type EntryRecord,
	type Harness,
	defineTool as piTool,
	type TaskId,
	type ToolExecutionResult,
	type ToolRegistration,
	type Tx,
} from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import type { TSchema } from "@earendil-works/pi-ai";
import { type Agent, type ResolvedSubagent, SUBAGENT_DEFAULTS } from "./definition.ts";
import { type BridgeFrame, BridgeKey, currentRequest, frame } from "./tool-context.ts";
import { Binding, newErrorId, Obligations } from "../sessions/obligations.ts";

/** A subagent's agent and the subagent levels left below it; initial on a lead conversation. */
export const Subagent = defineDoc({
	kind: "fabrial.subagent",
	scope: "conversation",
	history: "latest",
	fork: "initial",
	version: 1,
	initial: (): { agent: string | null; depth: number | null } => ({ agent: null, depth: null }),
});

export interface SubagentDeps {
	/** The Pi agent of a child running `agent` with `depth` subagent levels left below it. */
	childAgent(agent: Agent, depth: number): AgentChange;
}

/** Task kinds of background children; installed with Fabrial's internal extension so they resume after a restart. */
const ANCHOR_TASK = "fabrial.subagent-anchor";
const REPORTER_TASK = "fabrial.subagent-reporter";

/**
 * A lead's background children by name (Pi example 23). `calls` makes a rerun of a background call find the reporter
 * it started; `reported` keeps several inputs that end in one answer from reporting it twice.
 */
const Subagents = defineDoc({
	kind: "fabrial.subagents",
	scope: "conversation",
	history: "latest",
	fork: "initial",
	version: 1,
	initial: (): {
		next: Record<string, number>;
		agents: Record<string, { agent: string; conversationId: number; reported: number[] }>;
		calls: Record<string, number>;
	} => ({ next: {}, agents: {}, calls: {} }),
});

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

const answerText = (entry: EntryRecord | undefined) =>
	entry?.model
		?.flatMap((message) =>
			message.role === "assistant"
				? message.content.flatMap((block) => (block.type === "text" ? [block.text] : []))
				: [],
		)
		.join("\n") ?? "";

/** Logs a child's failure with an id and returns the generic message the lead sees; provider details stay in logs. */
function failure(
	context: Context,
	errorId: string,
	details: { subagent: string; reason: string; error?: unknown },
) {
	context.value(BridgeKey)?.host.logger.error("Subagent failed", { ...details, errorId });
	return `The subagent stopped without an answer (error id ${errorId}). You may retry or continue without it.`;
}

/** Sets up a new child in the caller's commit: its agent, its subagent levels, and the lead request it acts for. */
async function setUp(
	tx: Tx,
	child: ConversationId,
	subagent: ResolvedSubagent,
	depth: number,
	requestId: string,
	deps: SubagentDeps,
) {
	await configure(tx, child, deps.childAgent(subagent.agent, depth));
	Object.assign(await tx.doc(Subagent, child), { agent: subagent.agent.name, depth });
	// No chat binding: the child acts for the lead's request.
	(await tx.doc(Binding, child)).requestId = requestId;
}

/**
 * One subagent as a Pi tool. A foreground call owns its child conversation (Pi example 22): aborting the call aborts
 * the child, and a rerun after a restart finds the same child through the ownership index and the same submission by
 * request id. A background call hands a named child to an anchor task and each task to a reporter (Pi example 23).
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
	};
	if (subagent.background !== "never") {
		properties.name = {
			type: "string",
			description: `Continue an earlier background ${subagent.tool} child by its name (e.g. ${subagent.tool}-1), keeping its history`,
		};
		properties.background = {
			type: "boolean",
			description: "Run in the background and get the answer later as input",
		};
	}
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
			const args = input as { task: string; background?: boolean; name?: string };
			if (subagent.background === "never" && (args.background || args.name))
				return error(`${subagent.tool} cannot run in the background; call it without background.`);
			const bridge = frame(context);
			const depth =
				(await api.snapshot(Subagent, api.conversationId, context))?.depth ?? limits.depth;
			if (depth < 1) return error("This conversation cannot start subagents.");
			if (args.name || args.background || subagent.background === "always")
				return background(bridge, api, subagent, args, depth - 1, deps, context);
			const requestId = await currentRequest(bridge, api.conversationId, context);
			const child = await api.commit(async (tx) => {
				// Ownership records the child: a rerun of this call finds it instead of creating another.
				const existing = (await tx.scanConversations({ ownerTaskId: api.taskId }, 1)).items[0];
				if (existing) return existing.id;
				const created = await tx.createConversation({
					ownership: { kind: "task", taskId: api.taskId },
				});
				await setUp(tx, created.id, subagent, depth - 1, requestId, deps);
				return created.id;
			}, context);
			await api.details({ conversationId: child }, context);
			// The request id makes a rerun get back the submission it made before the restart.
			const submitted = `fabrial.subagent:${api.taskId}`;
			const submission = await (await api.conversation(child, context))!.submit(
				{ type: "input", content: args.task, requestId: submitted },
				context,
			);
			// Waiting on its child does not keep the Session open: a child waiting on a workflow parks it.
			bridge.waiting.add(api.taskId);
			const settled = await submission
				.wait(context)
				.finally(() => bridge.waiting.delete(api.taskId));
			const details = { conversationId: child };
			if (settled.status === "done" && settled.type === "input") {
				const answer = answerText(await api.commit((tx) => tx.entry(settled.answer), context));
				return {
					content: [{ type: "text", text: subagentResult(subagent.tool, "done", answer) }],
					details,
				};
			}
			const errorId = await api.memo("fabrial:subagent:error", newErrorId(), context);
			const message = failure(context, errorId, {
				subagent: subagent.tool,
				reason: settled.status === "unanswered" ? settled.reason : settled.status,
				...(settled.status === "unanswered" ? { error: settled.detail } : {}),
			});
			return {
				content: [{ type: "text", text: subagentResult(subagent.tool, "failed", message) }],
				isError: true,
				details,
			};
		},
	});
}

type Api = Parameters<ToolRegistration["execute"]>[1];
const BACKGROUND = { ownership: { kind: "conversation" }, background: true } as const;

/**
 * Starts or continues a named background child and returns at once. A new child is owned by an anchor task, a
 * background task of the lead that ends at once, so the lead's aborts and idle waits stop there. A reporter task, also
 * background, submits the task to the child and the answer to the lead. One commit records both, keyed by this call,
 * so a rerun returns the same child.
 */
async function background(
	bridge: BridgeFrame,
	api: Api,
	subagent: ResolvedSubagent,
	args: { task: string; name?: string },
	depth: number,
	deps: SubagentDeps,
	context: Context,
): Promise<ToolExecutionResult> {
	const state = await api.snapshot(Subagents, api.conversationId, context);
	if (args.name && state?.agents[args.name]?.agent !== subagent.agent.name)
		return error(
			`There is no ${subagent.tool} child called ${args.name}. Call ${subagent.tool} without a name to start one.`,
		);
	let reporter = state?.calls[api.taskId];
	if (reporter === undefined) {
		const request = await currentRequest(bridge, api.conversationId, context);
		reporter = await api.commit(async (tx) => {
			const own = await tx.doc(Subagents, api.conversationId);
			if (own.calls[api.taskId] !== undefined) return own.calls[api.taskId]!;
			let name = args.name;
			if (!name) {
				const anchor = await tx.createTask(Anchor, null, BACKGROUND);
				const child = await tx.createConversation({ ownership: { kind: "task", taskId: anchor } });
				await setUp(tx, child.id, subagent, depth, request, deps);
				name = `${subagent.tool}-${(own.next[subagent.tool] = (own.next[subagent.tool] ?? 0) + 1)}`;
				own.agents[name] = { agent: subagent.agent.name, conversationId: child.id, reported: [] };
			}
			const child = own.agents[name]!.conversationId as ConversationId;
			const input = { lead: api.conversationId, child, name, task: args.task, request };
			const id = await tx.createTask(Reporter, input, BACKGROUND);
			own.calls[api.taskId] = id;
			// Before the reporter submits the task: the child's run acts for this request, and stopping it reaches the child.
			(await tx.doc(Obligations)).requests[taskRequest(id)] = {
				conversationId: child,
				owes: "task",
				origin: request,
			};
			return id;
		}, context);
	}
	const { child, name } = (await api.commit((tx) => tx.task(reporter as TaskId), context))!
		.input as ReporterInput;
	await api.details({ conversationId: child, name }, context);
	const text = args.name
		? `Sent the task to ${name}. Its answer arrives later as input <subagent name="${name}">.`
		: `Started ${name} in the background. Its answer arrives later as input <subagent name="${name}">; continue meanwhile.`;
	return { content: [{ type: "text", text }], details: { conversationId: child, name } };
}

const terminal = (status: "completed" | "aborted") =>
	({
		status: "terminal",
		outcome: status === "completed" ? { status, result: null } : { status },
	}) as const;

/** Owns a background child: ends at once, so it is a background boundary that a host can still abort through. */
const Anchor = defineTask<null, { phase: "done" }, null>({
	name: ANCHOR_TASK,
	version: 1,
	initial: () => ({ phase: "done" }),
	phases: {
		done: (_anchor, runtime, context) => runtime.commit(() => terminal("completed"), context),
	},
	abort: (_anchor, runtime, context) => runtime.commit(() => terminal("aborted"), context),
});

type ReporterInput = {
	lead: ConversationId;
	child: ConversationId;
	name: string;
	task: string;
	/** The lead request the child acts for; its report answers there too. */
	request: string;
};
type ReporterState = { phase: "deliver" } | { phase: "report"; report?: string };

/** Request ids of a background child's task and of its report to the lead, by reporter task. */
const taskRequest = (reporter: number) => `fabrial.subagent:${reporter}`;
const reportRequest = (reporter: number) => `fabrial.subagent-report:${reporter}`;

/**
 * Delivers one task to a background child and reports its answer to the lead as a `followUp` (Pi example 23). Each
 * phase ends in a checkpoint and request ids dedupe both submissions, so a restart delivers and reports once.
 */
const Reporter = defineTask<ReporterInput, ReporterState, null>({
	name: REPORTER_TASK,
	version: 1,
	initial: () => ({ phase: "deliver" }),
	phases: {
		deliver: async (reporter, runtime, context) => {
			const { lead, child, name, task, request } = reporter.input;
			// Steers a busy child; starts a run on an idle one.
			const submission = await (await runtime.conversation(child, context))!.submit(
				{ type: "input", content: task, requestId: taskRequest(reporter.id), whenBusy: "steer" },
				context,
			);
			const settled = await submission.wait(context);
			const failed =
				settled.status === "unanswered" && settled.reason !== "aborted"
					? failure(context, newErrorId(), {
							subagent: name,
							reason: settled.reason,
							error: settled.detail,
						})
					: undefined;
			// One commit decides the report, records the answer as reported, and registers the report's reply obligation
			// before it is submitted, so a restart neither decides again nor loses where the lead's answer posts.
			await runtime.commit(async (tx) => {
				let report: string | undefined;
				// Stopped, or withdrawn while queued: nothing to report.
				if (settled.status === "unanswered")
					report = failed && subagentResult(name, "failed", failed);
				else if (settled.type === "input") {
					const reported = (await tx.doc(Subagents, lead)).agents[name]!.reported;
					if (!reported.includes(settled.answer)) {
						reported.push(settled.answer);
						report = subagentResult(name, "done", answerText(await tx.entry(settled.answer)));
					}
				}
				// The report acts for the request that started the child: the lead's answer posts to its reply thread.
				if (report !== undefined)
					(await tx.doc(Obligations)).requests[reportRequest(reporter.id)] ??= {
						conversationId: lead,
						owes: "reply",
						origin: request,
					};
				return {
					status: "running",
					checkpoint: { phase: "report", ...(report === undefined ? {} : { report }) },
				};
			}, context);
		},
		report: async (reporter, runtime, context) => {
			const { report } = reporter.state.checkpoint as { report?: string };
			// A follow-up starts a run on an idle lead or waits for its current answer; Pi dedupes a replay by request id.
			if (report !== undefined) {
				const requestId = reportRequest(reporter.id);
				await (await runtime.conversation(reporter.input.lead, context))!.submit(
					{ type: "input", content: report, requestId, whenBusy: "followUp" },
					context,
				);
				context.value(BridgeKey)?.host.logger.debug("Reported to the lead", { requestId });
			}
			await runtime.commit(() => terminal("completed"), context);
		},
	},
	abort: (_reporter, runtime, context) => runtime.commit(() => terminal("aborted"), context),
});

/** The background task definitions; installed with an extension so they resume after a restart. */
export const subagentTasks = [Anchor, Reporter];

/** `subagent_stop`: aborts a background child's current work and drops its queued tasks (Pi `conversation.abort`). */
export function stopTool(): ToolRegistration {
	return piTool({
		name: "subagent_stop",
		description:
			"Stop a background subagent's current work by its name. It keeps its history and can be continued later.",
		parameters: {
			type: "object",
			properties: {
				name: { type: "string", description: "The subagent's name, e.g. researcher-1" },
			},
			required: ["name"],
			additionalProperties: false,
		} as unknown as TSchema,
		// A rerun after a crash could stop newer work; the model sees the interrupted call instead.
		replay: "unsafe",
		execute: async (input, api, context) => {
			const { name } = input as { name: string };
			const child = (await api.snapshot(Subagents, api.conversationId, context))?.agents[name];
			if (!child) return error(`There is no subagent called ${name}.`);
			await (await api.conversation(child.conversationId as ConversationId, context))!.abort(
				context,
			);
			return { content: [{ type: "text", text: `Stopped ${name}. It can be continued by name.` }] };
		},
	});
}

/** Whether background children of the Session still have to answer or report: they keep it open. */
export async function backgroundPending(harness: Harness, context: Context): Promise<boolean> {
	return (await harness.inspect(context)).tasks.some((task) => task.record.kind === REPORTER_TASK);
}
