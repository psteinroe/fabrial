import type { Context } from "@earendil-works/chord";
import type { TSchema } from "@earendil-works/pi-ai";
import {
	configure,
	defineTask,
	defineTool as piTool,
	LiveDoc,
	section as piSection,
	watchEvents,
	type AgentChange,
	type ConversationId,
	type EntryId,
	type EntryRecord,
	type Harness,
	type SubmissionId,
	type TaskId,
	type ToolExecutionResult,
	type ToolRegistration,
	type Tx,
} from "@earendil-works/pi-durable";
import { SUBAGENT_DEFAULTS, type Agent, type ResolvedSubagent } from "../agent.ts";
import type { Logger } from "../app.ts";
import { BridgeKey, cloneJson, currentRequest, frame, type BridgeFrame } from "./context.ts";
import { Binding, Invocation, Subagent, SubagentNames, SubagentWork } from "./documents.ts";
import { newErrorId } from "./errors.ts";

export interface SubagentDeps {
	/** The Pi agent of a child running `agent` with `depth` subagent levels left below it. */
	childAgent(agent: Agent, depth: number): AgentChange;
	/** Records the agent a conversation runs, for observers. */
	named(conversationId: number, agent: string): void;
	logger: Logger;
	/** The call working on each named foreground child. */
	busy: Map<string, number>;
	/** This integration's reporter task, from `reporterTask`. */
	reporter?: ReturnType<typeof reporterTask>;
}

/** Task kinds of background children; installed with Fabrial's internal extension so they resume after a restart. */
export const ANCHOR_TASK = "fabrial.subagent-anchor";
export const REPORTER_TASK = "fabrial.subagent-reporter";

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

/** Logs a child's failure once per id and returns the generic message the lead sees. */
function failure(
	deps: Pick<SubagentDeps, "logger">,
	errorId: string,
	fresh: boolean,
	details: { subagent: string; agent: string; reason: string; error?: unknown },
): string {
	// Provider details stay in logs; the lead sees a generic message with an id to find them.
	if (fresh) deps.logger.error("Subagent failed", { ...details, errorId });
	return `The subagent stopped without an answer (error id ${errorId}). You may retry or continue without it.`;
}

/** A slot whose child answered: it no longer counts, though its holder task may still be finishing. */
const FREED = "";

/** Frees `holder`'s slot in the commit that settles its child's answer. */
async function free(tx: Tx, lead: number, holder: number): Promise<void> {
	const slots = (await tx.doc(SubagentWork)).slots[lead];
	if (slots?.[holder] !== undefined) slots[holder] = FREED;
}

/**
 * Takes a slot of the lead's `limits.concurrent` for `holder`, or only checks for one without a holder. Slots are
 * durable and held by a task, so they count background children across driver cycles. A slot frees with the
 * commit that settles its child's answer, or when its holder ends without one. Returns the number running when the limit is reached.
 */
async function admit(
	tx: Tx,
	lead: number,
	holder: number | undefined,
	child: string,
	limit: number,
): Promise<number | undefined> {
	const work = await tx.doc(SubagentWork);
	// Read back what `??=` stores: changes to the assigned literal itself are not tracked.
	work.slots[lead] ??= {};
	const slots = work.slots[lead]!;
	// Admission is durable, so a rerun after a restart is never turned away by the limit.
	if (holder !== undefined && slots[holder] !== undefined) return undefined;
	for (const id of Object.keys(slots)) {
		const task = await tx.task(Number(id) as TaskId);
		if (!task || task.state.status === "terminal") delete slots[id];
	}
	// A freed slot keeps its holder, so a rerun of a call whose child already answered is still admitted.
	const running = new Set(Object.values(slots).filter((child) => child !== FREED));
	if (!running.has(child) && running.size >= limit) return running.size;
	if (holder !== undefined) slots[holder] = child;
	return undefined;
}

/**
 * One subagent as a Pi tool. A foreground call owns its child conversation (Pi example 22), so aborting the call
 * aborts the child, and a rerun after a restart finds the same child through the ownership index and the same
 * submission by request id. A named foreground child is continued as a fork of its newest conversation, owned by the
 * continuing call. A background call hands the child to an anchor task and the task to a reporter (Pi example 23).
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
	const full = (running: number) =>
		`${running} subagents are already running, the limit for this conversation. Wait for one of them to answer, then call ${subagent.tool} again.`;
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
			const depth =
				(await bridge.harness.snapshot(Subagent, api.conversationId, context))?.depth ??
				limits.depth;
			if (depth < 1) return error("This conversation cannot start subagents.");
			const names = (await bridge.harness.snapshot(SubagentNames, context))?.leads[
				api.conversationId
			];
			const previous = args.name ? names?.children[args.name] : undefined;
			if (args.name && (!previous || previous.agent !== subagent.agent.name))
				return error(
					`There is no ${subagent.tool} child called ${args.name}. Call ${subagent.tool} without a name to start one.`,
				);
			// A background child stays in the background: continuing it never waits for its answer.
			if (args.background || subagent.background === "always" || previous?.background)
				return background(bridge, api, subagent, args, depth - 1, limits.concurrent, deps, context);
			const refused = await api.commit(
				(tx) =>
					admit(
						tx,
						api.conversationId,
						api.taskId,
						args.name ?? `#${api.taskId}`,
						limits.concurrent,
					),
				context,
			);
			if (refused !== undefined) return error(full(refused));
			const scope = `${bridge.session}:${api.conversationId}`;
			let named: string | undefined;
			try {
				let child = await api.commit(
					async (tx) => (await tx.scanConversations({ ownerTaskId: api.taskId }, 1)).items[0]?.id,
					context,
				);
				if (child === undefined) {
					const source = await sourceOf(bridge, api, subagent, previous, context);
					if (typeof source === "string") return error(source);
					const requestId = await currentRequest(bridge, api.conversationId, context);
					child = await api.commit(async (tx) => {
						// Ownership records the child: a rerun of this call finds it instead of creating another.
						const existing = (await tx.scanConversations({ ownerTaskId: api.taskId }, 1)).items[0];
						if (existing) return existing.id;
						const ownership = { kind: "task", taskId: api.taskId } as const;
						const created = await newChild(tx, api.conversationId, subagent, args.name, {
							ownership,
							source,
							previous,
							depth: depth - 1,
							requestId,
							background: false,
							deps,
						});
						(await tx.doc(SubagentWork)).slots[api.conversationId]![api.taskId] = created.name;
						return created.id;
					}, context);
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
				if (named && deps.busy.get(named) === api.taskId) deps.busy.delete(named);
			}
		},
	});
}

type Api = Parameters<ToolRegistration["execute"]>[1];
type Previous = { agent: string; conversationId: number; background?: boolean };
type Source = { conversationId: ConversationId; at: EntryId } | undefined;

/** Where a new child starts: a continued foreground child's newest entry, the lead's transcript for a fork, or empty. */
async function sourceOf(
	bridge: BridgeFrame,
	api: Api,
	subagent: ResolvedSubagent,
	previous: Previous | undefined,
	context: Context,
): Promise<Source | string> {
	if (previous) {
		const at = (await entries(bridge, previous.conversationId as ConversationId, context))[0];
		if (!at) return "That child has no history to continue.";
		return { conversationId: previous.conversationId as ConversationId, at: at.id };
	}
	if (subagent.context !== "fork") return undefined;
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
	return { conversationId: api.conversationId, at: at.id };
}

/** Creates and names a child in the caller's commit. A continued child keeps its name and its own children. */
async function newChild(
	tx: Tx,
	lead: ConversationId,
	subagent: ResolvedSubagent,
	requested: string | undefined,
	options: {
		ownership: { kind: "task"; taskId: TaskId };
		source: Source;
		previous: Previous | undefined;
		depth: number;
		requestId: string;
		background: boolean;
		deps: SubagentDeps;
	},
): Promise<{ id: ConversationId; name: string }> {
	const { ownership, source, previous, depth } = options;
	const record = source
		? await tx.forkConversation(source.conversationId, source.at, { ownership })
		: await tx.createConversation({ ownership });
	await configure(tx, record.id, options.deps.childAgent(subagent.agent, depth));
	const registry = await tx.doc(SubagentNames);
	registry.leads[lead] ??= { next: {}, children: {} };
	const own = registry.leads[lead]!;
	const name =
		requested ??
		`${subagent.tool}-${(own.next[subagent.tool] = (own.next[subagent.tool] ?? 0) + 1)}`;
	own.children[name] = {
		agent: subagent.agent.name,
		conversationId: record.id,
		...(options.background ? { background: true } : {}),
	};
	const inherited = previous ? registry.leads[previous.conversationId] : undefined;
	if (inherited) registry.leads[record.id] = cloneJson(inherited);
	Object.assign(await tx.doc(Subagent, record.id), { agent: subagent.agent.name, name, depth });
	// No chat binding: the child acts for the lead's current request.
	(await tx.doc(Binding, record.id)).requestId = options.requestId;
	return { id: record.id, name };
}

/**
 * Starts or continues a background child and returns at once. A new child is owned by an anchor task, a background
 * task of the lead that ends at once, so the lead's aborts and idle waits stop there. A reporter task, also
 * background, submits the task to the child and the answer to the lead. One commit records both, keyed by this call,
 * so a rerun returns the same child.
 */
async function background(
	bridge: BridgeFrame,
	api: Api,
	subagent: ResolvedSubagent,
	args: { task: string; name?: string },
	depth: number,
	limit: number,
	deps: SubagentDeps,
	context: Context,
): Promise<ToolExecutionResult> {
	const names = (await bridge.harness.snapshot(SubagentNames, context))?.leads[api.conversationId];
	const previous = args.name ? names?.children[args.name] : undefined;
	let reporter = (await bridge.harness.snapshot(SubagentWork, context))?.calls[api.taskId];
	if (reporter === undefined) {
		const source = previous?.background
			? undefined
			: await sourceOf(bridge, api, subagent, previous, context);
		if (typeof source === "string") return error(source);
		const requestId = await currentRequest(bridge, api.conversationId, context);
		const run = (await bridge.runOf(api.conversationId, context)).id;
		const created = await api.commit(async (tx) => {
			const work = await tx.doc(SubagentWork);
			if (work.calls[api.taskId] !== undefined) return work.calls[api.taskId]!;
			const refused = await admit(tx, api.conversationId, undefined, args.name ?? "", limit);
			if (refused !== undefined) return `${refused}`;
			let child = previous?.background
				? { id: previous.conversationId as ConversationId, name: args.name! }
				: undefined;
			if (!child) {
				const anchor = await tx.createTask(Anchor, null, {
					ownership: { kind: "conversation" },
					background: true,
				});
				child = await newChild(tx, api.conversationId, subagent, args.name, {
					ownership: { kind: "task", taskId: anchor },
					source,
					previous,
					depth,
					requestId,
					background: true,
					deps,
				});
			}
			const id = await tx.createTask(
				deps.reporter!,
				{
					lead: api.conversationId,
					child: child.id,
					name: child.name,
					agent: subagent.agent.name,
					task: args.task,
					request: requestId,
				},
				{ ownership: { kind: "conversation" }, background: true },
			);
			work.calls[api.taskId] = id;
			work.reporters[id] = {
				lead: api.conversationId,
				child: child.id,
				name: child.name,
				run,
				report: null,
			};
			work.slots[api.conversationId]![id] = child.name;
			return id;
		}, context);
		if (typeof created === "string")
			return error(
				`${created} subagents are already running, the limit for this conversation. Wait for one of them to answer, then call ${subagent.tool} again.`,
			);
		reporter = created;
	}
	const started = (await bridge.harness.snapshot(SubagentWork, context))!.reporters[reporter]!;
	deps.named(started.child, subagent.agent.name);
	await api.details({ conversationId: started.child, name: started.name }, context);
	const text = previous?.background
		? `Sent the task to ${started.name}. Its answer arrives later as input <subagent name="${started.name}">.`
		: `Started ${started.name} in the background. Its answer arrives later as input <subagent name="${started.name}">; continue meanwhile.`;
	return {
		content: [{ type: "text", text }],
		details: { conversationId: started.child, name: started.name },
	};
}

/** Owns a background child: ends at once, so it is a background boundary that a host can still abort through. */
const Anchor = defineTask<null, { phase: "done" }, null>({
	name: ANCHOR_TASK,
	version: 1,
	initial: () => ({ phase: "done" }),
	phases: {
		done: (_anchor, runtime, context) =>
			runtime.commit(
				() => ({ status: "terminal", outcome: { status: "completed", result: null } }),
				context,
			),
	},
	abort: (_anchor, runtime, context) =>
		runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context),
});

type ReporterInput = {
	lead: number;
	child: number;
	name: string;
	agent: string;
	task: string;
	/** The lead request the child acts for; its report answers there too. */
	request: string;
};
type ReporterState = { phase: "deliver" } | { phase: "report"; report?: string };

/** The background task definitions; ship them with an installed extension so they resume after a restart. */
export const subagentTasks = (deps: SubagentDeps) => [
	Anchor,
	(deps.reporter ??= reporterTask(deps)),
];

/**
 * Delivers one task to a background child and reports its answer to the lead as a `followUp` (Pi example 23). Each
 * phase ends in a checkpoint and request ids dedupe both submissions, so a restart delivers and reports once.
 */
const reporterTask = (deps: SubagentDeps) =>
	defineTask<ReporterInput, ReporterState, null>({
		name: REPORTER_TASK,
		version: 1,
		initial: () => ({ phase: "deliver" }),
		phases: {
			deliver: async (reporter, runtime, context) => {
				const { child, name, agent, task } = reporter.input;
				deps.named(child, agent);
				const handle = (await runtime.conversation(child as ConversationId, context))!;
				// Steers a busy child; starts a run on an idle one.
				const submission = await handle.submit(
					{
						type: "input",
						content: task,
						requestId: `fabrial.subagent:${reporter.id}`,
						whenBusy: "steer",
					},
					context,
				);
				const bridge = context.value(BridgeKey);
				const stop = bridge
					? await watchActivity(bridge, child as ConversationId, agent, context)
					: undefined;
				let settled: Awaited<ReturnType<typeof submission.wait>>;
				try {
					settled = await submission.wait(context);
				} finally {
					await stop?.();
				}
				await bridge?.clearStatus?.(agent);
				const errorId = newErrorId();
				const failed =
					settled.status === "unanswered" && settled.reason !== "aborted"
						? failure(deps, errorId, true, {
								subagent: name,
								agent,
								reason: settled.reason,
								error: settled.detail,
							})
						: undefined;
				// One commit decides the report and records the answer as reported, so a restart does not decide again.
				await runtime.commit(async (tx) => {
					await free(tx, reporter.input.lead, reporter.id);
					const next = (report?: string) =>
						({
							status: "running",
							checkpoint: { phase: "report", ...(report === undefined ? {} : { report }) },
						}) as const;
					// Stopped, or withdrawn while queued: nothing to report.
					if (settled.status === "unanswered")
						return next(failed && subagentResult(name, "failed", failed));
					if (settled.type !== "input") return next();
					const work = await tx.doc(SubagentWork);
					work.reported[child] ??= [];
					const reported = work.reported[child]!;
					if (reported.includes(settled.answer)) return next();
					reported.push(settled.answer);
					return next(subagentResult(name, "done", answerText(await tx.entry(settled.answer))));
				}, context);
			},
			report: async (reporter, runtime, context) => {
				const { report } = reporter.state.checkpoint as { report?: string };
				let submissionId: number | null = null;
				if (report !== undefined) {
					const requestId = `fabrial.subagent-report:${reporter.id}`;
					// The report acts for the request that started the child, so `reply` answers it there.
					const invocation = await runtime.snapshot(Invocation, reporter.input.request, context);
					if (invocation)
						await runtime.commit(async (tx) => {
							await tx.doc(Invocation, requestId, cloneJson(invocation));
							return undefined;
						}, context);
					// A follow-up starts a run on an idle lead or waits for its current answer.
					const lead = (await runtime.conversation(
						reporter.input.lead as ConversationId,
						context,
					))!;
					submissionId = (
						await lead.submit(
							{ type: "input", content: report, requestId, whenBusy: "followUp" },
							context,
						)
					).id;
				}
				await runtime.commit(async (tx) => {
					const entry = (await tx.doc(SubagentWork)).reporters[reporter.id];
					if (entry) entry.report = submissionId;
					return { status: "terminal", outcome: { status: "completed", result: null } };
				}, context);
			},
		},
		abort: (_reporter, runtime, context) =>
			runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context),
	});

/** `subagent_stop`: aborts a child's current work and drops its queued tasks (Pi `conversation.abort`). */
export function stopTool(): ToolRegistration {
	return piTool({
		name: "subagent_stop",
		description:
			"Stop a subagent's current work by its name. It keeps its history and can be continued later.",
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
			const child = (await frame(context).harness.snapshot(SubagentNames, context))?.leads[
				api.conversationId
			]?.children[name];
			if (!child) return error(`There is no subagent called ${name}.`);
			await (await api.conversation(child.conversationId as ConversationId, context))!.abort(
				context,
			);
			return { content: [{ type: "text", text: `Stopped ${name}. It can be continued by name.` }] };
		},
	});
}

/** Lists the lead's children and whether each is working, from durable state; rendered only when there are some. */
export const subagentsSection = piSection("subagents", async (input, context) => {
	const bridge = context.value(BridgeKey);
	const children = bridge
		? ((await bridge.harness.snapshot(SubagentNames, context))?.leads[input.conversationId]
				?.children ?? {})
		: {};
	const lines: string[] = [];
	for (const [name, child] of Object.entries(children)) {
		const live = await bridge!.harness.snapshot(
			LiveDoc,
			child.conversationId as ConversationId,
			context,
		);
		lines.push(
			`- ${name} (${child.agent}${child.background ? ", background" : ""}): ${live?.run ? "working" : "idle"}`,
		);
	}
	if (!lines.length) return "";
	return [
		"Your subagents. Call a subagent's tool with its name to continue it, or subagent_stop to stop it.",
		...lines,
	].join("\n");
});

/** Whether background children of the Session still have to answer or report. */
export async function backgroundPending(harness: Harness, context: Context): Promise<boolean> {
	return (await harness.inspect(context)).tasks.some(
		(task) => task.record.kind === REPORTER_TASK && task.record.state.status !== "terminal",
	);
}

/** The lead's answer to the newest report of children a run started, once it has answered it. */
export async function reportedAnswer(
	harness: Harness,
	run: string,
	lead: ConversationId,
	context: Context,
): Promise<string | undefined> {
	const work = await harness.snapshot(SubagentWork, context);
	const reports = Object.values(work?.reporters ?? {})
		.filter((entry) => entry.run === run && entry.lead === lead && entry.report !== null)
		.map((entry) => entry.report!)
		.sort((a, b) => a - b);
	const newest = reports.at(-1);
	if (newest === undefined) return undefined;
	const status = await (await harness.submission(newest as SubmissionId, context))?.status(context);
	if (status?.status !== "done" || status.type !== "input") return undefined;
	return answerText(await harness.commit((tx) => tx.entry(status.answer), context));
}

/**
 * Aborts the background children a run started or continued, for cancelling its interaction. Pi's background
 * boundary keeps them alive when only the lead's run is aborted; cancellation reaches past it explicitly.
 */
export async function abortBackground(
	harness: Harness,
	run: string,
	context: Context,
): Promise<void> {
	const work = await harness.snapshot(SubagentWork, context);
	const inspection = await harness.inspect(context);
	const live = new Set(inspection.tasks.map((task) => task.record.id as number));
	for (const [id, entry] of Object.entries(work?.reporters ?? {})) {
		if (entry.run !== run || !live.has(Number(id))) continue;
		await harness.abortTask(Number(id) as TaskId, context);
		for (const submission of inspection.submissions)
			if (submission.requestId === `fabrial.subagent:${id}`)
				await harness.abortSubmission(submission.id, context);
		const child = await harness.snapshot(LiveDoc, entry.child as ConversationId, context);
		if (child?.run) await harness.abortTask(child.run.taskId, context);
	}
}

async function entries(bridge: BridgeFrame, conversationId: ConversationId, context: Context) {
	const conversation = await bridge.harness.conversation(conversationId, context);
	return conversation ? (await conversation.entries({}, 100, undefined, context)).items : [];
}

/** Shows a child's activity on the lead's status ("researcher: Thinking…") until the returned stop is called. */
async function watchActivity(
	bridge: BridgeFrame,
	child: ConversationId,
	label: string,
	context: Context,
): Promise<() => Promise<void>> {
	const status = bridge.status;
	if (!status) return async () => {};
	const tools = (await (await bridge.harness.conversation(child, context))!.agent(context)).tools;
	const events = await watchEvents(bridge.harness, child, context);
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
	return async () => {
		await events.stop();
	};
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
	const stop = await watchActivity(bridge, child, subagent.agent.name, context);
	bridge.waiting.add(api.taskId);
	let settled: Awaited<ReturnType<typeof submission.wait>>;
	try {
		settled = await submission.wait(context);
	} finally {
		bridge.waiting.delete(api.taskId);
		await stop();
	}
	await bridge.clearStatus?.(subagent.agent.name);
	const details = { conversationId: child, name };
	const answered =
		settled.status === "done" && settled.type === "input" ? settled.answer : undefined;
	const entry = await api.commit(async (tx) => {
		await free(tx, api.conversationId, api.taskId);
		return answered === undefined ? undefined : tx.entry(answered);
	}, context);
	if (answered !== undefined) {
		return {
			content: [{ type: "text", text: subagentResult(name, "done", answerText(entry)) }],
			details,
		};
	}
	const fresh = newErrorId();
	const errorId = await api.memo("fabrial:subagent:error", fresh, context);
	const message = failure(deps, errorId, errorId === fresh, {
		subagent: name,
		agent: subagent.agent.name,
		reason: settled.status === "unanswered" ? settled.reason : settled.status,
		error: settled.status === "unanswered" ? settled.detail : undefined,
	});
	return {
		content: [
			{
				type: "text",
				text: subagentResult(name, "failed", message),
			},
		],
		isError: true,
		details,
	};
}
