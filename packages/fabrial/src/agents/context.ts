import { defineTool as piTool, LiveDoc } from "@earendil-works/pi-durable";
import type { TSchema } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import { createContextKey } from "@earendil-works/chord/context";
import type {
	ConversationId,
	Harness,
	HookApi,
	ToolExecutionApi,
	ToolRegistration,
} from "@earendil-works/pi-durable";
import type { SendReceipt } from "../send.ts";
import {
	storedFollowOptions,
	type ConversationRef,
	type FollowReceipt,
	type UnfollowReceipt,
} from "../follows.ts";
import {
	parseSchema,
	type AnyWorkflow,
	type DurableExecution,
	type EvaluateResult,
	type Evaluator,
	type FabrialHost,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type StateDefinition,
	type ThreadIO,
	type ToolStateHandle,
} from "../modules.ts";
import { bindMetadata } from "../context.ts";
import {
	applyState,
	Binding,
	ChildResults,
	Conversations,
	stateDraft,
	Invocation,
	Obligations,
	Subagent,
	ChildIntents,
	EFFECT_WORKFLOW,
	REPORT_WORKFLOW,
	type ChildIntent,
} from "./documents.ts";

export interface BridgeFrame {
	host: FabrialHost;
	execution: DurableExecution;
	evaluator: Evaluator;
	harness: Harness;
	/** The Pi Session key this execution hosts. */
	session: string;
	/**
	 * Tasks waiting on a workflow (or a managed effect, e.g. a thread post), by task id; while every running task
	 * waits, the Session's execution closes Pi and suspends until one of them settles or input arrives.
	 */
	children: Map<number, { executionId: string; detached: boolean }>;
	/** Subagent calls waiting for their child's answer; they do not keep the Session open. */
	waiting: Set<number>;
	/**
	 * Resolves with a waited child's committed result, memoized per child: once its reporter settled, or as the
	 * reporter's `child` signal arrives. Without one (the reporter was cancelled, or signalled an earlier Session
	 * execution) it waits until the Session closes, and the tool runs again when the Session's execution resumes on the
	 * reporter's settlement. Frees the task's slot in `children`.
	 */
	waitChild(taskId: number, executionId: string, context: Context): Promise<JsonObject>;
	/** Shows a status on the lead's progress surface, if it has one. */
	status?: (text: string) => Promise<void>;
	/** Clears a child's activity shown with `status` under `label`, once the child answered. */
	clearStatus?: (label: string) => Promise<void>;
}
export const BridgeKey = createContextKey<BridgeFrame>("fabrial.pi.bridge");
/** Tool context without the per-call signal. */
export type FabrialFields = Omit<import("../agent.ts").ToolContext, "signal">;
export type StagedState = {
	definition: StateDefinition;
	changes: ((draft: JsonObject) => void | JsonObject)[];
	value: JsonObject;
};
export const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

export function frame(context: Context): BridgeFrame {
	const value = context.value(BridgeKey);
	if (!value) throw new Error("Fabrial tools and instructions must run inside an agent run");
	return value;
}

export async function fields(
	context: Context,
	conversationId: ConversationId,
	api?: ToolExecutionApi,
	staged = new Map<string, StagedState>(),
	detached = false,
): Promise<FabrialFields> {
	const bridge = frame(context);
	const requestId = await currentRequest(bridge, conversationId, context);
	const invocation = (await bridge.harness.snapshot(Invocation, requestId, context))!;
	const obligation = (await bridge.harness.snapshot(Obligations, context))?.requests[requestId];
	if (!obligation) throw new Error(`Missing Fabrial obligation for request ${requestId}`);
	// Tools, and the workflows they start, follow in this conversation by default. A subagent's tools follow in the
	// root conversation: the Session is the lead's, the request is the lead's, and replyTo is that request's thread.
	const metadata: InvocationMetadata = {
		...(invocation as InvocationMetadata),
		conversation: {
			session: bridge.session,
			agent: await agentOf(bridge.harness, obligation.conversationId as ConversationId, context),
		} satisfies ConversationRef,
	};
	const prefix = `fabrial:pi:${conversationId}:${api?.taskId ?? "section"}`;
	const ids = new Map<string, number>();
	const operationId = (id: string) => {
		if (id.startsWith("fabrial:") || id.includes("#"))
			throw new Error(`Reserved operation id: ${id}`);
		const count = ids.get(id) ?? 0;
		ids.set(id, count + 1);
		return count ? `${id}#${count}` : id;
	};
	/** `child` is a reporter (`waited` start): its result is the committed outcome of the workflow it ran. */
	const childResult = async (child: string): Promise<Json> => {
		let result = (await bridge.harness.snapshot(ChildResults, context))?.results[child];
		if (!result) {
			await registerChild(
				bridge,
				requestId,
				api!.taskId,
				{ executionId: child, detached: false },
				context,
			);
			result = await bridge.waitChild(api!.taskId, child, context);
		}
		if (result.status !== "completed")
			throw new Error(JSON.stringify(result.error ?? result.reason ?? result.status));
		return result.output ?? null;
	};
	const operation = async (id: string, method: string, args: Json[]): Promise<Json> => {
		if (!api) throw new Error("Prompt sections cannot perform managed external operations");
		const key = `${prefix}:${operationId(id)}`;
		const child = await startChild(
			bridge,
			requestId,
			key,
			EFFECT_WORKFLOW,
			{ method, args },
			metadata,
			{ detached: false, waited: true },
			context,
		);
		return childResult(child);
	};
	const launch = async (id: string, workflow: AnyWorkflow, input: Json, waited: boolean) => {
		if (!api) throw new Error("Prompt sections cannot start workflows");
		const registered = bridge.host.workflows.find((candidate) => candidate.name === workflow.name);
		if (!registered) throw new Error(`Workflow ${workflow.name} is not registered`);
		const parsed = registered.definition.input
			? await parseSchema(registered.definition.input, input, `${registered.name} input`)
			: input;
		const key = operationId(id);
		return startChild(
			bridge,
			requestId,
			`${prefix}:${key}`,
			registered.name,
			parsed,
			metadata,
			{ detached, waited },
			context,
		);
	};
	const start = (id: string, workflow: AnyWorkflow, input: Json = null) =>
		launch(id, workflow, input, false);
	const ctx = bindMetadata<FabrialFields>(
		{
			actor: metadata.requestedBy,
			clients: bridge.host.clients(),
			thread:
				metadata.replyTo && bridge.host.chat()
					? {
							post: (id, message) =>
								operation(id, "post", [cloneJson(message) as Json]) as unknown as ReturnType<
									ThreadIO["post"]
								>,
							update: async (id, message, content) => {
								await operation(id, "update", [
									cloneJson(message) as unknown as Json,
									cloneJson(content) as Json,
								]);
							},
							history: (id, options) =>
								operation(id, "history", [options ?? {}]) as ReturnType<ThreadIO["history"]>,
						}
					: undefined,
			start: start as FabrialFields["start"],
			invoke: (async (id: string, workflow: AnyWorkflow, input: Json = null) => {
				if (!api) throw new Error("Prompt sections cannot invoke workflows");
				return childResult(await launch(id, workflow, input, true));
			}) as FabrialFields["invoke"],
			// Sent from a managed child execution, through the same path as workflow `ctx.send`.
			send: async (id, agent, options) => {
				if (!api) throw new Error("Prompt sections cannot send");
				const home = options.home && "post" in options.home ? options.home.ref : options.home;
				return (await operation(id, "send", [
					agent.name,
					cloneJson({ ...options, home }) as unknown as Json,
				])) as SendReceipt;
			},
			follow: async (id, resource, options) => {
				if (!api) throw new Error("Prompt sections cannot follow");
				return (await operation(id, "follow", [
					cloneJson(resource) as unknown as Json,
					storedFollowOptions(options),
				])) as FollowReceipt;
			},
			unfollow: async (id, resource, options) => {
				if (!api) throw new Error("Prompt sections cannot unfollow");
				return (await operation(id, "unfollow", [
					cloneJson(resource) as unknown as Json,
					storedFollowOptions(options),
				])) as UnfollowReceipt;
			},
			evaluate: async (id, request) => {
				if (!api) throw new Error("Prompt sections cannot evaluate models");
				const key = `fabrial:pi:evaluate:${operationId(id)}`;
				const saved = await api.memo(key, context);
				if (saved) return saved as unknown as EvaluateResult<typeof request.questions>;
				const result = await bridge.evaluator.evaluate(
					request,
					context.abortSignal ?? bridge.execution.signal,
				);
				return (await api.memo(
					key,
					cloneJson(result) as unknown as Json,
					context,
				)) as unknown as EvaluateResult<typeof request.questions>;
			},
			state: <T extends JsonObject>(definition: StateDefinition<T>): ToolStateHandle<T> => {
				const key = `${definition.scope}:${definition.name}`;
				const load = async () => {
					let entry = staged.get(key);
					if (!entry) {
						const value = await bridge.harness.commit(
							async (tx) => cloneJson(await stateDraft(tx, definition, conversationId)),
							context,
						);
						entry = {
							definition: definition as unknown as StateDefinition,
							value,
							changes: [],
						};
						staged.set(key, entry);
					}
					return entry;
				};
				return {
					get: async () => cloneJson((await load()).value) as T,
					update: async (change) => {
						if (!api) throw new Error("Prompt sections cannot mutate state");
						const entry = await load();
						const draft = cloneJson(entry.value) as T;
						entry.value = await parseSchema(definition.schema, change(draft) ?? draft);
						entry.changes.push(change as (draft: JsonObject) => void | JsonObject);
						return cloneJson(entry.value) as T;
					},
				};
			},
		},
		metadata,
	);
	waitedStarts.set(ctx, (id, workflow, input) => launch(id, workflow, input as never, true));
	return ctx;
}

const waitedStarts = new WeakMap<
	object,
	(id: string, workflow: AnyWorkflow, input: Json) => Promise<string>
>();
/** Starts a workflow a tool waits on, behind its reporter (see `startChild`); returns the reporter's execution id. */
export function startWaited(
	ctx: FabrialFields,
	id: string,
	workflow: AnyWorkflow,
	input: Json,
): Promise<string> {
	return waitedStarts.get(ctx)!(id, workflow, input);
}

/** A reporter's result is `{ child, result }`, the committed outcome of the child it ran; else it never reported. */
export function childOutcome(reporter: JsonObject): JsonObject {
	return reporter.status === "completed"
		? ((reporter.output as { result: JsonObject }).result ?? reporter)
		: reporter;
}

/**
 * The request a conversation's current run acts for: the request of its newest placed input (a steer joins the run,
 * so what runs after it acts for it), or the request a background child's task or report acts for (its obligation's
 * origin), else its binding (the lead request a foreground child was called for). Children record their lead request
 * before their input is submitted, so a lead's later steer leaves them acting for the request they were started for.
 */
export async function currentRequest(
	bridge: BridgeFrame,
	conversationId: ConversationId,
	context: Context,
): Promise<string> {
	const binding = await bridge.harness.snapshot(Binding, conversationId, context);
	if (!binding?.requestId)
		throw new Error("This conversation has no Fabrial invocation/chat binding (forks are unbound)");
	// The bridge commits this request's identity before submission and never serializes live clients.
	// Pi appends each input to the run as it is placed, so the newest participating input is the last one.
	const live = await bridge.harness.snapshot(LiveDoc, conversationId, context);
	const obligations = (await bridge.harness.snapshot(Obligations, context))?.requests ?? {};
	for (const inputId of [...(live?.run?.inputs ?? [])].reverse()) {
		const record = await (await bridge.harness.submission(inputId, context))?.status(context);
		if (record?.status !== "placed" || !record.requestId) continue;
		const requestId = obligations[record.requestId]?.origin ?? record.requestId;
		if (await bridge.harness.snapshot(Invocation, requestId, context)) return requestId;
	}
	if (!(await bridge.harness.snapshot(Invocation, binding.requestId, context)))
		throw new Error(`Missing Fabrial invocation ${binding.requestId}`);
	return binding.requestId;
}

/** The agent a conversation runs, from the Session's own documents: a child's `Subagent` doc, else the lead's entry. */
export async function agentOf(
	reader: Pick<HookApi, "snapshot">,
	conversationId: ConversationId,
	context: Context,
): Promise<string> {
	const child = (await reader.snapshot(Subagent, conversationId, context))?.agent;
	if (child) return child;
	const ids = (await reader.snapshot(Conversations, context))?.ids ?? {};
	return Object.keys(ids).find((name) => ids[name] === conversationId) ?? "unknown";
}

/**
 * Persist the child's intent BEFORE starting, linked to the request it acts for (none for a reply's post), so stopping
 * the request cancels the child. Attached children are cancelled with the Session's execution; a lost start receipt
 * reconciles by global key.
 *
 * A child the Session waits on (`waited`) runs behind a reporter: the reporter runs it as its own attached child,
 * waits until Conductor committed its outcome, and only then signals this Session's execution. The intent, and the
 * returned id, are the reporter's.
 */
export async function startChild(
	bridge: BridgeFrame,
	request: string | null,
	key: string,
	workflow: string,
	input: Json,
	metadata: InvocationMetadata,
	options: { detached: boolean; waited: boolean },
	context: Context,
): Promise<string> {
	const { detached, waited } = options;
	const dedupeKey = JSON.stringify(["fabrial.pi.child", bridge.session, key]);
	const intent = await bridge.harness.commit(async (tx) => {
		const obligation = request === null ? undefined : (await tx.doc(Obligations)).requests[request];
		if (request !== null && !obligation)
			throw new Error(`Missing Fabrial obligation for request ${request}`);
		// A stop cancels what its requests' tools started; nothing starts for them afterwards.
		if (obligation?.stopped) throw new Error(`Request ${request} was stopped`);
		const saved = await tx.doc(ChildIntents, key, {
			workflow: waited ? REPORT_WORKFLOW : workflow,
			input: waited
				? { workflow, input: cloneJson(input ?? null), session: bridge.execution.executionId }
				: cloneJson(input ?? null),
			metadata: { ...cloneJson(metadata), triggerEvent: null, ownerWorkflow: null },
			dedupeKey,
			detached,
			executionId: null,
		});
		if (obligation) {
			obligation.intents ??= [];
			if (!obligation.intents.includes(key)) obligation.intents.push(key);
		}
		return cloneJson(saved) as ChildIntent;
	}, context);
	return intent.executionId ?? launch(bridge, key, intent, context);
}

/**
 * Starts an intent's execution, or gets the one already started for it: the operation id and the global dedupe key
 * are the intent's. Saves its execution id.
 */
async function launch(
	bridge: Pick<BridgeFrame, "harness" | "execution">,
	key: string,
	intent: ChildIntent,
	context: Context,
): Promise<string> {
	const executionId = await bridge.execution.start(key, intent.workflow, intent.input, {
		metadata: intent.metadata,
		dedupeKey: intent.dedupeKey,
		cancelWithParent: !intent.detached,
	});
	await bridge.harness.commit(async (tx) => {
		(await tx.doc(ChildIntents, key, intent)).executionId = executionId;
	}, context);
	return executionId;
}

/** A tool's task waits on `child`: links it to the request the tool acts for, so stopping the request cancels it. */
export async function registerChild(
	bridge: BridgeFrame,
	request: string,
	taskId: number,
	child: { executionId: string; detached: boolean },
	context: Context,
): Promise<void> {
	bridge.children.set(taskId, child);
	await bridge.harness.commit(async (tx) => {
		const obligation = (await tx.doc(Obligations)).requests[request];
		if (!obligation) throw new Error(`Missing Fabrial obligation for request ${request}`);
		obligation.children ??= {};
		obligation.children[String(taskId)] = child;
	}, context);
}

/**
 * Cancels the workflows and effects the tools of `requests` started or wait on, except detached ones. Call it once the
 * requests are `stopped`: an intent still starting (no execution id yet) is started or found by its start key, then
 * cancelled, and no later start joins them.
 */
export async function cancelChildren(
	bridge: Pick<BridgeFrame, "harness" | "execution">,
	cancel: (executionId: string, reason: string) => Promise<unknown>,
	requests: Iterable<string>,
	reason: string,
	context: Context,
): Promise<void> {
	const { harness } = bridge;
	const obligations = (await harness.snapshot(Obligations, context))?.requests ?? {};
	for (const request of requests) {
		const obligation = obligations[request];
		if (!obligation) continue;
		for (const key of obligation.intents ?? []) {
			const intent = await harness.snapshot(ChildIntents, key, context);
			if (!intent || intent.detached) continue;
			await cancel(intent.executionId ?? (await launch(bridge, key, intent, context)), reason);
		}
		for (const child of Object.values(obligation.children ?? {}))
			if (!child.detached) await cancel(child.executionId, reason);
	}
}

export async function commitStaged(
	api: ToolExecutionApi,
	context: Context,
	staged: Map<string, StagedState>,
	change: (tx: import("@earendil-works/pi-durable").Tx) => Promise<void>,
): Promise<void> {
	await api.commit(async (tx) => {
		for (const entry of staged.values())
			if (entry.changes.length)
				await applyState(tx, entry.definition, api.conversationId, entry.changes);
		await change(tx);
	}, context);
}

/**
 * The built-in `no_reply` tool of runs that answer input with a reply thread: ends the run without posting its
 * final answer. The reason stays in the transcript as the tool call's argument.
 */
export function noReplyTool(): ToolRegistration {
	return piTool({
		name: "no_reply",
		description:
			"End your turn without posting a reply, e.g. for chatter that is not for you or a notification that needs no answer. Call it alone, as the only tool call of your response.",
		parameters: {
			type: "object",
			properties: {
				reason: { type: "string", description: "Why no reply is needed; not shown to anyone" },
			},
			additionalProperties: false,
		} as unknown as TSchema,
		execute: async () => ({
			content: [{ type: "text" as const, text: "No reply posted." }],
			control: { terminate: true },
		}),
	});
}
