// A tool's `ctx`: the request a conversation's run acts for, its actor and clients, and managed operations.
import type { TaskContext } from "pgconductor-js";
import type { ClassifierResult, Models } from "@earendil-works/pi-ai";
import {
	type ConversationId,
	type Harness,
	type HookApi,
	LiveDoc,
	type ToolExecutionApi,
} from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import { createContextKey } from "@earendil-works/chord/context";
import type { FabrialHost } from "../app.ts";
import { cloneJson, type Json, type JsonObject, parseSchema } from "../schema.ts";
import type { ToolContext } from "./definition.ts";
import {
	type StagedState,
	type StateDefinition,
	stateDraft,
	type ToolStateHandle,
} from "../state.ts";
import { Binding, Invocation, Obligations } from "../sessions/obligations.ts";
import { bindMetadata, type InvocationMetadata } from "../workflows/metadata.ts";
import {
	type ConversationRef,
	type FollowReceipt,
	storedFollowOptions,
	type UnfollowReceipt,
} from "../routing/follows.ts";
import { ChildResults, EFFECT_WORKFLOW, startChild } from "../sessions/workflows.ts";
import type { AnyWorkflow } from "../workflows/definition.ts";
import type { Thread } from "../chat/messages.ts";
import type { SendReceipt } from "../sessions/delivery.ts";
import { evaluate } from "../models.ts";
import { Subagent } from "./subagents.ts";
import { Conversations } from "../sessions/host.ts";

export interface BridgeFrame {
	host: FabrialHost;
	/** Conductor's context of the Session's execution. */
	ctx: TaskContext;
	/** The instance's Pi AI models, for `ctx.evaluate`. */
	models: Models;
	harness: Harness;
	/** The Pi Session key this execution hosts. */
	session: string;
	/**
	 * Tasks waiting on a workflow (or a managed effect, e.g. a thread post), by task id; while every running task
	 * waits, the Session's execution closes Pi and suspends until one of them settles or input arrives.
	 */
	children: Map<number, { executionId: string; detached: boolean }>;
	/**
	 * Tasks of non-idempotent tools running now. The Session never closes Pi while one runs, also not while it waits on
	 * a workflow or effect: Pi reports an unsafe tool it reopens to as interrupted, so it waits in process instead.
	 */
	unsafe: Set<number>;
	/** Subagent calls waiting for their child's answer; they do not keep the Session open. */
	waiting: Set<number>;
	/**
	 * Resolves with a waited child's committed result, memoized per child: once its reporter settled, or as the
	 * reporter's `child` signal arrives. Without one (the reporter was cancelled, or signalled an earlier Session
	 * execution) it waits until the Session closes, and an idempotent tool runs again when the Session's execution
	 * resumes on the reporter's settlement. Frees the task's slot in `children`.
	 */
	waitChild(taskId: number, executionId: string, context: Context): Promise<JsonObject>;
}
export const BridgeKey = createContextKey<BridgeFrame>("fabrial.pi.bridge");
/** Tool context without the per-call signal and idempotency key. */
type FabrialFields = Omit<ToolContext, "signal" | "idempotencyKey">;

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
	/**
	 * `child` is a reporter (`waited` start): its result is the committed outcome of the workflow it ran. An outcome
	 * other than completed throws as JSON, so a workflow tool's model sees its status (failed or cancelled).
	 */
	const childResult = async (child: string): Promise<Json> => {
		let result = (await bridge.harness.snapshot(ChildResults, context))?.results[child];
		if (!result) {
			bridge.children.set(api!.taskId, { executionId: child, detached: false });
			result = await bridge.waitChild(api!.taskId, child, context);
		}
		if (result.status !== "completed") throw new Error(JSON.stringify(result));
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
	return bindMetadata<FabrialFields>(
		{
			actor: metadata.requestedBy,
			clients: bridge.host.clients(),
			thread:
				metadata.replyTo && bridge.host.chat()
					? {
							post: (id, message) =>
								operation(id, "post", [cloneJson(message) as Json]) as unknown as ReturnType<
									Thread["post"]
								>,
							update: async (id, message, content) => {
								await operation(id, "update", [
									cloneJson(message) as unknown as Json,
									cloneJson(content) as Json,
								]);
							},
							history: (id, options) =>
								operation(id, "history", [options ?? {}]) as ReturnType<Thread["history"]>,
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
				if (saved) return saved as unknown as ClassifierResult;
				const result = await evaluate(
					bridge.models,
					request,
					context.abortSignal ?? bridge.ctx.signal,
				);
				return (await api.memo(
					key,
					cloneJson(result) as unknown as Json,
					context,
				)) as unknown as ClassifierResult;
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
