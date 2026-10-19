import { defineTool as piTool, LiveDoc } from "@earendil-works/pi-durable";
import type { TSchema } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import { createContextKey } from "@earendil-works/chord/context";
import type {
	ConversationId,
	Harness,
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
	type WorkflowInput,
	type WorkflowOutput,
} from "../index.ts";
import {
	applyState,
	Binding,
	ChildResults,
	stateDraft,
	Runs,
	Invocation,
	ChildIntents,
	EFFECT_WORKFLOW,
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
	 * Tasks waiting on a workflow, by task id; while every running task waits, the Session closes. A short managed
	 * effect (`effect`, e.g. a thread post) keeps it open instead.
	 */
	children: Map<number, { executionId: string; detached: boolean; effect?: boolean }>;
	/** Subagent calls waiting for their child's answer; they do not keep the Session open. */
	waiting: Set<number>;
	/** The run a conversation's current request belongs to, and the agent whose conversation runs it. */
	runOf(conversationId: ConversationId, context: Context): Promise<{ id: string; agent: string }>;
	/** Resolves with a workflow's result once its completion signal arrives; frees the task's slot in `children`. */
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
	const run = await bridge.runOf(conversationId, context);
	// Tools, and the workflows they start, follow in this conversation by default.
	const metadata: InvocationMetadata = {
		...(invocation as InvocationMetadata),
		conversation: { session: bridge.session, agent: run.agent } satisfies ConversationRef,
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
	const childResult = async (child: string, effect = false): Promise<Json> => {
		let result = (await bridge.harness.snapshot(ChildResults, context))?.results[child];
		if (!result) {
			await registerChild(
				bridge,
				run.id,
				api!.taskId,
				{ executionId: child, detached: false },
				context,
				effect,
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
			run.id,
			key,
			EFFECT_WORKFLOW,
			{ method, args },
			metadata,
			false,
			context,
		);
		return childResult(child, true);
	};
	const start = async <W extends AnyWorkflow>(id: string, workflow: W, input: WorkflowInput<W>) => {
		if (!api) throw new Error("Prompt sections cannot start workflows");
		const registered = bridge.host.workflows.find((candidate) => candidate.name === workflow.name);
		if (!registered) throw new Error(`Workflow ${workflow.name} is not registered`);
		const parsed = registered.definition.input
			? await parseSchema(registered.definition.input, input, `${registered.name} input`)
			: input;
		const key = operationId(id);
		return startChild(
			bridge,
			run.id,
			`${prefix}:${key}`,
			registered.name,
			parsed,
			metadata,
			detached,
			context,
		);
	};
	return {
		actor: metadata.requestedBy,
		requestedBy: metadata.requestedBy,
		clients: bridge.host.clients(),
		interaction: metadata,
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
		start,
		invoke: async (id, workflow, input) => {
			if (!api) throw new Error("Prompt sections cannot invoke workflows");
			const child = await start(id, workflow, input);
			return (await childResult(child)) as WorkflowOutput<typeof workflow>;
		},
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
	};
}

/**
 * The request a conversation's current run acts for: the request of its first input, or its binding. A subagent's
 * input has no invocation of its own, so it acts for the lead request bound when it was called.
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
	const live = await bridge.harness.snapshot(LiveDoc, conversationId, context);
	const inputId = live?.run?.inputs[0];
	const submission = inputId ? await bridge.harness.submission(inputId, context) : undefined;
	const requestId = (await submission?.status(context))?.requestId;
	if (requestId && (await bridge.harness.snapshot(Invocation, requestId, context)))
		return requestId;
	if (!(await bridge.harness.snapshot(Invocation, binding.requestId, context)))
		throw new Error(`Missing Fabrial invocation ${binding.requestId}`);
	return binding.requestId;
}

/** Persist the cancellation relationship BEFORE starting. A lost start receipt reconciles by global key. */
export async function startChild(
	bridge: BridgeFrame,
	run: string,
	key: string,
	workflow: string,
	input: Json,
	metadata: InvocationMetadata,
	detached: boolean,
	context: Context,
): Promise<string> {
	const dedupeKey = JSON.stringify(["fabrial.pi.child", bridge.session, key]);
	const { piChild: _piChild, ...inherited } = cloneJson(metadata);
	const intent = await bridge.harness.commit(async (tx) => {
		const saved = await tx.doc(ChildIntents, key, {
			workflow,
			input: cloneJson(input ?? null),
			// `piChild` makes the settlement of a child the Session waits for a signal to it.
			metadata: {
				...inherited,
				triggerEvent: null,
				ownerWorkflow: null,
				...(detached ? {} : { piChild: bridge.session }),
			},
			dedupeKey,
			detached,
			executionId: null,
		});
		const record = (await tx.doc(Runs)).runs[run];
		if (!record) throw new Error("Missing Pi bridge run binding");
		record.intents ??= [];
		if (!record.intents.includes(key)) record.intents.push(key);
		return cloneJson(saved) as ChildIntent;
	}, context);
	if (intent.executionId) return intent.executionId;
	const executionId = await bridge.execution.start(key, intent.workflow, intent.input, {
		metadata: intent.metadata,
		dedupeKey: intent.dedupeKey,
	});
	await bridge.harness.commit(async (tx) => {
		(await tx.doc(ChildIntents, key, intent)).executionId = executionId;
	}, context);
	return executionId;
}

export async function registerChild(
	bridge: BridgeFrame,
	run: string,
	taskId: number,
	child: { executionId: string; detached: boolean },
	context: Context,
	effect = false,
): Promise<void> {
	bridge.children.set(taskId, effect ? { ...child, effect } : child);
	await bridge.harness.commit(async (tx) => {
		const record = (await tx.doc(Runs)).runs[run];
		if (!record) throw new Error("Missing Pi bridge run binding");
		record.children[String(taskId)] = child;
	}, context);
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
