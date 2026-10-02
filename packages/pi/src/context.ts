import { LiveDoc } from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import { createContextKey, awaitWithContext } from "@earendil-works/chord/context";
import type {
	ConversationId,
	Harness,
	ToolExecutionApi,
	PromptInput,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type AnyWorkflow,
	type Clients,
	type DurableExecution,
	type EvaluateQuestion,
	type EvaluateRequest,
	type EvaluateResult,
	type Evaluator,
	type FabrialHost,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type Principal,
	type StateDefinition,
	type ThreadIO,
	type ToolStateHandle,
	type WorkflowInput,
	type WorkflowOutput,
} from "fabrial";
import { applyState, Binding, ChildResults, stateDraft, Runs, Invocation } from "./documents.ts";

export interface BridgeFrame {
	host: FabrialHost;
	execution: DurableExecution;
	evaluator: Evaluator;
	harness: Harness;
	thread?: ThreadIO;
	children: Map<number, { executionId: string; detached: boolean }>;
}
export const BridgeKey = createContextKey<BridgeFrame>("fabrial.pi.bridge");
export interface FabrialFields {
	actor: Principal | null;
	requestedBy: Principal | null;
	clients: Clients;
	interaction: InvocationMetadata;
	thread:
		| {
				post(id: string, message: Parameters<ThreadIO["post"]>[0]): ReturnType<ThreadIO["post"]>;
				update(
					id: string,
					message: Parameters<ThreadIO["update"]>[0],
					content: Parameters<ThreadIO["update"]>[1],
				): Promise<void>;
				history(
					id: string,
					options?: Parameters<ThreadIO["history"]>[0],
				): ReturnType<ThreadIO["history"]>;
		  }
		| undefined;
	state<T extends JsonObject>(definition: StateDefinition<T>): ToolStateHandle<T>;
	invoke<W extends AnyWorkflow>(
		id: string,
		workflow: W,
		input: WorkflowInput<W>,
	): Promise<WorkflowOutput<W>>;
	start<W extends AnyWorkflow>(id: string, workflow: W, input: WorkflowInput<W>): Promise<string>;
	evaluate<Q extends Record<string, EvaluateQuestion>>(
		id: string,
		request: EvaluateRequest<Q>,
	): Promise<EvaluateResult<Q>>;
}
export type ToolContext = import("./bind.ts").BoundToolApi & FabrialFields;
export type SectionContext = PromptInput & FabrialFields;
export type StagedState = {
	definition: StateDefinition;
	interactionId: string;
	changes: ((draft: JsonObject) => void | JsonObject)[];
	value: JsonObject;
};
export const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

export function frame(context: Context): BridgeFrame {
	const value = context.value(BridgeKey);
	if (!value) throw new Error("Fabrial Pi adapters must run inside the @fabrial/pi bridge");
	return value;
}

export async function fields(
	context: Context,
	conversationId: ConversationId,
	api?: ToolExecutionApi,
	staged = new Map<string, StagedState>(),
): Promise<FabrialFields> {
	const bridge = frame(context);
	const binding = await bridge.harness.snapshot(Binding, conversationId, context);
	if (!binding?.requestId)
		throw new Error("This conversation has no Fabrial invocation/chat binding (forks are unbound)");
	// The bridge commits this request's identity before submission and never serializes live clients.
	const live = await bridge.harness.snapshot(LiveDoc, conversationId, context);
	const inputId = live?.run?.inputs[0];
	const submission = inputId ? await bridge.harness.submission(inputId, context) : undefined;
	const requestId = (await submission?.status(context))?.requestId ?? binding.requestId;
	const invocation = await bridge.harness.snapshot(Invocation, requestId, context);
	if (!invocation) throw new Error(`Missing Fabrial invocation ${binding.requestId}`);
	const metadata = invocation as InvocationMetadata;
	const prefix = `pi:${conversationId}:${api?.taskId ?? "section"}`;
	const ids = new Map<string, number>();
	const operationId = (id: string) => {
		const count = ids.get(id) ?? 0;
		ids.set(id, count + 1);
		return count ? `${id}:${count}` : id;
	};
	const operation = <T extends Json | void>(id: string, fn: () => Promise<T>) => {
		if (!api) throw new Error("Prompt sections cannot perform managed external operations");
		return bridge.execution.step(`${prefix}:${operationId(id)}`, fn);
	};
	const start = async <W extends AnyWorkflow>(id: string, workflow: W, input: WorkflowInput<W>) => {
		if (!api) throw new Error("Prompt sections cannot start workflows");
		const groups = workflow.definition.access?.invoke;
		if (
			groups &&
			(!metadata.requestedBy ||
				!(
					await Promise.all(
						(Array.isArray(groups) ? groups : [groups]).map((group) =>
							bridge.host.directory.isMember(metadata.requestedBy!, group),
						),
					)
				).some(Boolean))
		)
			throw new Error(`Not authorized to invoke ${workflow.name}`);
		const parsed = workflow.definition.input
			? await parseSchema(workflow.definition.input, input, `${workflow.name} input`)
			: input;
		const key = operationId(id);
		return bridge.execution.start(`${prefix}:${key}`, workflow.name, parsed, {
			metadata: { ownsThread: false },
			dedupeKey: `${requestId}:${prefix}:${key}`,
		});
	};
	return {
		actor: metadata.requestedBy,
		requestedBy: metadata.requestedBy,
		clients: bridge.host.clients(),
		interaction: metadata,
		thread: bridge.thread
			? {
					post: (id, message) =>
						operation(
							id,
							async () => cloneJson(await bridge.thread!.post(message)) as unknown as JsonObject,
						) as unknown as ReturnType<ThreadIO["post"]>,
					update: (id, message, content) =>
						operation(id, () => bridge.thread!.update(message, content)),
					history: (id, options) => operation(id, () => bridge.thread!.history(options ?? {})),
				}
			: undefined,
		start,
		invoke: async (id, workflow, input) => {
			if (!api) throw new Error("Prompt sections cannot invoke workflows");
			const child = await start(id, workflow, input);
			const result = (await bridge.harness.snapshot(ChildResults, context))?.results[child];
			if (result) {
				if (result.status !== "completed")
					throw new Error(JSON.stringify(result.error ?? result.reason ?? result.status));
				return result.output as WorkflowOutput<typeof workflow>;
			}
			await registerChild(bridge, api.taskId, { executionId: child, detached: false }, context);
			return awaitWithContext(new Promise<never>(() => {}), context);
		},
		evaluate: async (id, request) => {
			if (!api) throw new Error("Prompt sections cannot evaluate models");
			const saved = await api.memo(`evaluate:${id}`, context);
			if (saved) return saved as unknown as EvaluateResult<typeof request.questions>;
			const result = await bridge.evaluator.evaluate(
				request,
				context.abortSignal ?? bridge.execution.signal,
			);
			return (await api.memo(
				`evaluate:${id}`,
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
						async (tx) =>
							cloneJson(await stateDraft(tx, definition, metadata.interactionId, conversationId)),
						context,
					);
					entry = {
						definition: definition as unknown as StateDefinition,
						interactionId: metadata.interactionId,
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

export async function registerChild(
	bridge: BridgeFrame,
	taskId: number,
	child: { executionId: string; detached: boolean },
	context: Context,
): Promise<void> {
	bridge.children.set(taskId, child);
	await bridge.harness.commit(async (tx) => {
		const run = (await tx.doc(Runs)).runs[bridge.execution.executionId];
		if (!run) throw new Error("Missing Pi bridge run binding");
		run.children[String(taskId)] = child;
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
				await applyState(
					tx,
					entry.definition,
					entry.interactionId,
					api.conversationId,
					entry.changes,
				);
		await change(tx);
	}, context);
}
