import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { EvaluateQuestion, EvaluateRequest, EvaluateResult } from "./evaluate.ts";
import type { GroupDefinition, Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { Clients } from "./register.ts";
import type { AgentRef, AgentRunOptions, InvocationMetadata } from "./runtime.ts";
import type { Schema } from "./schema.ts";
import type { JsonObject as StateValue } from "./json.ts";
import type { StateDefinition, WorkflowStateHandle } from "./state.ts";
import type { Awaitable, Thread } from "./thread.ts";
import type { TriggerPayload, TriggerSpec } from "./trigger.ts";

export interface WorkflowContext {
	readonly executionId: string;
	readonly interactionId: string;
	/** The thread this execution replies into. Absent for observers and executions without a surface. */
	readonly thread: Thread | undefined;
	readonly clients: Clients;
	/** Who asked for this work. */
	readonly actor: Principal | null;
	readonly metadata: InvocationMetadata;
	readonly signal: AbortSignal;
	/** Escape hatch to unwrapped runtime handles (Conductor execution, Chat SDK, …). */
	readonly raw: Record<string, unknown>;

	step<T extends Json | void>(id: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(id: string, duration: string | number): Promise<void>;

	agent<T extends Json = Json>(id: string, agent: AgentRef, options: AgentRunOptions): Promise<T>;
	evaluate<Q extends Record<string, EvaluateQuestion>>(
		id: string,
		request: EvaluateRequest<Q>,
	): Promise<EvaluateResult<Q>>;
	state<T extends StateValue>(state: StateDefinition<T>): WorkflowStateHandle<T>;

	waitForApproval(id: string, options: ApprovalOptions): Promise<ApprovalDecision>;
	requestApproval(id: string, options: ApprovalOptions): Promise<ApprovalHandle>;

	/** Durably wait for the first awaitable to settle. */
	race<R extends Record<string, Awaitable>>(id: string, awaitables: R): Promise<RaceResult<R>>;
	/** Timer awaitable for `race`. */
	timer(duration: string | number): Awaitable<null>;

	/** Call another workflow and return its result. Cancelled with this execution unless `detached`. */
	invoke<W extends AnyWorkflow>(
		id: string,
		workflow: W,
		input: WorkflowInput<W>,
		options?: { detached?: boolean },
	): Promise<WorkflowOutput<W>>;
	/** Start an independent execution without waiting; returns its execution id. */
	start<W extends AnyWorkflow>(id: string, workflow: W, input: WorkflowInput<W>): Promise<string>;
	/** Transfer the interaction (thread, final response) to another workflow; return its result from `run`. */
	handoff<W extends AnyWorkflow>(
		id: string,
		workflow: W,
		input: WorkflowInput<W>,
	): Promise<HandoffResult>;

	/** True when `principal` is a member of `group` (resolved now). */
	isMember(principal: Principal, group: GroupDefinition): Promise<boolean>;
}

export type RaceResult<R extends Record<string, Awaitable>> = {
	[K in keyof R]: { key: K; value: R[K] extends Awaitable<infer T> ? T : never };
}[keyof R];

export interface HandoffResult extends JsonObject {
	handedOffTo: string;
	executionId: string;
}

/** Information about the current run, passed to middleware. */
export interface ExecutionInfo {
	readonly executionId: string;
	readonly workflow: string;
	readonly metadata: InvocationMetadata;
}

export type WorkflowMiddleware = (
	execution: ExecutionInfo,
	ctx: WorkflowContext,
	next: (ctx: WorkflowContext) => Promise<Json | void>,
) => Promise<Json | void>;

export interface WorkflowAccess {
	/** Who may invoke this workflow (directly or as a tool). */
	invoke?: GroupDefinition | readonly GroupDefinition[];
}

export interface WorkflowDefinition<
	TName extends string,
	TInput extends Json,
	TOutput extends Json | void,
	TTriggers extends readonly TriggerSpec<any>[],
> {
	name: TName;
	/** Shown to agents when exposed as a tool. */
	description?: string;
	/** Input schema for `invoke`, `handoff`, `start`, and `asTool()`. */
	input?: Schema<TInput>;
	output?: Schema<TOutput>;
	triggers?: TTriggers;
	cron?: { schedule: string; name?: string; replyTo?: import("./thread.ts").Surface }[];
	access?: WorkflowAccess;
	concurrency?: number;
	retries?: { maxAttempts?: number };
	run(
		this: void,
		input: TInput | TriggerPayload<TTriggers[number]>,
		ctx: WorkflowContext,
	): Promise<TOutput>;
}

export interface Workflow<
	TName extends string = string,
	TInput extends Json = Json,
	TOutput extends Json | void = Json | void,
> {
	readonly kind: "fabrial.workflow";
	readonly name: TName;
	readonly definition: WorkflowDefinition<TName, TInput, TOutput, readonly TriggerSpec<any>[]>;
	/** Expose as an agent tool (interpreted by `@fabrial/pi`). */
	asTool(options?: { name?: string; description?: string; detached?: boolean }): WorkflowTool;
}

export interface WorkflowTool {
	readonly kind: "fabrial.workflow-tool";
	readonly workflow: Workflow<string, any, any>;
	readonly name: string;
	readonly description: string;
	readonly detached: boolean;
}

export type AnyWorkflow = Workflow<string, any, any>;
export type WorkflowInput<W> = W extends Workflow<string, infer I, any> ? I : never;
export type WorkflowOutput<W> = W extends Workflow<string, any, infer O> ? O : never;

export function defineWorkflow<
	const TName extends string,
	TInput extends Json = never,
	TOutput extends Json | void = void,
	const TTriggers extends readonly TriggerSpec<any>[] = [],
>(
	definition: WorkflowDefinition<TName, TInput, TOutput, TTriggers>,
): Workflow<TName, TInput, TOutput> {
	const workflow: Workflow<TName, TInput, TOutput> = {
		kind: "fabrial.workflow",
		name: definition.name,
		definition: definition as unknown as Workflow<TName, TInput, TOutput>["definition"],
		asTool: (options = {}) => ({
			kind: "fabrial.workflow-tool",
			workflow,
			name: options.name ?? definition.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"),
			description: options.description ?? definition.description ?? definition.name,
			detached: options.detached ?? false,
		}),
	};
	return workflow;
}
