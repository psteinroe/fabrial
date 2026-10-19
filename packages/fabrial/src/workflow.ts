import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { EvaluateQuestion, EvaluateRequest, EvaluateResult } from "./evaluate.ts";
import type { GroupDefinition, Principal } from "./identity.ts";
import type { Json } from "./json.ts";
import type { AgentRef, AgentRunOptions, InvocationMetadata } from "./runtime.ts";
import type { Schema } from "./schema.ts";
import type { SendOptions, SendReceipt } from "./send.ts";
import type { ResourceRef } from "./plugin.ts";
import type { SubscribeOptions, SubscriptionReceipt, UnsubscribeReceipt } from "./subscriptions.ts";
import type { JsonObject as StateValue } from "./json.ts";
import type { StateDefinition, WorkflowStateHandle } from "./state.ts";
import type { Awaitable, Thread } from "./thread.ts";
import type { TriggerPayload, TriggerSpec } from "./trigger.ts";

export interface WorkflowContext<TClients = Record<string, unknown>> {
	readonly executionId: string;
	readonly interactionId: string;
	/** The thread this execution replies into. Absent for observers and executions without a surface. */
	readonly thread: Thread | undefined;
	readonly clients: TClients;
	/** Who asked for this work. */
	readonly actor: Principal | null;
	readonly metadata: InvocationMetadata;
	readonly signal: AbortSignal;

	step<T extends Json | void>(id: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(id: string, duration: string | number): Promise<void>;

	/** Call: submit to the agent and wait for its answer. */
	agent<T extends Json = Json>(id: string, agent: AgentRef, options: AgentRunOptions): Promise<T>;
	/**
	 * Send: deliver input to the conversation `[session, agent]` and return. An idle conversation starts a
	 * run; a busy one gets it by kind and priority. The agent's final answer posts to the input's reply thread.
	 */
	send(id: string, agent: AgentRef, options: SendOptions): Promise<SendReceipt>;
	/**
	 * Route a resource's events (e.g. `github.pullRequest({ repo, number })`) into a conversation: by
	 * default the calling agent's, replying where the current request replies. Re-subscribing updates it.
	 */
	subscribe(
		id: string,
		resource: ResourceRef<any>,
		options?: SubscribeOptions,
	): Promise<SubscriptionReceipt>;
	/** Remove the subscription of a resource for a conversation (default: the calling agent's). */
	unsubscribe(
		id: string,
		resource: ResourceRef<any>,
		options?: Pick<SubscribeOptions, "to">,
	): Promise<UnsubscribeReceipt>;
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
	/** True when `principal` is a member of `group` (resolved now). */
	isMember(principal: Principal, group: GroupDefinition): Promise<boolean>;
}

export type RaceResult<R extends Record<string, Awaitable>> = {
	[K in keyof R]: { key: K; value: R[K] extends Awaitable<infer T> ? T : never };
}[keyof R];

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

export interface WorkflowDefinition<
	TName extends string,
	TInput extends Json,
	TOutput extends Json | void,
	TTriggers extends readonly TriggerSpec<any>[],
	TClients = Record<string, unknown>,
> {
	name: TName;
	/** Shown to agents that list this workflow in `tools`. */
	description?: string;
	/** Input schema for `invoke`, `start`, and agent tool calls. */
	input?: Schema<TInput>;
	output?: Schema<TOutput>;
	triggers?: TTriggers;
	cron?: { schedule: string; name?: string; replyTo?: import("./thread.ts").Surface }[];
	concurrency?: number;
	retries?: { maxAttempts?: number };
	run(
		this: void,
		input: TInput | TriggerPayload<TTriggers[number]>,
		ctx: WorkflowContext<TClients>,
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
	return {
		kind: "fabrial.workflow",
		name: definition.name,
		definition: definition as unknown as Workflow<TName, TInput, TOutput>["definition"],
	};
}
