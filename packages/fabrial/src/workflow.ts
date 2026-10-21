import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { EvaluateQuestion, EvaluateRequest, EvaluateResult } from "./evaluate.ts";
import type { GroupDefinition, Principal } from "./identity.ts";
import type { Json } from "./json.ts";
import type { Origin } from "./events.ts";
import type { AgentRef, AgentResult, AgentRunOptions } from "./runtime.ts";
import type { Schema } from "./schema.ts";
import type { SendOptions, SendReceipt } from "./send.ts";
import type { ResourceRef } from "./plugin.ts";
import type { FollowOptions, FollowReceipt, UnfollowReceipt } from "./follows.ts";
import type { JsonObject as StateValue } from "./json.ts";
import type { StateDefinition, WorkflowStateHandle } from "./state.ts";
import type { Awaitable, Thread } from "./thread.ts";
import type { TriggerPayload, TriggerSpec } from "./trigger.ts";

export interface WorkflowContext<TClients = Record<string, unknown>> {
	/** For diagnostics: the durable execution running this workflow. */
	readonly executionId: string;
	/** The thread this execution replies into. Absent for observers and executions without a surface. */
	readonly thread: Thread | undefined;
	readonly clients: TClients;
	/** Who asked for this work. */
	readonly actor: Principal | null;
	readonly signal: AbortSignal;

	step<T extends Json | void>(id: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(id: string, duration: string | number): Promise<void>;

	/** Call: submit to the agent and wait for its answer. */
	agent<O extends Schema | undefined = undefined>(
		id: string,
		agent: AgentRef,
		options: AgentRunOptions<O>,
	): Promise<AgentResult<O>>;
	/**
	 * Send: deliver input to the conversation `[session, agent]` and return. An idle conversation starts a
	 * run; a busy one gets it by kind and priority. The agent's final answer posts to the input's reply thread.
	 */
	send(id: string, agent: AgentRef, options: SendOptions): Promise<SendReceipt>;
	/**
	 * Follow a resource (e.g. `github.pullRequest("acme/app#42")`) in a conversation: its events arrive as
	 * notifications until `unfollow`. From a workflow, pass `to: { agent, session }`; final replies to the
	 * notifications post to this execution's thread (or `to.home`). Following again replaces the options.
	 */
	follow(id: string, resource: ResourceRef<any>, options?: FollowOptions): Promise<FollowReceipt>;
	/** Stop following a resource (default: the calling agent's conversation). */
	unfollow(
		id: string,
		resource: ResourceRef<any>,
		options?: Pick<FollowOptions, "to">,
	): Promise<UnfollowReceipt>;
	evaluate<Q extends Record<string, EvaluateQuestion>>(
		id: string,
		request: EvaluateRequest<Q>,
	): Promise<EvaluateResult<Q>>;
	/** Session-scoped state only: agent-scoped state belongs to a conversation, which a workflow does not select. */
	state<T extends StateValue>(state: StateDefinition<T, "session">): WorkflowStateHandle<T>;

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
	/** Who asked for this work. */
	readonly actor: Principal | null;
	/** The event that started the request, if any. */
	readonly origin: Origin | null;
}

/**
 * Wraps every attempt of a run, including each resume after a suspension. `next` settles when the
 * attempt ends: it resolves when the run returns, suspends or is cancelled, and rejects when it throws.
 */
export type WorkflowMiddleware = (
	execution: ExecutionInfo,
	ctx: WorkflowContext,
	next: (ctx: WorkflowContext) => Promise<void>,
) => Promise<void>;

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
