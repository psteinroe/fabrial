/**
 * Ports between Fabrial core and its runtimes. Core implements the workflow context, approvals, replies,
 * routing, and identity on top of these; adapters implement them:
 *
 * - `DurableRuntime` → `runtime/conductor` (PG Conductor; a thin mapping onto its public API)
 * - `ChatPort`       → `chat` (Chat SDK)
 * - `AgentPort`, `StatePort`, `Evaluator` → `agents` (Pi Durable, pi-ai)
 *
 * Every durable operation maps onto the Conductor call of the same name (README, "Boundary").
 */
import type { EventFilter, Origin } from "./events.ts";
import type { ExternalIdentity, Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface, ThreadRef } from "./thread.ts";

/** Serializable invocation context carried by every execution (Conductor execution metadata, item 2). */
export interface InvocationMetadata extends JsonObject {
	/** Correlates one request: derived from the source event (or the run), it keys request ids. */
	interactionId: string;
	origin: (Origin & JsonObject) | null;
	/** Where `ctx.thread` points. `null` for executions without a surface (observers, cron without replyTo). */
	replyTo: (Surface & JsonObject) | null;
	requestedBy: (Principal & JsonObject) | null;
	// Requests delivered by `ctx.send` also carry `send: SentRequest` (kind and priority). Tools and the
	// workflows they start carry `conversation: ConversationRef`, the agent conversation they belong to.
	// Executions a Session waits on carry `notify`, the Session's execution id: they send it a `child`
	// signal (`ChildSignal`) as they settle. Starts never inherit `notify`.
}

/** Name of the signal an execution started with `notify` metadata sends that execution as it settles. */
export const CHILD_SIGNAL = "child";

/**
 * Payload of a `child` signal (id `child:<executionId>`): the outcome the execution is about to settle with,
 * sent before Conductor commits it. Sent when the handler completes and when it fails on its final attempt;
 * cancellation sends nothing (the waiter's execution branch covers it).
 */
export interface ChildSignal extends JsonObject {
	executionId: string;
	result: ExecutionResult & JsonObject;
}

export type DurationInput = string | number;

/**
 * A one-shot event subscription (Conductor `ctx.subscribe`): it holds the first matching event emitted
 * after it became active, even while the execution is still running. Memoized by its id.
 */
export interface Subscription {
	readonly kind: "fabrial.subscription";
	readonly id: string;
}

/**
 * Branch of `waitForAny`: a subscription receives its event, an execution settles, or this execution
 * receives a signal with that name (Conductor `ctx.waitForAny`).
 */
export type WaitBranch = Subscription | { execution: string } | { signal: string };

export type WaitForAnyResult =
	| { key: string; kind: "event"; event: { name: string; payload: JsonObject } }
	| { key: string; kind: "execution"; result: ExecutionResult }
	| { key: string; kind: "signal"; signal: { id: string; name: string } }
	| { key: "timeout"; kind: "timeout" };

export type ExecutionResult =
	| { status: "completed"; output: Json }
	| { status: "failed"; error: string }
	| { status: "cancelled"; reason: string };

/** The abort reason of a cancelled execution's `signal` (Conductor's). */
const CANCELLED = "cancelled";

/** True when an execution's `signal` aborted because the execution was cancelled, not because its attempt ended. */
export function isCancelled(signal: AbortSignal): boolean {
	return (
		signal.aborted && (signal.reason as { reason?: unknown } | undefined)?.reason === CANCELLED
	);
}

/** A child terminal failure, distinguishable from ordinary handler failures. */
export class ExecutionError extends Error {
	constructor(
		readonly executionId: string,
		readonly result: Exclude<ExecutionResult, { status: "completed" }>,
	) {
		super(result.status === "cancelled" ? result.reason : result.error);
		this.name = "ExecutionError";
	}
}

/**
 * Data sent to one execution (Conductor item 12). `id` dedupes: sending an id to an execution again stores
 * it once. Delivery is at least once per id, so handlers must be idempotent by `id`.
 */
export interface RuntimeSignal {
	name: string;
	id: string;
	payload: Json;
}

/**
 * Signal the non-terminal execution of `workflow` for `key`, or start one that receives the signal first.
 * At most one non-terminal execution exists per (workflow, key); a settled one releases its key. `input`
 * and `metadata` are used only when an execution starts. Keyed starts are independent (not cancelled with
 * the caller).
 */
export interface SignalWithStartOptions {
	workflow: string;
	key: string;
	signal: RuntimeSignal;
	input?: Json;
}

/** One running attempt of a workflow execution, as seen by core. Replays from the top on every resume. */
export interface DurableExecution {
	readonly executionId: string;
	readonly workflow: string;
	readonly metadata: InvocationMetadata;
	/**
	 * Aborted on cancellation (cooperative; checked at the next durable operation), and when the attempt ends
	 * otherwise (shutdown, suspension). `isCancelled(signal)` tells a cancellation apart.
	 */
	readonly signal: AbortSignal;
	/**
	 * Operation ids: core reserves the `fabrial:` prefix for framework operations and the `#` character for
	 * repetition suffixes (`x`, `x#1`, `x#2`). Adapters namespace their own receipts under `fabrial:`.
	 */
	/** Memoized step: `fn` runs at most once to completion per id (at-least-once on crash). */
	step<T extends Json | void>(id: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(id: string, ms: number): Promise<void>;
	/** Subscribe to one matching event without suspending; wait on it with `waitForAny`. */
	subscribe(id: string, options: { event: string; filter?: EventFilter }): Promise<Subscription>;
	/**
	 * First of several branches wins, memoized by `id`. The timeout starts at the first call, so a
	 * resume never extends it; `key: "timeout"` is reserved. Losing subscriptions close; losing
	 * executions keep running and can be waited on again.
	 */
	waitForAny(
		id: string,
		branches: Record<string, WaitBranch>,
		options?: { timeoutMs?: number },
	): Promise<WaitForAnyResult>;
	/** True when this attempt is the last one: a failure now settles the execution as failed. */
	finalAttempt(): Promise<boolean>;
	/** Memoized: wait until an execution settles. Throws if it is unknown or removed by retention. */
	waitForResult(id: string, executionId: string): Promise<ExecutionResult>;
	/** Start a child and wait for its result. Cancelled with the parent unless `detached` (Conductor item 5). */
	invoke(
		id: string,
		workflow: string,
		input: Json,
		options?: {
			metadata?: Partial<InvocationMetadata>;
			detached?: boolean;
			/** On timeout the call throws; an attached child is cancelled, a detached one keeps running. */
			timeoutMs?: number;
		},
	): Promise<Json>;
	/**
	 * Start an execution without waiting; idempotent per id (Conductor item 7). Returns its id. Independent
	 * unless `cancelWithParent`: then cancelling this execution cancels it too.
	 */
	start(
		id: string,
		workflow: string,
		input: Json,
		options?: {
			metadata?: Partial<InvocationMetadata>;
			dedupeKey?: string;
			cancelWithParent?: boolean;
		},
	): Promise<string>;
	/**
	 * Listen for signals by name, once per name and attempt; register again on every replay. Signals stored
	 * before the listener registers are delivered first, in order, then later ones while the handler runs,
	 * one at a time. A signal counts as delivered when `handler` returns; a throwing handler fails the
	 * attempt. Signals that arrive while the execution is suspended wait for its next attempt (they don't
	 * wake it). If listened-for signals are undelivered when the workflow handler returns, the execution is
	 * re-queued instead of settled, and its next attempt receives them.
	 */
	onSignal(
		name: string,
		handler: (payload: Json, info: { id: string }) => Promise<void> | void,
	): void;
	/** Memoized per id like `start`. Starts inherit this execution's metadata, merged with `metadata`. */
	signalWithStart(
		id: string,
		options: SignalWithStartOptions & { metadata?: Partial<InvocationMetadata> },
	): Promise<string>;
	/** Emit an event from inside an execution; `id` dedupes like `DurableRuntime.emit` (not memoized). */
	emit(
		event: string,
		payload: JsonObject,
		options?: { id?: string; metadata?: Partial<InvocationMetadata> },
	): Promise<void>;
}

/**
 * Registered with the runtime by core. App workflows have no event triggers: the routing workflow starts
 * them. Internal workflows (the router) trigger on events directly.
 */
export interface RuntimeWorkflow {
	name: string;
	/** Event triggers: each matching event starts one execution, with the event's payload and metadata. */
	triggers: { event: string; filter?: EventFilter }[];
	/** Cron deliveries carry replyTo into invocation metadata and own the thread when a surface is supplied. */
	cron?: { schedule: string; name: string; replyTo?: Surface }[];
	concurrency?: number;
	retries?: { maxAttempts?: number };
	/**
	 * Days a settled execution stays readable (`getExecution`, result waits); kept forever by default.
	 * Never 0 for executions others wait on: a wait registered after removal fails.
	 */
	retentionDays?: number;
	/**
	 * Wraps every attempt, including each resume (Conductor middleware). `next` settles when the attempt
	 * ends: it resolves when the handler returns, suspends or is cancelled, and rejects when it throws.
	 * The value passed to `next` reaches `handler` as `attempt`.
	 */
	attempt?: (
		execution: DurableExecution,
		next: (attempt: unknown) => Promise<void>,
	) => Promise<void>;
	/** Its return value is the execution's native result (`getExecution`, result waits). */
	handler: (input: Json, execution: DurableExecution, attempt?: unknown) => Promise<Json | void>;
}

export interface RuntimeEvent {
	name: string;
	/** Fields filterable by triggers and waits. */
	filterable: readonly string[];
}

export interface DurableRuntime {
	/** Optional clock, primarily for deterministic tests. Defaults to Date.now in core. */
	now?(): number;
	register(definition: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void;
	/**
	 * Emit an event (Conductor `emit`). `id` is its native dedupe key: it dedupes redeliveries per event
	 * name across the whole database, for as long as Conductor retains the event's dispatch (one day).
	 */
	emit(
		event: string,
		payload: JsonObject,
		options: { id?: string; metadata: InvocationMetadata },
	): Promise<void>;
	/** Start an execution from outside any execution (tests, scripts). Not deduped; apps route through events. */
	invoke(workflow: string, input: Json, options: { metadata: InvocationMetadata }): Promise<string>;
	cancel(executionId: string, reason?: string): Promise<boolean>;
	/** An execution of this runtime: its workflow and, once settled, its result. Null if unknown or removed. */
	getExecution(
		executionId: string,
	): Promise<{ workflow: string; result: ExecutionResult | null } | null>;
	/** See `SignalWithStartOptions`. Returns the id of the execution that received the signal. */
	signalWithStart(
		options: SignalWithStartOptions & { metadata: InvocationMetadata },
	): Promise<string>;
	/** Store a signal for an execution (child signals, tests, scripts). False if it is settled or unknown. */
	signal(executionId: string, signal: RuntimeSignal): Promise<boolean>;
	start(): Promise<void>;
	stop(): Promise<void>;
}

/** Live I/O for one chat thread. Not durable by itself; core wraps intentional operations in steps. */
export interface ThreadIO {
	readonly ref: ThreadRef;
	readonly channelId: string;
	readonly isDM: boolean;
	post(message: OutboundMessage): Promise<MessageRef>;
	update(message: MessageRef, content: OutboundMessage): Promise<void>;
	/** Replaceable progress status ("Thinking…"); `null` clears it. Best effort, never durable. */
	setStatus(text: string | null): Promise<void>;
	history(options: { limit?: number; sinceLastBotReply?: boolean }): Promise<ChatMessage[]>;
	/** Subscribe the thread in Chat SDK, so its later messages arrive as subscribed messages. */
	subscribe(): Promise<void>;
	/** Fabrial's routing state in Chat SDK thread state (30-day TTL, refreshed on write). */
	getState(): Promise<ThreadRoutingState | null>;
	/**
	 * Atomic read-modify-write of the routing state under a per-thread lock (Chat SDK state adapter lock),
	 * across processes. `fn` may run more than once; keep it pure. Returning `null` clears the state.
	 */
	updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null>;
}

/** A conversation following a thread: `[session, agent]`. */
export interface Follow extends JsonObject {
	session: string;
	agent: string;
}

/**
 * A conversation following a resource thread (`ctx.follow`): the plugin's events for the thread reach it as
 * notifications, whose final replies go to `replyTo` (the conversation's home).
 */
export type ResourceFollow = Follow & {
	/** Resource key, e.g. `github:acme/app#42`. */
	resource: string;
	replyTo: (Surface & JsonObject) | null;
	/** Priority per notification kind, overriding the plugin's default. */
	priority?: Record<string, import("./send.ts").InputPriority>;
	/** Every notification kind, not only the plugin's defaults. */
	events?: "all";
};

/** Fabrial's state of a thread: follows and the progress status message. */
export type ThreadRoutingState = JsonObject & {
	statusMessageId?: string | null;
	/** Conversations following this thread, whatever their session kind: its messages reach them. */
	follows?: Follow[];
	/** Conversations following the resource of this thread (a pull request): its events notify them. */
	resourceFollows?: ResourceFollow[];
};

/** Port implemented by the chat module. */
export interface ChatPort {
	thread(ref: ThreadRef): Promise<ThreadIO>;
	/** Resolve a surface to a thread, creating a new thread in a channel lazily. */
	resolve(surface: Surface): Promise<ThreadIO>;
	openDM(identity: ExternalIdentity): Promise<ThreadIO>;
	/** Short-lived message visible only to one user (e.g. "you are not an approver"). */
	postEphemeral(thread: ThreadRef, identity: ExternalIdentity, text: string): Promise<void>;
}

export interface AgentRunOptions {
	input: Json;
	/** Let the agent execution survive cancellation of its caller (and a timeout). */
	detached?: boolean;
	/** Stop waiting after this long; an attached call is cancelled, a detached one keeps running. */
	timeout?: DurationInput;
	/** Standard Schema for a typed structured result. */
	output?: unknown;
}

/** Port implemented by the agents module. Core calls it from `ctx.agent`. */
export interface AgentPort {
	/** Internal workflows the agent bridge needs (e.g. the execution hosting a Session). */
	workflows(): RuntimeWorkflow[];
	run(
		execution: DurableExecution,
		id: string,
		agent: AgentRef,
		options: AgentRunOptions,
	): Promise<Json>;
	/** Deliver a rendered input to the conversation `[request.session, agent]` without waiting. */
	send(
		execution: DurableExecution,
		id: string,
		agent: AgentRef,
		request: import("./send.ts").SendRequest,
	): Promise<import("./send.ts").SendReceipt>;
	/** Deliver input from ingress, outside any execution: `signalWithStart` to the conversation's Session. */
	deliver?(
		agent: AgentRef,
		request: import("./send.ts").SendRequest,
	): Promise<import("./send.ts").SendReceipt>;
	/** A stop in a followed thread: cancel the conversation's current run (Pi abort; the Session stays). */
	stop?(conversation: Follow, id: string, reason: string): Promise<void>;
}

/** Minimal shape core needs from `f.defineAgent`. */
export interface AgentRef {
	readonly kind: "fabrial.agent";
	readonly name: string;
}

/** Port implemented by the agents module for `ctx.state(S)` in workflow code. Each call is one durable operation. */
export interface StatePort {
	get<T extends JsonObject>(
		execution: DurableExecution,
		id: string,
		state: import("./state.ts").StateDefinition<T>,
	): Promise<T>;
	update<T extends JsonObject>(
		execution: DurableExecution,
		id: string,
		state: import("./state.ts").StateDefinition<T>,
		fn: (draft: T) => void | T,
	): Promise<T>;
}
