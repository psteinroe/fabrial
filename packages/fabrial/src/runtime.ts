/**
 * Ports between Fabrial core and its runtimes. Core implements the workflow context, approvals, replies,
 * routing, and identity on top of these; adapters implement them:
 *
 * - `DurableRuntime` → `runtime/conductor` (PG Conductor), `fabrial/testing` (in memory)
 * - `ChatPort`       → `chat` (Chat SDK)
 * - `AgentPort`, `StatePort`, `Evaluator` → `agents` (Pi Durable, pi-ai)
 *
 * Method names and shapes mirror PGCONDUCTOR.md so that SHIMs can be deleted as Conductor lands features.
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
}

export type DurationInput = string | number;

/**
 * Opaque, durable position in the event stream (Conductor item 1). Events emitted after a cursor satisfy
 * waits that pass it as `after`, even if they were emitted before the wait registered. Every consumed
 * event returns its own cursor, so successive waits can continue exactly after the last consumed event.
 */
export type EventCursor = string;

/** Branch of `waitForAny`. Timers use an absolute deadline (epoch ms), fixed once by the caller. */
export type WaitBranch =
	| { kind: "event"; event: string; filter?: EventFilter; after?: EventCursor }
	| { kind: "execution"; executionId: string }
	| { kind: "timer"; at: number };

export type WaitForAnyResult =
	| {
			key: string;
			kind: "event";
			event: { name: string; payload: JsonObject };
			cursor: EventCursor;
	  }
	| { key: string; kind: "execution"; result: ExecutionResult }
	| { key: string; kind: "timer" };

export type ExecutionResult =
	| { status: "completed"; output: Json }
	| { status: "failed"; error: string }
	| { status: "cancelled"; reason: string };

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
 * the caller) and ignore the workflow's `mutex`: the key already serializes them.
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
	/** Aborted on cancellation (cooperative; checked at the next durable operation). */
	readonly signal: AbortSignal;
	/**
	 * Operation ids: core reserves the `fabrial:` prefix for framework operations and the `#` character for
	 * repetition suffixes (`x`, `x#1`, `x#2`). Adapters namespace their own receipts under `fabrial:`.
	 */
	/** Memoized step: `fn` runs at most once to completion per id (at-least-once on crash). */
	step<T extends Json | void>(id: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(id: string, ms: number): Promise<void>;
	/** Memoized: the current event-stream position, for later `after` waits (take it before side effects). */
	cursor(id: string): Promise<EventCursor>;
	/**
	 * Wait for one matching event emitted after `after` (or after registration when omitted), until the
	 * absolute `deadline` (epoch ms). Returns null on timeout (Conductor item 1).
	 */
	waitForEvent(
		id: string,
		options: { event: string; filter?: EventFilter; after?: EventCursor; deadline?: number },
	): Promise<{ name: string; payload: JsonObject; cursor: EventCursor } | null>;
	/** First of several branches wins (Conductor item 6). */
	waitForAny(id: string, branches: Record<string, WaitBranch>): Promise<WaitForAnyResult>;
	/** Start a child and wait for its result. Cancelled with the parent unless `detached` (Conductor item 5). */
	invoke(
		id: string,
		workflow: string,
		input: Json,
		options?: {
			metadata?: Partial<InvocationMetadata>;
			detached?: boolean;
			timeoutMs?: number;
			mutex?: string;
		},
	): Promise<Json>;
	/** Start an independent execution without waiting; idempotent per id (Conductor item 7). Returns its id. */
	start(
		id: string,
		workflow: string,
		input: Json,
		options?: { metadata?: Partial<InvocationMetadata>; dedupeKey?: string; mutex?: string },
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
	/** Emit an event from inside an execution (not memoized; wrap in `step` if needed). */
	emit(
		event: string,
		payload: JsonObject,
		options?: { id?: string; metadata?: Partial<InvocationMetadata> },
	): Promise<void>;
	cancel(executionId: string, reason?: string): Promise<boolean>;
}

/** Registered with the runtime by core. Triggers are resolved by core (ownership); see `DurableRuntime.emit`. */
export interface RuntimeWorkflow {
	name: string;
	/** Event triggers. Owner triggers only receive events whose `owner` matches this workflow. */
	triggers: { event: string; filter?: EventFilter; role: "owner" | "observer" }[];
	/** Cron deliveries carry replyTo into invocation metadata and own the thread when a surface is supplied. */
	cron?: { schedule: string; name: string; replyTo?: Surface }[];
	concurrency?: number;
	retries?: { maxAttempts?: number };
	/** Strict per-key mutual exclusion (Conductor item 4); key computed from input + metadata. */
	mutex?: (input: Json, metadata: InvocationMetadata) => string | undefined;
	handler: (input: Json, execution: DurableExecution) => Promise<Json | void>;
	/** Runtime calls this after terminal settlement (success, failure, cancellation), never suspension.
	 * Must be retried after restart on delivery failure; implementations must be idempotent. */
	onSettled?: (
		executionId: string,
		metadata: InvocationMetadata,
		result: ExecutionResult,
	) => Promise<void>;
}

export interface RuntimeEvent {
	name: string;
	/** Fields filterable by triggers and waits. */
	filterable: readonly string[];
}

/** Event emitted by core when any Fabrial execution reaches a terminal state. */
export const EXECUTION_SETTLED_EVENT = "fabrial.execution.settled";
/** Emitted by an agent Session when a `ctx.agent` call's run settles; `requestId` names the call. */
export const AGENT_SETTLED_EVENT = "fabrial.agent.settled";

export interface DurableRuntime {
	/** Optional clock, primarily for deterministic tests. Defaults to Date.now in core. */
	now?(): number;
	register(definition: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void;
	/**
	 * Emit an event. `owner` names the single workflow (if any) whose owner trigger receives it;
	 * observer triggers always match. `id` dedupes redeliveries (Conductor item 8).
	 */
	emit(
		event: string,
		payload: JsonObject,
		options: {
			id?: string;
			metadata: InvocationMetadata;
			owner?: string;
			/** One ingress spanning event categories: dedupe triggered runs by (dispatchId, workflow). */
			dispatchId?: string;
			/**
			 * The ingress's canonical owner, passed on every emission of that ingress: the owner workflow only
			 * runs from this event (as owner), never from a competing observer emission.
			 */
			dispatchOwner?: { workflow: string; event: string };
		},
	): Promise<void>;
	invoke(
		workflow: string,
		input: Json,
		options: { metadata: InvocationMetadata; dedupeKey?: string },
	): Promise<string>;
	cancel(executionId: string, reason?: string): Promise<boolean>;
	/** See `SignalWithStartOptions`. Returns the id of the execution that received the signal. */
	signalWithStart(
		options: SignalWithStartOptions & { metadata: InvocationMetadata },
	): Promise<string>;
	/** Store a signal for an execution. False if it is settled or unknown. */
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

/** Fabrial's state of a thread: ingress receipts, follows, and the progress status message. */
export type ThreadRoutingState = JsonObject & {
	/**
	 * Last 100 ingress receipts (subject to the thread TTL). A receipt with `pending` was routed, but its
	 * delivery isn't confirmed yet: a redelivery replays that route instead of routing again.
	 */
	ingestedDedupeIds?: {
		kind: "message" | "cancellation";
		id: string;
		at: number;
		pending?: { route: "follow" | "new"; conversations: Follow[] };
	}[];
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
	/** Let the agent execution survive cancellation of its caller. */
	detached?: boolean;
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
	/** Called for every terminal settlement: completions resume tools, cancellations cancel calls. */
	settled?(
		executionId: string,
		metadata: InvocationMetadata,
		result: ExecutionResult,
	): Promise<void>;
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
