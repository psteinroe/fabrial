/**
 * What Fabrial's own executions use on top of Conductor's `TaskContext`: the invocation metadata every execution
 * carries, child results as Fabrial reports them, and a child call whose failure the caller can catch.
 */
import type { ExecutionInfo, ExecutionOutcome, TaskContext } from "pgconductor-js";
import type { Origin } from "./events.ts";
import type { Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { Surface } from "./thread.ts";

/** Serializable invocation context carried by every execution (Conductor execution metadata, item 2). */
export interface InvocationMetadata extends JsonObject {
	/** Correlates one request: derived from the source event (or the run), it keys request ids. */
	requestId: string;
	origin: (Origin & JsonObject) | null;
	/** Where `ctx.thread` points. `null` for executions without a surface (observers, cron without replyTo). */
	replyTo: (Surface & JsonObject) | null;
	requestedBy: (Principal & JsonObject) | null;
	// Requests delivered by `ctx.send` also carry `send: SentRequest` (kind and priority). Tools and the
	// workflows they start carry `conversation: ConversationRef`, the agent conversation they belong to.
}

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

/** How a settled execution ended, as Fabrial reports it: a completed one's result is its `output`. */
export function executionResult(
	outcome: ExecutionOutcome<unknown> | { status: "timeout" },
): ExecutionResult {
	if (outcome.status === "timeout") throw new Error("Unexpected result wait timeout");
	return outcome.status === "completed"
		? { status: "completed", output: (outcome.result ?? null) as Json }
		: outcome;
}

/** How an execution ended, from Conductor's view of it; null while it runs or waits. */
export function settlement(info: ExecutionInfo): ExecutionResult | null {
	if (info.status === "completed")
		return { status: "completed", output: (info.result as Json | undefined) ?? null };
	if (info.status === "failed")
		return { status: "failed", error: info.error ?? "Execution failed" };
	if (info.status === "cancelled")
		return { status: "cancelled", reason: info.error ?? "Cancelled by user" };
	return null;
}

type Native = Pick<
	TaskContext,
	| "step"
	| "sleep"
	| "subscribe"
	| "waitForEvent"
	| "waitForAny"
	| "waitForResult"
	| "invoke"
	| "invokeAndWait"
	| "schedule"
	| "unschedule"
	| "sendSignal"
	| "signalWithStart"
	| "emit"
>;

const NATIVE = [
	"step",
	"sleep",
	"subscribe",
	"waitForEvent",
	"waitForAny",
	"waitForResult",
	"invoke",
	"invokeAndWait",
	"schedule",
	"unschedule",
	"sendSignal",
	"signalWithStart",
	"emit",
] as const;

/** Conductor's own methods of a context, from its prototype: unaffected by wrappers merged onto it. */
export function nativeMethods(ctx: TaskContext): Native {
	const proto = Object.getPrototypeOf(ctx) as Record<string, (...args: unknown[]) => unknown>;
	return Object.fromEntries(NATIVE.map((name) => [name, proto[name]!.bind(ctx)])) as Native;
}

const contextMetadata = new WeakMap<object, InvocationMetadata>();

/** Binds a workflow or tool context to its invocation metadata, which contexts do not expose. */
export function bindMetadata<T extends object>(ctx: T, metadata: InvocationMetadata): T {
	contextMetadata.set(ctx, metadata);
	return ctx;
}

/**
 * The invocation metadata behind a context. An execution's is its Conductor metadata (one object per attempt, which
 * a lazy first post updates), or none of a request's for an execution without metadata (cron).
 */
export function metadataOf(ctx: object): InvocationMetadata {
	let metadata = contextMetadata.get(ctx);
	if (!metadata && ctx instanceof Object && "executionId" in ctx && "onSignal" in ctx) {
		const execution = ctx as TaskContext;
		metadata = (execution.metadata as InvocationMetadata | undefined) ?? {
			requestId: execution.executionId,
			origin: null,
			replyTo: null,
			requestedBy: null,
		};
		contextMetadata.set(ctx, metadata);
	}
	if (!metadata) throw new Error("Expected a Fabrial context");
	return metadata;
}

/** A child's metadata: this execution's, with the fields `override` defines replaced. */
export function childMetadata(
	ctx: TaskContext,
	override?: Partial<InvocationMetadata>,
): InvocationMetadata {
	const result: InvocationMetadata = { ...metadataOf(ctx) };
	for (const [key, value] of Object.entries(override ?? {}))
		if (value !== undefined) result[key] = value;
	return result;
}

/**
 * Start `workflow` as a child and wait for its result, memoized by `key`. Unlike `invokeAndWait`, a failed or cancelled
 * child reaches the caller's catch as an `ExecutionError`. Cancelled with the caller unless `detached`; on timeout the
 * call throws, and an attached child is cancelled while a detached one keeps running.
 */
export async function startAndWait(
	ctx: TaskContext,
	key: string,
	workflow: string,
	input: JsonObject,
	options: { metadata?: Partial<InvocationMetadata>; detached?: boolean; timeoutMs?: number } = {},
): Promise<Json> {
	const native = nativeMethods(ctx);
	const child = await native.invoke(
		`__fabrial:invoke:${key}`,
		{ name: workflow, queue: ctx.queue } as never,
		input as never,
		{
			metadata: childMetadata(ctx, options.metadata),
			cancelWithParent: !options.detached,
		} as never,
	);
	const outcome = await native.waitForResult<Json>(
		key,
		child,
		options.timeoutMs !== undefined ? { timeout: options.timeoutMs } : undefined,
	);
	if (outcome.status === "timeout") {
		// A native timeout leaves the child running; an attached call owns its child.
		if (!options.detached) await ctx.cancel(child, { reason: "Parent invocation timed out" });
		throw new Error(`Child execution timed out after ${options.timeoutMs}ms`);
	}
	if (outcome.status === "completed") return outcome.result ?? null;
	throw new ExecutionError(child, outcome);
}
