/**
 * What Fabrial's own executions use on top of Conductor's `TaskContext`: child results as Fabrial reports them, a child
 * call whose failure the caller can catch, and durations.
 */
import type { ExecutionInfo, ExecutionOutcome, TaskContext } from "pgconductor-js";
import type { Json, JsonObject } from "./schema.ts";
import { childMetadata, type InvocationMetadata } from "./workflows/metadata.ts";

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

/** Conductor's context methods Fabrial calls unwrapped. */
export type Native = Pick<TaskContext, (typeof NATIVE)[number]>;

/** Conductor's own methods of a context, from its prototype: unaffected by wrappers merged onto it. */
export function nativeMethods(ctx: TaskContext): Native {
	const proto = Object.getPrototypeOf(ctx) as Record<string, (...args: unknown[]) => unknown>;
	const native = Object.fromEntries(
		NATIVE.map((name) => [name, proto[name]!.bind(ctx)]),
	) as unknown as Native;
	// Conductor's waitForResult calls `this.waitForAny`, which Fabrial's context wraps: this one stays native.
	native.waitForResult = (async (key: string, executionId: string, options?: object) => {
		const winner = await native.waitForAny(
			key,
			{ result: { execution: executionId } },
			options as never,
		);
		return winner.key === "timeout" ? { status: "timeout" } : winner.outcome;
	}) as Native["waitForResult"];
	return native;
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

/** Milliseconds of a duration: ms, or `"<n><unit>"` in ms, s, m, h, d or w (Conductor's `parseDuration` has no weeks). */
export function duration(value: string | number): number {
	if (typeof value === "number") {
		if (!Number.isFinite(value) || value < 0) throw new Error("Invalid duration");
		return value;
	}
	const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/.exec(value);
	if (!match) throw new Error(`Invalid duration: ${value}`);
	return duration(
		Number(match[1]) *
			{ ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[match[2]!]!,
	);
}
