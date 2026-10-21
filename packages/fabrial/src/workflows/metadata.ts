// Invocation metadata: the request context every execution carries, and how children inherit it.
import type { TaskContext } from "pgconductor-js";
import type { JsonObject } from "../schema.ts";
import type { Origin } from "../routing/events.ts";
import type { Surface } from "../chat/messages.ts";
import type { Principal } from "../identity.ts";

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
