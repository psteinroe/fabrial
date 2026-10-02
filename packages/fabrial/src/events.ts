import type { Json, JsonObject } from "./json.ts";
import type { Schema } from "./schema.ts";

/**
 * A typed event. Plugins declare events under a key (`issueCreated`); the registered event name is
 * `${plugin.id}.${key}` (e.g. `sentry.issueCreated`).
 */
export interface EventDefinition<TPayload extends JsonObject = JsonObject> {
	readonly kind: "fabrial.event";
	readonly payload: Schema<TPayload>;
	/** Top-level scalar payload fields that triggers may filter on. */
	readonly filterable: readonly (keyof TPayload & string)[];
}

export function defineEvent<TPayload extends JsonObject>(definition: {
	payload: Schema<TPayload>;
	filterable?: readonly (keyof TPayload & string)[];
}): EventDefinition<TPayload> {
	return {
		kind: "fabrial.event",
		payload: definition.payload,
		filterable: definition.filterable ?? [],
	};
}

export type InferEventPayload<E> = E extends EventDefinition<infer P> ? P : never;

/** One alternative for a filter field. Same semantics as PG Conductor event filters. */
export type FilterAlternative =
	| string
	| number
	| boolean
	| null
	| { prefix: string }
	| { numeric: [string, number] | [string, number, string, number] }
	| { exists: boolean }
	| { "anything-but": string | number | boolean | null };

/** Fields are ANDed; alternatives within a field are ORed. */
export type EventFilter<TPayload = JsonObject> = {
	[K in keyof TPayload & string]?: FilterAlternative[];
};

/** Options when emitting an event. */
export interface EmitOptions {
	/** Idempotency key: a repeated id is a no-op (webhook redelivery). */
	id?: string;
	/** Where the event came from (e.g. a Sentry issue, a Slack message). Serializable references only. */
	origin?: Origin;
}

/** Serializable reference to the source of an event. `provider` is the plugin id. */
export interface Origin {
	provider: string;
	[key: string]: Json;
}
