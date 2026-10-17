import type { EventFilter } from "./events.ts";
import type { JsonObject } from "./json.ts";
import type { Surface } from "./thread.ts";

export interface TriggerSpec<TPayload extends JsonObject = JsonObject> {
	readonly kind: "fabrial.trigger";
	/** Full event name, `${pluginId}.${eventKey}`. */
	readonly event: string;
	readonly filter?: EventFilter<TPayload>;
	/** Where `ctx.thread` points when the event has no thread of its own. */
	readonly replyTo?: Surface;
	/** Observers always run, independently, without owning the thread (`ctx.thread` is absent). */
	readonly observe?: boolean;
	/**
	 * Higher wins when several workflows' triggers match the same event. Defaults to the number of
	 * filtered fields. Equal specificity among owners on the same event fails `app.start()`.
	 */
	readonly specificity?: number;
	/** @internal phantom */
	readonly __payload?: TPayload;
}

/** Build a trigger. Plugins wrap this in typed helpers, e.g. `slack.mentioned({ channel })`. */
export function trigger<TPayload extends JsonObject>(spec: {
	event: string;
	filter?: EventFilter<TPayload>;
	replyTo?: Surface;
	observe?: boolean;
	specificity?: number;
}): TriggerSpec<TPayload> {
	return { kind: "fabrial.trigger", ...spec };
}

export type TriggerPayload<T> = T extends TriggerSpec<infer P> ? P : never;
