// Events and triggers: declarations, filter matching and overlap, and the standard chat events.
import type { Json, JsonObject, Schema } from "../schema.ts";
import type { ChatMessage, Surface } from "../chat/messages.ts";
import type { ExternalIdentity } from "../identity.ts";

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
	/** Optional response destination and authenticated requester, supplied by ingress. */
	replyTo?: Surface;
	requestedBy?: ExternalIdentity;
}

/** Serializable reference to the source of an event. `provider` is the plugin id. */
export interface Origin {
	provider: string;
	[key: string]: Json;
}

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

function compare(value: number, operator: string, bound: number): boolean {
	switch (operator) {
		case "=":
			return value === bound;
		case ">":
			return value > bound;
		case ">=":
			return value >= bound;
		case "<":
			return value < bound;
		case "<=":
			return value <= bound;
		default:
			throw new Error(`Invalid numeric filter operator: ${operator}`);
	}
}

export function matchesAlternative(value: unknown, alternative: FilterAlternative): boolean {
	if (alternative === null || typeof alternative !== "object") return value === alternative;
	if ("exists" in alternative) return (value !== undefined) === alternative.exists;
	if ("prefix" in alternative)
		return typeof value === "string" && value.startsWith(alternative.prefix);
	if ("anything-but" in alternative)
		return (
			value !== undefined &&
			(value === null || ["string", "number", "boolean"].includes(typeof value)) &&
			value !== alternative["anything-but"]
		);
	const [op, bound, op2, bound2] = alternative.numeric;
	return (
		typeof value === "number" &&
		compare(value, op, bound) &&
		(op2 === undefined || compare(value, op2, bound2!))
	);
}

export function matchesFilter(payload: JsonObject, filter: EventFilter = {}): boolean {
	return Object.entries(filter).every(([field, alternatives]) =>
		alternatives?.some((a) =>
			matchesAlternative(Object.hasOwn(payload, field) ? payload[field] : undefined, a),
		),
	);
}

/** Conservative overlap check: disjoint literals/prefixes/ranges are proven; uncertain pairs overlap. */
export function filtersOverlap(a: EventFilter = {}, b: EventFilter = {}): boolean {
	return Object.keys(a).every((field) => {
		if (!Object.hasOwn(b, field)) return true;
		return a[field]!.some((x) => b[field]!.some((y) => alternativesOverlap(x, y)));
	});
}

function alternativesOverlap(a: FilterAlternative, b: FilterAlternative): boolean {
	const objectA = a !== null && typeof a === "object";
	const objectB = b !== null && typeof b === "object";
	if (!objectA) return matchesAlternative(a, b);
	if (!objectB) return matchesAlternative(b, a);
	if ("exists" in a) return a.exists || ("exists" in b && !b.exists);
	if ("exists" in b) return b.exists;
	if ("prefix" in a && "prefix" in b)
		return a.prefix.startsWith(b.prefix) || b.prefix.startsWith(a.prefix);
	if (("prefix" in a && "numeric" in b) || ("numeric" in a && "prefix" in b)) return false;
	if ("numeric" in a && "numeric" in b) {
		const bounds = [...a.numeric, ...b.numeric];
		const points = bounds.filter((v): v is number => typeof v === "number");
		points.push(-Infinity, Infinity);
		points.sort((x, y) => x - y);
		const candidates = [...points, ...points.slice(1).map((p, i) => (p + points[i]!) / 2)];
		return candidates.some((n) => matchesAlternative(n, a) && matchesAlternative(n, b));
	}
	return true;
}

export type ChatEventPayload = ChatMessage & { isNewThread: boolean };
export type ChatEventName = "mentioned" | "message" | "dm";

/** Standard Schema without requiring a particular schema library in provider plugins. */
const messageSchema = {
	"~standard": {
		version: 1 as const,
		vendor: "fabrial.chat",
		validate(value: unknown) {
			if (typeof value !== "object" || value === null) {
				return { issues: [{ message: "Expected a chat message" }] };
			}
			const message = value as Record<string, unknown>;
			const strings = ["provider", "threadId", "channelId", "messageId", "text", "sentAt"];
			const author = message.author as ChatMessage["author"] | undefined;
			if (
				strings.some((key) => typeof message[key] !== "string") ||
				["isMention", "isDM", "isNewThread"].some((key) => typeof message[key] !== "boolean") ||
				!author ||
				typeof author.isBot !== "boolean" ||
				(author.name !== null && typeof author.name !== "string") ||
				!author.identity ||
				["provider", "installationId", "subjectId"].some(
					(key) => typeof author.identity[key] !== "string",
				)
			) {
				return { issues: [{ message: "Invalid normalized chat message" }] };
			}
			return { value: value as ChatEventPayload };
		},
	},
};

export function chatEvents() {
	const event = defineEvent<ChatEventPayload>({
		payload: messageSchema,
		filterable: ["channelId", "isDM", "isMention", "threadId", "isNewThread"],
	});
	// Keep filter keys literal: core's generic EventDefinition constraint otherwise inverts keyof variance.
	const standard = {
		...event,
		filterable: ["channelId", "isDM", "isMention", "threadId", "isNewThread"] as const,
	};
	return { mentioned: standard, message: standard, dm: standard };
}

export function chatTrigger(
	provider: string,
	event: ChatEventName,
	filters: { channel?: string; thread?: string; isNewThread?: boolean; observe?: boolean } = {},
) {
	const filter: EventFilter<ChatEventPayload> = {};
	if (filters.channel !== undefined) filter.channelId = [filters.channel];
	if (filters.thread !== undefined) filter.threadId = [filters.thread];
	if (filters.isNewThread !== undefined) filter.isNewThread = [filters.isNewThread];
	return trigger<ChatEventPayload>({
		event: `${provider}.${event}`,
		filter,
		observe: filters.observe,
		specificity: filters.thread ? 2 : filters.channel ? 1 : 0,
	});
}
