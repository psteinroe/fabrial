import type { Origin } from "./events.ts";
import type { ExternalIdentity } from "./identity.ts";
import type { JsonObject } from "./json.ts";
import type { Follow, ThreadRoutingState } from "./runtime.ts";
import type { Surface } from "./thread.ts";

/** The one event ingress emits; `fabrial.route` triggers on it. */
export const INGRESS_EVENT = "fabrial.ingress";
/** The internal workflow that routes every ingress event. */
export const ROUTE_WORKFLOW = "fabrial.route";

/**
 * Verified, normalized input from a webhook or chat, as `fabrial.ingress` carries it. Ingress emits it once
 * per provider delivery: the emit dedupes by `kind` and `id` (and the event name of plugin events), for as
 * long as Conductor retains the event's dispatch (about one to two days). A redelivery after that routes
 * again.
 */
export interface IngressEvent extends JsonObject {
	/** The provider's delivery or message id. */
	id: string;
	/** A plugin event, a chat message, a card action (approval click), or a stop in a thread. */
	kind: "event" | "message" | "action" | "stop";
	/** Event categories: the plugin event's name, or a message's (`slack.message`, `slack.mentioned`). */
	events: string[];
	/** The plugin event's payload, the `ChatMessage`, or the action (`actionId`, `value`, `messageId`). */
	payload: JsonObject;
	origin: (Origin & JsonObject) | null;
	/** Where replies go: the event's surface, or the thread of a message, action or stop. */
	replyTo: (Surface & JsonObject) | null;
	/** The authenticated sender, resolved to a principal by the router. */
	actor: (ExternalIdentity & JsonObject) | null;
}

/** The emit's dedupe key of an ingress event. */
export const ingressKey = (ingress: IngressEvent): string =>
	JSON.stringify(
		ingress.kind === "event"
			? [ingress.kind, ingress.events[0], ingress.id]
			: [ingress.kind, ingress.id],
	);

/** A workflow the router starts, and the event category it starts on. */
export interface RoutedWorkflow extends JsonObject {
	workflow: string;
	event: string;
}

/**
 * How the router routed one ingress event, persisted in its `route` step: the conversations that receive it
 * (followers, or the agent whose trigger won), the owner workflow, and the observers. `ignored` when nothing
 * handles it: it leaves no follow record and no thread subscription.
 */
export interface RouteDecision extends JsonObject {
	route: "follow" | "new" | "ignored";
	conversations: Follow[];
	owner: RoutedWorkflow | null;
	observers: RoutedWorkflow[];
	/** Where the owner's (or the agent's) replies go. */
	replyTo: (Surface & JsonObject) | null;
	/** The winning trigger's event category, else the input's first. */
	event: string;
}

/** Add a follow record for a conversation (idempotent). */
export function addFollow(state: ThreadRoutingState | null, follow: Follow): ThreadRoutingState {
	const base = state ?? {};
	if (base.follows?.some((f) => f.session === follow.session && f.agent === follow.agent))
		return base;
	return {
		...base,
		follows: [...(base.follows ?? []), { session: follow.session, agent: follow.agent }],
	};
}
