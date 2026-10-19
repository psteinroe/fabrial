import type { ThreadRoutingState } from "./runtime.ts";

const TOMBSTONE_LIMIT = 100;
type IngressKind = NonNullable<ThreadRoutingState["ingestedDedupeIds"]>[number]["kind"];

export function hasIngress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
): boolean {
	return state?.ingestedDedupeIds?.some((entry) => entry.kind === kind && entry.id === id) ?? false;
}

/** Clear interaction-scoped data, retaining only bounded ingress receipts and follows. */
export function clearInteraction(state: ThreadRoutingState | null): ThreadRoutingState | null {
	return state?.ingestedDedupeIds?.length || state?.follows?.length
		? {
				interactionId: null,
				handlerExecutionId: null,
				agentActive: false,
				statusMessageId: null,
				bufferedReplies: [],
				...(state.ingestedDedupeIds
					? { ingestedDedupeIds: state.ingestedDedupeIds.slice(-TOMBSTONE_LIMIT) }
					: {}),
				...(state.follows?.length ? { follows: state.follows } : {}),
			}
		: null;
}

type Follow = NonNullable<ThreadRoutingState["follows"]>[number];

/** Add a follow record for a conversation (idempotent). */
export function addFollow(state: ThreadRoutingState | null, follow: Follow): ThreadRoutingState {
	const base = state ?? {
		interactionId: null,
		handlerExecutionId: null,
		agentActive: false,
		statusMessageId: null,
		bufferedReplies: [],
	};
	if (base.follows?.some((f) => f.session === follow.session && f.agent === follow.agent))
		return base;
	return {
		...base,
		follows: [...(base.follows ?? []), { session: follow.session, agent: follow.agent }],
	};
}

export function rememberIngress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
	at: number,
): ThreadRoutingState {
	return {
		...(state ?? {
			interactionId: null,
			handlerExecutionId: null,
			agentActive: false,
			statusMessageId: null,
			bufferedReplies: [],
		}),
		ingestedDedupeIds: [...(state?.ingestedDedupeIds ?? []), { kind, id, at }].slice(
			-TOMBSTONE_LIMIT,
		),
	};
}
