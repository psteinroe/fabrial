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

/** Clear interaction-scoped data, retaining only bounded ingress receipts. */
export function clearInteraction(state: ThreadRoutingState | null): ThreadRoutingState | null {
	return state?.ingestedDedupeIds?.length
		? {
				interactionId: null,
				handlerExecutionId: null,
				agentActive: false,
				statusMessageId: null,
				bufferedReplies: [],
				ingestedDedupeIds: state.ingestedDedupeIds.slice(-TOMBSTONE_LIMIT),
			}
		: null;
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
