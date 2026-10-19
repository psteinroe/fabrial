import type { Follow, ThreadRoutingState } from "./runtime.ts";

const TOMBSTONE_LIMIT = 100;
type Receipt = NonNullable<ThreadRoutingState["ingestedDedupeIds"]>[number];
type IngressKind = Receipt["kind"];

export function ingress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
): Receipt | undefined {
	return state?.ingestedDedupeIds?.find((entry) => entry.kind === kind && entry.id === id);
}

export function hasIngress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
): boolean {
	return !!ingress(state, kind, id);
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

/** Record an ingress receipt; `pending` marks a routed delivery that isn't confirmed yet. */
export function rememberIngress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
	at: number,
	pending?: Receipt["pending"],
): ThreadRoutingState {
	return {
		...state,
		ingestedDedupeIds: [
			...(state?.ingestedDedupeIds ?? []),
			{ kind, id, at, ...(pending ? { pending } : {}) },
		].slice(-TOMBSTONE_LIMIT),
	};
}

/** Confirm a pending receipt once its delivery succeeded. */
export function confirmIngress(
	state: ThreadRoutingState | null,
	kind: IngressKind,
	id: string,
): ThreadRoutingState | null {
	if (!ingress(state, kind, id)?.pending) return state;
	return {
		...state,
		ingestedDedupeIds: state!.ingestedDedupeIds!.map((entry) =>
			entry.kind === kind && entry.id === id ? { kind, id, at: entry.at } : entry,
		),
	};
}
