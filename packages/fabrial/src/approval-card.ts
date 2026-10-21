import type { ApprovalCardContent } from "./approval.ts";
import type { Principal } from "./identity.ts";
import type { Json } from "./json.ts";
import type { Card, CardAction } from "./thread.ts";

/**
 * What every card of an approval shows, from its persisted request: never from text a model supplied
 * after the request was made.
 */
export interface Proposal {
	title: string;
	details: Json;
	/** The verified requester, or null when no person requested it. */
	requester: Principal | null;
	/** A link to the source thread or a description of it; null without one. */
	source: string | null;
	/** Epoch ms. */
	deadline: number;
	content: ApprovalCardContent | null;
}

/** How an approval ended, as its final cards show it. */
export interface Outcome {
	label: string;
	/** ISO time. */
	at: string;
	reason?: string;
}

const time = (ms: number | string) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");

/** The details verbatim: in a code block, so markdown in them cannot change what the card shows. */
function details(value: Json): Card["code"] {
	return typeof value === "string"
		? { content: value }
		: { language: "json", content: JSON.stringify(value, null, 2) };
}

function card(
	proposal: Proposal,
	text: string,
	fields: { label: string; value: string }[],
	actions: CardAction[],
): Card {
	const { requester, content } = proposal;
	return {
		title: proposal.title,
		text,
		fields: [
			{
				label: "Requested by",
				value: requester ? (requester.name ?? requester.id) : "No requester (automated)",
			},
			{ label: "Source", value: proposal.source ?? "None" },
			...fields,
			...(content?.fields ?? []),
		],
		code: details(proposal.details),
		...(content?.text ? { footer: content.text } : {}),
		actions,
	};
}

/** A pending card: the proposal, its requester, source and deadline, then the app's content. */
export function pendingCard(proposal: Proposal, status: string, actions: CardAction[]): Card {
	return card(proposal, status, [{ label: "Expires", value: time(proposal.deadline) }], actions);
}

/** A decided, cancelled, expired or failed card keeps the proposal and adds the outcome. */
export function finalCard(proposal: Proposal, outcome: Outcome): Card {
	return card(
		proposal,
		outcome.label,
		[
			{ label: "Closed", value: time(outcome.at) },
			...(outcome.reason ? [{ label: "Reason", value: outcome.reason }] : []),
		],
		[],
	);
}
