// What approval cards show: the persisted proposal, never text a model supplied later.
import type { Json } from "../schema.ts";
import type { Principal } from "../identity.ts";
import type { ApprovalCardContent } from "./request.ts";
import type { Card, CardAction } from "../chat/messages.ts";

/**
 * What every card of an approval shows, from its persisted request: never from text a model supplied
 * after the request was made.
 */
export interface Proposal {
	title: string;
	details: Json;
	/** The verified requester, or null when no person requested it. */
	requester: Principal | null;
	/** The authenticated input the request came from (a link and its reference); null without one. */
	source: string | null;
	/** The thread replies go to, when it is not the source; null otherwise. */
	thread?: string | null;
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

/** Labels only Fabrial sets: app content cannot pose as them. */
export const RESERVED_LABELS = [
	"Requested by",
	"Source",
	"Reply thread",
	"Expires",
	"Closed",
	"Reason",
	"Status",
	"Approvers",
	"Decided by",
];

/** The display name with the stable id it belongs to; unmapped identities are marked. */
export function requesterLabel(requester: Principal | null): string {
	if (!requester) return "No requester (automated)";
	const id = requester.known ? requester.id : `unmapped ${requester.id}`;
	return requester.name ? `${requester.name} (${id})` : id;
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
	const { content } = proposal;
	return {
		title: proposal.title,
		text,
		fields: [
			{ label: "Requested by", value: requesterLabel(proposal.requester) },
			{ label: "Source", value: proposal.source ?? "None" },
			...(proposal.thread ? [{ label: "Reply thread", value: proposal.thread }] : []),
			...fields,
		],
		code: details(proposal.details),
		// The app's content, labelled as such: never among the fields Fabrial verified.
		...(content?.text || content?.fields?.length
			? {
					section: {
						title: "From the requesting app (not verified by Fabrial)",
						...(content.text ? { text: content.text } : {}),
						...(content.fields?.length ? { fields: content.fields } : {}),
					},
				}
			: {}),
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
