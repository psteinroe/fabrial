import type { GroupDefinition, Principal, UserDefinition } from "./identity.ts";
import type { Json } from "./json.ts";
import type { Awaitable, Surface } from "./thread.ts";

/**
 * App content added to an approval card in its own labelled section, after the proposal; it never
 * replaces it. Field labels Fabrial sets ("Requested by", "Source", "Expires", …) are rejected.
 */
export interface ApprovalCardContent {
	text?: string;
	fields?: { label: string; value: string }[];
}

export interface ApprovalOptions {
	title: string;
	/**
	 * Shown verbatim on every card; also what the approval binds to (a changed proposal needs a new
	 * approval).
	 */
	details: Json;
	approvers: GroupDefinition | UserDefinition | readonly (GroupDefinition | UserDefinition)[];
	/** Duration string ("24h") or ms. Must stay below 30 days (Chat SDK thread-state TTL). */
	timeout?: string | number;
	/**
	 * Extra card content, shown in a section labelled as the app's, after the proposal, the verified
	 * requester, the source, the reply thread, and the deadline, which every card shows.
	 */
	card?: ApprovalCardContent;
	/**
	 * Where the approver card is posted (e.g. a channel) instead of each approver's DM. It grants no
	 * authority: only assigned approvers may decide, whoever sees the card.
	 */
	destination?: Surface;
	/** Show [Cancel request] on the requester's status message. Default true. */
	requesterControls?: boolean;
}

export type DecisionStatus = "approved" | "rejected" | "expired" | "cancelled";

export interface ApprovalDecision {
	status: DecisionStatus;
	approved: boolean;
	decidedBy: Principal | null;
	/** The rejection reason an approver gave, or the reason passed to `cancel`. */
	comment?: string;
	decidedAt: string;
}

export interface ApprovalHandle {
	readonly approvalId: string;
	/** Assigned approvers, resolved when the request was made. */
	readonly approvers: Principal[];
	/** Awaitable for `ctx.race`, or `await approval.wait(id)`. */
	decision(): Awaitable<ApprovalDecision & Record<string, Json>>;
	wait(id: string): Promise<ApprovalDecision>;
	/** Durable: cancel the pending request; the card shows "Cancelled"/"Superseded". */
	cancel(id: string, reason: "cancelled" | "superseded" | (string & {})): Promise<void>;
}
