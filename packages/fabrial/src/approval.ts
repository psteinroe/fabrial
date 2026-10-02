import type { GroupDefinition, Principal, UserDefinition } from "./identity.ts";
import type { Json } from "./json.ts";
import type { Card } from "./thread.ts";
import type { Awaitable } from "./thread.ts";

export interface ApprovalOptions {
	title: string;
	/** Shown on the card; also what the approval binds to (a changed proposal needs a new approval). */
	details: Json;
	approvers: GroupDefinition | UserDefinition | readonly (GroupDefinition | UserDefinition)[];
	/** Duration string ("24h") or ms. Must stay below 30 days (Chat SDK thread-state TTL). */
	timeout?: string | number;
	/** Custom card body; defaults to a rendering of title + details. */
	card?: Omit<Card, "actions">;
	/** Show [Cancel request] on the requester's status message. Default true. */
	requesterControls?: boolean;
}

export type DecisionStatus = "approved" | "rejected" | "expired" | "cancelled";

export interface ApprovalDecision {
	status: DecisionStatus;
	approved: boolean;
	decidedBy: Principal | null;
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
