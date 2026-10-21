// The decision execution of an approval: the first authorized click, a cancellation, the caller settling, delivery
// failure, or the deadline.
import type { TaskContext } from "pgconductor-js";
import type { Proposal } from "./cards.ts";
import {
	APPROVAL_DELIVERY_WORKFLOW,
	CARD_WORKFLOW,
	type CardDestination,
	type CardInput,
} from "./delivery.ts";
import type { CardAction } from "../chat/messages.ts";
import type { ApprovalDecision } from "./request.ts";
import type { WorkflowTask } from "../conductor.ts";
import type { JsonObject } from "../schema.ts";
import { nativeMethods } from "../execution.ts";
import { childMetadata } from "../workflows/metadata.ts";
import type { Principal } from "../identity.ts";

/** One execution per approval request that waits for its decision; its result is the final decision. */
export const APPROVAL_DECISION_WORKFLOW = "fabrial.approval.decision";
/** Authorized decisions only: the routing workflow emits a click after checking it. */
export const APPROVAL_DECIDED_EVENT = "fabrial.approval.decided";
/** `ApprovalHandle.cancel`: a signal to the decision execution, stored even before it first runs. */
export const APPROVAL_CANCEL_SIGNAL = "fabrial.approval.cancel";

export interface DecisionInput {
	/** The request execution. */
	approvalId: string;
	callerId: string;
	/** Epoch ms, persisted with the request: resumes never extend it. */
	deadline: number;
	proposal: Proposal;
	cards: ApprovalCard[];
}

/** One card of an approval: an approver card, or the requester's status card. */
export interface ApprovalCard {
	destination: CardDestination;
	/** Whether approvers decide on it: an approval fails when none of these could be posted. */
	approver: boolean;
	status: string;
	actions: CardAction[];
}

/**
 * The decision execution's result: the decision, or why the approval failed, and the label every card
 * shows.
 */
export type DecisionResult =
	| { decision: ApprovalDecision; label: string }
	| { error: string; label: string; at: string };

/** Why an approval fails when every approver card failed permanently. */
export const UNREACHABLE = "No approver could be reached";

/**
 * Subscribes once to the authorized decisions, then starts one cleanup per card, so a click on a card
 * just posted is never missed. The first decision wins; the caller settling first cancels the approval,
 * and every approver card failing for good fails it.
 */
export function approvalDecisionWorkflow(now: () => number): WorkflowTask {
	return {
		name: APPROVAL_DECISION_WORKFLOW,
		triggers: [],
		// No retention: a caller may first wait on its decision any time later.
		handler: async (input, ctx) => {
			const data = input as unknown as DecisionInput;
			// Registered before any wait. The handler returns only once the reason is kept, so a crash in
			// between redelivers it.
			const cancel = Promise.withResolvers<string>();
			const kept = Promise.withResolvers<void>();
			ctx.onSignal(APPROVAL_CANCEL_SIGNAL, async (payload) => {
				cancel.resolve((payload as { reason: string }).reason);
				await kept.promise;
			});
			try {
				return await decide(data, ctx, now, cancel.promise);
			} finally {
				kept.resolve();
			}
		},
	};
}

async function decide(
	data: DecisionInput,
	ctx: TaskContext,
	now: () => number,
	cancelled: Promise<string>,
): Promise<JsonObject> {
	const native = nativeMethods(ctx);
	const internal = { replyTo: null, ownerWorkflow: null, triggerEvent: null };
	const decided = await native.subscribe("decided", {
		event: { name: APPROVAL_DECIDED_EVENT },
		filter: { approvalId: [data.approvalId] },
	} as never);
	const cleanups = await Promise.all(
		data.cards.map((card, index) =>
			native.invoke(
				`card:${index}`,
				{ name: CARD_WORKFLOW, queue: ctx.queue },
				{
					requestExecutionId: data.approvalId,
					decisionId: ctx.executionId,
					proposal: data.proposal,
					...card,
				} satisfies CardInput as never,
				{ metadata: childMetadata(ctx, internal) } as never,
			),
		),
	);
	// The requester's status card is not an approver card.
	const delivery = await native.invoke(
		"delivery",
		{ name: APPROVAL_DELIVERY_WORKFLOW, queue: ctx.queue },
		{ cleanups: cleanups.filter((_id, index) => data.cards[index]!.approver) } as never,
		{ metadata: childMetadata(ctx, internal) } as never,
	);
	const winner = await native.waitForAny(
		"decision",
		{
			decided,
			cancel: { signal: APPROVAL_CANCEL_SIGNAL },
			caller: { execution: data.callerId },
			delivery: { execution: delivery },
		},
		// At least 1ms: a deadline passed before the wait registered (a resume after a crash) still expires it.
		{ timeout: Math.max(1, data.deadline - now()) },
	);
	if ("outcome" in winner && winner.key === "delivery") {
		const error =
			winner.outcome.status === "completed"
				? UNREACHABLE
				: `Approval delivery ${winner.outcome.status}`;
		return {
			error,
			label: error,
			at: new Date(now()).toISOString(),
		} satisfies DecisionResult;
	}
	const decidedAt = new Date(now()).toISOString();
	let result: DecisionResult;
	if ("signal" in winner) {
		const reason = await native.step("cancelled", () => cancelled);
		result = {
			decision: {
				status: "cancelled",
				approved: false,
				decidedBy: null,
				decidedAt,
				comment: reason,
			},
			label: reason === "superseded" ? "Superseded" : "Cancelled",
		};
	} else if ("event" in winner) {
		const event = winner.event.payload as JsonObject;
		const status = event.status as ApprovalDecision["status"];
		const actor = event.actor as unknown as Principal | null;
		const name = actor?.name ?? actor?.id;
		result = {
			decision: {
				status,
				approved: status === "approved",
				decidedBy: actor,
				decidedAt,
				...(typeof event.comment === "string" ? { comment: event.comment } : {}),
			},
			label:
				status === "approved"
					? `Approved by ${name}`
					: status === "rejected"
						? `Rejected by ${name}`
						: "Cancelled",
		};
	} else
		result = {
			decision: {
				status: winner.key === "timeout" ? "expired" : "cancelled",
				approved: false,
				decidedBy: null,
				decidedAt,
			},
			label: winner.key === "timeout" ? "Expired" : "Cancelled",
		};
	return result as unknown as JsonObject;
}
