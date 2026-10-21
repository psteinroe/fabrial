import type { FabrialHost } from "./app.ts";
import type { ApprovalCardContent, ApprovalDecision } from "./approval.ts";
import type { Proposal } from "./approval-card.ts";
import { CLEANUP_WORKFLOW, type CardDestination, type CleanupInput } from "./cleanup.ts";
import { samePrincipal } from "./directory.ts";
import { definedGroup, type Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { TaskContext } from "pgconductor-js";
import { childMetadata, executionResult, nativeMethods } from "./execution.ts";
import type { RuntimeWorkflow } from "./runtime.ts";
import type { CardAction, Surface } from "./thread.ts";

/** One execution per approval request; its result is the request the routing workflow authorizes clicks against. */
export const APPROVAL_REQUEST_WORKFLOW = "fabrial.approval.request";
/** One execution per approval request that waits for its decision; its result is the final decision. */
export const APPROVAL_DECISION_WORKFLOW = "fabrial.approval.decision";
/** One execution per approval that settles early only when no approver card could be delivered. */
export const APPROVAL_DELIVERY_WORKFLOW = "fabrial.approval.delivery";
/** Authorized decisions only: the routing workflow emits a click after checking it. */
export const APPROVAL_DECIDED_EVENT = "fabrial.approval.decided";
/** `ApprovalHandle.cancel`: a signal to the decision execution, stored even before it first runs. */
export const APPROVAL_CANCEL_SIGNAL = "fabrial.approval.cancel";

/**
 * An approval request as stored in its execution's result. Assignment is by identity and group id, never
 * by resolver: a click is checked against the assigned users and the groups' current members.
 */
export interface ApprovalRequest {
	/** The execution that requested the approval, and the operation id it used. */
	callerId: string;
	operationId: string;
	assigned: { users: Principal[]; groups: string[] };
	/** Who received a card: the assigned users and the groups' members when it was requested. */
	approvers: Principal[];
	/** The verified requester: may cancel the request from its status card, unless `requesterControls` is false. */
	requester: Principal | null;
	/** The authenticated input the request came from: a link and its reference. */
	source: string | null;
	/** The thread replies go to, when it is not the source. */
	thread: string | null;
	/** Where the approver card goes instead of the approvers' DMs; grants no authority. */
	destination: Surface | null;
	/** Epoch ms. */
	deadline: number;
	title: string;
	details: Json;
	requesterControls: boolean;
	content: ApprovalCardContent | null;
}

export type ApprovalAction = "approve" | "reject" | "cancel";

/**
 * Longer than the longest approval (30 days), so a request stays readable while clicks can still
 * arrive. Only the routing workflow and the decision execution wait on these; callers wait on the
 * decision, which is kept forever.
 */
const REQUEST_RETENTION_DAYS = 60;

export function approvalRequestWorkflow(): RuntimeWorkflow {
	return {
		name: APPROVAL_REQUEST_WORKFLOW,
		triggers: [],
		retentionDays: REQUEST_RETENTION_DAYS,
		handler: async (input) => input,
	};
}

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
 * Waits on the approver cards' cleanups in turn and returns once one completes, which it does only after
 * its card was posted and the approval decided. It settles before the decision only when every approver
 * card failed for good.
 */
export function approvalDeliveryWorkflow(): RuntimeWorkflow {
	return {
		name: APPROVAL_DELIVERY_WORKFLOW,
		triggers: [],
		retentionDays: REQUEST_RETENTION_DAYS,
		handler: async (input, _execution, ctx) => {
			const { cleanups } = input as { cleanups: string[] };
			const native = nativeMethods(ctx);
			for (const [index, id] of cleanups.entries())
				if ((await native.waitForResult(`card:${index}`, id)).status === "completed")
					return { reached: true };
			return { reached: false };
		},
	};
}

/**
 * Subscribes once to the authorized decisions, then starts one cleanup per card, so a click on a card
 * just posted is never missed. The first decision wins; the caller settling first cancels the approval,
 * and every approver card failing for good fails it.
 */
export function approvalDecisionWorkflow(now: () => number): RuntimeWorkflow {
	return {
		name: APPROVAL_DECISION_WORKFLOW,
		triggers: [],
		// No retention: a caller may first wait on its decision any time later.
		handler: async (input, _execution, ctx) => {
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
				{ name: CLEANUP_WORKFLOW, queue: ctx.queue },
				{
					requestExecutionId: data.approvalId,
					decisionId: ctx.executionId,
					proposal: data.proposal,
					...card,
				} satisfies CleanupInput as never,
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

/** The request of an approval id from a click: null for unknown ids and executions of other workflows. */
export async function loadRequest(
	host: FabrialHost,
	ctx: TaskContext,
	approvalId: string,
): Promise<ApprovalRequest | null> {
	const native = nativeMethods(ctx);
	const found = await native.step("approval", async () => {
		const info = await host.runtime.getExecution(approvalId);
		return info?.workflow === APPROVAL_REQUEST_WORKFLOW;
	});
	if (!found) return null;
	// Started before any card was posted, so it settles right after a fast click at the latest.
	const result = executionResult(await native.waitForResult("approval:result", approvalId));
	return result.status === "completed" ? (result.output as unknown as ApprovalRequest) : null;
}

/** Whether `actor` may take `action` on `request`. Groups are resolved now; unknown groups grant nothing. */
export async function authorized(
	host: FabrialHost,
	request: ApprovalRequest,
	action: ApprovalAction,
	actor: Principal | null,
): Promise<boolean> {
	if (!actor) return false;
	if (action === "cancel")
		return (
			request.requesterControls && !!request.requester && samePrincipal(actor, request.requester)
		);
	if (request.assigned.users.some((user) => samePrincipal(user, actor))) return true;
	for (const id of request.assigned.groups) {
		const group = definedGroup(id);
		if (group && (await host.directory.isMember(actor, group))) return true;
	}
	return false;
}
