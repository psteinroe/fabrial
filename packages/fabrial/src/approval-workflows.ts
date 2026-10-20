import type { FabrialHost } from "./app.ts";
import type { ApprovalDecision } from "./approval.ts";
import { CLEANUP_WORKFLOW, type CardDestination, type CleanupInput } from "./cleanup.ts";
import { samePrincipal } from "./directory.ts";
import { definedGroup, type Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { DurableExecution, RuntimeWorkflow } from "./runtime.ts";

/** One execution per approval request; its result is the request the routing workflow authorizes clicks against. */
export const APPROVAL_REQUEST_WORKFLOW = "fabrial.approval.request";
/** One execution per approval request that waits for its decision; its result is the final decision. */
export const APPROVAL_DECISION_WORKFLOW = "fabrial.approval.decision";
/** Authorized decisions only: the routing workflow emits a click after checking it. */
export const APPROVAL_DECIDED_EVENT = "fabrial.approval.decided";

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
	/** May cancel the request from its status card, unless `requesterControls` is false. */
	requester: Principal | null;
	/** Epoch ms. */
	deadline: number;
	title: string;
	details: Json;
	requesterControls: boolean;
	card: Json | null;
}

export type ApprovalAction = "approve" | "reject" | "cancel";

/**
 * Longer than the longest approval (30 days), so a request stays readable while clicks can still
 * arrive and while waits on it register.
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
	title: string;
	cards: { destination: CardDestination; card: Json }[];
}

/** The decision execution's result: the decision and the label every card shows. */
export interface DecisionResult {
	decision: ApprovalDecision;
	label: string;
}

/**
 * Subscribes once to the authorized decisions, then starts one cleanup per card, so a click on a card
 * just posted is never missed. The first decision wins; the caller settling first cancels the approval.
 */
export function approvalDecisionWorkflow(now: () => number): RuntimeWorkflow {
	return {
		name: APPROVAL_DECISION_WORKFLOW,
		triggers: [],
		retentionDays: REQUEST_RETENTION_DAYS,
		handler: async (input, execution) => {
			const data = input as unknown as DecisionInput;
			const decided = await execution.subscribe("decided", {
				event: APPROVAL_DECIDED_EVENT,
				filter: { approvalId: [data.approvalId] },
			});
			await Promise.all(
				data.cards.map((card, index) =>
					execution.start(
						`card:${index}`,
						CLEANUP_WORKFLOW,
						{
							requestExecutionId: data.approvalId,
							decisionId: execution.executionId,
							callerId: data.callerId,
							title: data.title,
							...card,
						} satisfies CleanupInput as unknown as Json,
						{ metadata: { replyTo: null, ownerWorkflow: null, triggerEvent: null } },
					),
				),
			);
			const winner = await execution.waitForAny(
				"decision",
				{ decided, caller: { execution: data.callerId } },
				{ timeoutMs: Math.max(0, data.deadline - now()) },
			);
			const decidedAt = new Date(now()).toISOString();
			let result: DecisionResult;
			if (winner.kind === "event") {
				const event = winner.event.payload;
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
								: event.reason === "superseded"
									? "Superseded"
									: "Cancelled",
				};
			} else
				result = {
					decision: {
						status: winner.kind === "timeout" ? "expired" : "cancelled",
						approved: false,
						decidedBy: null,
						decidedAt,
					},
					label: winner.kind === "timeout" ? "Expired" : "Cancelled",
				};
			return result as unknown as JsonObject;
		},
	};
}

/** The request of an approval id from a click: null for unknown ids and executions of other workflows. */
export async function loadRequest(
	host: FabrialHost,
	execution: DurableExecution,
	approvalId: string,
): Promise<ApprovalRequest | null> {
	const found = await execution.step("approval", async () => {
		const info = await host.runtime.getExecution(approvalId);
		return info?.workflow === APPROVAL_REQUEST_WORKFLOW;
	});
	if (!found) return null;
	// Started before any card was posted, so it settles right after a fast click at the latest.
	const result = await execution.waitForResult("approval:result", approvalId);
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
