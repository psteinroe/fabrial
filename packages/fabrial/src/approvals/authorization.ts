// Who may decide an approval: clicks are checked against the request before the decision sees them.
import type { TaskContext } from "pgconductor-js";
import { Modal, TextInput } from "chat";
import type { FabrialHost } from "../app.ts";
import type { IngressEvent } from "../routing/route.ts";
import type { InvocationMetadata } from "../workflows/metadata.ts";
import type { Json } from "../schema.ts";
import { executionResult, nativeMethods } from "../execution.ts";
import type { ThreadRef } from "../chat/messages.ts";
import { APPROVAL_DECIDED_EVENT } from "./decision.ts";
import { APPROVAL_REQUEST_WORKFLOW, type ApprovalRequest } from "./request.ts";
import { definedGroup, type Principal, samePrincipal } from "../identity.ts";

/** Longest rejection reason kept (characters). */
const MAX_REASON = 3000;

/** An approval click: only authorized clicks before the deadline reach the waiting decision, deduped by the click's id. */
export async function decideAction(
	host: FabrialHost,
	ctx: TaskContext,
	ingress: IngressEvent,
	actor: InvocationMetadata["requestedBy"],
): Promise<Json> {
	const native = nativeMethods(ctx);
	const chat = host.chat();
	// Only authorized clicks reach the waiting approval, deduped by the click's id.
	const action = ingress.payload as {
		actionId: string;
		value: string | null;
		messageId: string;
	};
	const [, verb, approvalId] = /^fabrial\.approval\.(approve|reject|cancel):(.+)$/.exec(
		action.actionId,
	)!;
	const request = await loadRequest(host, ctx, approvalId!);
	const allowed = await native.step(
		"authorize",
		async () => !!request && (await authorized(host, request, verb as ApprovalAction, actor)),
	);
	if (!allowed) {
		await native.step("denied", async () => {
			if (ingress.actor && ingress.replyTo)
				await chat?.postEphemeral(
					ingress.replyTo as ThreadRef,
					ingress.actor,
					"You are not authorized to decide this approval.",
				);
		});
		return { authorized: false };
	}
	// The persisted deadline holds here too: the decision may not have registered its timeout yet,
	// and a retry reuses the authorization above.
	const decided = await native.step("decide", async () => {
		if (host.now() >= request!.deadline) return false;
		await host.conductor.emit(
			APPROVAL_DECIDED_EVENT as never,
			{
				approvalId: approvalId!,
				status: verb === "approve" ? "approved" : verb === "reject" ? "rejected" : "cancelled",
				actor: actor as unknown as Json,
				messageId: action.messageId,
				// A rejection's reason, from the reject modal; approve and cancel buttons carry none.
				...(verb === "reject" && action.value?.trim()
					? { comment: action.value.trim().slice(0, MAX_REASON) }
					: {}),
			} as never,
			{
				dedupe_key: ingress.id,
				metadata: {
					requestId: approvalId!,
					origin: { provider: ingress.actor!.provider },
					replyTo: null,
					requestedBy: actor,
				},
			},
		);
		return true;
	});
	if (!decided) {
		await native.step("expired", async () => {
			if (ingress.actor && ingress.replyTo)
				await chat?.postEphemeral(
					ingress.replyTo as ThreadRef,
					ingress.actor,
					"This approval has expired.",
				);
		});
		return { authorized: true, expired: true };
	}
	return { authorized: true };
}

export type ApprovalAction = "approve" | "reject" | "cancel";

/** The request of an approval id from a click: null for unknown ids and executions of other workflows. */
export async function loadRequest(
	host: FabrialHost,
	ctx: TaskContext,
	approvalId: string,
): Promise<ApprovalRequest | null> {
	const native = nativeMethods(ctx);
	const found = await native.step("approval", async () => {
		// Ids come from clicks and other untrusted input: anything but a UUID of this app's requests is unknown.
		if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(approvalId))
			return false;
		const info = await host.conductor.getExecution(approvalId);
		return info?.queue === host.queue && info.taskKey === APPROVAL_REQUEST_WORKFLOW;
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

/** The modal Reject opens for an optional reason. */
export const REJECT_MODAL = "fabrial.approval.reject";

export interface RejectContext {
	actionId: string;
	threadId: string;
	messageId: string;
}

/** The modal Reject opens for an optional reason; its submit is the decision. */
export function rejectModal(context: RejectContext) {
	return Modal({
		callbackId: REJECT_MODAL,
		title: "Reject request",
		submitLabel: "Reject",
		privateMetadata: JSON.stringify(context),
		children: [
			TextInput({
				id: "reason",
				label: "Reason",
				optional: true,
				multiline: true,
				maxLength: 3000,
			}),
		],
	});
}
