// Delivery of approval cards: one execution per card, and the monitor that fails an approval no approver could be
// reached for.
import type { TaskContext } from "pgconductor-js";
import type { WorkflowTask } from "../conductor.ts";
import { REQUEST_RETENTION_DAYS } from "./request.ts";
import { executionResult, nativeMethods } from "../execution.ts";
import type { Principal } from "../identity.ts";
import {
	editMessage,
	type MessageRef,
	postMessage,
	postTo,
	type Surface,
} from "../chat/messages.ts";
import type { ApprovalCard, DecisionResult } from "./decision.ts";
import { finalCard, type Outcome, pendingCard, type Proposal } from "./cards.ts";
import type { Json, JsonObject } from "../schema.ts";
import type { FabrialHost } from "../app.ts";
import type { ChatConnection } from "../chat/connection.ts";

/** One execution per approval that settles early only when no approver card could be delivered. */
export const APPROVAL_DELIVERY_WORKFLOW = "fabrial.approval.delivery";

/**
 * Waits on the approver cards' cleanups in turn and returns once one completes, which it does only after
 * its card was posted and the approval decided. It settles before the decision only when every approver
 * card failed for good.
 */
export function approvalDeliveryWorkflow(): WorkflowTask {
	return {
		name: APPROVAL_DELIVERY_WORKFLOW,
		triggers: [],
		retentionDays: REQUEST_RETENTION_DAYS,
		handler: async (input, ctx) => {
			const { cleanups } = input as { cleanups: string[] };
			const native = nativeMethods(ctx);
			for (const [index, id] of cleanups.entries())
				if ((await native.waitForResult(`card:${index}`, id)).status === "completed")
					return { reached: true };
			return { reached: false };
		},
	};
}

export const CARD_WORKFLOW = "fabrial.cleanup";

/** Where an approval card goes: an approver's DM (origin provider first), or a surface. */
export type CardDestination =
	| { kind: "dm"; approver: Principal; provider: string | null }
	| { kind: "surface"; surface: Surface };

export interface CardInput extends ApprovalCard {
	requestExecutionId: string;
	/** The decision execution: its result closes the card. It settles when the caller does at the latest. */
	decisionId: string;
	proposal: Proposal;
}

/** Retried attempts repeat only the card's failed post or update; a card still failing is given up. */
export const CARD_ATTEMPTS = 3;

/**
 * One independent execution per approval card: posts it, waits for the decision, and relabels it
 * without actions. It survives cancellation of the caller and restarts.
 */
export async function deliverCard(input: Json, ctx: TaskContext, host: FabrialHost): Promise<void> {
	const native = nativeMethods(ctx);
	const data = input as unknown as CardInput;
	const chat = host.chat();
	if (!chat) throw new Error("Approvals require a chat integration");
	const warn = (message: string, error: unknown) =>
		host.logger.warn(message, {
			approvalId: data.requestExecutionId,
			error: error instanceof Error ? error.message : String(error),
		});
	const ref = (await native
		.step("post", async () => {
			const card = { card: pendingCard(data.proposal, data.status, data.actions) };
			const { destination } = data;
			return (await (destination.kind === "surface"
				? postTo(chat, destination.surface, card)
				: postMessage(await openDM(chat, destination), card))) as unknown as JsonObject;
		})
		.catch((error: unknown) => {
			warn("Approval card delivery failed", error);
			throw error;
		})) as unknown as MessageRef;
	// Only the decision: a card posted after both it and the caller settled must show the decision.
	const result = executionResult(await native.waitForResult("closed", data.decisionId));
	await native
		.step("update", async () => {
			const outcome: Outcome =
				result.status === "completed"
					? closed(result.output as unknown as DecisionResult)
					: { label: "Cancelled", at: new Date().toISOString() };
			await editMessage(chat, ref, { card: finalCard(data.proposal, outcome) });
		})
		.catch((error: unknown) => {
			warn("Approval card update failed", error);
			throw error;
		});
}

function closed(result: DecisionResult): Outcome {
	if ("error" in result) return { label: result.label, at: result.at };
	const { status, comment, decidedAt } = result.decision;
	// A cancel's own reason shows; the labelled ones ("cancelled", "superseded") already do.
	const reason =
		status === "rejected" ||
		(status === "cancelled" && comment !== "cancelled" && comment !== "superseded")
			? comment
			: undefined;
	return { label: result.label, at: decidedAt, ...(reason ? { reason } : {}) };
}

/** The approver's DM, on their identity of the preferred provider first. */
async function openDM(
	chat: ChatConnection,
	{ approver, provider }: Extract<CardDestination, { kind: "dm" }>,
) {
	const identities = approver.identities
		.map((identity, index) => ({ identity, index }))
		.sort(
			(a, b) =>
				Number(b.identity.provider === provider) - Number(a.identity.provider === provider) ||
				a.index - b.index,
		);
	for (const { identity } of identities) {
		try {
			return await chat.openDM(identity);
		} catch {
			/* Try the next provider identity. */
		}
	}
	throw new Error(`Approver ${approver.id} has no identity whose provider can open DMs`);
}

/** The per-card execution as a Conductor task. */
export function cardWorkflow(host: FabrialHost): WorkflowTask {
	return {
		name: CARD_WORKFLOW,
		triggers: [],
		retries: { maxAttempts: CARD_ATTEMPTS },
		handler: (input, ctx) => deliverCard(input, ctx, host),
	};
}
