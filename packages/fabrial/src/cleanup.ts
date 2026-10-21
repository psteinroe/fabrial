import type { FabrialHost } from "./app.ts";
import { finalCard, pendingCard, type Outcome, type Proposal } from "./approval-card.ts";
import type { ApprovalCard, DecisionResult } from "./approval-workflows.ts";
import type { Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import { editMessage, postMessage, postTo, type ChatConnection } from "./chat/threads.ts";
import type { TaskContext } from "pgconductor-js";
import { executionResult, nativeMethods } from "./execution.ts";
import type { MessageRef, Surface } from "./thread.ts";

export const CLEANUP_WORKFLOW = "fabrial.cleanup";

/** Where an approval card goes: an approver's DM (origin provider first), or a surface. */
export type CardDestination =
	| { kind: "dm"; approver: Principal; provider: string | null }
	| { kind: "surface"; surface: Surface };

export interface CleanupInput extends ApprovalCard {
	requestExecutionId: string;
	/** The decision execution: its result closes the card. It settles when the caller does at the latest. */
	decisionId: string;
	proposal: Proposal;
}

/** Retried attempts repeat only the card's failed post or update; a card still failing is given up. */
export const CLEANUP_ATTEMPTS = 3;

/**
 * One independent execution per approval card: posts it, waits for the decision, and relabels it
 * without actions. It survives cancellation of the caller and restarts.
 */
export async function cleanup(input: Json, ctx: TaskContext, host: FabrialHost): Promise<void> {
	const native = nativeMethods(ctx);
	const data = input as unknown as CleanupInput;
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
