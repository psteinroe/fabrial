import type { FabrialHost } from "./app.ts";
import type { DecisionResult } from "./approval-workflows.ts";
import type { Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { DurableExecution, ThreadIO } from "./runtime.ts";
import type { MessageRef, OutboundMessage, Surface } from "./thread.ts";

export const CLEANUP_WORKFLOW = "fabrial.cleanup";

/** Where an approval card goes: an approver's DM (origin provider first) or the requester's surface. */
export type CardDestination =
	| { kind: "dm"; approver: Principal; provider: string | null }
	| { kind: "surface"; surface: Surface };

export interface CleanupInput {
	requestExecutionId: string;
	/** The decision execution: its result labels the card. */
	decisionId: string;
	/** The execution that requested the approval: settling first cancels the card. */
	callerId: string;
	destination: CardDestination;
	/** Card content, with its actions. */
	card: Json;
	title: string;
}

/** Retried attempts repeat only the card's failed post or update; a card still failing is given up. */
export const CLEANUP_ATTEMPTS = 3;

/**
 * One independent execution per approval card: posts it, waits for the decision or the caller to
 * settle, and relabels it without actions. It survives cancellation of the caller and restarts.
 */
export async function cleanup(
	input: Json,
	execution: DurableExecution,
	host: FabrialHost,
): Promise<void> {
	const data = input as unknown as CleanupInput;
	const chat = host.chat();
	if (!chat) throw new Error("Approvals require a chat integration");
	const warn = (message: string, error: unknown) =>
		host.logger.warn(message, {
			approvalId: data.requestExecutionId,
			error: error instanceof Error ? error.message : String(error),
		});
	const ref = (await execution
		.step("post", async () => {
			const io = await open(host, data.destination);
			return (await io.post({
				card: data.card,
			} as unknown as OutboundMessage)) as unknown as JsonObject;
		})
		.catch((error: unknown) => {
			warn("Approval card delivery failed", error);
			throw error;
		})) as unknown as MessageRef;
	// The decision settles before a caller waiting on it can, so a valid decision is never "Cancelled".
	const winner = await execution.waitForAny("closed", {
		decision: { execution: data.decisionId },
		caller: { execution: data.callerId },
	});
	const label =
		winner.key === "decision" && winner.kind === "execution" && winner.result.status === "completed"
			? (winner.result.output as unknown as DecisionResult).label
			: "Cancelled";
	await execution
		.step("update", async () => {
			const io = await chat.thread({
				kind: "thread",
				provider: ref.provider,
				threadId: ref.threadId,
			});
			await io.update(ref, { card: { title: data.title, text: label, actions: [] } });
		})
		.catch((error: unknown) => {
			warn("Approval card update failed", error);
			throw error;
		});
}

async function open(host: FabrialHost, destination: CardDestination): Promise<ThreadIO> {
	const chat = host.chat()!;
	if (destination.kind === "surface")
		return destination.surface.kind === "thread"
			? chat.thread(destination.surface)
			: chat.resolve(destination.surface);
	const { approver, provider } = destination;
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
