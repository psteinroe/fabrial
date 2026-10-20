import type { Json, JsonObject } from "./json.ts";
import type { FabrialHost } from "./app.ts";
import type { DurableExecution, WaitBranch } from "./runtime.ts";
import { EXECUTION_SETTLED_EVENT } from "./runtime.ts";
import type { MessageRef } from "./thread.ts";

export const CLEANUP_WORKFLOW = "fabrial.cleanup";
export const APPROVAL_CLOSED_EVENT = "fabrial.approval.closed";
export const PRESENTATION_EVENT = "fabrial.presentation";

/** Independent durable watcher: presentation cleanup must survive cancellation and process restarts. */
export async function cleanup(
	input: Json,
	execution: DurableExecution,
	host: FabrialHost,
): Promise<void> {
	const data = input as JsonObject;
	const after =
		typeof data.after === "string" ? data.after : await execution.cursor("fabrial:cursor");
	const branches: Record<string, WaitBranch> = {
		settled: {
			kind: "event",
			after,
			event: EXECUTION_SETTLED_EVENT,
			filter: { executionId: [data.executionId as string] },
		},
		presented: {
			kind: "event",
			after,
			event: PRESENTATION_EVENT,
			filter: { presentationId: [data.presentationId as string] },
		},
	};
	if (typeof data.approvalId === "string")
		branches.closed = {
			kind: "event",
			after,
			event: APPROVAL_CLOSED_EVENT,
			filter: { approvalId: [data.approvalId] },
		};
	const refs: MessageRef[] = [];
	// A decided approval names its label; anything else settling first cancels the cards.
	let label = "Cancelled";
	for (let attempt = 0; ; attempt++) {
		const winner = await execution.waitForAny(`fabrial:wait:${attempt}`, branches);
		if (winner.kind === "event")
			for (const branch of Object.values(branches))
				if (branch.kind === "event") branch.after = winner.cursor;
		if (winner.key === "presented" && winner.kind === "event") {
			refs.push(winner.event.payload.ref as unknown as MessageRef);
			continue;
		}
		if (winner.key === "closed" && winner.kind === "event")
			label = winner.event.payload.label as string;
		break;
	}
	// Only approval cards register a presentation with this watcher.
	const chat = host.chat();
	if (!data.approvalId || !chat) return;
	// Each card updates on its own: a deleted card or provider error doesn't block the others, and a
	// retry of this workflow repeats only the cards that failed.
	let failures = 0;
	for (const [index, ref] of refs.entries())
		try {
			await execution.step(`fabrial:cleanup:${index}`, async () => {
				const io = await chat.thread({
					kind: "thread",
					provider: ref.provider,
					threadId: ref.threadId,
				});
				await io.update(ref, { card: { title: data.title as string, text: label, actions: [] } });
			});
		} catch (error) {
			failures++;
			host.logger.warn("Approval card update failed", {
				approvalId: data.approvalId,
				messageId: ref.messageId,
				error: error instanceof Error ? error.message : String(error),
			});
		}
	if (failures) throw new Error(`${failures} approval card update(s) failed`);
}
