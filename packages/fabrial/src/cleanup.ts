import type { Json, JsonObject } from "./json.ts";
import type { FabrialHost } from "./app.ts";
import type { DurableExecution, WaitBranch } from "./runtime.ts";
import { EXECUTION_SETTLED_EVENT } from "./runtime.ts";
import type { MessageRef } from "./thread.ts";
import { clearInteraction } from "./routing.ts";

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
	for (let attempt = 0; ; attempt++) {
		const winner = await execution.waitForAny(`fabrial:wait:${attempt}`, branches);
		if (winner.kind === "event")
			for (const branch of Object.values(branches))
				if (branch.kind === "event") branch.after = winner.cursor;
		if (winner.key === "closed") return;
		if (winner.key === "presented" && winner.kind === "event") {
			refs.push(winner.event.payload.ref as unknown as MessageRef);
			continue;
		}
		break;
	}
	await execution.step("fabrial:cleanup", async () => {
		const chat = host.chat();
		if (!chat) return;
		for (const ref of refs) {
			const io = await chat.thread({
				kind: "thread",
				provider: ref.provider,
				threadId: ref.threadId,
			});
			if (data.approvalId)
				await io.update(ref, {
					card: { title: data.title as string, text: "Cancelled", actions: [] },
				});
			else {
				const state = await io.getState();
				if (
					state?.interactionId === execution.metadata.interactionId &&
					state.handlerExecutionId === data.executionId
				) {
					try {
						await io.setStatus(null);
					} catch (error) {
						host.logger.warn("Could not clear thread status", { error });
					}
					await io.updateState((latest) =>
						latest?.interactionId === execution.metadata.interactionId &&
						latest.handlerExecutionId === data.executionId
							? clearInteraction(latest)
							: latest,
					);
				}
			}
		}
	});
}
