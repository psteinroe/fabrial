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
	const branches: Record<string, WaitBranch> = {
		settled: {
			kind: "event",
			event: EXECUTION_SETTLED_EVENT,
			filter: { executionId: [data.executionId as string] },
		},
		presented: {
			kind: "event",
			event: PRESENTATION_EVENT,
			filter: { presentationId: [data.presentationId as string] },
		},
	};
	if (typeof data.approvalId === "string")
		branches.closed = {
			kind: "event",
			event: APPROVAL_CLOSED_EVENT,
			filter: { approvalId: [data.approvalId] },
		};
	const refs: MessageRef[] = [];
	for (let attempt = 0; ; attempt++) {
		const winner = await execution.waitForAny(`wait:${attempt}`, branches);
		if (winner.key === "closed") return;
		if (winner.key === "presented" && winner.kind === "event") {
			refs.push(winner.event.payload.ref as unknown as MessageRef);
			continue;
		}
		break;
	}
	await execution.step("cleanup", async () => {
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
				if (state?.handlerExecutionId === data.executionId) {
					try {
						await io.setStatus(null);
					} catch (error) {
						host.logger.warn("Could not clear thread status", { error });
					}
					await io.setState(null);
				}
			}
		}
	});
}
