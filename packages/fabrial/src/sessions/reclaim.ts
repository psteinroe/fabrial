// Daily deletion of finished `ctx.agent` call Sessions.
import type { FabrialHost } from "../app.ts";
import type { PiOptions } from "./host.ts";
import type { WorkflowTask } from "../conductor.ts";
import { nativeMethods, settlement } from "../execution.ts";
import { CALL_SESSION, SESSION_WORKFLOW } from "./delivery.ts";
import { PostgresStorage } from "./storage.ts";

const RECLAIM_WORKFLOW = "fabrial.sessions.reclaim";

export function reclaimWorkflow(host: FabrialHost, options: PiOptions): WorkflowTask {
	const retainDays = options.retainCallSessionsDays ?? 7;
	if (!(retainDays >= 0)) throw new RangeError("retainCallSessionsDays must not be negative");
	const batchSize = options.reclaimBatchSize ?? 100;
	if (!(batchSize >= 1)) throw new RangeError("reclaimBatchSize must be at least 1");
	/**
	 * Deletes the Pi storage of `ctx.agent` calls (`run:` Sessions, never a thread's) whose Session execution settled at
	 * least `retainCallSessionsDays` ago: nothing reopens them once the caller stored the outcome, and a call cancelled
	 * while suspended runs no cleanup of its own. Settlement is read with `getExecution` from the execution that last
	 * hosted the Session; a Session leased, never hosted, or whose execution is unknown is kept. One step per batch, in
	 * key order.
	 */
	const reclaim: WorkflowTask = {
		name: RECLAIM_WORKFLOW,
		triggers: [],
		cron:
			options.reclaimSchedule === false
				? []
				: [{ schedule: options.reclaimSchedule ?? "30 3 * * *", name: RECLAIM_WORKFLOW }],
		concurrency: 1,
		// Nothing waits on it.
		retentionDays: 30,
		handler: async (_input, ctx) => {
			const native = nativeMethods(ctx);
			const cutoff = await native.step("cutoff", async () => host.now() - retainDays * 86_400_000);
			let after = CALL_SESSION;
			let reclaimed = 0;
			let batches = 0;
			for (;;) {
				const batch = (await native.step(`batch:${batches++}`, async () => {
					const candidates = await PostgresStorage.reclaimable(
						options.sql,
						CALL_SESSION,
						after,
						batchSize,
					);
					let count = 0;
					for (const { key, executionId } of candidates) {
						const info = await host.conductor.getExecution(executionId);
						if (info?.queue !== host.queue || info.taskKey !== SESSION_WORKFLOW) continue;
						const settledAt = settlement(info) && (info.completedAt ?? info.failedAt)?.getTime();
						if (!settledAt || settledAt > cutoff) continue;
						if (await PostgresStorage.reclaim(options.sql, key, executionId)) count++;
					}
					return {
						reclaimed: count,
						next: candidates.length < batchSize ? null : candidates.at(-1)!.key,
					};
				})) as { reclaimed: number; next: string | null };
				reclaimed += batch.reclaimed;
				if (batch.next === null) break;
				after = batch.next;
			}
			if (reclaimed) host.logger.info("Reclaimed finished agent call Sessions", { reclaimed });
			return { reclaimed, batches };
		},
	};
	return reclaim;
}
