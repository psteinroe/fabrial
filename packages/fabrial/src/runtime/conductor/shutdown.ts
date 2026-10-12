import type { Execution, ExecutionResult } from "pgconductor-js/src/database-client";
import type { AnyTask, Orchestrator } from "./pgconductor.ts";

type Worker = {
	readonly signal: AbortSignal;
	executeSingleTask(
		task: AnyTask,
		execution: Execution,
	): Promise<ExecutionResult | ExecutionResult[]>;
	executeBatchTask(task: AnyTask, key: string, executions: Execution[]): Promise<ExecutionResult[]>;
};

// SHIM(conductor#11): native workers drain queued claims after stop aborts their
// signal. createTaskSignal then aborts synchronously, BEFORE the worker installs
// its abort listener. A handler's checkpoint/step hangs up forever, leaving the
// worker's abort race unresolved. Do not enter handlers with an aborted worker;
// return the native release outcome instead, preserving work for the next start.
// Apply to internal workers too (including batched event dispatch).
export function releaseQueuedExecutionsOnStop(orchestrator: Orchestrator): void {
	const { workers } = orchestrator as unknown as { workers: Worker[] };
	const released = (execution: Execution): ExecutionResult => ({
		execution_id: execution.id,
		queue: execution.queue,
		task_key: execution.task_key,
		status: "released",
	});
	for (const worker of workers) {
		const single = worker.executeSingleTask.bind(worker);
		worker.executeSingleTask = (task, execution) =>
			worker.signal.aborted ? Promise.resolve(released(execution)) : single(task, execution);
		const batch = worker.executeBatchTask.bind(worker);
		worker.executeBatchTask = (task, key, executions) =>
			worker.signal.aborted
				? Promise.resolve(executions.map(released))
				: batch(task, key, executions);
	}
}
