// SHIM(conductor#9): pgconductor-js has no Node build, exports map, or type declarations yet, and its
// entry point does not export defineTask, TaskSchemas, or EventSchemas. Import its source directly
// until it does.
export {
	Conductor,
	Orchestrator,
	WaitForEventTimeoutError,
	defineEvent,
	parseDuration,
} from "pgconductor-js/src/index";
export type { DurationInput, EventDefinition, FilterForEvent } from "pgconductor-js/src/index";
export { defineTask } from "pgconductor-js/src/task-definition";
export { EventSchemas, TaskSchemas } from "pgconductor-js/src/schemas";
export type { TaskContext } from "pgconductor-js/src/task-context";
export type { AnyTask } from "pgconductor-js/src/task";
export type { WorkerConfig } from "pgconductor-js/src/worker";
export type { Logger } from "pgconductor-js/src/lib/logger";
export { compileEventFilter } from "pgconductor-js/src/event-trigger-validation";
export type { EventFilterTerm } from "pgconductor-js/src/database-client";
