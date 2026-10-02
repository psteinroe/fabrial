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
