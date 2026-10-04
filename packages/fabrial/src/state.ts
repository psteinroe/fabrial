import type { JsonObject } from "./json.ts";
import type { Schema } from "./schema.ts";

export type StateScope = "interaction" | "thread" | "agent";

/**
 * State that agents see and change and that outlives a single step. Backed by Pi documents in the
 * thread's Pi Session (implemented by `@fabrial/pi`).
 */
export interface StateDefinition<T extends JsonObject = JsonObject> {
	readonly kind: "fabrial.state";
	readonly name: string;
	readonly scope: StateScope;
	readonly schema: Schema<T>;
	readonly initial: () => T;
	/** How agents listing this state see it in their system prompt. `undefined` omits it. */
	readonly render?: (state: T) => string | undefined;
	/** Pi document version; bump with a migration when the shape changes. */
	readonly version: number;
	readonly migrate?: (old: unknown, fromVersion: number) => T;
}

export function defineState<T extends JsonObject>(definition: {
	name: string;
	scope: StateScope;
	schema: Schema<T>;
	initial: () => T;
	render?: (state: T) => string | undefined;
	version?: number;
	migrate?: (old: unknown, fromVersion: number) => T;
}): StateDefinition<T> {
	return { kind: "fabrial.state", version: 1, ...definition };
}

/** `ctx.state(S)` in workflow code: durable operations with ids. */
export interface WorkflowStateHandle<T extends JsonObject> {
	get(id: string): Promise<T>;
	/** Mutate a draft (or return a replacement); returns the new value. */
	update(id: string, fn: (draft: T) => void | T): Promise<T>;
}

/** `ctx.state(S)` in Pi tool code: no ids, committed with the tool result. */
export interface ToolStateHandle<T extends JsonObject> {
	get(): Promise<T>;
	update(fn: (draft: T) => void | T): Promise<T>;
}
