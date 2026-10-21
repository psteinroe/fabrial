import type { JsonObject } from "./json.ts";
import type { Schema } from "./schema.ts";

export type StateScope = "thread" | "agent";

/**
 * State that agents see and change and that outlives a single step. Backed by Pi documents in the
 * thread's Pi Session (implemented in `src/agents`).
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

/**
 * The states each Fabrial instance defined, by name. A workflow's `ctx.state` runs in its Session's host, which
 * resolves the definition by name: schemas, initializers and functions do not travel in signals.
 */
export const definedStates = new WeakMap<object, Map<string, StateDefinition>>();

export function registerState<T extends JsonObject>(
	definition: StateDefinition<T>,
	scope: object,
): StateDefinition<T> {
	let states = definedStates.get(scope);
	if (!states) definedStates.set(scope, (states = new Map()));
	if (states.has(definition.name)) throw new Error(`State ${definition.name} is already defined`);
	states.set(definition.name, definition as unknown as StateDefinition);
	return definition;
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
