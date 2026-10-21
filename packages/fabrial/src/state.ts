import type { JsonObject } from "./json.ts";
import type { Schema } from "./schema.ts";

/**
 * `"session"`: one value per Session, shared by the workflows on that Session's thread and the conversations
 * following it; a `ctx.agent` call's tools see their own call Session's value. `"agent"`: one value per
 * conversation, for agent tools only.
 */
export type StateScope = "session" | "agent";

/**
 * State that agents see and change and that outlives a single step. Backed by Pi documents in the
 * Pi Session (implemented in `src/agents`).
 */
export interface StateDefinition<
	T extends JsonObject = JsonObject,
	S extends StateScope = StateScope,
> {
	readonly kind: "fabrial.state";
	readonly name: string;
	readonly scope: S;
	readonly schema: Schema<T>;
	readonly initial: () => T;
	/** How agents listing this state see it in their system prompt. `undefined` omits it. */
	readonly render?: (state: T) => string | undefined;
	/** Pi document version; bump with a migration when the shape changes. */
	readonly version: number;
	readonly migrate?: (old: unknown, fromVersion: number) => T;
}

export function defineState<T extends JsonObject, S extends StateScope>(definition: {
	name: string;
	scope: S;
	schema: Schema<T>;
	initial: () => T;
	render?: (state: T) => string | undefined;
	version?: number;
	migrate?: (old: unknown, fromVersion: number) => T;
}): StateDefinition<T, S> {
	return { kind: "fabrial.state", version: 1, ...definition };
}

/**
 * The states each Fabrial instance defined, by name. A workflow's `ctx.state` runs in its Session's host, which
 * resolves the definition by name: schemas, initializers and functions do not travel in signals.
 */
export const definedStates = new WeakMap<object, Map<string, StateDefinition>>();

/**
 * Running apps per instance. An app decides at `start()` whether it runs Pi, from the agents and states
 * defined by then, so neither can be defined while one of the instance's apps runs; after they stop it can.
 */
const startedScopes = new WeakMap<object, number>();

/** Counts a started app of `scope`; the returned function counts it stopped. */
export function sealDefinitions(scope: object): () => void {
	startedScopes.set(scope, (startedScopes.get(scope) ?? 0) + 1);
	return () => {
		const running = (startedScopes.get(scope) ?? 1) - 1;
		if (running > 0) startedScopes.set(scope, running);
		else startedScopes.delete(scope);
	};
}

export function assertDefinable(scope: object, what: string): void {
	if (startedScopes.has(scope))
		throw new Error(
			`${what} is defined after app.start(); define agents and state before starting the app`,
		);
}

export function registerState<T extends JsonObject, S extends StateScope>(
	definition: StateDefinition<T, S>,
	scope: object,
): StateDefinition<T, S> {
	assertDefinable(scope, `State ${definition.name}`);
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
