import {
	type ConversationId,
	defineDoc,
	defineDocFamily,
	type ToolExecutionApi,
	type Tx,
} from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import { type JsonObject, parseSchema, type Schema } from "./schema.ts";

export const ToolReceipt = defineDoc({
	kind: "fabrial.tool-result",
	scope: "task",
	version: 1,
	initial: (): { result: JsonObject | null } => ({ result: null }),
});

export async function stateDraft<T extends JsonObject>(
	tx: Tx,
	definition: StateDefinition<T>,
	conversationId?: ConversationId,
) {
	const common = {
		kind: `fabrial.state.${definition.name}`,
		version: definition.version,
		initial: definition.initial,
		...(definition.migrate ? { migrate: definition.migrate } : {}),
	};
	if (definition.scope === "agent") {
		if (!conversationId)
			throw new Error(
				`Agent state ${definition.name} requires a Pi conversation; use it inside an agent tool`,
			);
		return tx.doc(
			defineDoc({ ...common, scope: "conversation", history: "rewindable", fork: "asOf" }),
			conversationId,
		);
	}
	const token = defineDocFamily({
		family: true,
		...common,
		scope: "session",
		initial: (_seed: string) => definition.initial(),
	});
	return tx.doc(token, "session", "");
}

async function applyState<T extends JsonObject>(
	tx: Tx,
	definition: StateDefinition<T>,
	conversationId: ConversationId | undefined,
	changes: readonly ((draft: T) => void | T)[],
): Promise<T> {
	const draft = await stateDraft(tx, definition, conversationId);
	let value = JSON.parse(JSON.stringify(draft)) as T;
	for (const change of changes) value = change(value) ?? value;
	value = await parseSchema(definition.schema, value, `state ${definition.name}`);
	for (const key of Object.keys(draft)) if (!(key in value)) delete draft[key];
	Object.assign(draft, value);
	return value;
}
export type StagedState = {
	definition: StateDefinition;
	changes: ((draft: JsonObject) => void | JsonObject)[];
	value: JsonObject;
};

export async function commitStaged(
	api: ToolExecutionApi,
	context: Context,
	staged: Map<string, StagedState>,
	change: (tx: import("@earendil-works/pi-durable").Tx) => Promise<void>,
): Promise<void> {
	await api.commit(async (tx) => {
		for (const entry of staged.values())
			if (entry.changes.length)
				await applyState(tx, entry.definition, api.conversationId, entry.changes);
		await change(tx);
	}, context);
}

/**
 * `"session"`: one value per Session, shared by the tools of its conversations; a `ctx.agent` call's tools see their
 * own call Session's value. `"agent"`: one value per conversation. Only agent tools use state.
 */
export type StateScope = "session" | "agent";

/**
 * State that agents see and change and that outlives a single step. Backed by Pi documents in the
 * Pi Session (`stateDraft` below).
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

/** The states each Fabrial instance defined, by name: a name is its Pi document's kind, so it is unique. */
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

/** `ctx.state(S)` in Pi tool code: no ids, committed with the tool result. */
export interface ToolStateHandle<T extends JsonObject> {
	get(): Promise<T>;
	update(fn: (draft: T) => void | T): Promise<T>;
}
