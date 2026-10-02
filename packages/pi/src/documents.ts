import {
	defineDoc,
	defineDocFamily,
	type ConversationId,
	type Tx,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type InvocationMetadata,
	type JsonObject,
	type StateDefinition,
} from "fabrial";

export const Conversations = defineDoc({
	kind: "fabrial.conversations",
	scope: "session",
	version: 1,
	initial: (): { ids: Record<string, number> } => ({ ids: {} }),
});
export const Invocation = defineDocFamily({
	family: true,
	kind: "fabrial.invocation",
	scope: "session",
	version: 1,
	initial: (metadata: InvocationMetadata) => ({ ...metadata, actor: metadata.requestedBy }),
});
export const Binding = defineDoc({
	kind: "fabrial.binding",
	scope: "conversation",
	history: "latest",
	fork: "initial",
	version: 1,
	initial: (): { requestId: string | null } => ({ requestId: null }),
});
export const ChildResults = defineDoc({
	kind: "fabrial.children",
	scope: "session",
	version: 1,
	initial: (): { results: Record<string, JsonObject> } => ({ results: {} }),
});
export const Runs = defineDoc({
	kind: "fabrial.runs",
	scope: "session",
	version: 1,
	initial: (): {
		runs: Record<
			string,
			{
				conversationId: number;
				children: Record<string, { executionId: string; detached: boolean }>;
			}
		>;
	} => ({ runs: {} }),
});

export const ToolReceipt = defineDoc({
	kind: "fabrial.tool-result",
	scope: "task",
	version: 1,
	initial: (): { result: JsonObject | null } => ({ result: null }),
});

export async function stateDraft<T extends JsonObject>(
	tx: Tx,
	definition: StateDefinition<T>,
	interactionId: string,
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
	return tx.doc(token, definition.scope === "thread" ? "thread" : interactionId, "");
}

export async function applyState<T extends JsonObject>(
	tx: Tx,
	definition: StateDefinition<T>,
	interactionId: string,
	conversationId: ConversationId | undefined,
	changes: readonly ((draft: T) => void | T)[],
): Promise<T> {
	const draft = await stateDraft(tx, definition, interactionId, conversationId);
	let value = JSON.parse(JSON.stringify(draft)) as T;
	for (const change of changes) value = change(value) ?? value;
	value = await parseSchema(definition.schema, value, `state ${definition.name}`);
	for (const key of Object.keys(draft)) if (!(key in value)) delete draft[key];
	Object.assign(draft, value);
	return value;
}
