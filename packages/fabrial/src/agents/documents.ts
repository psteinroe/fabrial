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
	type Json,
	type StateDefinition,
} from "../index.ts";

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
/** A subagent's identity and the subagent levels left below it; initial on a lead conversation. */
export const Subagent = defineDoc({
	kind: "fabrial.subagent",
	scope: "conversation",
	history: "latest",
	fork: "initial",
	version: 1,
	initial: (): { agent: string | null; name: string | null; depth: number | null } => ({
		agent: null,
		name: null,
		depth: null,
	}),
});
/** Named children per calling conversation: a continued child is forked, so a name moves to its newest conversation. */
export const SubagentNames = defineDoc({
	kind: "fabrial.subagent-names",
	scope: "session",
	version: 1,
	initial: (): {
		leads: Record<
			string,
			{
				next: Record<string, number>;
				children: Record<string, { agent: string; conversationId: number; background?: boolean }>;
			}
		>;
	} => ({ leads: {} }),
});
/**
 * Subagent work that outlives a driver cycle. `slots` count running children per lead for `limits.concurrent`: one per
 * holder task (a foreground call or a background reporter), released when the holder ends. `calls` make a rerun of a
 * background call find its reporter; `reported` keeps several inputs that end in one answer from reporting it twice.
 */
export const SubagentWork = defineDoc({
	kind: "fabrial.subagent-work",
	scope: "session",
	version: 1,
	initial: (): {
		slots: Record<string, Record<string, string>>;
		calls: Record<string, number>;
		reporters: Record<
			string,
			{ lead: number; child: number; name: string; run: string; report: number | null }
		>;
		reported: Record<string, number[]>;
	} => ({ slots: {}, calls: {}, reporters: {}, reported: {} }),
});
export const ChildResults = defineDoc({
	kind: "fabrial.children",
	scope: "session",
	version: 1,
	initial: (): { results: Record<string, JsonObject> } => ({ results: {} }),
});
export const WorkflowReceipts = defineDocFamily({
	family: true,
	kind: "fabrial.workflow-state-receipt",
	scope: "session",
	version: 1,
	initial: (_seed: string): { value: JsonObject | null } => ({ value: null }),
});

export interface ChildIntent extends JsonObject {
	workflow: string;
	input: Json;
	metadata: InvocationMetadata;
	dedupeKey: string;
	detached: boolean;
	executionId: string | null;
}
export const ChildIntents = defineDocFamily({
	family: true,
	kind: "fabrial.child-intent",
	scope: "session",
	version: 1,
	initial: (intent: ChildIntent): ChildIntent => intent,
});
export const EFFECT_WORKFLOW = "fabrial.pi.effect";

export const Runs = defineDoc({
	kind: "fabrial.runs",
	scope: "session",
	version: 1,
	initial: (): {
		runs: Record<
			string,
			{
				conversationId: number;
				/** The agent whose conversation runs; missing on runs of the removed per-request drivers. */
				agent?: string;
				/** A `ctx.agent` call: its request, the calling execution, and whether it outlives it. */
				call?: { requestId: string; caller: string; detached: boolean };
				/** Settled: its call's answer was emitted, or it was cancelled. */
				done?: boolean;
				children: Record<string, { executionId: string; detached: boolean }>;
				intents?: string[];
				/** Error ids of failed runs, by the request whose thread got the error reply; assigned once. */
				errors?: Record<string, string>;
				requests?: Record<
					string,
					{
						content: string;
						submissionId: number | null;
						delivery?: import("../send.ts").Delivery;
						/** Sent with `ctx.send` or a subscription, so its answer posts to its reply thread. */
						sent?: boolean;
					}
				>;
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
	return tx.doc(token, "thread", "");
}

export async function applyState<T extends JsonObject>(
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
