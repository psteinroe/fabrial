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
} from "../modules.ts";

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
 * Subagent work that outlives a Session's opening. `slots` count running children per lead for `limits.concurrent`: one per
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
		reporters: Record<string, { lead: number; child: number; name: string }>;
		reported: Record<string, number[]>;
	} => ({ slots: {}, calls: {}, reporters: {}, reported: {} }),
});
export const ChildResults = defineDoc({
	kind: "fabrial.children",
	scope: "session",
	version: 1,
	initial: (): { results: Record<string, JsonObject> } => ({ results: {} }),
});
/** How often the Session's execution suspended on its tools and inputs: names each wait. */
export const Suspensions = defineDoc({
	kind: "fabrial.suspensions",
	scope: "session",
	version: 1,
	initial: (): { count: number } => ({ count: 0 }),
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
/** Runs a child a Session waits on and reports its outcome once committed (see `startChild`). */
export const REPORT_WORKFLOW = "fabrial.pi.report";
/** Name of the signal a reporter sends the Session's execution, id `child:<child execution id>`. */
export const CHILD_SIGNAL = "child";
/** Payload of a `child` signal: the reporter's execution id, and the committed outcome of its child. */
export interface ChildSignal extends JsonObject {
	executionId: string;
	result: JsonObject;
}

/** A `ctx.agent` call's Session execution returns its outcome: the answer, or why there is none. */
export type CallOutcome =
	| { status: "completed"; output: Json }
	| { status: "failed"; error: string; executionId: string };

/**
 * What Fabrial owes for one request submitted to Pi, beyond what Pi tracks: committed before the request is submitted,
 * so a crash in between leaves the obligation for its redelivery. Pi owns the submission and its lifecycle (looked up
 * by request id); this holds only what Pi cannot know.
 */
export type Obligation = {
	/** The conversation the request is submitted to. */
	conversationId: number;
	/**
	 * `reply`: its answer posts to the reply thread of its invocation (sent input, or a background child's report).
	 * `call`: its answer is a `ctx.agent` call's outcome. `task`: nothing; a background child's task acts for `origin`.
	 */
	owes: "reply" | "call" | "task";
	/** The request whose invocation it acts for (follows, reply thread), when not its own: a background child's. */
	origin?: string;
	/** Nothing more is owed: its answer or error reply is posting or skipped, its call settled, or it was stopped. */
	fulfilled?: true;
	/** A stop cancelled the workflows its tools started; its tools start none afterwards. */
	stopped?: true;
	/** A settled call's outcome: its Session execution's result. */
	outcome?: CallOutcome;
	/** The error id of its error reply; assigned once. */
	errorId?: string;
	/**
	 * Final and error replies posted for it, by their effect's intent key: the run settles only once each post's outcome
	 * arrived. A post that failed for good stays `failed` (logged, never retried or answered with an error reply).
	 */
	deliveries?: Record<
		string,
		{ executionId: string; status: "pending" | "posted" | "failed"; error?: string }
	>;
	/** Workflows and effects its tools started, by intent key: stopping the request cancels the attached ones. */
	intents?: string[];
	/** Workflows its tools wait on, by task id. */
	children?: Record<string, { executionId: string; detached: boolean }>;
};
export const Obligations = defineDoc({
	kind: "fabrial.obligations",
	scope: "session",
	version: 1,
	initial: (): { requests: Record<string, Obligation> } => ({ requests: {} }),
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
	return tx.doc(token, "session", "");
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
