// Workflow definitions and their Conductor tasks.
import type { ExecutionOutcome, TaskContext } from "pgconductor-js";
import type { Native } from "../execution.ts";
import type { ClassifierResult } from "@earendil-works/pi-ai";
import type { WorkflowTask } from "../conductor.ts";
import { type Json, type JsonObject, parseSchema, type Schema } from "../schema.ts";
import type { InvocationMetadata } from "./metadata.ts";
import type { Surface, Thread } from "../chat/messages.ts";
import type { GroupDefinition, Principal } from "../identity.ts";
import type { AgentRef, AgentResult, AgentRunOptions } from "../agents/definition.ts";
import type { SendOptions, SendReceipt } from "../sessions/delivery.ts";
import type { ResourceRef } from "../plugin.ts";
import type { FollowOptions, FollowReceipt, UnfollowReceipt } from "../routing/follows.ts";
import type { EvaluateRequest } from "../models.ts";
import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "../approvals/request.ts";
import type { Origin, TriggerPayload, TriggerSpec } from "../routing/events.ts";

/** An app workflow as a Conductor task: its output schema validates (and transforms) the result. */
export function workflowTask(
	workflow: AnyWorkflow,
	attempt: WorkflowTask["attempt"],
): WorkflowTask {
	return {
		name: workflow.name,
		// The routing workflow starts app workflows; they have no event triggers.
		triggers: [],
		cron: workflow.definition.cron?.map((c) => ({
			schedule: c.schedule,
			name: c.name ?? workflow.name,
			replyTo: c.replyTo,
		})),
		concurrency: workflow.definition.concurrency,
		retries: workflow.definition.retries,
		attempt,
		handler: async (input, raw, tick) => {
			const ctx = raw as unknown as WorkflowContext;
			// A static cron tick carries no metadata: it replies where its trigger says.
			if (tick?.replyTo) await ctx.fabrial.replyTo(tick.replyTo);
			const output = await workflow.definition.run(input, ctx);
			// The output schema validates (and transforms) the result before the run records it.
			const label = `workflow ${workflow.name} output`;
			if (workflow.definition.output)
				return workflowObject(await parseSchema(workflow.definition.output, output, label), label);
			return output === undefined || output === null ? null : workflowObject(output, label);
		},
	};
}

/** Conductor's context methods Fabrial wraps (typed for Fabrial workflows below) or keeps to itself. */
type WrappedNative = keyof Native;

/** Fabrial's override of a child's metadata: merged onto this execution's (routing-only fields cleared). */
type ChildMetadata =
	| Partial<InvocationMetadata>
	| ((metadata: InvocationMetadata) => Partial<InvocationMetadata>);

/**
 * A workflow's `ctx`: Conductor's `TaskContext` (durable steps, sleeps, waits, signals, child executions)
 * plus Fabrial's fields. Operation keys are Conductor's: a repeated key addresses the same operation, so
 * loops pass distinct keys; keys beginning with `fabrial:` are reserved. Children and events inherit this
 * execution's invocation metadata; internal workflows and `fabrial.*` events are not destinations.
 */
export interface WorkflowContext<TClients = Record<string, unknown>> extends Omit<
	TaskContext,
	WrappedNative
> {
	/** The thread this execution replies into. Absent for observers and executions without a surface. */
	readonly thread: Thread | undefined;
	readonly clients: TClients;
	/** Who asked for this work. */
	readonly actor: Principal | null;

	step<T extends Json | void>(key: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(key: string, duration: string | number): Promise<void>;
	/** Conductor `subscribe`, by event name. */
	subscribe(
		key: string,
		options: { event: string; filter?: Record<string, unknown> },
	): Promise<{ key: string; wait(options?: { timeout?: string | number }): Promise<unknown> }>;
	waitForEvent(
		key: string,
		options: { event: string; filter?: Record<string, unknown>; timeout?: string | number },
	): Promise<{ name: string; payload: JsonObject }>;
	/** Start a workflow without waiting (Conductor `invoke`); returns its execution id. */
	invoke<W extends AnyWorkflow>(
		key: string,
		workflow: W,
		...args: WorkflowArgs<
			W,
			[options?: { cancelWithParent?: boolean; dedupe_key?: string; metadata?: ChildMetadata }]
		>
	): Promise<string>;
	/**
	 * Start a workflow and wait for its output (Conductor `invokeAndWait`). A failed child fails this
	 * execution; to handle it, `invoke` then `waitForResult`.
	 */
	invokeAndWait<W extends AnyWorkflow>(
		key: string,
		workflow: W,
		...args: WorkflowArgs<W, [options?: { timeout?: string | number; metadata?: ChildMetadata }]>
	): Promise<WorkflowOutput<W>>;
	/** Wait until an execution settles; its outcome, with the workflow's output object as `result`. */
	waitForResult(
		key: string,
		executionId: string,
		options?: { timeout?: string | number },
	): Promise<ExecutionOutcome<JsonObject | null> | { status: "timeout" }>;
	/** Conductor `waitForAny`: the first branch to win; execution and signal branches are kept under `key`. */
	waitForAny: TaskContext["waitForAny"];
	schedule<W extends AnyWorkflow>(
		workflow: W,
		name: string,
		options: { cron: string },
		...args: WorkflowArgs<W>
	): Promise<void>;
	unschedule(workflow: AnyWorkflow, name: string): Promise<void>;
	sendSignal(
		executionId: string,
		signal: { name: string; id: string; payload?: Json },
	): Promise<boolean>;
	signalWithStart<W extends AnyWorkflow>(
		key: string,
		options: {
			task: W;
			key: string;
			signal: { name: string; id: string; payload?: Json };
			input?: WorkflowInput<W>;
			metadata?: ChildMetadata;
		},
	): Promise<string>;
	emit(
		event: string,
		payload: JsonObject,
		options?: { dedupe_key?: string; metadata?: ChildMetadata },
	): Promise<string>;

	/** Call: submit to the agent and wait for its answer. */
	agent<O extends Schema | undefined = undefined>(
		id: string,
		agent: AgentRef,
		options: AgentRunOptions<O>,
	): Promise<AgentResult<O>>;
	/**
	 * Send: deliver input to the conversation `[session, agent]` and return. An idle conversation starts a
	 * run; a busy one gets it by kind and priority. The agent's final answer posts to the input's reply thread.
	 */
	send(id: string, agent: AgentRef, options: SendOptions): Promise<SendReceipt>;
	/**
	 * Follow a resource (e.g. `github.pullRequest("acme/app#42")`) in a conversation: its events arrive as
	 * notifications until `unfollow`. From a workflow, pass `to: { agent, session }`; final replies to the
	 * notifications post to this execution's thread (or `to.home`). Following again replaces the options.
	 */
	follow(id: string, resource: ResourceRef<any>, options?: FollowOptions): Promise<FollowReceipt>;
	/** Stop following a resource (default: the calling agent's conversation). */
	unfollow(
		id: string,
		resource: ResourceRef<any>,
		options?: Pick<FollowOptions, "to">,
	): Promise<UnfollowReceipt>;
	/** A classifier judgment with Pi AI's question and result types; durable under `id`. */
	evaluate(id: string, request: EvaluateRequest): Promise<ClassifierResult>;

	waitForApproval(id: string, options: ApprovalOptions): Promise<ApprovalDecision>;
	requestApproval(id: string, options: ApprovalOptions): Promise<ApprovalHandle>;

	/** True when `principal` is a member of `group` (resolved now). */
	isMember(principal: Principal, group: GroupDefinition): Promise<boolean>;
	/** For Fabrial's own helpers: Conductor's unwrapped methods and this execution's metadata. */
	readonly fabrial: {
		readonly native: Native;
		readonly metadata: InvocationMetadata;
		/** Points `thread` (and `replyTo`) at `surface`: a static cron tick's destination. */
		replyTo(surface: Surface): Promise<void>;
	};
}

/** Information about the current run, passed to middleware. */
export interface ExecutionInfo {
	readonly executionId: string;
	readonly workflow: string;
	/** Who asked for this work. */
	readonly actor: Principal | null;
	/** The event that started the request, if any. */
	readonly origin: Origin | null;
}

/**
 * Wraps every attempt of a run, including each resume after a suspension (Conductor middleware). `next`
 * settles when the attempt ends: it resolves when the run returns, suspends or is cancelled, and rejects
 * when it throws. `ctx` is the run's context; middleware may add fields to it, not replace its methods.
 */
export type WorkflowMiddleware = (
	execution: ExecutionInfo,
	ctx: WorkflowContext,
	next: () => Promise<void>,
) => Promise<void>;

export interface WorkflowDefinition<
	TName extends string,
	TInput extends JsonObject,
	TOutput extends JsonObject | null | void,
	TTriggers extends readonly TriggerSpec<any>[],
	TClients = Record<string, unknown>,
> {
	name: TName;
	/** Shown to agents that list this workflow in `tools`. */
	description?: string;
	/**
	 * Input schema for `invoke`, `invokeAndWait`, and agent tool calls. Input and output are JSON objects,
	 * stored by Conductor as they are: both schemas must be object schemas (`z.object`).
	 */
	input?: Schema<TInput>;
	output?: Schema<TOutput>;
	triggers?: TTriggers;
	cron?: { schedule: string; name?: string; replyTo?: Surface }[];
	concurrency?: number;
	retries?: { maxAttempts?: number };
	run(
		this: void,
		input: TInput | TriggerPayload<TTriggers[number]>,
		ctx: WorkflowContext<TClients>,
	): Promise<TOutput>;
}

export interface Workflow<
	TName extends string = string,
	TInput extends JsonObject = JsonObject,
	TOutput extends JsonObject | null | void = JsonObject | void,
> {
	readonly kind: "fabrial.workflow";
	readonly name: TName;
	readonly definition: WorkflowDefinition<TName, TInput, TOutput, readonly TriggerSpec<any>[]>;
}

export type AnyWorkflow = Workflow<string, any, any>;
export type WorkflowInput<W> = W extends Workflow<string, infer I, any> ? I : never;
export type WorkflowOutput<W> = W extends Workflow<string, any, infer O> ? O : never;
/** The arguments of `invoke` and `invokeAndWait`: a workflow without `input` takes none, and runs with `{}`. */
export type WorkflowArgs<W, Rest extends unknown[] = []> = [WorkflowInput<W>] extends [never]
	? [input?: null, ...Rest]
	: [input: WorkflowInput<W>, ...Rest];

export function defineWorkflow<
	const TName extends string,
	TInput extends JsonObject = never,
	TOutput extends JsonObject | null | void = void,
	const TTriggers extends readonly TriggerSpec<any>[] = [],
>(
	definition: WorkflowDefinition<TName, TInput, TOutput, TTriggers>,
): Workflow<TName, TInput, TOutput> {
	for (const field of ["input", "output"] as const) {
		const type = nonObjectType(definition[field] as ZodLike | undefined);
		if (type !== undefined)
			throw new Error(
				`Workflow ${definition.name}: ${field} must be an object schema (z.object), not ${type}`,
			);
	}
	return {
		kind: "fabrial.workflow",
		name: definition.name,
		definition: definition as unknown as Workflow<TName, TInput, TOutput>["definition"],
	};
}

type ZodLike = {
	_zod?: {
		def?: { type?: string; in?: ZodLike; out?: ZodLike; innerType?: ZodLike; options?: ZodLike[] };
	};
};
/** Zod schema types whose values are never JSON objects. */
const NON_OBJECT_TYPES = new Set([
	"string",
	"number",
	"bigint",
	"boolean",
	"symbol",
	"null",
	"undefined",
	"void",
	"never",
	"nan",
	"literal",
	"enum",
	"template_literal",
	"array",
	"tuple",
	"set",
	"map",
	"date",
	"file",
	"function",
	"promise",
]);
/**
 * The type of a zod schema whose values are definitely not objects, looking through pipes (both
 * sides), wrappers and unions. Anything else (objects, records, `any`, transforms, other Standard
 * Schemas) passes: each value is checked when a run starts and ends.
 */
function nonObjectType(schema: ZodLike | undefined): string | undefined {
	const def = schema?._zod?.def;
	if (!def?.type) return undefined;
	if (NON_OBJECT_TYPES.has(def.type)) return def.type;
	if (def.type === "pipe") return nonObjectType(def.in) ?? nonObjectType(def.out);
	if (def.innerType) return nonObjectType(def.innerType);
	if (def.options?.length) {
		const types = def.options.map(nonObjectType);
		return types.every((type) => type !== undefined) ? types.join(" | ") : undefined;
	}
	return undefined;
}

/** Workflow input and output are JSON objects (Conductor's payload and result): anything else is rejected. */
export function workflowObject(value: unknown, label: string): JsonObject {
	if (value === null || typeof value !== "object" || Array.isArray(value))
		throw new Error(`Invalid ${label}: expected a JSON object`);
	return value as JsonObject;
}
