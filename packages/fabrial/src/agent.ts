import type { EvaluateQuestion, EvaluateRequest, EvaluateResult } from "./evaluate.ts";
import type { Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { AgentRef } from "./runtime.ts";
import type { Schema } from "./schema.ts";
import type { AgentSession, SendOptions, SendReceipt } from "./send.ts";
import type { ResourceRef } from "./plugin.ts";
import type { FollowOptions, FollowReceipt, UnfollowReceipt } from "./follows.ts";
import type { StateDefinition, ToolStateHandle } from "./state.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface } from "./thread.ts";
import type { TriggerPayload, TriggerSpec } from "./trigger.ts";
import type { AnyWorkflow, WorkflowInput, WorkflowOutput } from "./workflow.ts";

export type ThinkingLevel = "off" | "low" | "medium" | "high";

/** Read-only context for per-run instructions. */
export interface InstructionContext<TClients = Record<string, unknown>> {
	/** Who sent the message this run answers. */
	readonly actor: Principal | null;
	readonly clients: TClients;
	state<T extends JsonObject>(definition: StateDefinition<T>): { get(): Promise<T> };
}

/** A static string, or a function rendered per run (`undefined` omits it). */
export type Instructions<TClients = Record<string, unknown>> =
	| string
	| ((ctx: InstructionContext<TClients>) => string | undefined | Promise<string | undefined>);

/** Durable thread operations available to tools. Each takes an id, like workflow operations. */
export interface ToolThread {
	post(id: string, message: OutboundMessage): Promise<MessageRef>;
	update(id: string, message: MessageRef, content: OutboundMessage): Promise<void>;
	history(
		id: string,
		options?: { limit?: number; sinceLastBotReply?: boolean },
	): Promise<ChatMessage[]>;
}

export interface ToolContext<
	TClients = Record<string, unknown>,
> extends InstructionContext<TClients> {
	readonly thread: ToolThread | undefined;
	/** Without an id; changes commit atomically with the tool result. */
	state<T extends JsonObject>(definition: StateDefinition<T>): ToolStateHandle<T>;
	invoke<W extends AnyWorkflow>(
		id: string,
		workflow: W,
		input: WorkflowInput<W>,
	): Promise<WorkflowOutput<W>>;
	start<W extends AnyWorkflow>(id: string, workflow: W, input: WorkflowInput<W>): Promise<string>;
	evaluate<Q extends Record<string, EvaluateQuestion>>(
		id: string,
		request: EvaluateRequest<Q>,
	): Promise<EvaluateResult<Q>>;
	/** Send input to a conversation and return, as in workflows; `replyTo` defaults to this request's. */
	send(id: string, agent: AgentRef, options: SendOptions): Promise<SendReceipt>;
	/**
	 * Follow a resource in this agent's conversation, as in workflows: its events arrive as notifications,
	 * whose final replies post where this request replies (the conversation's home).
	 */
	follow(id: string, resource: ResourceRef<any>, options?: FollowOptions): Promise<FollowReceipt>;
	unfollow(
		id: string,
		resource: ResourceRef<any>,
		options?: Pick<FollowOptions, "to">,
	): Promise<UnfollowReceipt>;
	/** Aborted when the agent run is cancelled or the call is interrupted. */
	readonly signal: AbortSignal;
}

/** Strings are passed to the model as is; other JSON values are serialized. */
export type ToolResult = Json | void;

export interface ToolDefinition<TInput = any, TClients = Record<string, unknown>> {
	/** Name the model calls; letters, digits, `_` and `-`. */
	name: string;
	description: string;
	/** Standard Schema with Standard JSON Schema support (e.g. zod ≥ 4.2). Validated before `run`. */
	input: Schema<TInput>;
	/** A call interrupted by a crash runs again. Otherwise the model sees an interrupted result. */
	idempotent?: boolean;
	run(input: TInput, ctx: ToolContext<TClients>): Promise<ToolResult> | ToolResult;
}

export interface Tool<TInput = any> extends ToolDefinition<TInput, any> {
	readonly kind: "fabrial.tool";
}

/** What an agent can call: tools and workflows (by name and description). */
export type AgentTool = Tool | ToolDefinition | AnyWorkflow;

export interface ToolkitDefinition<TClients = Record<string, unknown>> {
	name: string;
	tools?: readonly AgentTool[];
	instructions?: Instructions<TClients> | readonly Instructions<TClients>[];
}

export interface Toolkit extends ToolkitDefinition<any> {
	readonly kind: "fabrial.toolkit";
}

/** The event an agent's `session` function receives: its triggers' payloads, or any JSON for `ctx.send`. */
export type AgentEvent<TTriggers extends readonly TriggerSpec<any>[]> = [
	TriggerPayload<TTriggers[number]>,
] extends [never]
	? any
	: TriggerPayload<TTriggers[number]>;

export interface AgentDefinition<
	TPluginId extends string = string,
	TClients = Record<string, unknown>,
	TTriggers extends readonly TriggerSpec<any>[] = readonly TriggerSpec<any>[],
> {
	/** Stable: the continuation key; labels traces and progress. */
	name: string;
	/** What the agent does and when to use it: the tool description when it is a subagent. */
	description: string;
	/** `"provider/model"`, e.g. `"anthropic/claude-sonnet-5"`. */
	model?: string;
	thinking?: ThinkingLevel;
	instructions?: Instructions<TClients> | readonly Instructions<TClients>[];
	tools?: readonly AgentTool[];
	/** Plugin ids and app toolkits. */
	toolkits?: readonly (TPluginId | Toolkit)[];
	/** States rendered into the prompt via their `render`. */
	state?: readonly StateDefinition<any>[];
	/** Agents this one delegates to, one tool each: an agent, `"self"` (a copy of this one), or options. */
	subagents?: readonly SubagentEntry[];
	limits?: SubagentLimits;
	/**
	 * Events delivered to one of the agent's conversations, like workflow triggers (plugin helpers such as
	 * `slack.mentioned({ channel })`). They compete with workflow triggers: the most specific one wins.
	 */
	triggers?: TTriggers;
	/**
	 * Which conversation an event reaches: one per thread (`"thread"`, default), one per channel (`"channel"`),
	 * or the app's key computed from the event (e.g. `(issue) => \`ticket:${issue.id}\``).
	 */
	session?: AgentSession<AgentEvent<TTriggers>>;
	/**
	 * Where conversations started by events without a thread (a Sentry issue, a schedule) live, e.g.
	 * `slack.channel("C_BUGS")`: the first final reply creates the thread, which the conversation follows.
	 */
	home?: Surface;
}

/** A subagent with options. */
export interface SubagentOptions {
	agent: Agent | "self";
	/** `"fresh"` (default): the child sees only its task. `"fork"`: it inherits the lead's transcript so far. */
	context?: "fresh" | "fork";
	/** Whether the model may (`"allow"`, default), must, or must not run the child in the background. */
	background?: "allow" | "always" | "never";
}

export type SubagentEntry = Agent | "self" | SubagentOptions;

export interface SubagentLimits {
	/** Levels of subagents below this agent; with 1 (default) children get no subagent tools. */
	depth?: number;
	/** Children running at once per lead conversation (default 5). */
	concurrent?: number;
}

/** A subagent entry resolved against its lead: `"self"` is the lead, options are filled in. */
export interface ResolvedSubagent {
	/** The tool name the model calls. */
	readonly tool: string;
	readonly agent: Agent;
	readonly context: "fresh" | "fork";
	readonly background: "allow" | "always" | "never";
}

export interface Agent extends AgentRef {
	readonly definition: AgentDefinition<string, any>;
}

/** Payload of `hooks.agent.onModelResponse`. */
export interface ModelResponse {
	readonly agent: string;
	readonly model: { provider: string; modelId: string };
	readonly text: string;
	readonly toolCalls: readonly { id: string; name: string; arguments: JsonObject }[];
	readonly stopReason: string;
	readonly error?: string;
	readonly usage?: { input: number; output: number; totalTokens: number };
}

/** Payload of `hooks.agent.onToolCall`, after the tool settled. */
export interface ToolCall {
	readonly agent: string;
	readonly callId: string;
	readonly name: string;
	readonly arguments: JsonObject;
	readonly result: string;
	readonly isError: boolean;
}

/** Observers on every agent. Best effort: they may repeat on recovery, and errors are logged. */
export interface AgentObservers {
	onModelResponse?: (response: ModelResponse) => void | Promise<void>;
	onToolCall?: (call: ToolCall) => void | Promise<void>;
}

/** Registry of agents per instance, keyed by name when the app starts. */
export const definedAgents = new WeakMap<object, Set<Agent>>();

export function defineAgent(definition: AgentDefinition<string, any>, scope: object): Agent {
	if (!definition.name) throw new Error("Agent name must not be empty");
	if (typeof definition.description !== "string" || !definition.description.trim())
		throw new Error(`Agent ${definition.name} needs a description`);
	if (definition.model !== undefined) parseModel(definition.model);
	for (const spec of definition.triggers ?? []) {
		if ((spec as TriggerSpec | undefined)?.kind !== "fabrial.trigger")
			throw new Error(`Agent ${definition.name}: triggers must be plugin triggers`);
		if (spec.observe)
			throw new Error(
				`Agent ${definition.name}: observe triggers belong on workflows; an agent's trigger receives the event`,
			);
	}
	const { session, home } = definition;
	if (
		session !== undefined &&
		session !== "thread" &&
		session !== "channel" &&
		typeof session !== "function"
	)
		throw new Error(`Agent ${definition.name}: session must be "thread", "channel", or a function`);
	if (home !== undefined && home?.kind !== "thread" && home?.kind !== "channel")
		throw new Error(`Agent ${definition.name}: home must be a thread or channel surface`);
	const agent: Agent = { kind: "fabrial.agent", name: definition.name, definition };
	validateSubagents(agent);
	let agents = definedAgents.get(scope);
	if (!agents) definedAgents.set(scope, (agents = new Set()));
	agents.add(agent);
	return agent;
}

/** The name the model calls a workflow or subagent by: letters, digits, and `_`. */
export const toolName = (name: string) => name.replaceAll(/[^a-zA-Z0-9_]/g, "_");

export const SUBAGENT_DEFAULTS = { depth: 1, concurrent: 5 } as const;

export function resolveSubagents(lead: Agent): ResolvedSubagent[] {
	return (lead.definition.subagents ?? []).map((entry) => {
		const options: SubagentOptions =
			entry === "self" || (typeof entry === "object" && "kind" in entry) ? { agent: entry } : entry;
		const agent = options.agent === "self" ? lead : options.agent;
		return {
			tool: toolName(agent.name),
			agent,
			context: options.context ?? "fresh",
			background: options.background ?? "allow",
		};
	});
}

/** Tool names of an agent's own tools and app toolkits; plugin toolkits are checked when the app starts. */
export function ownToolNames(definition: AgentDefinition<string, any>): string[] {
	const name = (tool: AgentTool) => (isWorkflow(tool) ? toolName(tool.name) : tool.name);
	return [
		"no_reply",
		...(definition.tools ?? []).map(name),
		...(definition.toolkits ?? []).flatMap((entry) =>
			typeof entry === "string" ? [] : (entry.tools ?? []).map(name),
		),
	];
}

function validateSubagents(lead: Agent): void {
	const { name, subagents = [], limits = {} } = lead.definition;
	for (const [key, value] of Object.entries(limits))
		if (value !== undefined && (!Number.isInteger(value) || value < 1))
			throw new Error(`Agent ${name}: limits.${key} must be a positive integer`);
	for (const entry of subagents) {
		const options =
			typeof entry === "object" && !("kind" in entry) ? (entry as SubagentOptions) : undefined;
		const agent: unknown = options ? options.agent : entry;
		if (agent !== "self" && (agent as Agent | undefined)?.kind !== "fabrial.agent")
			throw new Error(`Agent ${name}: a subagent must be an agent, "self", or { agent }`);
		if (options?.context !== undefined && !["fresh", "fork"].includes(options.context))
			throw new Error(`Agent ${name}: subagent context must be "fresh" or "fork"`);
		if (
			options?.background !== undefined &&
			!["allow", "always", "never"].includes(options.background)
		)
			throw new Error(`Agent ${name}: subagent background must be "allow", "always", or "never"`);
	}
	const taken = new Set(ownToolNames(lead.definition));
	for (const subagent of resolveSubagents(lead)) {
		if (taken.has(subagent.tool))
			throw new Error(
				`Agent ${name}: subagent ${subagent.agent.name} is called ${subagent.tool}, which another tool or subagent already uses`,
			);
		taken.add(subagent.tool);
	}
}

export function defineTool<TInput>(definition: ToolDefinition<TInput, any>): Tool<TInput> {
	if (!/^[a-zA-Z0-9_-]{1,64}$/.test(definition.name))
		throw new Error(`Invalid tool name: ${definition.name}`);
	return { kind: "fabrial.tool", ...definition };
}

export function defineToolkit(definition: ToolkitDefinition<any>): Toolkit {
	if (!definition.name) throw new Error("Toolkit name must not be empty");
	return { kind: "fabrial.toolkit", ...definition };
}

export function parseModel(model: string): { provider: string; modelId: string } {
	const index = model.indexOf("/");
	if (index <= 0 || index === model.length - 1)
		throw new Error(`Model must be "provider/model", got "${model}"`);
	return { provider: model.slice(0, index), modelId: model.slice(index + 1) };
}

export function isWorkflow(tool: AgentTool): tool is AnyWorkflow {
	return "kind" in tool && tool.kind === "fabrial.workflow";
}
