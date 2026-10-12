import type { EvaluateQuestion, EvaluateRequest, EvaluateResult } from "./evaluate.ts";
import type { Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { AgentRef, InvocationMetadata } from "./runtime.ts";
import type { Schema } from "./schema.ts";
import type { StateDefinition, ToolStateHandle } from "./state.ts";
import type { ChatMessage, MessageRef, OutboundMessage } from "./thread.ts";
import type { AnyWorkflow, WorkflowInput, WorkflowOutput } from "./workflow.ts";

export type ThinkingLevel = "off" | "low" | "medium" | "high";

/** Read-only context for per-run instructions. */
export interface InstructionContext<TClients = Record<string, unknown>> {
	/** Who sent the message this run answers. */
	readonly actor: Principal | null;
	readonly requestedBy: Principal | null;
	readonly clients: TClients;
	readonly interaction: InvocationMetadata;
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

export interface AgentDefinition<
	TPluginId extends string = string,
	TClients = Record<string, unknown>,
> {
	/** Stable: the continuation key; labels traces and progress. */
	name: string;
	/** `"provider/model"`, e.g. `"anthropic/claude-sonnet-5"`. */
	model?: string;
	thinking?: ThinkingLevel;
	instructions?: Instructions<TClients> | readonly Instructions<TClients>[];
	tools?: readonly AgentTool[];
	/** Plugin ids and app toolkits. */
	toolkits?: readonly (TPluginId | Toolkit)[];
	/** States rendered into the prompt via their `render`. */
	state?: readonly StateDefinition<any>[];
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
	if (definition.model !== undefined) parseModel(definition.model);
	const agent: Agent = { kind: "fabrial.agent", name: definition.name, definition };
	let agents = definedAgents.get(scope);
	if (!agents) definedAgents.set(scope, (agents = new Set()));
	agents.add(agent);
	return agent;
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
