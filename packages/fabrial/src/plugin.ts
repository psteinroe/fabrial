// `fabrial/plugin`: what plugin authors add to the root API (events, routes, chat adapters, hooks).
import type { Adapter, Message, Thread } from "chat";
import type { EmitOptions, EventDefinition, InferEventPayload, Origin } from "./routing/events.ts";
import type { ExternalIdentity } from "./identity.ts";
import type { WorkflowMiddleware } from "./workflows/definition.ts";
import type { AgentObservers, Instructions, ToolDefinition } from "./agents/definition.ts";
import type { Json, JsonObject } from "./schema.ts";
import type { InputPriority } from "./sessions/delivery.ts";
import type { ThreadRef } from "./chat/messages.ts";

/** Context passed to a plugin's HTTP route handlers. */
export interface RouteContext<TEvents extends Record<string, EventDefinition<any>>, TClients> {
	/** Emit one of this plugin's events. Trigger matching, ownership, and dedup happen in Fabrial. */
	emit<K extends keyof TEvents & string>(
		event: K,
		payload: InferEventPayload<TEvents[K]>,
		options?: EmitOptions,
	): Promise<void>;
	readonly clients: TClients;
}

export type RouteHandler<TEvents extends Record<string, EventDefinition<any>>, TClients> = (
	request: Request,
	ctx: RouteContext<TEvents, TClients>,
) => Promise<Response> | Response;

/** Chat SDK capability: the one runtime type plugin authors see. */
export interface ChatCapability {
	/** Trusted provider installation namespace, never inferred from message display data. */
	installationId: string;
	/** Creates the Chat SDK platform adapter (e.g. `createSlackAdapter(...)`). */
	adapter: () => Adapter;
	/** Default model-context history loading for threads of this provider. */
	history?: { mode: "thread" | "since-last-reply"; limit?: number };
}

export interface IdentityCapability<TClients> {
	/** Optional profile lookup used for display names in cards and prompts. */
	lookup?: (
		identity: ExternalIdentity,
		clients: TClients,
	) => Promise<{ name?: string } | undefined>;
}

export interface PluginDefinition<
	TId extends string = string,
	TEvents extends Record<string, EventDefinition<any>> = Record<string, EventDefinition<any>>,
	TClients extends Record<string, unknown> = Record<string, unknown>,
> {
	/**
	 * Unique name; prefixes events (`sentry.issueCreated`). One installation per provider in this preview: a chat
	 * plugin's id is its adapter's name, so a second instance of the same provider is not supported.
	 */
	id: TId;
	/** Typed events, registered with the workflow runtime as `${id}.${key}`. */
	events?: TEvents;
	/** Webhook ingress, keyed `"METHOD /path"`, served by `app.fetch`. */
	routes?: Record<string, RouteHandler<TEvents, TClients>>;
	/** Chat SDK adapter: routes, typed message events, `ctx.thread`, cards, status rendering, history. */
	chat?: ChatCapability;
	/** Canonical live clients, exposed as `ctx.clients.*`; built once per app, so they must not capture a request. */
	clients?: () => TClients;
	/** Tools and instructions for agents that list this plugin's id in `toolkits`. */
	toolkit?: PluginToolkitFactory<TClients>;
	/** Model context for a trigger's origin, loaded durably when an agent starts from it. */
	context?: (origin: Origin, clients: TClients) => Promise<string | undefined>;
	hooks?: {
		/** Middleware around every workflow run/resume. */
		workflow?: WorkflowMiddleware[];
		/** Observers on every agent. */
		agent?: AgentObservers;
	};
	identity?: IdentityCapability<TClients>;
	/** Resource kinds conversations can follow (`ctx.follow`), keyed by kind name. */
	resources?: Record<string, ResourceKind<any>>;
	/** Validate definition-time options and acquire owned resources at app.start(), never in the factory. */
	init?: () => Promise<void> | void;
	/** Release owned resources at app.stop(). */
	shutdown?: () => Promise<void> | void;
}

export interface PluginToolkit<TClients = Record<string, unknown>> {
	tools: readonly ToolDefinition<any, TClients>[];
	instructions?: Instructions<TClients> | readonly Instructions<TClients>[];
}

export type PluginToolkitFactory<TClients = Record<string, unknown>> = (
	clients: TClients,
) => PluginToolkit<TClients>;

export type AnyPlugin = PluginDefinition<string, any, any>;

/** A plugin known to provide a toolkit; only these ids are accepted in an agent's `toolkits`. */
export type ToolkitPlugin<
	TId extends string = string,
	TEvents extends Record<string, EventDefinition<any>> = Record<string, EventDefinition<any>>,
	TClients extends Record<string, unknown> = Record<string, unknown>,
> = PluginDefinition<TId, TEvents, TClients> & { toolkit: PluginToolkitFactory<TClients> };

export type PluginClients<P> = P extends { clients?: () => infer C }
	? C extends object
		? C
		: never
	: never;

/**
 * Define a plugin. Pass a side-effect-free factory with definition-time options, or a plain object.
 * Defer credential validation and connections to init/adapter startup; unset env at import is safe.
 *
 * ```ts
 * export const sentry = definePlugin((options: SentryOptions) => ({ id: "sentry", events: {…}, routes: {…} }));
 * const billing = definePlugin({ id: "billing", events: {…} });
 * ```
 */
export function definePlugin<
	TArgs extends unknown[],
	const TId extends string,
	TEvents extends Record<string, EventDefinition<any>> = {},
	TClients extends Record<string, unknown> = {},
>(
	factory: (...args: TArgs) => ToolkitPlugin<TId, TEvents, TClients>,
): (...args: TArgs) => ToolkitPlugin<TId, TEvents, TClients>;
export function definePlugin<
	TArgs extends unknown[],
	const TId extends string,
	TEvents extends Record<string, EventDefinition<any>> = {},
	TClients extends Record<string, unknown> = {},
>(
	factory: (...args: TArgs) => PluginDefinition<TId, TEvents, TClients>,
): (...args: TArgs) => PluginDefinition<TId, TEvents, TClients>;
export function definePlugin<
	const TId extends string,
	TEvents extends Record<string, EventDefinition<any>> = {},
	TClients extends Record<string, unknown> = {},
>(plugin: ToolkitPlugin<TId, TEvents, TClients>): ToolkitPlugin<TId, TEvents, TClients>;
export function definePlugin<
	const TId extends string,
	TEvents extends Record<string, EventDefinition<any>> = {},
	TClients extends Record<string, unknown> = {},
>(plugin: PluginDefinition<TId, TEvents, TClients>): PluginDefinition<TId, TEvents, TClients>;
export function definePlugin(pluginOrFactory: unknown): unknown {
	return pluginOrFactory;
}

/** An event of a plugin, as resource kinds see it. `name` is the plugin-local key (`pullRequestReviewed`). */
export interface PluginEvent {
	name: string;
	payload: JsonObject;
}

/** Scalar details of a notification, rendered as attributes: the ids the plugin's tools need. */
export type NotificationAttributes = Record<string, string | number | boolean | null>;

/** How a resource event reads to an agent. */
export interface ResourceNotification {
	/** What happened, in the plugin's vocabulary (`review`, `comment`, `merged`); follows select kinds. */
	kind: string;
	/** Text the agent sees. */
	summary: string;
	/** Default priority (`normal` when unset); a follow's `priority` overrides it per kind. */
	priority?: InputPriority;
	attributes?: NotificationAttributes;
}

/**
 * A kind of resource a conversation can follow (a GitHub pull request). Follow records live in the
 * resource's thread state, so the kind maps references and events to threads.
 */
export interface ResourceKind<TRef extends JsonObject = JsonObject> {
	/** Key of a resource, unique within the plugin; Fabrial prefixes the plugin id (`github:acme/app#42`). */
	key(ref: TRef): string;
	/** The thread whose Chat SDK state holds the resource's follow records. */
	thread(ref: TRef): ThreadRef;
	/** Event keys of this plugin that can belong to resources of this kind. */
	events: readonly string[];
	/** Threads of the resources an event belongs to (a review belongs to its pull request's thread). */
	threads(event: PluginEvent): readonly ThreadRef[];
	/**
	 * Render an event as a notification; `undefined` delivers nothing. Drop untrusted authors here: not a
	 * permission, it keeps outside input from steering the agent.
	 */
	notify(event: PluginEvent): ResourceNotification | undefined;
	/** Notification kinds a follow receives by default; a follow with `events: "all"` receives every kind. */
	defaults: readonly string[];
}

/** A serializable reference to one resource, built by a plugin helper such as `github.pullRequest()`. */
export interface ResourceRef<TRef extends JsonObject = JsonObject> {
	readonly kind: "fabrial.resource";
	/** Plugin id. */
	readonly provider: string;
	/** Resource kind name in the plugin's `resources`. */
	readonly type: string;
	readonly ref: TRef;
}

/** Build a resource reference. Plugins wrap this in typed helpers, e.g. `github.pullRequest("acme/app#42")`. */
export function resource<TRef extends JsonObject>(
	provider: string,
	type: string,
	ref: TRef,
): ResourceRef<TRef> {
	return { kind: "fabrial.resource", provider, type, ref };
}

/** @internal JSON form of a resource reference, for durable operations. */
export type ResourceRefJson = {
	kind: "fabrial.resource";
	provider: string;
	type: string;
	ref: Json;
};
export { defineEvent, type EventDefinition, type InferEventPayload } from "./routing/events.ts";
export { trigger } from "./routing/events.ts";
export { chatEvents, chatTrigger } from "./routing/events.ts";
export { chatIdentity } from "./identity.ts";
export { literalCard } from "./chat/messages.ts";
export type { AgentObservers, ModelResponse, ToolCall } from "./agents/definition.ts";
export type { ExecutionInfo, WorkflowMiddleware } from "./workflows/definition.ts";

export interface ChatPluginCapability extends ChatCapability {
	/** Override root detection for adapters whose thread ids do not end in the root message id. */
	isNewThread?: (thread: Thread, message: Message) => boolean;
	/** Use an edited status post when the platform has no meaningful typing/status API. */
	status?: "typing" | "message";
	/** A link to a thread, shown on approval cards as their source. */
	permalink?: (thread: ThreadRef) => Promise<string | null>;
}

export function chatCapability(options: ChatPluginCapability): ChatPluginCapability {
	return options;
}
