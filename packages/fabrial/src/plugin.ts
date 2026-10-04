import type { EmitOptions, EventDefinition, InferEventPayload, Origin } from "./events.ts";
import type { ExternalIdentity } from "./identity.ts";
import type { WorkflowMiddleware } from "./workflow.ts";

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

/** Chat SDK capability, interpreted by `@fabrial/chat`. Kept opaque here so core has no Chat SDK dependency. */
export interface ChatCapability {
	/** Trusted provider installation namespace, never inferred from message display data. */
	installationId: string;
	/** Creates the Chat SDK platform adapter (e.g. `createSlackAdapter(...)`). */
	adapter: () => unknown;
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
	/** Unique name; prefixes events (`sentry.issueCreated`). A second installation is a second instance with its own id. */
	id: TId;
	/** Typed events, registered with the workflow runtime as `${id}.${key}`. */
	events?: TEvents;
	/** Webhook ingress, keyed `"METHOD /path"`, served by `app.fetch`. */
	routes?: Record<string, RouteHandler<TEvents, TClients>>;
	/** Chat SDK adapter: routes, typed message events, `ctx.thread`, cards, status rendering, history. */
	chat?: ChatCapability;
	/** Canonical live clients, exposed as `ctx.clients.*` and rebuilt on every run/resume. */
	clients?: (deps: { chat?: unknown }) => TClients;
	/** Pi extension for agents that select this plugin by id (built by `@fabrial/pi`). */
	extension?: (clients: TClients) => unknown;
	/** Model context for a trigger's origin, loaded durably when an agent starts from it. */
	context?: (origin: Origin, clients: TClients) => Promise<string | undefined>;
	hooks?: {
		/** Middleware around every workflow run/resume. */
		workflow?: WorkflowMiddleware[];
		/** Native Pi hooks installed on every harness (interpreted by `@fabrial/pi`). */
		agent?: unknown[];
	};
	identity?: IdentityCapability<TClients>;
	/** Validate definition-time options and acquire owned resources at app.start(), never in the factory. */
	init?: () => Promise<void> | void;
	/** Release owned resources at app.stop(). */
	shutdown?: () => Promise<void> | void;
}

export type AnyPlugin = PluginDefinition<string, any, any>;

export type PluginClients<P> = P extends { clients?: (deps: never) => infer C }
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
	factory: (...args: TArgs) => PluginDefinition<TId, TEvents, TClients>,
): (...args: TArgs) => PluginDefinition<TId, TEvents, TClients>;
export function definePlugin<
	const TId extends string,
	TEvents extends Record<string, EventDefinition<any>> = {},
	TClients extends Record<string, unknown> = {},
>(plugin: PluginDefinition<TId, TEvents, TClients>): PluginDefinition<TId, TEvents, TClients>;
export function definePlugin(pluginOrFactory: unknown): unknown {
	return pluginOrFactory;
}
