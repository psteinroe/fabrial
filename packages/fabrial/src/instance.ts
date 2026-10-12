import {
	defineAgent,
	defineTool,
	defineToolkit,
	type Agent,
	type AgentDefinition,
	type Tool,
	type ToolDefinition,
	type Toolkit,
	type ToolkitDefinition,
} from "./agent.ts";
import type { App, CoreConfig, Logger } from "./app.ts";
import type { ModelCredentials } from "./agents/models.ts";
import { buildApp } from "./core.ts";
import { defineGroup, type GroupDefinition, type UserDefinition } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";
import type { AnyPlugin, PluginClients, ToolkitPlugin } from "./plugin.ts";
import { production, type ProductionOverrides } from "./production.ts";
import { defineState, type StateDefinition, type StateScope } from "./state.ts";
import type { Schema } from "./schema.ts";
import type { TriggerSpec } from "./trigger.ts";
import { subscribe, type Subscription, type SubscriptionDefinition } from "./subscription.ts";
import {
	defineWorkflow,
	type AnyWorkflow,
	type Workflow,
	type WorkflowDefinition,
} from "./workflow.ts";

type UnionToIntersection<U> = (U extends unknown ? (value: U) => void : never) extends (
	value: infer I,
) => void
	? I
	: never;

/** Live clients merged from this instance's plugins, without an open index signature. */
export type ClientsOf<P extends readonly AnyPlugin[]> = [PluginClients<P[number]>] extends [never]
	? {}
	: UnionToIntersection<PluginClients<P[number]>>;

/** Ids of plugins that define a toolkit; agents list them in `toolkits`. app.start() checks again. */
export type ToolkitIdsOf<P extends readonly AnyPlugin[]> = P[number] extends infer Plugin
	? Plugin extends ToolkitPlugin<string, any, any>
		? Plugin["id"]
		: never
	: never;

export interface FabrialConfig<P extends readonly AnyPlugin[] = readonly AnyPlugin[]> {
	/** Postgres connection string. Workflows, agent sessions, and chat state share it. */
	database?: string;
	/** Credentials per model provider, e.g. `{ anthropic: { apiKey } }`. */
	models?: ModelCredentials;
	plugins: P;
	/** Known users; groups are referenced where they're used. */
	identity?: readonly UserDefinition[];
}

export interface AppConfig {
	workflows: readonly AnyWorkflow[];
	/** Static subscriptions from `f.subscribe`. */
	subscriptions?: readonly Subscription[];
	logger?: Logger;
}

export interface Fabrial<P extends readonly AnyPlugin[]> {
	readonly plugins: Readonly<P>;
	defineWorkflow<
		const TName extends string,
		TInput extends Json = never,
		TOutput extends Json | void = void,
		const TTriggers extends readonly TriggerSpec<any>[] = [],
	>(
		this: void,
		definition: WorkflowDefinition<TName, TInput, TOutput, TTriggers, ClientsOf<P>>,
	): Workflow<TName, TInput, TOutput>;
	defineAgent(this: void, definition: AgentDefinition<ToolkitIdsOf<P>, ClientsOf<P>>): Agent;
	defineTool<TInput>(this: void, definition: ToolDefinition<TInput, ClientsOf<P>>): Tool<TInput>;
	defineToolkit(this: void, definition: ToolkitDefinition<ClientsOf<P>>): Toolkit;
	defineState<T extends JsonObject>(
		this: void,
		definition: {
			name: string;
			scope: StateScope;
			schema: Schema<T>;
			initial: () => T;
			render?: (state: T) => string | undefined;
			version?: number;
			migrate?: (old: unknown, fromVersion: number) => T;
		},
	): StateDefinition<T>;
	/** Route a trigger's events into an agent conversation with `ctx.send`. */
	subscribe<const TTrigger extends TriggerSpec<any>>(
		this: void,
		definition: SubscriptionDefinition<TTrigger>,
	): Subscription;
	defineGroup(
		this: void,
		definition: Parameters<typeof defineGroup<ClientsOf<P>>>[0],
	): GroupDefinition<ClientsOf<P>>;
	app(this: void, config: AppConfig): App<ClientsOf<P>>;
}

/** Internal app construction for `fabrial/testing`. Not part of the public API. */
export const INTERNAL = Symbol.for("fabrial.internal");
export type InternalOverrides = ProductionOverrides &
	Partial<Pick<CoreConfig, "runtime" | "chat" | "agents" | "reservationTimeout">>;
export interface InternalInstance {
	app(config: AppConfig, overrides: InternalOverrides): App;
}

/** Create the instance: configuration and typed definitions. Nothing connects until app.start(). */
export function createFabrial<const P extends readonly AnyPlugin[]>(
	config: FabrialConfig<P>,
): Fabrial<P> {
	const plugins = Object.freeze([...config.plugins]) as unknown as P;
	const build = (appConfig: AppConfig, overrides: InternalOverrides = {}): App => {
		const { runtime, chat, agents, reservationTimeout, ...settings } = overrides;
		const integrations = runtime
			? { runtime, chat, agents }
			: production({ database: config.database, models: config.models ?? {} }, settings);
		return buildApp({
			...integrations,
			plugins,
			identity: config.identity,
			workflows: [
				...appConfig.workflows,
				...(appConfig.subscriptions ?? []).map((subscription) => subscription.workflow),
			],
			logger: appConfig.logger,
			reservationTimeout,
			definitionScope: instance,
		});
	};
	const instance: Fabrial<P> & { [INTERNAL]: InternalInstance } = {
		plugins,
		// Definitions are plain data; the instance only supplies their contextual types.
		defineWorkflow: defineWorkflow as Fabrial<P>["defineWorkflow"],
		defineAgent: (definition) => defineAgent(definition, instance),
		defineTool: defineTool as Fabrial<P>["defineTool"],
		defineToolkit,
		defineState,
		defineGroup,
		subscribe,
		app: (appConfig) => build(appConfig) as App<ClientsOf<P>>,
		[INTERNAL]: { app: build },
	};
	return instance;
}
