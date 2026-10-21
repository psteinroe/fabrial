import type { AnyPlugin, PluginClients, ToolkitPlugin } from "./plugin.ts";
import type { ModelCredentials } from "./models.ts";
import { defineGroup, type GroupDefinition, type UserDefinition } from "./identity.ts";
import {
	type AnyWorkflow,
	defineWorkflow,
	type Workflow,
	type WorkflowDefinition,
} from "./workflows/definition.ts";
import {
	type Agent,
	type AgentDefinition,
	defineAgent,
	defineTool,
	defineToolkit,
	type Tool,
	type ToolDefinition,
	type Toolkit,
	type ToolkitDefinition,
} from "./agents/definition.ts";
import {
	type App,
	buildApp,
	type CoreConfig,
	type HostedApp,
	type Logger,
	production,
	type ProductionOverrides,
} from "./app.ts";
import type { JsonObject, Schema } from "./schema.ts";
import type { TriggerSpec } from "./routing/events.ts";
import { defineState, registerState, type StateDefinition, type StateScope } from "./state.ts";

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
	/** Agents whose `triggers` receive events. Agents only reached by `ctx.send` or `ctx.agent` need no entry. */
	agents?: readonly Agent[];
	logger?: Logger;
}

export interface Fabrial<P extends readonly AnyPlugin[]> {
	readonly plugins: Readonly<P>;
	defineWorkflow<
		const TName extends string,
		TInput extends JsonObject = never,
		TOutput extends JsonObject | null | void = void,
		const TTriggers extends readonly TriggerSpec<any>[] = [],
	>(
		this: void,
		definition: WorkflowDefinition<TName, TInput, TOutput, TTriggers, ClientsOf<P>>,
	): Workflow<TName, TInput, TOutput>;
	defineAgent<const TTriggers extends readonly TriggerSpec<any>[] = []>(
		this: void,
		definition: AgentDefinition<ToolkitIdsOf<P>, ClientsOf<P>, TTriggers>,
	): Agent;
	defineTool<TInput>(this: void, definition: ToolDefinition<TInput, ClientsOf<P>>): Tool<TInput>;
	defineToolkit(this: void, definition: ToolkitDefinition<ClientsOf<P>>): Toolkit;
	defineState<T extends JsonObject, S extends StateScope>(
		this: void,
		definition: {
			name: string;
			scope: S;
			schema: Schema<T>;
			initial: () => T;
			render?: (state: T) => string | undefined;
			version?: number;
			migrate?: (old: unknown, fromVersion: number) => T;
		},
	): StateDefinition<T, S>;
	defineGroup(
		this: void,
		definition: Parameters<typeof defineGroup<ClientsOf<P>>>[0],
	): GroupDefinition<ClientsOf<P>>;
	app(this: void, config: AppConfig): App<ClientsOf<P>>;
}

/** Internal app construction for `fabrial/testing`. Not part of the public API. */
export const INTERNAL = Symbol.for("fabrial.internal");
export type InternalOverrides = Omit<ProductionOverrides, "conductor"> &
	Partial<Pick<CoreConfig, "chat" | "agents">> & {
		/** With `sql`, the app runs on that pool instead of the instance's database. */
		conductor?: Omit<CoreConfig["conductor"], "sql"> & { sql?: CoreConfig["conductor"]["sql"] };
	};
export interface InternalInstance {
	app(config: AppConfig, overrides: InternalOverrides): HostedApp;
}

/** Create the instance: configuration and typed definitions. Nothing connects until app.start(). */
export function createFabrial<const P extends readonly AnyPlugin[]>(
	config: FabrialConfig<P>,
): Fabrial<P> {
	const plugins = Object.freeze([...config.plugins]) as unknown as P;
	const build = (appConfig: AppConfig, overrides: InternalOverrides = {}): HostedApp => {
		const { conductor, chat, agents, ...settings } = overrides;
		const sql = conductor?.sql;
		if (sql && !agents)
			throw new Error("An app on an explicit Conductor pool needs agents (Pi options)");
		const integrations =
			sql && agents
				? { conductor: { ...conductor, sql }, chat, agents }
				: production(
						{ database: config.database, models: config.models ?? {} },
						{ ...settings, conductor },
					);
		return buildApp({
			...integrations,
			plugins,
			identity: config.identity,
			workflows: appConfig.workflows,
			routedAgents: appConfig.agents ?? [],
			logger: appConfig.logger,
			definitionScope: instance,
		});
	};
	const instance: Fabrial<P> & { [INTERNAL]: InternalInstance } = {
		plugins,
		// Definitions are plain data; the instance only supplies their contextual types.
		defineWorkflow: defineWorkflow as Fabrial<P>["defineWorkflow"],
		defineAgent: (definition) => defineAgent(definition as AgentDefinition, instance),
		defineTool: defineTool as Fabrial<P>["defineTool"],
		defineToolkit,
		defineState: (definition) => registerState(defineState(definition), instance),
		defineGroup,
		app: (appConfig) => build(appConfig) as App<ClientsOf<P>>,
		[INTERNAL]: { app: build },
	};
	return instance;
}
