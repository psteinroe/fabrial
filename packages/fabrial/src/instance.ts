import type { App, CoreConfig, Logger } from "./app.ts";
import { buildApp } from "./core.ts";
import { defineGroup, type GroupDefinition, type UserDefinition } from "./identity.ts";
import type { Json } from "./json.ts";
import type { AnyPlugin, PluginClients } from "./plugin.ts";
import type { TriggerSpec } from "./trigger.ts";
import { defineWorkflow, type Workflow, type WorkflowDefinition } from "./workflow.ts";

type UnionToIntersection<U> = (U extends unknown ? (value: U) => void : never) extends (
	value: infer I,
) => void
	? I
	: never;

/** Live clients merged from this instance's plugins, without an open index signature. */
export type ClientsOf<P extends readonly AnyPlugin[]> = [PluginClients<P[number]>] extends [never]
	? {}
	: UnionToIntersection<PluginClients<P[number]>>;

export type FabrialConfig = Omit<
	CoreConfig,
	"plugins" | "identity" | "logger" | "definitionScope"
> & {
	logger?: Logger;
};

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
	defineGroup(
		this: void,
		definition: Parameters<typeof defineGroup<ClientsOf<P>>>[0],
	): GroupDefinition<ClientsOf<P>>;
	app(this: void, config: FabrialConfig): App<ClientsOf<P>>;
}

/** Define the typed catalog once; wire independent runtimes with f.app(). */
export function createFabrial<const P extends readonly AnyPlugin[]>(config: {
	plugins: P;
	identity?: readonly UserDefinition[];
	logger?: Logger;
}): Fabrial<P> {
	const plugins = Object.freeze([...config.plugins]) as unknown as P;
	const instance: Fabrial<P> = {
		plugins,
		// Definitions are plain data; the instance only supplies their contextual types.
		defineWorkflow: defineWorkflow as Fabrial<P>["defineWorkflow"],
		defineGroup,
		app(appConfig) {
			return buildApp({
				...config,
				...appConfig,
				definitionScope: instance,
				logger: appConfig.logger ?? config.logger,
				plugins,
			}) as App<ClientsOf<P>>;
		},
	};
	return instance;
}
