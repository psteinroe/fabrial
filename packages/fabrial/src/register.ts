import type { AnyPlugin, PluginClients } from "./plugin.ts";

/**
 * Register the app's plugins once so that `ctx.clients`, trigger helpers, and plugin extension ids
 * are typed everywhere:
 *
 * ```ts
 * declare module "fabrial" {
 *   interface Register {
 *     plugins: typeof plugins; // the array passed to fabrial({ plugins })
 *   }
 * }
 * ```
 */
// oxlint-disable-next-line typescript/no-empty-object-type
export interface Register {}

export type RegisteredPlugins = Register extends { plugins: infer P extends readonly AnyPlugin[] }
	? P
	: readonly AnyPlugin[];

type UnionToIntersection<U> = (U extends unknown ? (x: U) => void : never) extends (
	x: infer I,
) => void
	? I
	: never;

/** Live clients of all registered plugins, merged. */
export type Clients = Register extends { plugins: infer P extends readonly AnyPlugin[] }
	? UnionToIntersection<PluginClients<P[number]>> & Record<string, unknown>
	: Record<string, unknown>;

/** Ids of registered plugins that provide a Pi extension. */
export type PluginExtensionId = Register extends { plugins: infer P extends readonly AnyPlugin[] }
	? Extract<P[number], { extension: unknown }>["id"]
	: string;
