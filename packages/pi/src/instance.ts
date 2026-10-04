import type { Context, JsonValue } from "@earendil-works/chord";
import type { TSchema } from "@earendil-works/pi-ai";
import type { PromptInput, PromptSection, ToolRegistration } from "@earendil-works/pi-durable";
import type { AnyPlugin, ClientsOf, Fabrial } from "fabrial";
import { defineTool, section, type ToolDefinition } from "./adapters.ts";
import { defineAgent, type AgentDefinition, type DefinedAgent } from "./agent.ts";
import type { FabrialFields } from "./context.ts";

/** Bind Pi definition helpers to an explicit Fabrial catalog, never to global declarations. */
export function withPi<const P extends readonly AnyPlugin[]>(
	fabrial: Fabrial<P>,
): {
	defineTool<TParameters extends TSchema, D extends JsonValue = JsonValue>(
		this: void,
		definition: ToolDefinition<TParameters, D, ClientsOf<P>>,
	): ToolRegistration<TParameters, D>;
	defineAgent(this: void, definition: AgentDefinition<P[number]["id"]>): DefinedAgent;
	section(
		this: void,
		key: string,
		render: (
			input: PromptInput,
			ctx: PromptInput & FabrialFields<ClientsOf<P>>,
			context: Context,
		) => string | undefined | Promise<string | undefined>,
		options?: { readonly tag?: boolean },
	): PromptSection;
} {
	return {
		defineTool,
		defineAgent: (definition: AgentDefinition<P[number]["id"]>) => defineAgent(definition, fabrial),
		section,
	} as ReturnType<typeof withPi<P>>;
}
