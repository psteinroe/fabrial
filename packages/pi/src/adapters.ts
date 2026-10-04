import type { Context, JsonValue } from "@earendil-works/chord";
import {
	defineTool as piTool,
	section as piSection,
	type PromptInput,
	type PromptSection,
	type ToolExecutionApi,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { Static, TSchema } from "@earendil-works/pi-ai";
import { bindApi, type BoundToolApi } from "./bind.ts";
import { ToolReceipt } from "./documents.ts";
import {
	cloneJson,
	commitStaged,
	fields,
	type FabrialFields,
	type StagedState,
} from "./context.ts";

export type ToolDefinition<P extends TSchema, D extends JsonValue, TClients> = Omit<
	ToolRegistration<P, D>,
	"execute"
> & {
	execute(
		args: Static<P>,
		ctx: BoundToolApi<D> & FabrialFields<TClients>,
		context: Context,
	): Promise<ToolExecutionResult<D>>;
};

/** Untyped runtime adapter. Use withPi(f) in application code. */
export function defineTool<P extends TSchema, D extends JsonValue = JsonValue>(
	definition: ToolDefinition<P, D, Record<string, unknown>>,
): ToolRegistration<P, D> {
	return piTool({
		...definition,
		replay: definition.replay ?? "safe",
		execute: async (args, api, context) => {
			const saved = await api.snapshot(ToolReceipt, api.taskId, context);
			if (saved?.result) return saved.result as unknown as ToolExecutionResult<D>;
			const staged = new Map<string, StagedState>();
			const additions = await fields(context, api.conversationId, api, staged);
			let output = "";
			let details: D | undefined;
			const apiWithCapture: ToolExecutionApi<D> = {
				...api,
				output: (chunk) => {
					output += typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
					api.output(chunk);
				},
				details: async (value, context) => {
					details = value;
					await api.details(value, context);
				},
			};
			const returned = await definition.execute(
				args,
				Object.assign({}, bindApi(apiWithCapture, context), additions),
				context,
			);
			const result: ToolExecutionResult<D> = {
				...returned,
				...(returned.content === undefined && output
					? { content: [{ type: "text", text: output }] }
					: {}),
				...(returned.details === undefined && details !== undefined ? { details } : {}),
			};
			// This native receipt and all state changes commit atomically. Pi appends its transcript result next.
			await commitStaged(api, context, staged, async (tx) => {
				const receipt = await tx.doc(ToolReceipt, api.taskId);
				receipt.result = cloneJson(result) as import("fabrial").JsonObject;
			});
			return result;
		},
	});
}

/** Untyped runtime adapter. Use withPi(f) in application code. */
export function section(
	key: string,
	render: (
		input: PromptInput,
		ctx: PromptInput & FabrialFields,
		context: Context,
	) => string | undefined | Promise<string | undefined>,
	options?: { readonly tag?: boolean },
): PromptSection {
	return piSection(
		key,
		async (input, context) =>
			render(input, Object.assign({}, input, await fields(context, input.conversationId)), context),
		options,
	);
}

/** A new Pi release adding one of these names must fail typecheck, rather than silently shadow Pi. */
type AssertNever<T extends never> = T;
export type ReservedToolNames = AssertNever<Extract<keyof ToolExecutionApi, keyof FabrialFields>>;
export type ReservedSectionNames = AssertNever<Extract<keyof PromptInput, keyof FabrialFields>>;
