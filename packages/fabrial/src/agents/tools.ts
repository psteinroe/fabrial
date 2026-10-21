import type { Context } from "@earendil-works/chord";
import {
	defineTool as piTool,
	section as piSection,
	type PromptSection,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { TSchema } from "@earendil-works/pi-ai";
import type { Instructions, ToolDefinition, ToolResult } from "../agent.ts";
import type { JsonObject } from "../json.ts";
import type { Schema } from "../schema.ts";
import { ToolReceipt } from "./documents.ts";
import { bindMetadata, metadataOf } from "../context.ts";
import { cloneJson, commitStaged, fields, type StagedState } from "./context.ts";

/**
 * JSON Schema for the model, from the Standard JSON Schema interface (`~standard.jsonSchema`).
 * Schema libraries without it (or schemas it cannot express) are rejected when the app starts.
 */
export function toJsonSchema(schema: Schema, label: string): Record<string, unknown> {
	const converter = (
		schema["~standard"] as {
			jsonSchema?: { input(options: { target: string }): Record<string, unknown> };
		}
	).jsonSchema;
	if (!converter)
		throw new Error(
			`${label}: the input schema must implement Standard JSON Schema (~standard.jsonSchema), e.g. zod >= 4.2`,
		);
	const { $schema: _dialect, ...json } = converter.input({ target: "draft-07" });
	if (json.type !== "object") throw new Error(`${label}: the input schema must describe an object`);
	return json;
}

/** Strings go to the model as is; other JSON values are serialized. */
export function toolContent(result: ToolResult): string {
	return typeof result === "string" ? result : JSON.stringify(result ?? null);
}

/** A Fabrial tool as a Pi tool: validated input, Fabrial context, and a receipt committed with staged state. */
export function piToolOf(tool: ToolDefinition): ToolRegistration {
	const parameters = toJsonSchema(tool.input, `Tool ${tool.name}`) as unknown as TSchema;
	return piTool({
		name: tool.name,
		description: tool.description,
		parameters,
		replay: tool.idempotent ? "safe" : "unsafe",
		execute: async (args, api, context) => {
			const saved = await api.snapshot(ToolReceipt, api.taskId, context);
			if (saved?.result) return saved.result as unknown as ToolExecutionResult;
			const validated = await tool.input["~standard"].validate(args);
			if (validated.issues)
				return {
					content: [
						{
							type: "text",
							text: `Invalid input for ${tool.name}: ${validated.issues.map((issue) => issue.message).join("; ")}`,
						},
					],
					isError: true,
				};
			const staged = new Map<string, StagedState>();
			const ctx = await fields(context, api.conversationId, api, staged);
			// Spreading drops the context's metadata binding, so bind the tool context again.
			const returned = await tool.run(
				validated.value,
				bindMetadata(
					{ ...ctx, signal: context.abortSignal ?? new AbortController().signal },
					metadataOf(ctx),
				),
			);
			const result: ToolExecutionResult = {
				content: [{ type: "text", text: toolContent(returned) }],
			};
			// The receipt and all state changes commit atomically. Pi appends its transcript result next.
			await commitStaged(api, context, staged, async (tx) => {
				const receipt = await tx.doc(ToolReceipt, api.taskId);
				receipt.result = cloneJson(result) as JsonObject;
			});
			return result;
		},
	});
}

/** Instructions as one Pi section, rendered per request with a read-only Fabrial context. */
export function instructionsSection(
	key: string,
	instructions: Instructions | readonly Instructions[] | undefined,
): PromptSection | undefined {
	const list =
		instructions === undefined ? [] : Array.isArray(instructions) ? instructions : [instructions];
	if (!list.length) return undefined;
	return piSection(
		key,
		async (input, context: Context) => {
			const dynamic = list.some((entry) => typeof entry === "function");
			const ctx = dynamic ? await fields(context, input.conversationId) : undefined;
			const parts = await Promise.all(
				list.map((entry) =>
					typeof entry === "string"
						? entry
						: entry({
								actor: ctx!.actor,
								clients: ctx!.clients,
								state: (definition: Parameters<NonNullable<typeof ctx>["state"]>[0]) => ({
									get: () => ctx!.state(definition).get(),
								}),
							}),
				),
			);
			const text = parts.filter((part): part is string => !!part).join("\n\n");
			return text || undefined;
		},
		{ tag: false },
	);
}
