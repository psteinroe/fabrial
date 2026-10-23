// Fabrial tools, workflow tools and instructions as Pi tools and prompt sections.
import {
	section as piSection,
	defineTool as piTool,
	type PromptSection,
	type ToolExecutionResult,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { TSchema } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import { type Instructions, type ToolDefinition, toolName, type ToolResult } from "./definition.ts";
import { cloneJson, type JsonObject, toJsonSchema } from "../schema.ts";
import { commitStaged, type StagedState, ToolReceipt } from "../state.ts";
import { createHash } from "node:crypto";
import { fields, frame } from "./tool-context.ts";
import { bindMetadata, metadataOf } from "../workflows/metadata.ts";
import type { AnyWorkflow } from "../workflows/definition.ts";

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
			const bridge = frame(context);
			// The call's identity in its Session: Pi keeps the task id across a replay.
			const idempotencyKey = createHash("sha256")
				.update(JSON.stringify([bridge.session, api.conversationId, api.taskId]))
				.digest("hex");
			if (!tool.idempotent) bridge.unsafe.add(api.taskId);
			let returned: ToolResult;
			try {
				// Spreading drops the context's metadata binding, so bind the tool context again.
				returned = await tool.run(
					validated.value,
					bindMetadata(
						{
							...ctx,
							idempotencyKey,
							signal: context.abortSignal ?? new AbortController().signal,
						},
						metadataOf(ctx),
					),
				);
			} finally {
				bridge.unsafe.delete(api.taskId);
			}
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

/**
 * The built-in `no_reply` tool of runs that answer input with a reply thread: ends the run without posting its
 * final answer. The reason stays in the transcript as the tool call's argument.
 */
export function noReplyTool(): ToolRegistration {
	return piTool({
		name: "no_reply",
		description:
			"End your turn without posting a reply, e.g. for chatter that is not for you or a notification that needs no answer. Call it alone, as the only tool call of your response.",
		parameters: {
			type: "object",
			properties: {
				reason: { type: "string", description: "Why no reply is needed; not shown to anyone" },
			},
			additionalProperties: false,
		} as unknown as TSchema,
		execute: async () => ({
			content: [{ type: "text" as const, text: "No reply posted." }],
			control: { terminate: true },
		}),
	});
}

export function workflowTool(workflow: AnyWorkflow, owner: string): ToolRegistration {
	const { input, description } = workflow.definition;
	const native = piTool({
		name: toolName(workflow.name),
		description: description ?? workflow.name,
		// Core validates the input again when the workflow starts.
		parameters: (input
			? toJsonSchema(input, `Workflow ${workflow.name} (a tool of ${owner})`)
			: { type: "object", properties: {}, additionalProperties: true }) as unknown as TSchema,
		replay: "safe",
		// Started behind a reporter like any waited child. A failed or cancelled workflow throws its outcome: Pi gives
		// the model the error.
		execute: async (input, api, context) => {
			const ctx = await fields(context, api.conversationId, api);
			const output = await ctx.invoke("workflow", workflow, input as never);
			return { content: [{ type: "text", text: JSON.stringify(output) }] };
		},
	});
	return native;
}
