// Scripted Pi AI models for tests, without credentials or network.
import type { AssistantMessage, MutableModels } from "@earendil-works/pi-ai";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { createModels as createPiModels } from "@earendil-works/pi-ai/models";
import type { JsonObject } from "../schema.ts";
import { parseModel } from "../models.ts";

/** A scripted model reply, built with `reply.text()` or `reply.toolCall()`. */
export interface ScriptedReply {
	readonly kind: "fabrial.scripted-reply";
	readonly message: AssistantMessage;
}
export const reply = {
	text: (text: string): ScriptedReply => ({
		kind: "fabrial.scripted-reply",
		message: fauxAssistantMessage(text),
	}),
	toolCall: (name: string, input: JsonObject): ScriptedReply => ({
		kind: "fabrial.scripted-reply",
		message: fauxAssistantMessage(fauxToolCall(name, input), { stopReason: "toolUse" }),
	}),
};
/** A transcript message as a scripted step sees it. */
export interface ScriptedMessage {
	readonly role: string;
	readonly toolName?: string;
	readonly content: unknown;
}
export type ScriptedStep =
	| ScriptedReply
	| ((context: { messages: readonly ScriptedMessage[] }) => ScriptedReply);
export interface ScriptedModel {
	/** Replace the remaining replies; each model request consumes the next step. */
	respond(steps: readonly ScriptedStep[]): void;
	/** Model requests so far. */
	readonly calls: number;
}
export type ScriptedAnswer =
	| { probability: number }
	| { choice: string; probabilities: Record<string, number> }
	| { score: number; confidence: number };
export interface ScriptedModels {
	/** A chat model, `"provider/model"`; one per provider. */
	model(name: string): ScriptedModel;
	/** A classifier for `ctx.evaluate`; answers keyed by question. */
	classifier(name: string, answers: (questions: string[]) => Record<string, ScriptedAnswer>): void;
}

/** Scripted models for `createIntegrationApp`, without credentials or network. */
export function scriptedModels(): ScriptedModels {
	const models = createPiModels();
	const handles = new Map<string, { modelId: string; model: ScriptedModel }>();
	const result: ScriptedModels = {
		model(name) {
			const { provider, modelId } = parseModel(name);
			const existing = handles.get(provider);
			if (existing) {
				if (existing.modelId !== modelId)
					throw new Error(`Scripted provider ${provider} already has model ${existing.modelId}`);
				return existing.model;
			}
			const faux = fauxProvider({ provider, models: [{ id: modelId }] });
			models.setProvider(faux.provider);
			const model: ScriptedModel = {
				respond: (steps) =>
					faux.setResponses(
						steps.map((step) =>
							typeof step === "function"
								? (context) => step({ messages: context.messages }).message
								: step.message,
						),
					),
				get calls() {
					return faux.state.callCount;
				},
			};
			handles.set(provider, { modelId, model });
			return model;
		},
		classifier(name, answers) {
			const { provider, modelId } = parseModel(name);
			const faux = fauxProvider({ provider });
			models.setProvider({
				...faux.provider,
				getAllModels: () => [{ ...faux.getModel(), id: modelId, type: "classifier" as const }],
				classify: async (_model, context) =>
					({
						api: faux.api,
						provider,
						model: modelId,
						answers: Object.fromEntries(
							Object.entries(answers(Object.keys(context.questions))).map(([key, answer]) => [
								key,
								"probability" in answer
									? { type: "bool" as const, probability: answer.probability }
									: "choice" in answer
										? { type: "choice" as const, ...answer }
										: { type: "score" as const, ...answer },
							]),
						),
						stopReason: "stop" as const,
						timestamp: Date.now(),
					}) as never,
			});
		},
	};
	scriptedPi.set(result, models);
	return result;
}

/** The Pi models behind each `scriptedModels()`, for tests building apps below the public API. */
export const scriptedPi = new WeakMap<object, MutableModels>();
