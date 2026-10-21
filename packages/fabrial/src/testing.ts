// `fabrial/testing`: apps on real Conductor with fake chat or scripted models.
import type { AssistantMessage, MutableModels } from "@earendil-works/pi-ai";
import { createModels as createPiModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { parseModel } from "./agent.ts";
import type { AppConfig, ClientsOf, Fabrial } from "./instance.ts";
import type { JsonObject } from "./json.ts";
import type { AnyPlugin } from "./plugin.ts";
import {
	createTestApp as create,
	internal,
	testingApp,
	type TestApp,
	type TestAppConfig,
	type TestingApp,
} from "./testing-internal.ts";

export { FakeChat, type FakePost } from "./fake-chat.ts";
export type { TestApp, TestAppConfig, TestingApp, ThreadRouting } from "./testing-internal.ts";
/** Conductor's test clock, as `TestApp.clock`. */
export type { TestClock } from "pgconductor-js/testing";

/**
 * Build an app on real Conductor from the same typed catalog as production, with fake chat and agents
 * and fast polling. Start it with `app.start()`; drive it through `chat`, `app.fetch` or
 * `runtime.invoke`, and wait for outcomes with `settled`.
 */
export function createTestApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: TestAppConfig,
): TestApp<ClientsOf<P>> {
	return create(instance, config);
}

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

const scripted = new WeakMap<ScriptedModels, MutableModels>();

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
	scripted.set(result, models);
	return result;
}

const defined = <T extends object>(value: T): Partial<T> =>
	Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as Partial<T>;

/**
 * Durable retries of transient model errors: failed attempt n retries while n <= `maxRetries`, after
 * min(`baseDelayMs` * 2^(n-1), `maxDelayMs`). Unset fields keep production's policy.
 */
export interface ModelRetryOptions {
	maxRetries?: number;
	baseDelayMs?: number;
	maxDelayMs?: number;
}

/**
 * Build the real app (Postgres, workflows, agents, chat state on the instance's database) with scripted
 * models instead of credentials, model retries off, and fast polling.
 */
export function createIntegrationApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: AppConfig & {
		models: ScriptedModels;
		/** Durable model retries: off by default; `true` for production's policy, or overrides of it (e.g. tiny delays). */
		modelRetries?: boolean | ModelRetryOptions;
		/** Worker and wait polling interval. Default 20ms. */
		pollIntervalMs?: number;
		/** Concurrent workflow executions per worker. Default 8. */
		concurrency?: number;
	},
): TestingApp<ClientsOf<P>> {
	const { workflows, agents: routed, logger, models, modelRetries = false } = config;
	const poll = config.pollIntervalMs ?? 20;
	const app = internal(instance).app(
		{ workflows, agents: routed, logger },
		{
			models: scripted.get(models),
			agentSettings: {
				retry:
					typeof modelRetries === "boolean"
						? { enabled: modelRetries }
						: {
								enabled: true,
								...defined({
									maxRetries: modelRetries.maxRetries,
									baseDelayMs: modelRetries.baseDelayMs,
									maxAgentDelayMs: modelRetries.maxDelayMs,
								}),
							},
			},
			conductor: {
				worker: {
					concurrency: config.concurrency ?? 8,
					pollIntervalMs: poll,
					flushIntervalMs: poll,
					fetchBatchSize: 10,
				},
			},
		},
	);
	return testingApp(app) as never;
}
