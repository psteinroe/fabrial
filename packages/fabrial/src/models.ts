import type {
	ClassifierContext,
	ClassifierResult,
	CredentialStore,
	Models,
	MutableModels,
} from "@earendil-works/pi-ai";
import { builtinModels, builtinProviders } from "@earendil-works/pi-ai/providers/all";
import { parseModel } from "./agent.ts";

/** Credentials per model provider, e.g. `{ anthropic: { apiKey } }`. */
export type ModelCredentials = Record<string, { apiKey: string | undefined }>;

/** Builtin pi-ai providers authenticated from the configured credentials only. Nothing connects here. */
export function createModels(credentials: ModelCredentials): {
	models: MutableModels;
	validate(): void;
} {
	const read = async (providerId: string) => {
		const key = credentials[providerId]?.apiKey;
		return key ? { type: "api_key" as const, key } : undefined;
	};
	const store: CredentialStore = {
		read,
		list: async () => [],
		modify: async (providerId) => read(providerId),
		delete: async () => {},
	};
	return {
		models: builtinModels({ credentials: store }),
		validate() {
			const known = new Set(builtinProviders().map((provider) => provider.id));
			for (const [provider, value] of Object.entries(credentials)) {
				if (!known.has(provider)) throw new Error(`Unknown model provider "${provider}"`);
				if (!value.apiKey) throw new Error(`Model provider "${provider}" requires an apiKey`);
			}
		},
	};
}

/** `ctx.evaluate`'s request: Pi AI's classifier context and the classifier, `"provider/model"`. */
export interface EvaluateRequest extends ClassifierContext {
	model: string;
}

/**
 * Classifies with Pi AI's `models.classify()`. An unknown classifier is a result with `stopReason: "error"`, as are
 * provider failures, so callers fall back explicitly. Callers make it durable: a step in workflows, a memo in tools.
 */
export async function evaluate(
	models: Models,
	{ model: name, ...context }: EvaluateRequest,
	signal?: AbortSignal,
): Promise<ClassifierResult> {
	const ref = parseModel(name);
	const model = models.getModelOfType("classifier", ref.provider, ref.modelId);
	if (model) return models.classify(model, context, signal ? { signal } : {});
	return {
		api: "unknown",
		provider: ref.provider,
		model: ref.modelId,
		answers: {},
		stopReason: "error",
		errorMessage: `Unknown classifier ${name}`,
		timestamp: Date.now(),
	};
}
