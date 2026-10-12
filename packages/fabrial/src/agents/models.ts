import type { CredentialStore, MutableModels } from "@earendil-works/pi-ai";
import { builtinModels, builtinProviders } from "@earendil-works/pi-ai/providers/all";

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
