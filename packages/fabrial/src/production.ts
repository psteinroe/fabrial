import { createPostgresState } from "@chat-adapter/state-pg";
import type { MutableModels } from "@earendil-works/pi-ai";
import type { ConversationRetryPolicy } from "@earendil-works/pi-durable";
import postgres, { type Sql } from "postgres";
import { pi, type PiOptions } from "./agents/pi.ts";
import { createModels, type ModelCredentials } from "./agents/models.ts";
import type { ChatIntegration, CoreConfig } from "./app.ts";
import { chat } from "./chat/index.ts";
import { conductor, type ConductorConfig } from "./runtime/conductor/index.ts";

/** Settings only `fabrial/testing` may change. */
export interface ProductionOverrides {
	models?: MutableModels;
	agentSettings?: PiOptions["settings"];
	conductor?: ConductorConfig;
}

/**
 * Durable retries of transient model errors (overload, rate limits, 5xx, network). Pi retries failed attempt
 * n while n <= maxRetries, after min(baseDelayMs * 2^(n-1), maxAgentDelayMs): 2+4+8+16+32+60+60+60+60 s, so
 * about 5 minutes of backoff before a sent request gets the "couldn't reach the model provider" reply.
 */
export const MODEL_RETRY: ConversationRetryPolicy = {
	enabled: true,
	maxRetries: 9,
	baseDelayMs: 2_000,
	maxAgentDelayMs: 60_000,
};

/** One lazily opened pool. Its identity is stable, so runtimes can hold it before app.start(). */
function lazyPool(url: string | undefined) {
	let pool: Sql | undefined;
	const current = (): Sql => {
		if (!pool) throw new Error("The Fabrial database is not open; call app.start() first");
		return pool;
	};
	const sql = new Proxy(function () {} as unknown as Sql, {
		apply: (_target, _this, args: unknown[]) => Reflect.apply(current(), undefined, args),
		get: (_target, key) => Reflect.get(current(), key),
	});
	return {
		sql,
		url,
		open() {
			if (!url) throw new Error("createFabrial requires a database connection string");
			pool ??= postgres(url, { max: 20, onnotice: () => {} });
		},
		async close() {
			const open = pool;
			pool = undefined;
			await open?.end();
		},
	};
}

/** Conductor, Pi, and Chat SDK on the instance's database. Side-effect free until app.start(). */
export function production(
	config: { database: string | undefined; models: ModelCredentials },
	overrides: ProductionOverrides = {},
): Pick<CoreConfig, "runtime" | "chat" | "agents" | "resources"> {
	const pool = lazyPool(config.database);
	const credentials = createModels(config.models);
	let state: ReturnType<typeof createPostgresState> | undefined;
	const chatIntegration: ChatIntegration = {
		kind: "fabrial.chat",
		connect(host) {
			state = createPostgresState({ url: pool.url! });
			return chat({ state }).connect(host);
		},
	};
	return {
		runtime: conductor({ sql: pool.sql }, overrides.conductor),
		chat: chatIntegration,
		agents: pi({
			models: overrides.models ?? credentials.models,
			sql: pool.sql,
			settings: {
				...overrides.agentSettings,
				retry: { ...MODEL_RETRY, ...overrides.agentSettings?.retry },
			},
		}),
		resources: {
			open() {
				if (!overrides.models) credentials.validate();
				pool.open();
			},
			async close() {
				const open = state;
				state = undefined;
				try {
					await open?.disconnect();
				} finally {
					await pool.close();
				}
			},
		},
	};
}
