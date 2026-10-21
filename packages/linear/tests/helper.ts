import { createHmac } from "node:crypto";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockLogger } from "@chat-adapter/tests";
import { connectChat, type ChatConnection } from "../../fabrial/src/chat/index.ts";
import type { FabrialHost } from "../../fabrial/src/app.ts";
import type { AnyPlugin } from "../../fabrial/src/plugin.ts";
import { vi } from "vitest";

export function connect(plugin: AnyPlugin): {
	host: FabrialHost;
	connection: ChatConnection;
	state: ReturnType<typeof createMemoryState>;
} {
	const state = createMemoryState();
	const host: FabrialHost = {
		plugins: [plugin],
		workflows: [],
		conductor: {
			emit: vi.fn(),
			invoke: vi.fn(),
			cancel: vi.fn(),
			signalWithStart: vi.fn(),
			signal: vi.fn(),
			getExecution: vi.fn(async () => null),
		} as never,
		orchestrator: {} as never,
		queue: "default",
		now: () => Date.now(),
		directory: {
			resolveIdentity: vi.fn(),
			identitiesFor: vi.fn(),
			members: vi.fn(),
			isMember: vi.fn(),
		},
		logger: createMockLogger(),
		clients: () => ({}),
		chat: () => connection,
		ingest: vi.fn(),
		receiveMessage: vi.fn(async () => "accepted" as const),
		receiveAction: vi.fn(),
		receiveCancellation: vi.fn(),
		resolvePrincipal: vi.fn(),
	};
	const connection = connectChat({ state }, host);
	return { host, connection, state };
}

export const options = {
	apiKey: "key",
	webhookSecret: "secret",
	userName: "bot",
	organizationId: "org",
	logger: createMockLogger(),
};
export function request(payload: unknown, signature?: string): Request {
	const body = JSON.stringify(payload);
	return new Request("https://example.com/linear/events", {
		method: "POST",
		body,
		headers: {
			"linear-delivery": "delivery-1",
			"linear-signature": signature ?? createHmac("sha256", "secret").update(body).digest("hex"),
		},
	});
}
