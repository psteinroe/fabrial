import { createHmac } from "node:crypto";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockLogger } from "@chat-adapter/tests";
import { chat } from "@fabrial/chat";
import type { AnyPlugin, FabrialHost } from "fabrial";
import { vi } from "vitest";

export function connect(plugin: AnyPlugin) {
	const state = createMemoryState();
	const host: FabrialHost = {
		plugins: [plugin],
		workflows: [],
		runtime: {
			register: vi.fn(),
			emit: vi.fn(),
			invoke: vi.fn(),
			cancel: vi.fn(),
			start: vi.fn(),
			stop: vi.fn(),
		},
		directory: {
			resolveIdentity: vi.fn(),
			identitiesFor: vi.fn(),
			members: vi.fn(),
			isMember: vi.fn(),
		},
		logger: createMockLogger(),
		clients: () => ({}),
		chat: () => connection.port,
		ingest: vi.fn(),
		receiveMessage: vi.fn(async () => "new" as const),
		receiveAction: vi.fn(),
		receiveCancellation: vi.fn(),
		resolvePrincipal: vi.fn(),
	};
	const connection = chat({ state }).connect(host);
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
