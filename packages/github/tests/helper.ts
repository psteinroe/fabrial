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
	token: "token",
	owner: "acme",
	webhookSecret: "secret",
	userName: "bot",
	botUserId: 99,
	logger: createMockLogger(),
};
export const alice = { id: 1, login: "alice", type: "User" };
export const repository = {
	id: 2,
	full_name: "acme/app",
	name: "app",
	owner: { ...alice, login: "acme" },
};
export function request(payload: unknown, event = "issue_comment", signature?: string): Request {
	const body = JSON.stringify(payload);
	return new Request("https://example.com/github/events", {
		method: "POST",
		body,
		headers: {
			"x-github-event": event,
			"x-github-delivery": "delivery-1",
			"x-hub-signature-256":
				signature ?? `sha256=${createHmac("sha256", "secret").update(body).digest("hex")}`,
		},
	});
}
