import { createHmac } from "node:crypto";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockLogger } from "@chat-adapter/tests";
import { connectChat, type ChatConnection } from "../../fabrial/src/chat/connection.ts";
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
