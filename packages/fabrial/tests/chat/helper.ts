import { createMockAdapter, createMockLogger } from "@chat-adapter/tests";
import { createMemoryState } from "@chat-adapter/state-memory";
import type { ChatInstance, StateAdapter } from "chat";
import type { FabrialHost, ThreadRoutingState } from "../internals.ts";
import { vi } from "vitest";
import { chat, chatCapability, chatEvents } from "../../src/chat/index.ts";

export const routingState: ThreadRoutingState = {
	ingestedDedupeIds: [{ kind: "message", id: "message-1", at: 1 }],
	statusMessageId: null,
	follows: [{ session: "slack:C1:root", agent: "support" }],
};

export function setup(state: StateAdapter = createMemoryState()) {
	let sdk: ChatInstance;
	const adapter = createMockAdapter("slack", {
		initialize: vi.fn(async (bot) => {
			sdk = bot;
		}),
		postChannelMessage: vi.fn(async () => ({ id: "root-1", threadId: "slack:C1:root-1", raw: {} })),
		postEphemeral: vi.fn(async () => ({
			id: "ephemeral-1",
			threadId: "slack:C1:root-1",
			raw: {},
			usedFallback: false,
		})),
	});
	const plugin = {
		id: "slack",
		events: chatEvents(),
		chat: chatCapability({ adapter: () => adapter, installationId: "acme" }),
	};
	const host: FabrialHost = {
		plugins: [plugin],
		workflows: [],
		runtime: {
			register: vi.fn(),
			emit: vi.fn(),
			invoke: vi.fn(),
			cancel: vi.fn(async () => true),
			signalWithStart: vi.fn(),
			signal: vi.fn(),
			getExecution: vi.fn(async () => null),
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
		receiveMessage: vi.fn(async () => "accepted" as const),
		receiveAction: vi.fn(),
		receiveCancellation: vi.fn(),
		resolvePrincipal: vi.fn(),
	};
	const connection = chat({ state }).connect(host);
	return { host, adapter, connection, state, sdk: () => sdk };
}
