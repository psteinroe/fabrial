// Each child owns an independent Chat instance and Postgres pool (no shared JS mutex).
import { createPostgresState } from "@chat-adapter/state-pg";
import { Chat, type Adapter } from "chat";
import { ChatConnection, type SDKState } from "../../../src/chat/connection.ts";
import { updateRoutingState } from "../../../src/routing/follows.ts";
import { chatCapability } from "../../../src/plugin.ts";

const state = createPostgresState({ url: process.env.STATE_URL! });
const adapter = {
	name: "slack",
	initialize: async () => {},
	channelIdFromThreadId: (id: string) => id.split(":").slice(0, 2).join(":"),
} as unknown as Adapter;
const bot = new Chat<Record<string, Adapter>, SDKState>({
	adapters: { slack: adapter },
	state,
	userName: "test",
});
const connection = new ChatConnection(
	bot,
	new Map([["slack", chatCapability({ adapter: () => adapter, installationId: "acme" })]]),
	console,
	{},
);
try {
	const thread = await connection.thread({
		kind: "thread",
		provider: "slack",
		threadId: "slack:C1:root",
	});
	process.send?.("ready");
	await new Promise<void>((resolve) => process.once("message", () => resolve()));
	await Promise.all(
		Array.from({ length: 20 }, (_, i) =>
			updateRoutingState(connection, thread, (current) => {
				if (!current) throw new Error("Missing initial state");
				return {
					...current,
					follows: [
						...(current.follows ?? []),
						{ session: `${process.env.WORKER_ID}:${i}`, agent: "a" },
					],
				};
			}),
		),
	);
} finally {
	await state.disconnect();
	process.disconnect?.();
}
