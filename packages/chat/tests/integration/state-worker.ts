// Each child owns an independent Chat instance and Postgres pool (no shared JS mutex).
import { createPostgresState } from "@chat-adapter/state-pg";
import { Chat, type Adapter } from "chat";
import type { FabrialHost } from "fabrial";
import { createChatPort, type SDKState } from "../../src/port.ts";
import { chatCapability } from "../../src/events.ts";

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
const port = createChatPort(
	bot,
	new Map([["slack", chatCapability({ adapter: () => adapter, installationId: "acme" })]]),
	{ logger: console } as unknown as FabrialHost,
);
try {
	const thread = await port.thread({
		kind: "thread",
		provider: "slack",
		threadId: "slack:C1:root",
	});
	process.send?.("ready");
	await new Promise<void>((resolve) => process.once("message", () => resolve()));
	await Promise.all(
		Array.from({ length: 20 }, (_, i) =>
			thread.updateState((current) => {
				if (!current) throw new Error("Missing initial state");
				return {
					...current,
					consumedReplyIds: [...(current.consumedReplyIds ?? []), `${process.env.WORKER_ID}:${i}`],
				};
			}),
		),
	);
} finally {
	await state.disconnect();
	process.disconnect?.();
}
