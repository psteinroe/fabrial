import { createPostgresState } from "@chat-adapter/state-pg";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { expect, it } from "vitest";
import { routingState, setup } from "../helper.ts";

it("persists thread routing state and subscription through a Postgres-backed restart", async () => {
	const container = await new PostgreSqlContainer("postgres:17-alpine").start();
	const first = createPostgresState({ url: container.getConnectionUri() });
	const second = createPostgresState({ url: container.getConnectionUri() });
	try {
		const ref = { kind: "thread" as const, provider: "slack", threadId: "slack:C1:root" };
		const env = setup(first);
		const thread = await env.connection.port.thread(ref);
		await first.set(`thread-state:${ref.threadId}`, { custom: "preserved" });
		await thread.setState(routingState);
		await first.disconnect();
		const restored = setup(second);
		const restoredThread = await restored.connection.port.thread(ref);
		expect(await restoredThread.getState()).toEqual(routingState);
		expect(await second.isSubscribed(ref.threadId)).toBe(true);
		expect(await second.get(`thread-state:${ref.threadId}`)).toEqual({
			custom: "preserved",
			fabrial: routingState,
		});
		const { rows } = await second
			.getClient()
			.query("SELECT expires_at FROM chat_state_cache WHERE cache_key LIKE $1", [
				"%thread-state:%",
			]);
		expect(new Date(rows[0].expires_at).getTime() - Date.now()).toBeGreaterThan(
			29 * 24 * 60 * 60 * 1000,
		);
		await restoredThread.setState(null);
		expect(await restoredThread.getState()).toBeNull();
	} finally {
		await first.disconnect();
		await second.disconnect();
		await container.stop();
	}
});
