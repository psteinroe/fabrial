import { fork } from "node:child_process";
import { once } from "node:events";
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
		await thread.updateState(() => routingState);
		// Two OS processes, separate Chat instances and pools, one durable lock namespace.
		const children = ["a", "b"].map((id) =>
			fork(new URL("./state-worker.ts", import.meta.url), {
				execArgv: ["--experimental-transform-types", "--disable-warning=ExperimentalWarning"],
				env: { ...process.env, STATE_URL: container.getConnectionUri(), WORKER_ID: id },
				stdio: ["ignore", "pipe", "pipe", "ipc"],
			}),
		);
		try {
			const exits = children.map((child) => once(child, "exit"));
			await Promise.all(
				children.map((child) =>
					Promise.race([
						once(child, "message"),
						once(child, "exit").then(([code]) => {
							throw new Error(`State worker exited early: ${code}`);
						}),
					]),
				),
			);
			for (const child of children) child.send("go");
			expect(await Promise.all(exits)).toEqual([
				[0, null],
				[0, null],
			]);
			const updated = await thread.getState();
			expect(updated?.consumedReplyIds).toHaveLength(41);
			expect(new Set(updated?.consumedReplyIds).size).toBe(41);
			expect(updated).toMatchObject({
				requesterId: "alice",
				participantIds: ["alice", "bob"],
				cancellationIds: ["stop-1"],
			});
			// Restore the codec fixture for the restart/TTL assertions below.
			await thread.updateState(() => routingState);
		} finally {
			for (const child of children) if (child.exitCode === null) child.kill();
		}
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
		await restoredThread.updateState(() => null);
		expect(await restoredThread.getState()).toBeNull();
	} finally {
		await first.disconnect();
		await second.disconnect();
		await container.stop();
	}
});
