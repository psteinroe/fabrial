import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import type { ConversationId } from "@earendil-works/pi-durable";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { LeaseLost, PostgresStorage, SessionBusy } from "../../src/index.ts";

let sql: Sql;
let container: Awaited<ReturnType<PostgreSqlContainer["start"]>>;
beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	sql = postgres(container.getConnectionUri());
	await PostgresStorage.migrate(sql);
	await PostgresStorage.migrate(sql);
});
afterAll(async () => {
	await sql?.end();
	await container?.stop();
});

registerStorageConformance(
	{ describe, it, expect },
	"Postgres storage conformance",
	async (use) => {
		const storage = await PostgresStorage.open(sql, randomUUID());
		try {
			await use(storage);
		} finally {
			await storage.close(BACKGROUND_CONTEXT);
		}
	},
);

it("excludes competing owners and fences expired owners after a per-Session takeover", async () => {
	const key = randomUUID();
	const first = await PostgresStorage.open(sql, key);
	await expect(PostgresStorage.open(sql, key)).rejects.toBeInstanceOf(SessionBusy);
	await sql`UPDATE fabrial_pi.sessions SET lease_until = clock_timestamp() - interval '1 second' WHERE key = ${key}`;
	const next = await PostgresStorage.open(sql, key);
	expect(next.epoch).toBeGreaterThan(first.epoch);
	await expect(first.commit([], BACKGROUND_CONTEXT)).rejects.toBeInstanceOf(LeaseLost);
	await first.close(BACKGROUND_CONTEXT);
	await expect(PostgresStorage.open(sql, key)).rejects.toBeInstanceOf(SessionBusy);
	await next.commit([], BACKGROUND_CONTEXT);
	await next.close(BACKGROUND_CONTEXT);
});

it("replays durable commits on reopen, preserving sequence and the global id namespace", async () => {
	const key = randomUUID();
	let storage = await PostgresStorage.open(sql, key);
	const id = await storage.mintId<ConversationId>();
	const seq = await storage.commit([{ type: "conversation", value: { id } }], BACKGROUND_CONTEXT);
	await storage.close(BACKGROUND_CONTEXT);
	storage = await PostgresStorage.open(sql, key);
	expect(await storage.conversation(id, BACKGROUND_CONTEXT)).toEqual({ id });
	expect(await storage.mintId()).toBeGreaterThan(id);
	expect(await storage.commit([], BACKGROUND_CONTEXT)).toBeGreaterThan(seq);
	await storage.close(BACKGROUND_CONTEXT);
});
