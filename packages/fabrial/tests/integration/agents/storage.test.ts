import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { type ConversationId, MemoryStorage } from "@earendil-works/pi-durable";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { LeaseLost, PostgresStorage, SessionBusy } from "../../../src/sessions/storage.ts";

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

/** Commits `count` empty writes to a fresh Session in one statement; returns its key. */
async function seeded(count: number) {
	const key = randomUUID();
	await sql`INSERT INTO fabrial_pi.sessions (key, seq) VALUES (${key}, ${count})`;
	await sql`INSERT INTO fabrial_pi.commits (session_key, seq, writes)
		SELECT ${key}, seq, '[]' FROM generate_series(1, ${count}::int) AS seq`;
	return key;
}

it("keeps its lease through a replay slower than the lease, and commits afterwards", async () => {
	const key = await seeded(12);
	// Applying each commit blocks the event loop for 25ms: 300ms of replay against a 100ms lease.
	const prepareCommit = Object.getOwnPropertyDescriptor(MemoryStorage.prototype, "prepareCommit")!
		.value as (this: MemoryStorage, ...args: Parameters<MemoryStorage["prepareCommit"]>) => never;
	let slow = true;
	const spy = vi.spyOn(MemoryStorage.prototype, "prepareCommit").mockImplementation(function (
		this: MemoryStorage,
		...args
	) {
		if (slow) for (const until = performance.now() + 25; performance.now() < until;);
		return prepareCommit.apply(this, args);
	});
	try {
		const storage = await PostgresStorage.open(sql, key, { leaseMs: 100 });
		slow = false;
		expect(spy).toHaveBeenCalledTimes(12);
		await expect(storage.commit([], BACKGROUND_CONTEXT)).resolves.toBe(13);
		expect(storage.lost.signal.aborted).toBe(false);
		await storage.close(BACKGROUND_CONTEXT);
	} finally {
		spy.mockRestore();
	}
});

// Replay cost grows with every commit a Session ever made: FABRIAL_BENCH=1 logs it for larger logs.
it("replays a Session of thousands of commits on open", async () => {
	for (const count of process.env.FABRIAL_BENCH ? [5_000, 20_000, 100_000] : [5_000]) {
		const key = await seeded(count);
		const started = performance.now();
		const storage = await PostgresStorage.open(sql, key);
		const ms = performance.now() - started;
		if (process.env.FABRIAL_BENCH)
			process.stderr.write(`open+replay of ${count} commits: ${ms.toFixed(0)}ms\n`);
		await expect(storage.commit([], BACKGROUND_CONTEXT)).resolves.toBe(count + 1);
		await storage.close(BACKGROUND_CONTEXT);
	}
});
