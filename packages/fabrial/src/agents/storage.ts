import { randomUUID } from "node:crypto";
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { MemoryStorage, type Seq, type StorageWrite } from "@earendil-works/pi-durable";
import type { Sql } from "postgres";

export class SessionBusy extends Error {}
export class LeaseLost extends Error {}

/** Pi owns record validation and materialization; Postgres owns admission and atomic persistence. */
export class PostgresStorage extends MemoryStorage {
	readonly owner: string;
	readonly epoch: number;
	readonly lost = new AbortController();
	private timer: ReturnType<typeof setInterval> | undefined;
	private closing = false;
	private tail: Promise<unknown> = Promise.resolve();

	private constructor(
		private readonly sql: Sql,
		readonly key: string,
		epoch: number,
		private readonly leaseMs: number,
		owner: string,
	) {
		super();
		this.epoch = epoch;
		this.owner = owner;
	}

	static async migrate(sql: Sql): Promise<void> {
		await sql.begin(async (sql) => {
			await sql`SELECT pg_advisory_xact_lock(739410812)`;
			await sql`CREATE SCHEMA IF NOT EXISTS fabrial_pi`;
			await sql`CREATE TABLE IF NOT EXISTS fabrial_pi.sessions (
			key text PRIMARY KEY, epoch bigint NOT NULL DEFAULT 0, owner uuid,
			lease_until timestamptz, seq bigint NOT NULL DEFAULT 0
		)`;
			await sql`CREATE TABLE IF NOT EXISTS fabrial_pi.commits (
			session_key text NOT NULL REFERENCES fabrial_pi.sessions(key),
			seq bigint NOT NULL, writes text NOT NULL, PRIMARY KEY (session_key, seq)
		)`;
		});
	}

	static async open(
		sql: Sql,
		key: string,
		options: { leaseMs?: number } = {},
	): Promise<PostgresStorage> {
		const leaseMs = options.leaseMs ?? 30_000;
		if (leaseMs < 100) throw new RangeError("leaseMs must be at least 100ms");
		const owner = randomUUID();
		await sql`INSERT INTO fabrial_pi.sessions (key) VALUES (${key}) ON CONFLICT DO NOTHING`;
		const rows = await sql`UPDATE fabrial_pi.sessions SET epoch = epoch + 1, owner = ${owner},
			lease_until = clock_timestamp() + ${leaseMs} * interval '1 millisecond'
			WHERE key = ${key} AND (owner IS NULL OR lease_until <= clock_timestamp()) RETURNING epoch`;
		if (!rows[0]) throw new SessionBusy(`Pi Session ${key} already has an owner`);
		const storage = new PostgresStorage(sql, key, Number(rows[0].epoch), leaseMs, owner);
		try {
			const commits =
				await sql`SELECT seq, writes FROM fabrial_pi.commits WHERE session_key = ${key} ORDER BY seq`;
			for (const commit of commits)
				storage
					.prepareCommit(
						JSON.parse(String(commit.writes)) as StorageWrite[],
						Number(commit.seq) as Seq,
					)
					.apply();
			storage.timer = setInterval(
				() => {
					void storage.renew().catch((error: unknown) => storage.lost.abort(error));
				},
				Math.floor(leaseMs / 3),
			);
			storage.timer.unref();
			return storage;
		} catch (error) {
			await storage.close(BACKGROUND_CONTEXT);
			throw error;
		}
	}

	private async renew(): Promise<void> {
		const rows = await this.sql`UPDATE fabrial_pi.sessions
			SET lease_until = clock_timestamp() + ${this.leaseMs} * interval '1 millisecond'
			WHERE key = ${this.key} AND owner = ${this.owner} AND epoch = ${this.epoch}
			AND lease_until > clock_timestamp() RETURNING epoch`;
		if (!rows.length) throw new LeaseLost(`Pi Session ${this.key} lease expired or was replaced`);
	}

	override commit(writes: readonly StorageWrite[], context: Context): Promise<Seq> {
		const operation = this.tail.then(async () => {
			if (this.closing) throw new Error("Storage is closed");
			this.lost.signal.throwIfAborted();
			context.abortSignal?.throwIfAborted();
			const prepared = this.prepareCommit(writes);
			try {
				await this.sql.begin(async (sql) => {
					const rows = await sql`UPDATE fabrial_pi.sessions SET seq = ${prepared.seq}
						WHERE key = ${this.key} AND owner = ${this.owner} AND epoch = ${this.epoch}
						AND lease_until > clock_timestamp() RETURNING epoch`;
					if (!rows.length)
						throw new LeaseLost(`Stale Pi Session owner: ${this.key} epoch ${this.epoch}`);
					await sql`INSERT INTO fabrial_pi.commits (session_key, seq, writes)
						VALUES (${this.key}, ${prepared.seq}, ${JSON.stringify(prepared.writes)})`;
				});
			} catch (error) {
				// A connection error may hide a committed transaction. Never continue from uncertain state.
				this.lost.abort(error);
				throw error;
			}
			return prepared.apply();
		});
		this.tail = operation.catch(() => {});
		return operation;
	}

	override async close(context: Context): Promise<void> {
		if (this.closing) return;
		this.closing = true;
		clearInterval(this.timer);
		await this.tail;
		await super.close(context);
		await this.sql`UPDATE fabrial_pi.sessions SET owner = NULL, lease_until = NULL
			WHERE key = ${this.key} AND owner = ${this.owner} AND epoch = ${this.epoch}`;
	}
}
