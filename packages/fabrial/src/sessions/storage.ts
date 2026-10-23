// Pi's storage on Postgres: a commit log per Session, fenced by a lease.
import { MemoryStorage, type Seq, type StorageWrite } from "@earendil-works/pi-durable";
import type { Sql } from "postgres";
import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Context } from "@earendil-works/chord";

/** Another owner holds the Session's lease: `owner`, until it expires in `expiresInMs` (database time). */
export class SessionBusy extends Error {
	constructor(
		message: string,
		readonly owner: string | null,
		readonly expiresInMs: number,
	) {
		super(message);
	}
}
export class LeaseLost extends Error {}

/** Commits replayed per query on open. */
const REPLAY_PAGE = 5000;

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
			// The Conductor execution that last hosted the Session: reclaim reads when it settled.
			await sql`ALTER TABLE fabrial_pi.sessions ADD COLUMN IF NOT EXISTS execution_id uuid`;
		});
	}

	static async open(
		sql: Sql,
		key: string,
		/** `executionId`: the Conductor execution hosting the Session, recorded for reclaim. */
		options: { leaseMs?: number; executionId?: string } = {},
	): Promise<PostgresStorage> {
		const leaseMs = options.leaseMs ?? 30_000;
		if (leaseMs < 100) throw new RangeError("leaseMs must be at least 100ms");
		const owner = randomUUID();
		await sql`INSERT INTO fabrial_pi.sessions (key) VALUES (${key}) ON CONFLICT DO NOTHING`;
		const rows = await sql`UPDATE fabrial_pi.sessions SET epoch = epoch + 1, owner = ${owner},
			lease_until = clock_timestamp() + ${leaseMs} * interval '1 millisecond',
			execution_id = coalesce(${options.executionId ?? null}::uuid, execution_id)
			WHERE key = ${key} AND (owner IS NULL OR lease_until <= clock_timestamp()) RETURNING epoch`;
		if (!rows[0]) {
			const [held] = await sql<{ owner: string | null; ms: number | null }[]>`SELECT owner,
				extract(epoch FROM lease_until - clock_timestamp()) * 1000 AS ms
				FROM fabrial_pi.sessions WHERE key = ${key}`;
			throw new SessionBusy(
				`Pi Session ${key} already has an owner`,
				held?.owner ?? null,
				Math.max(0, Number(held?.ms ?? 0)),
			);
		}
		const storage = new PostgresStorage(sql, key, Number(rows[0].epoch), leaseMs, owner);
		// Renewed from the moment it is taken: a replay may outlast the lease.
		storage.timer = setInterval(
			() => {
				void storage.renew().catch((error: unknown) => storage.lost.abort(error));
			},
			Math.floor(leaseMs / 3),
		);
		storage.timer.unref();
		try {
			// Replay blocks the timer while it applies commits, so it renews in between once a third of the lease passed.
			let renewed = performance.now();
			for (let after = 0; ;) {
				storage.lost.signal.throwIfAborted();
				const commits = await sql`SELECT seq, writes FROM fabrial_pi.commits
					WHERE session_key = ${key} AND seq > ${after} ORDER BY seq LIMIT ${REPLAY_PAGE}`;
				for (const commit of commits) {
					storage
						.prepareCommit(
							JSON.parse(String(commit.writes)) as StorageWrite[],
							Number(commit.seq) as Seq,
						)
						.apply();
					if (performance.now() - renewed < leaseMs / 3) continue;
					await storage.renew();
					renewed = performance.now();
				}
				if (commits.length < REPLAY_PAGE) break;
				after = Number(commits.at(-1)!.seq);
			}
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

	/**
	 * Up to `limit` Sessions whose key starts with `prefix`, after `after` in key order, without a live lease and
	 * hosted by a recorded execution: candidates for `reclaim`.
	 */
	static async reclaimable(
		sql: Sql,
		prefix: string,
		after: string,
		limit: number,
	): Promise<{ key: string; executionId: string }[]> {
		const rows = await sql<{ key: string; execution_id: string }[]>`
			SELECT key, execution_id FROM fabrial_pi.sessions
			WHERE key > ${after} AND starts_with(key, ${prefix}) AND execution_id IS NOT NULL
			AND (owner IS NULL OR lease_until <= clock_timestamp())
			ORDER BY key LIMIT ${limit}`;
		return rows.map((row) => ({ key: row.key, executionId: row.execution_id }));
	}

	/**
	 * Deletes a Session's storage, commits then Session, in one transaction; only while `executionId` still hosts it
	 * and nobody holds its lease. False if it was kept (or is already gone).
	 */
	static async reclaim(sql: Sql, key: string, executionId: string): Promise<boolean> {
		return sql.begin(async (sql) => {
			const [row] = await sql`SELECT 1 FROM fabrial_pi.sessions
				WHERE key = ${key} AND execution_id = ${executionId}::uuid
				AND (owner IS NULL OR lease_until <= clock_timestamp()) FOR UPDATE`;
			if (!row) return false;
			await sql`DELETE FROM fabrial_pi.commits WHERE session_key = ${key}`;
			await sql`DELETE FROM fabrial_pi.sessions WHERE key = ${key}`;
			return true;
		});
	}
}
