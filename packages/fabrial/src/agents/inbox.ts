import type { Sql } from "postgres";
import type { InvocationMetadata } from "../runtime.ts";
import type { Delivery } from "../send.ts";

const CHANNEL = "fabrial_inbox";
/** Catches notifications missed while the listener reconnects; LISTEN/NOTIFY wakes drivers first. */
export const INBOX_FALLBACK_MS = 5_000;

export interface InboxRow {
	request_id: string;
	content: string;
	delivery: Delivery;
	metadata: InvocationMetadata;
}

/**
 * Sent input queued for its conversation's driver, across processes. A row lives from `ctx.send` until
 * a driver has admitted it to Pi; then it is deleted. A later redelivery of the same request inserts it
 * again, and Pi's request-id dedup (scoped to the conversation) returns the existing submission.
 */
export class Inbox {
	private readonly waiters = new Map<string, Set<() => void>>();
	private listener: { unlisten(): Promise<void> } | undefined;

	constructor(private readonly sql: Sql) {}

	static async migrate(sql: Sql): Promise<void> {
		await sql.begin(async (sql) => {
			await sql`SELECT pg_advisory_xact_lock(739410813)`;
			await sql`CREATE SCHEMA IF NOT EXISTS fabrial`;
			await sql`CREATE TABLE IF NOT EXISTS fabrial.inbox (
			request_id text PRIMARY KEY, session_key text NOT NULL, agent text NOT NULL,
			seq bigserial, content text NOT NULL, delivery text NOT NULL, metadata jsonb NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`;
			await sql`CREATE INDEX IF NOT EXISTS inbox_conversation
			ON fabrial.inbox (session_key, agent, seq)`;
		});
	}

	async start(): Promise<void> {
		await Inbox.migrate(this.sql);
		// postgres.js re-listens after a reconnect; wake everyone for what was sent meanwhile.
		this.listener = await this.sql.listen(
			CHANNEL,
			(session) => this.wake(session),
			() => {
				for (const session of this.waiters.keys()) this.wake(session);
			},
		);
	}

	async stop(): Promise<void> {
		await this.listener?.unlisten();
		this.listener = undefined;
	}

	/** Queue an input and notify listeners of its Session, once per request id. */
	async enqueue(row: InboxRow & { session_key: string; agent: string }): Promise<void> {
		await this.sql`WITH queued AS (
			INSERT INTO fabrial.inbox (request_id, session_key, agent, content, delivery, metadata)
			VALUES (${row.request_id}, ${row.session_key}, ${row.agent}, ${row.content},
				${row.delivery}, ${this.sql.json(row.metadata as never)})
			ON CONFLICT (request_id) DO NOTHING RETURNING session_key
		) SELECT pg_notify(${CHANNEL}, session_key) FROM queued`;
	}

	/**
	 * Watch a Session before reading its inbox, so no notification falls between the read and the
	 * subscription. `due()` is true after a notification or once the fallback interval has passed.
	 */
	watch(session: string): { due(): boolean; close(): void } {
		let notified = true;
		let last = 0;
		const wake = () => {
			notified = true;
		};
		const set = this.waiters.get(session) ?? new Set();
		set.add(wake);
		this.waiters.set(session, set);
		return {
			due() {
				if (!notified && Date.now() - last < INBOX_FALLBACK_MS) return false;
				notified = false;
				last = Date.now();
				return true;
			},
			close: () => {
				set.delete(wake);
				if (!set.size && this.waiters.get(session) === set) this.waiters.delete(session);
			},
		};
	}

	pending(session: string, agent: string): Promise<InboxRow[]> {
		return this.sql<InboxRow[]>`SELECT request_id, content, delivery, metadata FROM fabrial.inbox
			WHERE session_key = ${session} AND agent = ${agent} ORDER BY seq`;
	}

	/** Prune inputs a driver has durably admitted to Pi. */
	async remove(requestIds: readonly string[]): Promise<void> {
		await this.sql`DELETE FROM fabrial.inbox WHERE request_id IN ${this.sql(requestIds)}`;
	}

	private wake(session: string) {
		for (const wake of this.waiters.get(session) ?? []) wake();
	}
}
