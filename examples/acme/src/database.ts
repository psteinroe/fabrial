import { definePlugin, type Json } from "fabrial";
import postgres, { type Options, type Sql } from "postgres";

// postgres.js supports simple:false at runtime but omits it from UnsafeQueryOptions.
// Extended protocol parses exactly one statement, even when there are no parameters.
const singleStatement = { prepare: true, simple: false };

/** Use a dedicated, least-privilege database role in production; SQL is not a sandbox. */
export class Database {
	constructor(private readonly pool: () => Sql) {}
	private get sql(): Sql {
		return this.pool();
	}
	async query(organisationId: string, query: string): Promise<Json> {
		return this.sql.begin(async (tx) => {
			await tx`SET TRANSACTION READ ONLY`;
			await tx`SELECT set_config('acme.organisation_id', ${organisationId}, true)`;
			const rows = await tx.unsafe(query, [], singleStatement);
			return JSON.parse(JSON.stringify(rows)) as Json;
		});
	}
	async preview(organisationId: string, query: string): Promise<Json> {
		return this.query(organisationId, `EXPLAIN (FORMAT JSON) ${query}`);
	}
	async execute(organisationId: string, query: string) {
		return this.sql.begin(async (tx) => {
			await tx`SELECT set_config('acme.organisation_id', ${organisationId}, true)`;
			const rows = await tx.unsafe(query, [], singleStatement);
			return { rowsChanged: rows.count };
		});
	}
	async recordChangelog(pr: { repo: string; number: number; title: string }) {
		await this
			.sql`INSERT INTO acme_changelog (repo, number, title) VALUES (${pr.repo}, ${pr.number}, ${pr.title}) ON CONFLICT (repo, number) DO NOTHING`;
	}
}
/** Values at definition; the plugin owns its pool from app.start() to app.stop(). */
export const databasePlugin = definePlugin((url: string | undefined, options: Options<{}> = {}) => {
	let pool: Sql | undefined;
	const database = new Database(() => {
		if (!pool) throw new Error("Database plugin is not started");
		return pool;
	});
	return {
		id: "database",
		clients: () => ({ database }),
		init() {
			if (!url) throw new Error("Database plugin requires APP_DATABASE_URL");
			pool ??= postgres(url, options);
		},
		async shutdown() {
			await pool?.end();
			pool = undefined;
		},
	};
});
