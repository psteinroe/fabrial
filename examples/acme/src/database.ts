import { definePlugin, type Json } from "fabrial";
import type { Sql } from "postgres";

// postgres.js supports simple:false at runtime but omits it from UnsafeQueryOptions.
// Extended protocol parses exactly one statement, even when there are no parameters.
const singleStatement = { prepare: true, simple: false };

/** Use a dedicated, least-privilege database role in production; SQL is not a sandbox. */
export class Database {
	constructor(private readonly sql: Sql) {}
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
export const databasePlugin = definePlugin((sql: Sql) => ({
	id: "database",
	clients: () => ({ database: new Database(sql) }),
}));
