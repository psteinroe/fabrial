import { z } from "zod";
import { f } from "../fabrial.ts";
import { runSql } from "../workflows/run-sql.ts";

export const Findings = f.defineState({
	name: "acme-findings",
	scope: "thread",
	schema: z.object({ notes: z.array(z.string()) }),
	initial: () => ({ notes: [] as string[] }),
	render: (s) => (s.notes.length ? `Known so far:\n- ${s.notes.join("\n- ")}` : undefined),
});
export const readOnlyQuery = f.defineTool({
	name: "read_only_query",
	description: "Read customer data; the database enforces a read-only transaction.",
	input: z.object({ organisationId: z.string(), sql: z.string() }),
	idempotent: true,
	async run({ organisationId, sql }, ctx) {
		const rows = await ctx.clients.database.query(organisationId, sql);
		await ctx.state(Findings).update((s) => {
			s.notes.push(`Queried organisation ${organisationId}: ${JSON.stringify(rows)}`);
		});
		return rows;
	},
});
export const supportModel = "anthropic/claude-sonnet-4-5";
export const supportAgent = f.defineAgent({
	name: "acme-support",
	model: supportModel,
	instructions:
		"Help support investigate customer issues. Read before proposing repairs. Use run_sql for writes; never bypass its human approval. Report declined or cancelled requests honestly. Return your answer without posting it.",
	tools: [readOnlyQuery, runSql],
	state: [Findings],
});
