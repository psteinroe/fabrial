import { z } from "zod";
import { slack } from "@fabrial/slack";
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
	description: "Investigates customer issues with read-only queries and proposes approved repairs.",
	model: supportModel,
	instructions:
		"Help support investigate customer issues. Read before proposing repairs. Use run_sql for writes; never bypass its human approval. Report declined or cancelled requests honestly. Return your answer without posting it.",
	tools: [readOnlyQuery, runSql],
	state: [Findings],
	// One conversation for the whole help channel, steered by whoever writes there.
	triggers: [slack.message({ channel: "C_HELP" })],
	session: "channel",
});
