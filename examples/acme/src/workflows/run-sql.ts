import { f } from "../fabrial.ts";
import { z } from "zod";
import { engineeringTriage, support } from "../identity.ts";

export const runSql = f.defineWorkflow({
	name: "run-sql",
	description: "Execute a customer repair SQL statement after a human reviews the exact proposal.",
	input: z.object({
		organisationId: z.string().min(1),
		sql: z.string().min(1),
		reason: z.string().min(1),
	}),
	access: { invoke: support },
	async run(input, ctx) {
		const preview = await ctx.step("preview", () =>
			ctx.clients.database.preview(input.organisationId, input.sql),
		);
		const decision = await ctx.waitForApproval("approve-sql", {
			title: "Apply this customer repair?",
			details: { ...input, preview },
			approvers: engineeringTriage,
			timeout: "24h",
		});
		if (!decision.approved)
			return { status: "declined", decision: decision.status, rowsChanged: 0 };
		const result = await ctx.step("execute", () =>
			ctx.clients.database.execute(input.organisationId, input.sql),
		);
		return { status: "executed", decision: decision.status, ...result };
	},
});
