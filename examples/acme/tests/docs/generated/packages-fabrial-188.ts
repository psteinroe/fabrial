// Generated from packages/fabrial/README.md:189 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, billing, slack, z } from "../prelude.ts";
const deploy = f.defineWorkflow({
	name: "deploy",
	input: z.object({ service: z.string(), version: z.string() }),
	async run(input, ctx) {
		const approval = await ctx.requestApproval("approve", {
			title: `Deploy ${input.service} ${input.version}?`,
			details: input,
			approvers: billing,
			destination: slack.channel("C_RELEASES"),
			card: { fields: [{ label: "Risk", value: "low" }] },
		});
		const first = await ctx.waitForAny("wait", { decision: approval.branch }, { timeout: "1h" });
		if (first.key === "timeout") {
			await approval.cancel("cancel", "No answer within an hour");
			return { status: "timed out" };
		}
		const decision = await approval.wait("decision");
		return decision.approved
			? { status: "deploying" }
			: { status: `declined: ${decision.comment ?? "no reason"}` };
	},
});
export {};
