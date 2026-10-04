import { f } from "../fabrial.ts";
import { linear } from "@fabrial/linear";
export const linearTriage = f.defineWorkflow({
	name: "linear-triage",
	triggers: [linear.issueCreated({ team: "ENG" })],
	async run(issue, ctx) {
		await ctx.thread!.post(
			"triage",
			`Thanks for reporting ${issue.identifier}: ${issue.title}. Engineering will triage this issue.`,
		);
		return { status: "triaged", issueId: issue.issueId };
	},
});
