import { f } from "../fabrial.ts";
import { github } from "@fabrial/github";
export const prReview = f.defineWorkflow({
	name: "pr-review",
	triggers: [github.pullRequestOpened({ repo: "acme/app" })],
	async run(pr, ctx) {
		await ctx.thread!.post(
			"review",
			`Review queued for #${pr.number}: ${pr.title}. Please include tests and a rollback plan.`,
		);
		return { status: "reviewed", number: pr.number };
	},
});
