import { defineWorkflow } from "fabrial";
import { github } from "@fabrial/github";
export const changelog = defineWorkflow({
	name: "changelog",
	triggers: [github.pullRequestOpened({ repo: "acme/app", observe: true })],
	async run(pr, ctx) {
		await ctx.step("record", () => ctx.clients.database.recordChangelog(pr));
		return { status: "recorded", number: pr.number };
	},
});
