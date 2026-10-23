import { f } from "../fabrial.ts";
import { github } from "@fabrial/github";
export const changelog = f.defineWorkflow({
	name: "changelog",
	triggers: [github.pullRequestOpened({ repo: "acme/app", observe: true })],
	async run(pr, ctx) {
		await ctx.step("record", () => ctx.clients.database.recordChangelog(pr));
		return { status: "recorded", number: pr.number };
	},
});
