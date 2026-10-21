// Generated from packages/fabrial/README.md:235 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, github, z } from "../prelude.ts";
const watch = f.defineTool({
	name: "watch_pr",
	description: "Follow a pull request's reviews in this conversation.",
	input: z.object({ pr: z.string() }),
	async run({ pr }, ctx) {
		await ctx.follow("follow", github.pullRequest(pr), { priority: { comment: "urgent" } });
		return `Following ${pr}.`;
	},
});
export {};
