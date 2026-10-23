// Generated from packages/slack/README.md:12 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";

const f = createFabrial({
	plugins: [
		slack({
			botToken: process.env.SLACK_BOT_TOKEN!,
			signingSecret: process.env.SLACK_SIGNING_SECRET!,
			workspace: "acme",
		}),
	],
});
// Whoever is in @triage when an approver clicks; rotate in Slack, no deploy.
const triage = slack.userGroup({ id: "triage", handle: "triage" });
export const announce = f.defineWorkflow({
	name: "announce",
	triggers: [slack.mentioned({ channel: "C_OPS" })],
	async run(message, ctx) {
		const decision = await ctx.waitForApproval("approve", {
			title: "Announce this?",
			details: message.text,
			approvers: triage,
		});
		if (decision.approved)
			await ctx.step("post", async () => {
				await ctx.clients.slack.chat.postMessage({ channel: "C_ALL", text: message.text });
			});
	},
});
export {};
