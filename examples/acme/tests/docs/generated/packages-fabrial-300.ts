// Generated from packages/fabrial/README.md:301 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, support, slack } from "../prelude.ts";
const route = f.defineWorkflow({
	name: "route",
	triggers: [slack.mentioned({ channel: "C_BUGS" })],
	async run(message, ctx) {
		const result = await ctx.evaluate("route", {
			model: "typesafe/jev-latest",
			state: { message: message.text },
			questions: {
				bug: {
					type: "bool",
					instructions: "Is this a bug report?",
					criteria: { true: "Yes", false: "No" },
				},
			},
		});
		const bug = result.answers.bug?.type === "bool" && result.answers.bug.probability >= 0.5;
		await ctx.send("deliver", support, { input: message, priority: bug ? "urgent" : "normal" });
	},
});
export {};
