// Generated from packages/fabrial/README.md:308 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, support, slack } from "../prelude.ts";
const route = f.defineWorkflow({
	name: "route",
	triggers: [slack.mentioned({ channel: "C_BUGS" })],
	async run(message, ctx) {
		const result = await ctx.evaluate("route", {
			model: "typesafe/jev-latest",
			state: { message: message.text },
			questions: { bug: { type: "boolean", instructions: "Is this a bug report?" } },
		});
		const bug = result.stopReason === "stop" && result.answers.bug?.value;
		await ctx.send("deliver", support, { input: message, priority: bug ? "urgent" : "normal" });
	},
});
export {};
