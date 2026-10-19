import { f } from "../fabrial.ts";
import { slack } from "@fabrial/slack";
import { supportAgent } from "../agents/support.ts";
export const channels = { support: "C_SUPPORT", bugs: "C_BUGS" };
export const routingModel = "typesafe/jev-latest";
export const generalAssistant = f.defineWorkflow({
	name: "general-assistant",
	triggers: [slack.mentioned()],
	async run(message, ctx) {
		const route = await ctx.evaluate("route", {
			model: routingModel,
			state: { message: message.text, channel: message.channelId },
			questions: {
				bug: {
					type: "boolean",
					instructions: "Is this a bug report rather than a support operation?",
				},
			},
		});
		if (
			message.channelId === channels.bugs ||
			(route.stopReason === "stop" &&
				route.answers.bug?.value &&
				route.answers.bug.probability >= 0.9)
		) {
			// The investigation is the support agent's answer here; nothing waits for replies in this thread.
			await ctx.thread!.post("ack", "I'll investigate this bug report.");
			const analysis = await ctx.agent<string>("investigate", supportAgent, {
				input: message.text,
			});
			await ctx.thread!.post("analysis", analysis);
			return { status: "investigated" };
		}
		// Explicit safe fallback on classifier error: the support agent's writes still need human approval.
		// The support agent answers in this thread, and later replies here go straight to it.
		await ctx.send("assistant", supportAgent, { input: message });
		return { status: "sent" };
	},
});
