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
					type: "bool",
					instructions: "Is this a bug report rather than a support operation?",
					criteria: { true: "A bug report", false: "A support operation" },
				},
			},
		});
		if (
			message.channelId === channels.bugs ||
			(route.stopReason === "stop" &&
				route.answers.bug?.type === "bool" &&
				route.answers.bug.probability >= 0.9)
		) {
			// The investigation is the support agent's answer here; nothing waits for replies in this thread.
			await ctx.thread!.post("ack", "I'll investigate this bug report.");
			const analysis = await ctx.agent("investigate", supportAgent, {
				input: message.text,
			});
			await ctx.thread!.post("analysis", analysis);
			return { status: "investigated" };
		}
		// Explicit safe fallback on classifier error: the support agent's writes still need human approval.
		// One conversation per thread (the agent's own trigger keeps #help channel-wide): it answers here, and later
		// replies here go straight to it.
		await ctx.send("assistant", supportAgent, { input: message, session: "thread" });
		return { status: "sent" };
	},
});
