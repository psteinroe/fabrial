import { defineWorkflow } from "fabrial";
import { slack } from "@fabrial/slack";
import { supportAgent } from "../agents/support.ts";
import { bugIntake } from "./bug-intake.ts";
export const channels = { support: "C_SUPPORT", bugs: "C_BUGS" };
export const routingModel = { provider: "jev", modelId: "route" };
export const generalAssistant = defineWorkflow({
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
			return ctx.handoff("bug-intake", bugIntake, {
				message: { text: message.text, channelId: message.channelId },
			});
		}
		// Explicit safe fallback on classifier error: the support agent still has deterministic access checks.
		const answer = await ctx.agent<string>("assistant", supportAgent, { input: message.text });
		await ctx.thread!.post("answer", answer);
		return { status: "answered" };
	},
});
