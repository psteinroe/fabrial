import { defineWorkflow } from "fabrial";
import { z } from "zod";
import { supportAgent } from "../agents/support.ts";
export const bugIntake = defineWorkflow({
	name: "bug-intake",
	input: z.object({ message: z.object({ text: z.string(), channelId: z.string() }) }),
	async run({ message }, ctx) {
		await ctx.thread!.post("ack", "I'll investigate this bug report.");
		const analysis = await ctx.agent<string>("investigate", supportAgent, { input: message.text });
		await ctx.thread!.post("analysis", analysis);
		return { status: "investigated" };
	},
});
