// Generated from packages/fabrial/README.md:57 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { shop, users, f, billing, lookupOrder, support, slack, z } from "../prelude.ts";
const refund = f.defineWorkflow({
	name: "refund",
	description: "Refund an order after billing approves it.",
	input: z.object({ orderId: z.string(), amount: z.number().positive(), reason: z.string() }),
	async run(input, ctx) {
		const order = await ctx.step("order", () => ctx.clients.shop.order(input.orderId));
		const decision = await ctx.waitForApproval("approve", {
			title: `Refund ${input.amount} € on ${input.orderId}?`,
			details: { ...input, customer: order.customer }, // shown verbatim; the approval binds to it
			approvers: billing, // users and groups: static, a rotation, or a Slack user group
			timeout: "24h",
		});
		if (!decision.approved) return { status: decision.status, reason: decision.comment ?? null };
		return ctx.step("refund", () => ctx.clients.shop.refund(input.orderId, input.amount));
	},
});

const desk = f.defineAgent({
	name: "desk",
	description: "Handles order questions and refunds in #support.",
	model: "anthropic/claude-sonnet-5",
	tools: [lookupOrder, refund], // a workflow is a tool named after it
	triggers: [slack.message({ channel: "C_SUPPORT" })],
});

export const app = f.app({ workflows: [refund], agents: [desk] });
export {};
