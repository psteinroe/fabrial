// Names the READMEs' ```ts check snippets use without defining them. Each snippet imports the ones it uses
// and doesn't declare (see readme.test.ts).
import { createFabrial, defineUser } from "fabrial";
import { definePlugin } from "fabrial/plugin";
import { github } from "@fabrial/github";
import { slack } from "@fabrial/slack";
import { z } from "zod";

export { github, slack, z };

/** A business client, as an app-local plugin. */
export const shop = definePlugin({
	id: "shop",
	clients: () => ({
		shop: {
			async order(orderId: string) {
				return { orderId, total: 42, customer: "acme" };
			},
			async refund(orderId: string, amount: number) {
				return { orderId, refunded: amount };
			},
		},
	}),
});

export const alice = defineUser({
	id: "alice",
	name: "Alice",
	identities: [slack.identity({ workspace: "acme", userId: "U_ALICE" })],
});
export const bob = defineUser({
	id: "bob",
	name: "Bob",
	identities: [slack.identity({ workspace: "acme", userId: "U_BOB" })],
});
export const users = [alice, bob];

export const f = createFabrial({
	database: process.env.DATABASE_URL,
	models: { anthropic: { apiKey: process.env.ANTHROPIC_API_KEY } },
	plugins: [
		slack({
			botToken: process.env.SLACK_BOT_TOKEN!,
			signingSecret: process.env.SLACK_SIGNING_SECRET!,
			workspace: "acme",
		}),
		github({ token: process.env.GITHUB_TOKEN!, webhookSecret: process.env.GITHUB_WEBHOOK_SECRET }),
		shop,
	],
	identity: users,
});

export const billing = f.defineGroup({ id: "billing", members: [alice, bob] });

export const lookupOrder = f.defineTool({
	name: "lookup_order",
	description: "Look up an order by id.",
	input: z.object({ orderId: z.string() }),
	idempotent: true,
	run: ({ orderId }, ctx) => ctx.clients.shop.order(orderId),
});

export const support = f.defineAgent({
	name: "support",
	description: "Answers questions about orders.",
	model: "anthropic/claude-sonnet-5",
	tools: [lookupOrder],
});
