import { createFabrial } from "fabrial";
import { withPi } from "@fabrial/pi";
import { slack } from "@fabrial/slack";
import { github } from "@fabrial/github";
import { linear } from "@fabrial/linear";
import { databasePlugin } from "./database.ts";
import { identity } from "./identity.ts";

// Catalog only: no workflows/agents or runtime connections. createApp supplies live values.
export const f = createFabrial({
	plugins: [
		slack({ workspace: "acme", botToken: "", signingSecret: "" }),
		github({
			owner: "acme",
			installationId: 42,
			token: "",
			webhookSecret: "unconfigured-catalog-only",
			botUserId: 99,
		}),
		linear({ organizationId: "acme", apiKey: "", webhookSecret: "unconfigured-catalog-only" }),
		databasePlugin(),
	],
	identity,
});
export const { defineTool, defineAgent, section } = withPi(f);
