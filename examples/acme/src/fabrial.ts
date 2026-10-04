import { createFabrial } from "fabrial";
import { withPi } from "@fabrial/pi";
import { slack } from "@fabrial/slack";
import { github } from "@fabrial/github";
import { linear } from "@fabrial/linear";
import { databasePlugin } from "./database.ts";
import { users } from "./users.ts";

const env = process.env;

// Values at definition; factories do not connect or validate credentials at import time.
export const f = createFabrial({
	plugins: [
		slack({
			workspace: env.SLACK_WORKSPACE!,
			teamId: env.SLACK_TEAM_ID,
			botToken: env.SLACK_BOT_TOKEN!,
			signingSecret: env.SLACK_SIGNING_SECRET!,
		}),
		github({
			owner: env.GITHUB_OWNER,
			installationId: env.GITHUB_INSTALLATION_ID ? Number(env.GITHUB_INSTALLATION_ID) : undefined,
			token: env.GITHUB_TOKEN!,
			webhookSecret: env.GITHUB_WEBHOOK_SECRET,
			botUserId: env.GITHUB_BOT_USER_ID ? Number(env.GITHUB_BOT_USER_ID) : undefined,
		}),
		linear({
			organizationId: env.LINEAR_ORGANIZATION_ID!,
			apiKey: env.LINEAR_API_KEY!,
			webhookSecret: env.LINEAR_WEBHOOK_SECRET,
		}),
		databasePlugin(env.APP_DATABASE_URL),
	],
	identity: users,
});
export const { defineTool, defineAgent, section } = withPi(f);
