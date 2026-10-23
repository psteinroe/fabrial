// Generated from packages/fabrial/README.md:22 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { users } from "../prelude.ts";
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";

const f = createFabrial({
	database: process.env.DATABASE_URL, // Postgres: workflows, agent sessions, and chat state
	models: { anthropic: { apiKey: process.env.ANTHROPIC_API_KEY } },
	plugins: [
		slack({
			botToken: process.env.SLACK_BOT_TOKEN!,
			signingSecret: process.env.SLACK_SIGNING_SECRET!,
			workspace: "acme", // identity namespace for Slack users
		}),
	],
});

const hello = f.defineAgent({
	name: "hello",
	description: "Answers questions in Slack.",
	model: "anthropic/claude-sonnet-5",
	instructions: "Answer briefly and kindly.",
	triggers: [slack.mentioned()],
});

export const app = f.app({ workflows: [], agents: [hello] });
await app.start(); // migrates Postgres, validates credentials, starts workers
// Serve app.fetch (Slack posts to /slack/events); call app.stop() on shutdown.
export {};
