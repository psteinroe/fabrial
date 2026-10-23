// Generated from packages/github/README.md:4 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { github } from "@fabrial/github";
import { createFabrial } from "fabrial";

const plugin = github({
	token: process.env.GITHUB_TOKEN!,
	owner: "acme", // set explicitly for organization repositories
	webhookSecret: process.env.GITHUB_WEBHOOK_SECRET!,
	installationId: "acme", // optional for PATs; defaults to "default"
	userName: "fabrial[bot]",
	botUserId: 12345,
});
const f = createFabrial({ plugins: [plugin] });
// Define workflows and agents on f; build the app with f.app({ workflows, agents }).
export {};
