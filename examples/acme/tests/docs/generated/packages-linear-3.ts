// Generated from packages/linear/README.md:4 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { linear } from "@fabrial/linear";
import { createFabrial } from "fabrial";

const plugin = linear({
	apiKey: process.env.LINEAR_API_KEY!, // or accessToken: a fixed OAuth token
	webhookSecret: process.env.LINEAR_WEBHOOK_SECRET!,
	organizationId: "your-organization-uuid",
	userName: "fabrial",
});
const f = createFabrial({ plugins: [plugin] });
// Define workflows and agents on f; build the app with f.app({ workflows, agents }).
export {};
