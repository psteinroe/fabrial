import { defineUser } from "fabrial";
import { slack } from "@fabrial/slack";
import { github } from "@fabrial/github";
import { linear } from "@fabrial/linear";

export const alice = defineUser({
	id: "alice",
	name: "Alice",
	identities: [
		slack.identity({ workspace: process.env.SLACK_WORKSPACE!, userId: "U_ALICE" }),
		github.identity({ installationId: process.env.GITHUB_INSTALLATION_ID, userId: 1 }),
		linear.identity({ organizationId: process.env.LINEAR_ORGANIZATION_ID!, userId: "lin_alice" }),
	],
});
export const bob = defineUser({
	id: "bob",
	name: "Bob",
	identities: [
		slack.identity({ workspace: process.env.SLACK_WORKSPACE!, userId: "U_BOB" }),
		github.identity({ installationId: process.env.GITHUB_INSTALLATION_ID, userId: 2 }),
		linear.identity({ organizationId: process.env.LINEAR_ORGANIZATION_ID!, userId: "lin_bob" }),
	],
});
export const users = [alice, bob];
