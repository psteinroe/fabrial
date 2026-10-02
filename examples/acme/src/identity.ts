import { defineGroup, defineUser, weeklyRotation } from "fabrial";
import { slack } from "@fabrial/slack";
import { github } from "@fabrial/github";
import { linear } from "@fabrial/linear";

export const alice = defineUser({
	id: "alice",
	name: "Alice",
	identities: [
		slack.identity({ workspace: "acme", userId: "U_ALICE" }),
		github.identity({ installationId: "42", userId: 1 }),
		linear.identity({ organizationId: "acme", userId: "lin_alice" }),
	],
});
export const bob = defineUser({
	id: "bob",
	name: "Bob",
	identities: [
		slack.identity({ workspace: "acme", userId: "U_BOB" }),
		github.identity({ installationId: "42", userId: 2 }),
		linear.identity({ organizationId: "acme", userId: "lin_bob" }),
	],
});
export const support = defineGroup({ id: "support", members: [alice] });
export const engineeringTriage = defineGroup({
	id: "engineering-triage",
	resolve: ({ now }) => [weeklyRotation([bob, alice], { start: "2026-01-05", now })],
});
// An alternative to deploying changes to the rotation: manage @triage in Slack.
export const slackEngineeringTriage = slack.userGroup({
	id: "slack-engineering-triage",
	handle: "triage",
});
export const identity = [alice, bob, support, engineeringTriage, slackEngineeringTriage];
