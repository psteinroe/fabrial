import { weeklyRotation } from "fabrial";
import { slack } from "@fabrial/slack";
import { f } from "./fabrial.ts";
import { alice, bob } from "./users.ts";

export const support = f.defineGroup({ id: "support", members: [alice] });
export const engineeringTriage = f.defineGroup({
	id: "engineering-triage",
	resolve: ({ now }) => [weeklyRotation([bob, alice], { start: "2026-01-05", now })],
});
// An alternative to deploying changes to the rotation: manage @triage in Slack.
export const slackEngineeringTriage = slack.userGroup({
	id: "slack-engineering-triage",
	handle: "triage",
});
