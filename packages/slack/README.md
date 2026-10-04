# @fabrial/slack

`slack({ botToken, signingSecret, workspace, teamId? })` provides Slack chat events, a WebClient, installation-scoped identities and live user-group membership.

Inbound events and interactive actions must match the native Slack team ID. Supply `teamId`, or it is resolved from the bot token with `auth.test` at start (fail closed if unavailable). `workspace` remains the configured identity namespace, not necessarily the native team ID. Signature verification remains in the Slack SDK.

A minimal SDK subclass disables Slack's independent pre-dispatch `markEventDelivered`/retry cache, including old markers. No delivery is marked before durable host ingress; core is the sole durable dedupe authority. This deliberately favors retryable ingress over SDK receipt caching, so a failed message/action ingress can be processed on retry.

## Instance wiring

```ts
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";

const triage = slack.userGroup({ id: "triage", handle: "triage" });
const f = createFabrial({
	plugins: [slack({ botToken, signingSecret, workspace })],
	identity: [triage],
});
const workflow = f.defineWorkflow({
	name: "slack-auth",
	async run(_input, ctx) {
		await ctx.step("auth", () => ctx.clients.slack.auth.test());
	},
});
const app = f.app({ runtime, chat, workflows: [workflow] });
```

`userGroup` resolves through a structurally typed group context containing a Slack WebClient. It works with any catalog that includes Slack and fails explicitly if the live client is missing. Test overrides use `f.app({ …, plugins: { slack: slack(testOptions) } })`.
