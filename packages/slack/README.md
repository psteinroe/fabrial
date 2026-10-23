# @fabrial/slack

`slack({ botToken, signingSecret, workspace, teamId? })` provides Slack chat events, a WebClient, installation-scoped identities and live user-group membership.

Inbound events and interactive actions must match the native Slack team ID. Supply `teamId`, or it is resolved from the bot token with `auth.test` at start (fail closed if unavailable). `workspace` remains the configured identity namespace, not necessarily the native team ID. Signature verification remains in the Slack SDK.

A minimal SDK subclass disables Slack's independent pre-dispatch `markEventDelivered`/retry cache, including old markers. No delivery is marked before durable host ingress; core is the sole durable dedupe authority. This deliberately favors retryable ingress over SDK receipt caching, so a failed message/action ingress can be processed on retry.

## Instance wiring

```ts check
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";

const f = createFabrial({
	plugins: [
		slack({
			botToken: process.env.SLACK_BOT_TOKEN!,
			signingSecret: process.env.SLACK_SIGNING_SECRET!,
			workspace: "acme",
		}),
	],
});
// Whoever is in @triage when an approver clicks; rotate in Slack, no deploy.
const triage = slack.userGroup({ id: "triage", handle: "triage" });
export const announce = f.defineWorkflow({
	name: "announce",
	triggers: [slack.mentioned({ channel: "C_OPS" })],
	async run(message, ctx) {
		const decision = await ctx.waitForApproval("approve", {
			title: "Announce this?",
			details: message.text,
			approvers: triage,
		});
		if (decision.approved)
			await ctx.step("post", async () => {
				await ctx.clients.slack.chat.postMessage({ channel: "C_ALL", text: message.text });
			});
	},
});
```

Helpers: `slack.mentioned({ channel?, thread?, observe? })`, `slack.message({ channel, … })`, `slack.newThread({ channel, … })`, `slack.dm()`, `slack.channel(id)` (a surface for `replyTo`, `home`, and approval `destination`), `slack.identity({ workspace, userId })`, and `slack.userGroup({ id, handle })`. `userGroup` resolves live membership through `ctx.clients.slack` and never caches it. `ctx.clients.slack` is a `WebClient`; tests spy on `app.clients().slack`. The factory is side-effect free; credentials are checked at `app.start()`.

One Slack workspace per app in this preview. Approval cards link their source thread with `chat.getPermalink`, and Reject opens a modal for an optional reason.

## Toolkit

Agents that list `"slack"` in `toolkits` get `slack_post({ channel, text })`: a message in a channel (by id) with the plugin's client. Final replies post to the conversation's home on their own; the tool is for writing somewhere else.
