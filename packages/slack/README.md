# @fabrial/slack

`slack({ botToken, signingSecret, workspace, teamId? })` provides Slack chat events, a WebClient, installation-scoped identities and live user-group membership.

Inbound events and interactive actions must match the native Slack team ID. Supply `teamId`, or it is resolved from the bot token with `auth.test` at start (fail closed if unavailable). `workspace` remains the configured identity namespace, not necessarily the native team ID. Signature verification remains in the Slack SDK.

A minimal SDK subclass disables Slack's independent pre-dispatch `markEventDelivered`/retry cache, including old markers. No delivery is marked before durable host ingress; core is the sole durable dedupe authority. This deliberately favors retryable ingress over SDK receipt caching, so a failed message/action ingress can be processed on retry.
