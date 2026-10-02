# @fabrial/github

```ts
import { github } from "@fabrial/github";

const plugin = github({
	token: process.env.GITHUB_TOKEN!,
	webhookSecret: process.env.GITHUB_WEBHOOK_SECRET!,
	installationId: "acme", // optional for PATs; defaults to "default"
	userName: "fabrial[bot]",
	botUserId: 12345,
});
```

Supports PAT credentials or a single GitHub App installation (`appId`, `privateKey`, numeric `installationId`). Credentials and adapter options are passed through to Chat SDK. Set `botUserId` to prevent self-reply loops. Verification requires an explicit `webhookSecret` or `webhookVerifier`; it does not silently use environment credentials.

Configure two GitHub webhook subscriptions:

- `POST /github/events`: issue comments (both issues and PR conversations), plus PR review comments; standard `mentioned`, `message`, `dm` events supplied by `@fabrial/chat`.
- `POST /github/webhook`: `pull_request` and `issues` opened events. Unsupported actions are acknowledged without emitting. Invalid signatures/payloads fail closed, and `x-github-delivery` supplies durable dedupe.

Use with `chat({ state })`. `ctx.clients.github` is the adapter's authenticated Octokit. Lifecycle events provide `repo`, `id`, `number`, `title`, `body`, `url`, `authorId`, `authorLogin`, `threadId`, installation-scoped origin/actor metadata, and a reply destination on the conversation. PR threads are `github:owner/repo:number`; issue threads are `github:owner/repo:issue:number`; reviews retain their own SDK review-thread codec. History is bounded to 50 messages; status uses edited messages.

Helpers are named exports and properties on `github`:

- `mentioned({ repo?, thread?, observe? })`: global < repository < thread specificity.
- `pullRequestOpened({ repo?, observe? })`, `issueOpened({ repo?, observe? })`: global < repository specificity.
- `identity({ userId, installationId? })` or `identity({ login, installationId? })`.

Inbound identities use stable numeric GitHub user IDs, including lifecycle authors. Login identities support profile lookup, but **are not automatically equated to numeric identities by core**. Use `userId` in `defineUser` for inbound principal/approval matching. Login alias canonicalization needs an identity-directory contract extension.

Lifecycle routes use core's `EmitOptions.replyTo` for the PR/issue thread and `requestedBy` for its author (not the webhook sender). Multiple plugin aliases and request-scoped multi-tenant clients are not supported by the current Chat provider-id contract.
