# @fabrial/github

```ts
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
// Define workflows with f.defineWorkflow; wire runtime/chat with f.app({ runtime, chat, workflows }).
```

Supports PAT credentials or a single GitHub App installation (`appId`, `privateKey`, numeric `installationId`). Credentials and adapter options are passed through to Chat SDK. Set `botUserId` to prevent self-reply loops. Verification requires an explicit `webhookSecret` or `webhookVerifier`; it does not silently use environment credentials.

Configure two GitHub webhook subscriptions:

- `POST /github/events`: issue comments (both issues and PR conversations), plus PR review comments; standard `mentioned`, `message`, `dm` events supplied by `chatEvents()` from `fabrial`.
- `POST /github/webhook`: `pull_request` (opened, closed, reopened, labeled, unlabeled, synchronize), `issues` opened, `pull_request_review` (submitted), `issue_comment` and `pull_request_review_comment` (created, on PRs), `check_suite` and `check_run` (completed). Unsupported actions are acknowledged without emitting. Invalid signatures/payloads fail closed, and `x-github-delivery` supplies durable dedupe.

Use with `chat({ state })`. `ctx.clients.github` is the adapter's authenticated Octokit. Lifecycle events provide `repo`, `id`, `number`, `title`, `body`, `url`, `authorId`, `authorLogin`, `threadId`, installation-scoped origin/actor metadata, and a reply destination on the conversation. PR threads are `github:owner/repo:number`; issue threads are `github:owner/repo:issue:number`; reviews retain their own SDK review-thread codec. History is bounded to 50 messages; status uses edited messages.

Helpers are named exports and properties on `github`:

- `mentioned({ repo?, thread?, observe? })`: global < repository < thread specificity.
- `pullRequestOpened({ repo?, observe? })`, `issueOpened({ repo?, observe? })`: global < repository specificity.
- `identity({ userId, installationId? })` or `identity({ login, installationId? })`.

Inbound identities use stable numeric GitHub user IDs, including lifecycle authors. Login identities support profile lookup, but **are not automatically equated to numeric identities by core**. Use `userId` in `defineUser` for inbound principal/approval matching. Login alias canonicalization needs an identity-directory contract extension.

Lifecycle routes use core's `EmitOptions.replyTo` for the PR/issue thread and `requestedBy` for its author (not the webhook sender). Multiple plugin aliases and request-scoped multi-tenant clients are not supported by the current Chat provider-id contract.

Both ingress paths reject payloads outside the configured App installation. PATs restrict repository owners to `owner`, or default to the token's authenticated user (resolved via GitHub API and cached; failures remain retryable). An explicit `owner` additionally restricts App repositories. The owner in `repository.full_name` and, when present, `repository.owner.login` must agree with that scope. A numeric PAT `installationId` also rejects conflicting payload installation IDs when present; string installation IDs remain identity namespaces.

## Pull request subscriptions

`github.pullRequest({ repo, number })` is a resource for `ctx.subscribe`. The webhook events behind it are `pullRequestClosed` (`merged`), `pullRequestReopened`, `pullRequestLabeled`, `pullRequestSynchronized`, `pullRequestReviewed` (`state`, `association`), `pullRequestCommented` (`association`, `inline`, `path`), `checkSuiteCompleted`, and `checkRunCompleted` (`pullRequests`, `conclusion`). They render as notifications of the following kinds:

| Kind       | Event                        | Default priority           |
| ---------- | ---------------------------- | -------------------------- |
| `commit`   | `synchronize`                | normal                     |
| `review`   | review submitted             | normal                     |
| `comment`  | PR comment or review comment | normal                     |
| `merged`   | closed with `merged`         | normal                     |
| `closed`   | closed without merge         | normal                     |
| `reopened` | reopened                     | normal                     |
| `label`    | labeled / unlabeled          | low                        |
| `check`    | check suite / run completed  | low if passed, else normal |

- **Presets.** `review` delivers commits, reviews, comments, merge, close, and reopen. `working` adds labels and checks.
- **End condition.** `merged-or-closed`.
- **Filter.** Comments and reviews count only from `OWNER`, `MEMBER`, or `COLLABORATOR` author associations. Bot accounts count only when they are this app's own bot (`botUserId`).
- **Toolkit.** Agents that list `"github"` in `toolkits` get `github_subscribe_pull_request({ repo, number, preset? })`, which follows the PR until it is merged or closed, and `github_unsubscribe_pull_request({ repo, number })`. Both act on the agent's own conversation.
- **Exactly once.** PR comments reach subscriptions only through `/github/webhook`. Chat conversations on `/github/events` don't feed subscriptions, so a comment that arrives on both endpoints is still delivered to a subscription once.
