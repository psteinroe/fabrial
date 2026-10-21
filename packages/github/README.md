# @fabrial/github

```ts check
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
```

Supports PAT credentials or a single GitHub App installation (`appId`, `privateKey`, numeric `installationId`). Credentials and adapter options are passed through to Chat SDK. The app's own bot is `botUserId` when set, else the account Chat SDK detects from the credentials; set it explicitly where detection can't run (e.g. installation-token providers). Verification requires an explicit `webhookSecret` or `webhookVerifier`; it does not silently use environment credentials.

Configure two GitHub webhook subscriptions:

- `POST /github/events`: issue comments (both issues and PR conversations), plus PR review comments; standard `mentioned`, `message`, `dm` events supplied by `chatEvents()` from `fabrial/plugin`.
- `POST /github/webhook`: `pull_request` (opened, closed, reopened, labeled, unlabeled, synchronize), `issues` opened, `pull_request_review` (submitted), `issue_comment` and `pull_request_review_comment` (created, on PRs), `check_suite` and `check_run` (completed). Unsupported actions are acknowledged without emitting. Invalid signatures/payloads fail closed, and `x-github-delivery` supplies durable dedupe.

`ctx.clients.github` is the adapter's authenticated Octokit. Lifecycle events provide `repo`, `id`, `number`, `title`, `body`, `url`, `authorId`, `authorLogin`, `threadId`, installation-scoped origin/actor metadata, and a reply destination on the conversation. PR threads are `github:owner/repo:number`; issue threads are `github:owner/repo:issue:number`; reviews retain their own SDK review-thread codec. History is bounded to 50 messages; status uses edited messages.

Helpers are named exports and properties on `github`:

- `mentioned({ repo?, thread?, observe? })`: global < repository < thread specificity.
- `pullRequestOpened({ repo?, observe? })`, `issueOpened({ repo?, observe? })`: global < repository specificity.
- `pullRequestClosed({ repo?, number?, merged? })`, `pullRequestReopened({ repo?, number? })`, `pullRequestLabeled({ repo?, number?, label? })`, `pullRequestSynchronized({ repo?, number? })`, `pullRequestReviewed({ repo?, number?, state? })`, `pullRequestCommented({ repo?, number? })`: pull request lifecycle triggers; each set filter field narrows the trigger and adds specificity. All take `observe?`.
- `checkSuiteCompleted({ repo?, conclusion? })`, `checkRunCompleted({ repo?, conclusion? })`: check lifecycle triggers, with the same specificity rules and `observe?`.
- `identity({ userId, installationId? })`: `userId` is the numeric account id (`gh api users/<login> --jq .id`); anything else throws.

Identities are numeric GitHub account ids, the same ids webhooks and comments carry, so a `defineUser` identity matches inbound principals and approvals. Logins are not identities: they can be renamed and never match an inbound id.

Lifecycle routes use core's `EmitOptions.replyTo` for the PR/issue thread and `requestedBy` for its author (not the webhook sender). One installation per app in this preview: the plugin id is the Chat SDK adapter's name, so a second GitHub plugin can't be added, and clients are not request-scoped.

Both ingress paths reject payloads outside the configured App installation. PATs restrict repository owners to `owner`, or default to the token's authenticated user (resolved via GitHub API and cached; failures remain retryable). An explicit `owner` additionally restricts App repositories. The owner in `repository.full_name` and, when present, `repository.owner.login` must agree with that scope. A numeric PAT `installationId` also rejects conflicting payload installation IDs when present; string installation IDs remain identity namespaces.

## Author trust

Comments and reviews reach agents only from `OWNER`, `MEMBER`, or `COLLABORATOR` author associations, or from this app's own bot; other bots and outsiders are dropped. The one policy applies on every path:

- `/github/events`: the adapter drops untrusted comments and review comments before Chat SDK dispatches them, so they never become `mentioned`/`message` events;
- thread history: untrusted comments are left out of fetched history (so a history window may hold fewer than 50 messages);
- `/github/webhook`: untrusted `pullRequestCommented` and `pullRequestReviewed` are not emitted, so neither workflows nor follows see them.

The other lifecycle events (opened, closed, reopened, labeled, synchronize, checks) are deliberately public facts: anyone who can open an issue or pull request triggers `issueOpened`/`pullRequestOpened`, with their title and body. Their payloads carry the author or actor (`authorId`, `actorId`, `actorType`) and `requestedBy` the author's identity; a workflow that acts on an outsider's request should check them.

Chat SDK's GitHub adapter has no author filter, so the policy lives here; an upstream `shouldProcessMessage`/author-filter hook on the adapter would let it shrink to configuration.

## Following pull requests

`github.pullRequest("acme/app#42")` (or `{ repo, number }`) is a resource for `ctx.follow`; the follow record lives in the PR thread's Chat SDK state. The webhook events behind it are `pullRequestClosed` (`merged`), `pullRequestReopened`, `pullRequestLabeled`, `pullRequestSynchronized`, `pullRequestReviewed` (`state`, `association`), `pullRequestCommented` (`association`, `inline`, `path`), `checkSuiteCompleted`, and `checkRunCompleted` (`pullRequests`, `conclusion`). They render as notifications of the following kinds:

| Kind       | Event                        | Default priority                                           | Attributes                |
| ---------- | ---------------------------- | ---------------------------------------------------------- | ------------------------- |
| `commit`   | `synchronize`                | normal                                                     | `pr`, `sha`               |
| `review`   | review submitted             | urgent if changes requested, low from own bot, else normal | `pr`, `review`, `state`   |
| `comment`  | PR comment or review comment | low from own bot, else normal                              | `pr`, `comment`, `inline` |
| `merged`   | closed with `merged`         | normal                                                     | `pr`                      |
| `closed`   | closed without merge         | normal                                                     | `pr`                      |
| `reopened` | reopened                     | normal                                                     | `pr`                      |
| `label`    | labeled / unlabeled          | low                                                        | `pr`, `label`             |
| `check`    | check suite / run completed  | low if passed, else normal                                 | `name`, `conclusion`      |

- **Defaults.** A follow receives commits, reviews, comments, merge, close, and reopen. `events: "all"` adds labels and checks. `priority: { review: "low" }` overrides the defaults per kind.
- **Lifetime.** A follow lasts until `ctx.unfollow`; merge and close are delivered, and the agent decides.
- **Filter.** Comments and reviews follow the [author trust](#author-trust) policy. This app's own comments and reviews are `low`, so they don't wake the agent.
- **Toolkit.** Agents that list `"github"` in `toolkits` get, all acting for the agent's own conversation or with the app's client:
  - `github_follow_pull_request({ pr, events? })` and `github_unfollow_pull_request({ pr })`;
  - `github_comment({ pr, body })`: a comment on the PR's conversation;
  - `github_reply_to_review({ pr, review | comment, body })`: with `comment`, a reply in an inline review comment's thread; with `review`, a PR comment that links the review (GitHub has no reply to a review itself).
  - `pr` is `owner/name#number`, the form notifications render.
- **Exactly once.** PR comments reach followers only through `/github/webhook`. Chat conversations on `/github/events` don't read resource follows, so a comment that arrives on both endpoints is still delivered to a follower once.
