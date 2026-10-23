# Acme: a Fabrial reference app

A private workspace example, not a hosted service. It shows every part of the model on one app: Slack, GitHub, and Linear plugins, two agents with triggers, a router workflow, a SQL repair workflow with an approval, PR follows, a subagent, and observers.

## Layout

- `src/fabrial.ts`: the instance (`createFabrial({ database, models, plugins, identity })`). It reads `process.env` at definition and imports only plugins and `src/users.ts`, never workflows, so every definition can import `f`. Factories are side-effect free: importing the app with unset environment is safe, `app.start()` checks it. Set the environment **before importing** the app.
- `src/users.ts`: Alice and Bob with their Slack, GitHub, and Linear identities. `src/groups.ts`: `engineeringTriage`, a weekly rotation of Bob and Alice from 2026-01-05 (`f.defineGroup`), and `slackEngineeringTriage`, the live members of Slack's `@triage`.
- `src/database.ts`: an app-local plugin with a least-privilege business database client (`ctx.clients.database`).
- `src/app.ts`: `f.app({ workflows, agents })`, with the two agents that have triggers.

## Use cases

1. **Support in #help** (`src/agents/support.ts`). `acme-support` has a trigger on every message in `C_HELP` and `session: "channel"`: one conversation for the whole channel. Each answer posts in the asker's thread, and the conversation follows every thread it answered. Its `read_only_query` tool runs SQL in a read-only transaction and records findings in session state (`Findings`), which the agent sees in its prompt.
2. **Repairs with approval** (`src/workflows/run-sql.ts`). `run-sql` is a tool of the support agent. It previews the statement with `EXPLAIN`, then waits for `engineeringTriage` to approve the exact proposal (organisation, SQL, reason, preview) in a DM card. Rejection, expiry, and cancellation return a declined result, with the rejection reason when the approver gave one, and the agent reports it. Anyone in the thread may ask; the approval is the permission check.
3. **Mentions elsewhere** (`src/workflows/general-assistant.ts`). The router classifies the message with `ctx.evaluate` (`typesafe/jev-latest`). Bug reports (and everything in `#bugs`) are investigated inline: `ctx.agent` calls the support agent and the workflow posts the analysis. Everything else goes to `acme-support` with `ctx.send` and `session: "thread"`, overriding the agent's channel session, so each thread outside #help is a conversation of its own and later replies go straight to the agent. A missing or failing classifier falls back to support.
4. **Development in #dev** (`src/agents/developer.ts`). `acme-developer` gets every `C_DEV` message, one conversation per thread, with the `github` toolkit. `open_pull_request` opens a PR and `ctx.follow`s it: reviews and comments arrive as notifications (changes requested is `urgent`), the agent answers on GitHub with `github_reply_to_review` and in the Slack thread with its final answer, and it unfollows after the merge. Its subagent `acme-researcher` (tool `acme_researcher`) has only read-only code search, in the foreground or in the background.
5. **GitHub and Linear lifecycle** (`src/workflows/pr-review.ts`, `changelog.ts`, `linear-triage.ts`). `pr-review` owns `pullRequestOpened` on acme/app and comments on the PR; `changelog` observes the same event (`observe: true`) and records it without a thread. `linear-triage` comments on new ENG issues.

## Run / embed

Configure these environment variables:

| Variable                 | Purpose                                                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`           | Fabrial's Postgres database: workflows, agent sessions, and chat state.                                          |
| `ANTHROPIC_API_KEY`      | Model credentials for the support agent.                                                                         |
| `TYPESAFE_API_KEY`       | Model credentials for the routing classifier.                                                                    |
| `APP_DATABASE_URL`       | Separate least-privilege business database; the database plugin creates its pool at start and closes it at stop. |
| `SLACK_BOT_TOKEN`        | Slack bot token.                                                                                                 |
| `SLACK_SIGNING_SECRET`   | Slack webhook signing secret.                                                                                    |
| `SLACK_WORKSPACE`        | Stable identity namespace, shared with users.                                                                    |
| `SLACK_TEAM_ID`          | Native team ID; optional, resolved through `auth.test` if omitted.                                               |
| `GITHUB_TOKEN`           | GitHub PAT.                                                                                                      |
| `GITHUB_WEBHOOK_SECRET`  | GitHub webhook signing secret.                                                                                   |
| `GITHUB_OWNER`           | Repository owner login (defaults to authenticated PAT user if omitted).                                          |
| `GITHUB_INSTALLATION_ID` | Numeric installation namespace, matching user identities and signed payloads when present.                       |
| `GITHUB_BOT_USER_ID`     | Numeric GitHub bot user ID.                                                                                      |
| `LINEAR_API_KEY`         | Linear API key.                                                                                                  |
| `LINEAR_WEBHOOK_SECRET`  | Linear webhook signing secret.                                                                                   |
| `LINEAR_ORGANIZATION_ID` | Linear organization ID, shared with users and signed payloads.                                                   |

Load `schema.sql` into `APP_DATABASE_URL`, replace the sample user, channel, and team ids with your own, and serve `app.fetch`:

- `/slack/events`: messages, mentions, approval clicks, and the rejection modal.
- `/github/events`: PR and issue comments; `/github/webhook`: PR, review, comment, and check lifecycle events.
- `/linear/events`: comments; `/linear/webhook`: issue lifecycle events.

**Security:** these are illustrative business queries, not a SQL sandbox. Use
least-privilege database roles, enforce tenant authorization/RLS, and restrict
resource usage before deploying. Single-statement prepared execution prevents
multi-statement transaction escapes; Postgres enforces read-only queries. The
organisation setting alone does not enforce tenant access. Fabrial's database credentials
should not be exposed to application SQL in production (`APP_DATABASE_URL`).

## Tests

```sh
pnpm vitest run --project examples
pnpm --filter @fabrial/example-acme typecheck
```

Docker is required. `tests/global-setup.ts` starts PostgreSQL 17 with testcontainers and sets both database URLs and provider test values before the app is imported. Integration tests build the app with `createIntegrationApp(f, { workflows, agents, models: scriptedModels() })`: the real durable runtime, agent sessions, Chat SDK with the Slack adapter and Postgres chat state, and scripted models and classifier. Signed Slack messages and clicks and signed GitHub and Linear webhooks go through `app.fetch`; outbound Slack, GitHub, and Linear API calls are mocked. Each scenario resets durable, chat, and business state; the restart scenarios stop the app and start a new one on the same database.

Covered: approval, duplicate click, rejection with a reason, requester cancellation, a non-approver's click, a restart during an approval, the #help channel conversation across threads and a restart, independent per-thread conversations from the router, inline bug investigation, the PR owner and observer, Linear triage, the #dev follow lifecycle with a restart and a redelivered webhook, foreground and background subagents, and read-only and multiple-statement SQL rejection. Tests pin the on-call week so Bob is the approver.

`tests/unit/readme.test.ts` checks that the READMEs' `ts check` snippets in `tests/docs/generated` are current (regenerate with `node examples/acme/tests/docs/snippets.ts`); `pnpm typecheck` compiles them against `tests/docs/prelude.ts`.

A restart test proves one write in its scenario, not exactly-once SQL across a crash during the write: a step cannot commit an external write and its receipt atomically. A production repair needs business-level idempotency or reconciliation.
