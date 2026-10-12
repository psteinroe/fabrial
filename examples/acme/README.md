# Acme — Fabrial v1 reference app

A private workspace example, not a hosted service. The app has one Slack entry
workflow, a support agent, a SQL repair workflow with triage approval, bug handoff,
a GitHub PR review owner/changelog observer, and Linear issue triage.

`src/fabrial.ts` creates the instance with `createFabrial({ database, models,
plugins, identity })`, reading values from `process.env` at definition. It imports
only plugins and `src/users.ts`, never workflows or groups. Agents, tools, and state
are defined with `f.defineAgent`, `f.defineTool`, and `f.defineState`. `src/groups.ts` uses `f.defineGroup` for typed clients and is imported
by workflows; groups are referenced directly, not registered in `identity`.
Factories are side-effect free: importing the app with unset env is safe, but
`app.start()` requires valid configuration. Set env **before importing** the app;
changing env after import does not reconfigure plugins.

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

`src/app.ts` is the whole wiring:

```ts
export const app = f.app({ workflows });
await app.start(); // migrates Postgres, validates credentials, starts workers
// Mount app.fetch in your Node HTTP server; call app.stop() on shutdown.
```

Load `schema.sql` in `APP_DATABASE_URL`. Replace the sample external user IDs,
channel IDs, team keys, and model references in source with your own.
`supportAgent` uses `"anthropic/claude-sonnet-4-5"`; routing calls
`ctx.evaluate` with the classifier `"typesafe/jev-latest"`. A missing or erroring
classifier explicitly falls back to support; `#bugs` always hands off. Define
agents before `app.start()`.

Endpoints:

- `/slack/events`: mentions and signed interactive approval actions.
- `/github/events`: comments; `/github/webhook`: PR/issue lifecycle events.
- `/linear/events`: comments; `/linear/webhook`: issue lifecycle events.

The SQL tool's schema includes organisation, exact SQL and reason. Its preview is
an `EXPLAIN` in a read-only transaction. Approval binds to that proposal, and the
approval is the only permission check: anyone in the thread can ask for it. Bob and Alice rotate weekly from 2026-01-05. Alternatively,
replace `engineeringTriage` with `slackEngineeringTriage` for live `@triage`
membership. Rejection/cancellation return a declined tool result without executing
SQL. The support agent records read-only query findings in rendered thread state.

**Security:** these are illustrative business queries, not a SQL sandbox. Use
least-privilege database roles, enforce tenant authorization/RLS, and restrict
resource usage before deploying. Single-statement prepared execution prevents
multi-statement transaction escapes; Postgres enforces read-only queries. The
organisation setting alone does not enforce tenant access. Fabrial's database credentials
should not be exposed to application SQL in production (`APP_DATABASE_URL`).

## Tests

```sh
pnpm vitest run examples/acme
pnpm --filter @fabrial/example-acme typecheck
pnpm oxlint --type-aware --deny-warnings examples/acme
pnpm oxfmt examples/acme
```

Docker is required. Integration tests build the app with
`createIntegrationApp(f, { workflows, models: scriptedModels() })` from
`fabrial/testing`: **real PostgreSQL 17 (testcontainers), the real durable runtime
and agent sessions, and scripted models**, including a scripted classifier. They use the **real Chat SDK, Slack plugin/adapter and
Postgres Chat state**, not `FakeChat`. Signed Slack mentions/actions and signed
GitHub/Linear lifecycle webhooks go through `app.fetch`. Outbound Slack Web API,
Octokit and Linear API responses are mocked; Linear's startup profile lookup is
seeded. `@chat-adapter/tests` supplies the inspectable logger. No live tokens or
model network calls are needed. Global setup starts Postgres and sets both database URLs and provider test values
before importing the app. Each scenario resets durable/Chat state and business
tables on those same databases; the
restart test stops the app and starts a new one on the same database.

Coverage: approval + duplicate click, rejection, requester cancellation,
non-approver notice then approval, restart during approval, PR owner + observer,
bug handoff, Linear triage, and database-enforced read-only/multiple-statement
rejection. Assertions poll durable state, never sleep arbitrary durations. Tests
pin the on-call week so Bob is the approver regardless of the calendar.

## Explicit limits

Workflow-tool children retain the originating thread without inheriting
trigger ownership; no example-local runtime workaround is needed. The passing
unit regression checks the child metadata contract, and the real cancellation
scenario checks the emitted child metadata and requester card in the origin thread.
Settled chat-message routing state retains ingress tombstones with
`interactionId: null`, rather than disappearing. Direct lifecycle ingress creates
no routing tombstone. Approval tests click on card delivery without waiting for a
native subscription: durable event cursors retain pre-registration decisions.

- **Conductor boundaries still apply:** strict mutexes are soft across
  workers. See the shim table in `packages/fabrial/README.md`. The restart test waits for suspended
  approval work before stopping, and proves one write in that scenario, **not exactly-once SQL across a crash during a database effect**.
  Conductor step memoization cannot atomically commit an external DB write and
  its runtime receipt. A production repair needs business-level idempotency or
  reconciliation. No extra execution/approval tables are introduced here.

## Support channel subscription

`src/subscriptions.ts` declares `supportChannel`: every message in `#help` (`C_HELP`) goes to `acme-support` in one conversation for the whole channel (`session: "channel"`). The agent answers each asker with its `reply` tool, in the asker's thread. The integration test covers two threads sharing that conversation, targeted replies, no automatic final post, and a restart after ingress.

## Following a pull request from Slack

`src/agents/developer.ts` defines `acme-developer`. `devThreads` in `src/subscriptions.ts` sends every `#dev` (`C_DEV`) thread to it, one conversation per thread. Its `open_pull_request` tool opens a PR with `ctx.clients.github` and calls `ctx.subscribe` with the `review` preset, `until: "merged-or-closed"`, and the `acme-pr-review` policy, which makes changes-requested reviews `urgent`. The e2e test runs the whole flow: Slack message, PR opened, signed review webhook (delivered twice), the agent's `reply` in the original Slack thread, and the merge webhook removing the subscription.
