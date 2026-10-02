# Acme — Fabrial v1 reference app

A private workspace example, not a hosted service. The app has one Slack entry
workflow, a support agent, a SQL repair workflow with triage approval, bug handoff,
a GitHub PR review owner/changelog observer, and Linear issue triage.

## Run / embed

Supply Postgres pools, a Chat SDK state adapter, Pi AI `Models`, and provider
credentials to `createApp` in `src/app.ts`:

```ts
const app = createApp({
	sql, // caller-owned postgres.js pool for runtime + Pi
	databaseSql, // optional separate application database pool
	state, // e.g. createPostgresState({ url: DATABASE_URL })
	models, // Pi AI Models with authenticated providers
	slack: { workspace: "acme", botToken, signingSecret },
	github: { installationId: "acme", token, webhookSecret, botUserId: 99 },
	linear: { organizationId: "acme", apiKey, webhookSecret: linearSecret },
});
await app.start();
// Mount app.fetch in your Node HTTP server; call app.stop() on shutdown.
// Close caller-owned pools after stopping the app.
```

Load `schema.sql` in the application database. Replace the sample external user
IDs, channel IDs, installation namespaces, and model references in source with
your own. `supportAgent` uses `anthropic/claude-sonnet-4-5`; routing uses a Jev
classifier registered as `jev/route`. A missing/erroring classifier explicitly
falls back to support; `#bugs` always hands off. Models are injectable, not fetched
or initialized by the app. Define agents before `app.start()`.

Endpoints:

- `/slack/events`: mentions and signed interactive approval actions.
- `/github/events`: comments; `/github/webhook`: PR/issue lifecycle events.
- `/linear/events`: comments; `/linear/webhook`: issue lifecycle events.

The SQL tool's schema includes organisation, exact SQL and reason. Its preview is
an `EXPLAIN` in a read-only transaction. Approval binds to that proposal; only
support can invoke it. Bob and Alice rotate weekly from 2026-01-05. Alternatively,
replace `engineeringTriage` with `slackEngineeringTriage` for live `@triage`
membership. Rejection/cancellation return a declined tool result without executing
SQL. The support agent records read-only query findings in rendered thread state.

**Security:** these are illustrative business queries, not a SQL sandbox. Use
least-privilege database roles, enforce tenant authorization/RLS, and restrict
resource usage before deploying. Single-statement prepared execution prevents
multi-statement transaction escapes; Postgres enforces read-only queries. The
organisation setting alone does not enforce tenant access. Runtime/Pi credentials
should not be exposed to application SQL in production (`databaseSql`).

## Tests

```sh
pnpm vitest run examples/acme
pnpm --filter @fabrial/example-acme typecheck
pnpm oxlint --type-aware --deny-warnings examples/acme
pnpm oxfmt examples/acme
```

Docker is required. Integration tests use **real PostgreSQL 17 (testcontainers),
real Conductor, real Pi Durable and Pi AI's scripted faux provider**, including a
scripted classifier. They use the **real Chat SDK, Slack plugin/adapter and
Postgres Chat state**, not `FakeChat`. Signed Slack mentions/actions and signed
GitHub/Linear lifecycle webhooks go through `app.fetch`. Outbound Slack Web API,
Octokit and Linear API responses are mocked; Linear's startup profile lookup is
seeded. `@chat-adapter/tests` supplies the inspectable logger. No live tokens or
model network calls are needed. Separate databases isolate each scenario; the
restart test uses new runtime, Pi and Chat instances on the same database.

Coverage: approval + duplicate click, rejection, requester cancellation,
non-approver notice then approval, restart during approval, PR owner + observer,
bug handoff, Linear triage, and database-enforced read-only/multiple-statement
rejection. Assertions poll durable state, never sleep arbitrary durations. Tests
pin the on-call week so Bob is the approver regardless of the calendar.

## Package issues / explicit limits

1. **Lost tool response binding:** `packages/pi/src/context.ts`, `fields().start`
   supplies only `{ ownsThread: false }`. Inherited `triggerEvent` and
   `ownerWorkflow` then reach `packages/fabrial/src/core.ts`'s observer check,
   which treats the tool child as a trigger observer and strips `replyTo`.
   Without a workaround, `runSql` executes/approves in the DM but cannot post the
   requester's Cancel card in the origin thread. Expected: child calls retain
   response context, without becoming thread owners. The minimal **expected-fail**
   regression is `tests/unit/tool-response-regression.test.ts`.
   `src/tool-response-workaround.ts` clears only these trigger markers for
   `run-sql` starts using the public runtime port; no packages are patched.
2. **Ambient Timer visibility:** the vendored Conductor source references `Timer`;
   `@fabrial/conductor`'s package-local ambient declaration is not transitively
   included by an app typecheck. `src/conductor-node.d.ts` supplies the same Node
   timer alias locally. Without it, app typecheck reports TS2304 at
   `vendor/postgres-conductor/packages/pgconductor-js/src/lib/batching-async-queue.ts:31`.
3. **Existing Conductor boundaries still apply:** native pre-registration events
   can be lost; strict mutexes are soft across workers. Tests wait for a real
   approval subscription before clicking, including after an unauthorized click. This is not
   a production guarantee for arbitrarily fast clicks or multi-replica ingress.
   See `packages/conductor/README.md`. The restart test proves one write in that
   scenario, **not exactly-once SQL across a crash during a database effect**.
   Conductor step memoization cannot atomically commit an external DB write and
   its runtime receipt. A production repair needs business-level idempotency or
   reconciliation. No extra execution/approval tables are introduced here.
