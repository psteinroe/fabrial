# Acme — Fabrial v1 reference app

A private workspace example, not a hosted service. The app has one Slack entry
workflow, a support agent, a SQL repair workflow with triage approval, bug handoff,
a GitHub PR review owner/changelog observer, and Linear issue triage.

`src/fabrial.ts` owns the typed plugin/identity catalog and exports `withPi(f)` helpers. Definitions import that module; it never imports workflows. Its catalog-only placeholder credentials/database are replaced by `createApp(deps)` using `f.app({ ..., plugins: { slack, github, linear, database } })`. Tests inject live/fake values through those type-checked overrides; there is no `declare module` augmentation.

## Run / embed

Supply Postgres pools, a Chat SDK state adapter, Pi AI `Models`, and provider
credentials to `createApp` in `src/app.ts`:

```ts
const app = createApp({
	sql, // caller-owned postgres.js pool for runtime + Pi
	databaseSql, // optional separate application database pool
	state, // e.g. createPostgresState({ url: DATABASE_URL })
	models, // Pi AI Models with authenticated providers
	slack: { workspace: "acme", teamId: "T_ACME", botToken, signingSecret },
	github: { owner: "acme", installationId: 42, token, webhookSecret, botUserId: 99 },
	linear: { organizationId: "acme", apiKey, webhookSecret: linearSecret },
});
await app.start();
// Mount app.fetch in your Node HTTP server; call app.stop() on shutdown.
// Close caller-owned pools after stopping the app.
```

Load `schema.sql` in the application database. Replace the sample external user
IDs, channel IDs, installation namespaces, and model references in source with
your own. Slack's `workspace` is the identity namespace; `teamId` is the native
team ID (or omit it to resolve via `auth.test`). GitHub's `owner` scopes repositories;
the numeric installation ID must match signed payloads when present. Linear's
`organizationId` must match signed lifecycle and comment payloads. These values
must also agree with the identities in `src/identity.ts`. `supportAgent` uses
`anthropic/claude-sonnet-4-5`; routing uses a Jev classifier registered as `jev/route`. A missing/erroring classifier explicitly
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

## Explicit limits

Pi workflow-tool children now retain the originating thread without inheriting
trigger ownership; no example-local runtime workaround is needed. The passing
unit regression checks the child metadata contract, and the real-Pi cancellation
scenario checks the emitted child metadata and requester card in the origin thread.
Settled chat-message routing state retains ingress tombstones with
`interactionId: null`, rather than disappearing. Direct lifecycle ingress creates
no routing tombstone. Approval tests click on card delivery without waiting for a
native subscription: durable event cursors retain pre-registration decisions.

1. **Ambient Timer visibility:** the vendored Conductor source references `Timer`;
   `@fabrial/conductor`'s package-local ambient declaration is not transitively
   included by an app typecheck. `src/conductor-node.d.ts` supplies the same Node
   timer alias locally. Without it, app typecheck reports TS2304 at
   `vendor/postgres-conductor/packages/pgconductor-js/src/lib/batching-async-queue.ts:31`.
2. **Existing Conductor boundaries still apply:** strict mutexes are soft across
   workers. See `packages/conductor/README.md`. The restart test waits for suspended
   approval work before stopping, and proves one write in that scenario, **not exactly-once SQL across a crash during a database effect**.
   Conductor step memoization cannot atomically commit an external DB write and
   its runtime receipt. A production repair needs business-level idempotency or
   reconciliation. No extra execution/approval tables are introduced here.
