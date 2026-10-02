# PG Conductor changes for Fabrial

This is a handover for work in [`psteinroe/postgres-conductor`](https://github.com/psteinroe/postgres-conductor). [Fabrial](./PLAN.md) builds on Conductor and assumes the behavior below. Its rule is that Conductor gaps are fixed in Conductor rather than worked around in Fabrial, so this file is the list of things to fix.

Current behavior was checked against `main` @ `33228e3` ("fix(workflows): complete terminal settlement for cancelled and failed workflows (#49)"). Paths are relative to the Conductor repo. Re-check each item before starting, because main moves.

## Context: how Fabrial uses Conductor

- `defineWorkflow` registers a Conductor task. Plugin triggers (Slack, GitHub, Linear, Sentry, …) become typed Conductor events and event triggers.
- Every durable Fabrial operation is a Conductor step or wait keyed by an explicit ID: `ctx.thread.post("ack", …)`, `ctx.agent("investigate", …)`, `ctx.evaluate("route", …)`, `ctx.waitForApproval("approve-sql", …)`.
- Fabrial carries a small serializable **invocation context** through every execution, rebuilds live objects (`ctx.thread`, `ctx.clients`, `ctx.actor`) from it on every start and resume, and passes it to child workflows. Its shape is `{ interactionId, origin, replyTo, requestedBy }`.
- Approvals and thread replies are Fabrial-internal events that a suspended execution waits on, correlated by `interactionId` / approval ID.
- Agent runs (Pi Durable) are driven inside Conductor executions. A Pi Session must have at most one owner at a time.
- Cancellation is structured. Cancelling an execution cancels the calls it is waiting on unless a call was made `detached`. Handoffs start independent executions.

## Summary

| #   | Item                                                | Status on main                                           | Priority                                 |
| --- | --------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------- |
| 1   | Race-safe `waitForEvent`                            | Gap, documented                                          | **P0**: approvals can be lost            |
| 2   | Execution metadata, propagated                      | Gap. `trace_context` is a working precedent              | **P0**                                   |
| 3   | Middleware / per-run context hook                   | Gap. Context is static                                   | **P0**                                   |
| 4   | Strict per-key mutual exclusion                     | Gap. `groupConcurrency` is documented as soft            | P1                                       |
| 5   | Cancellation: opt-out propagation + distinct status | Partly there                                             | P1                                       |
| 6   | Wait for the first of several conditions            | Gap                                                      | P1                                       |
| 7   | Start a child without waiting (durable, deduped)    | Gap from inside `ctx`                                    | P1                                       |
| 8   | Idempotent `emit` + emit options                    | Gap: `emit(event, payload)` takes no options             | **P0**                                   |
| 9   | Node-compatible package                             | Gap: Bun-only build, no `exports`/types, `catalog:` deps | **P0**: Fabrial vendors source meanwhile |

Fabrial can be prototyped against 1–3 and 8 alone. Items 4–7 are needed before the v1 reference flow (Slack → agent → `run-sql` tool workflow → approval in a DM → resume) is reliable.

---

## 1. Race-safe `waitForEvent`

**Current.** `ctx.waitForEvent` (`packages/pgconductor-js/src/task-context.ts`, `waitForEvent`) loads the cached step, then calls `db.registerEventWait(...)` and hangs up. `docs/content/api/task-context.md` says:

> The subscription becomes active when registration commits; an event racing registration or timeout is not guaranteed to win. Earlier events do not satisfy the wait.

**Why it breaks Fabrial.** `waitForApproval` must post the approval card and then wait for the click. If the post happens in a step before the wait registers, a fast click is lost and the execution waits until it times out. Posting after registration is impossible, because `waitForEvent` hangs up. Thread replies (`ctx.thread.waitForReply`) have the same problem.

**Needed.** A way to guarantee that an event emitted after the side effect that causes it satisfies the wait. Options, in order of preference:

1. **Correlated buffered events.** Events carry an optional correlation key (or the filter already acts as one). `waitForEvent` with `since: <step-start | timestamp | event-id>`, or a `buffered: true` mode, matches events emitted after the wait's _step began_, not only after registration commits. The first run of the step records its start position before user code runs any side effect.
2. **Register-then-act.** Split into `ctx.subscribe(stepKey, { event, filter })`, which registers durably and returns, and `await subscription.wait({ timeout })`. Code in between, such as posting the card, runs while the subscription is already active.
3. **Atomic emit-and-wait.** A wait whose registration commits in the same transaction as a step result.

**Acceptance.**

- A test where the event is emitted from inside the step that precedes the wait (before registration commits) and the wait resolves with it.
- An event emitted _before_ the wait's step started still does not satisfy it, so stale clicks from an earlier approval are ignored.
- Exactly-once delivery to the waiter still holds, and a timeout racing a buffered event resolves deterministically.

---

## 2. Execution metadata, propagated

**Current.** No user metadata exists on executions. But `trace_context jsonb` already does exactly what Fabrial needs, for OTel only:

- column on `pgconductor._private_executions` (`migrations/0000000001_setup.sql`)
- set from `conductor.invoke` / `conductor.emit` (`src/conductor.ts`, `trace_context: this.telemetry.traceContext()`)
- carried from event → dispatched execution and parent → child (`src/query-builder.ts`: `parent.trace_context as parent_trace_context`, `p.trace_context` on insert)

**Needed.** A user-facing, typed `metadata` with the same propagation:

```ts
await conductor.emit("slack.mentioned", payload, {
	metadata: { fabrial: { interactionId, replyTo, origin, requestedBy } },
});
await conductor.invoke(task, payload, { metadata }); // explicit
await ctx.invoke("child", task, payload); // inherits parent metadata
await ctx.invoke("child", task, payload, {
	metadata: (m) => ({ ...m, fabrial: { ...m.fabrial, replyTo: other } }),
}); // override
await ctx.emit("x", payload); // events emitted from a run inherit too
ctx.metadata; // readonly, typed
```

- Inherited on: event → triggered execution, `ctx.invoke` children, `ctx.emit`, cron executions from dynamic schedules (`ctx.schedule`), and DLQ deliveries.
- Optional typing via a `metadata` schema on `Conductor.create` (Standard Schema, like event payloads).
- Size limit, documented. Fabrial metadata is small references, not payloads.
- Prior art: Temporal headers + context propagators, Hatchet `additional_metadata` (propagates event → run → child), Trigger.dev `metadata.parent` / `metadata.root`.

**Acceptance.** Metadata is visible in the handler for event-triggered, invoked, child, cron, retried, and resumed executions, and an override on `ctx.invoke` affects only that child.

---

## 3. Middleware: per-run context transformation

**Current.** `Conductor.create({ context })` is static. `src/worker.ts` passes the same `extraContext` object to every `TaskContext.create(...)`. The only special case is the maintenance task, which gets `{ ...this.extraContext, db, tasks }`. Nothing runs per execution or per resume.

**Needed.** A middleware chain that runs on **every** start and resume, before the handler, and can extend `ctx` and wrap execution:

```ts
Conductor.create({
  context: { … },
  middleware: [
    async ({ execution, ctx }, next) => {
      // execution: id, task_key, queue, metadata (item 2), attempt, parent id, root id
      const thread = rebuildThread(execution.metadata.fabrial?.replyTo);
      return als.run({ executionId: execution.id }, () => next({ ...ctx, thread }));
    },
  ],
});
```

- Typed: each middleware's added keys are inferred into the handler's `ctx` (like Inngest `transformFunctionInput`).
- Can wrap the handler (try/finally, `AsyncLocalStorage`, tracing spans).
- Must see whether the run is a first start or a resume, and must not break `abortAndHangup` control flow (a hang-up rejection has to pass through untouched).
- Prior art: Inngest `transformFunctionInput` / `wrapFunctionHandler`, Trigger.dev `middleware` + `locals`, Temporal interceptors.

**Acceptance.** Middleware runs once per attempt (including after `sleep`, `waitForEvent`, and child resumes), added context is typed in handlers, and errors thrown in middleware fail the attempt like handler errors.

---

## 4. Strict per-key mutual exclusion

**Current.** `groupConcurrency` limits active executions per `(queue, task, group)`. `docs/content/task-execution/concurrency.md` says limits are **"intentionally soft across concurrent workers"** (claims use `FOR UPDATE SKIP LOCKED`, `src/query-builder.ts` around `group_concurrency_limit`).

**Why Fabrial needs it.** A Pi Durable storage backend has no cross-process locking: "one process owns a storage at a time". Fabrial runs one Pi Session per chat thread and drives it from a Conductor execution grouped by Session ID with `groupConcurrency: 1`. A soft limit can let two workers own the same Session at once. Fabrial also uses an epoch fence on every Pi commit, so the worst case is wasted work rather than corruption, but the claim itself should be exclusive.

**Needed.** A strict mode, for example `groupConcurrency: { limit: 1, strict: true }` or a separate `mutex: (payload, metadata) => key`. The guarantee: at most one _running_ execution per key across all workers, including during retries, lease expiry, and worker loss, where the old lease must be revoked before another claim. Expose the claim token / epoch to the handler (`ctx.claimToken`) so Fabrial can use it as the Pi fencing epoch (see the `fix/claim-token-fencing` branch).

**Acceptance.** A concurrency test with N workers racing on one key, where the running count never exceeds 1, and a test where a worker dies mid-run and another worker takes over only after its lease expires, with a strictly greater claim token.

---

## 5. Cancellation: opt-out propagation and a distinct status

**Current** (`docs/content/task-execution/cancellation.md`, `ctx.cancel` / `conductor.cancel`):

- Pending executions are marked failed with "Cancelled by user". Running executions get `ctx.signal` aborted and are checked at step boundaries.
- Cancelling an execution also cancels the executions it is waiting on. Waiting parents fail with "Child execution failed: <reason>".

That matches Fabrial's default (structured cancellation). Two things are missing:

1. **Opt-out per call.** `ctx.invoke("audit", task, payload, { cancelWithParent: false })`: the parent still waits for the result, but cancelling the parent does not cancel this child. Fabrial exposes this as `{ detached: true }`.
2. **A distinct terminal status.** Cancelled and failed both look like failures today. Fabrial needs to tell them apart, e.g. to render an approval card as "Cancelled" rather than "Failed", and to return `decision.status = "cancelled"`. Either add a `cancelled` status or a `cancelled: true` flag plus a reason on the execution, on the parent-visible child error, and in `ctx` (`ctx.signal.reason`).

Also confirm that cancelling an execution suspended in `waitForEvent` or `sleep` settles it immediately and removes its subscription.

**Acceptance.** Parent → child (default) and parent → child (`cancelWithParent: false`) cancellation tests. The child's cancellation is distinguishable from failure in the parent's caught error and in the stored execution.

---

## 6. Wait for the first of several conditions

**Current.** `ctx.waitForEvent` waits for one event definition with one filter. `ctx.invoke` waits for one child. `ctx.sleep` waits for one timer. Nothing combines them.

**Why Fabrial needs it.** `ctx.race("approval-or-reply", { decision, reply })` has to wake on whichever comes first: an approval decision, a thread reply from the requester, a child result, or a timer. Fabrial could funnel its _own_ internal events through a single event name and discriminate, but racing a child result or timer against an event needs Conductor.

**Needed.**

```ts
const winner = await ctx.waitForAny("approval-or-reply", {
	decision: { event: approvalDecided, filter: { approvalId: [id] } },
	reply: { event: threadReplied, filter: { interactionId: [iid] } },
	child: { execution: childExecutionId }, // optional
	timeout: "24h",
});
// winner: { key: "decision", event } | { key: "reply", event } | { key: "child", result } | { key: "timeout" }
```

Losing subscriptions are removed atomically when one wins, and the result is cached under the step key like `waitForEvent`. Item 1's race-safety applies to every branch.

**Acceptance.** Each branch can win, and only one wins under concurrent emits.

---

## 7. Start a child without waiting

**Current.** `ctx.invoke` always hangs up and waits (`abortAndHangup({ reason: "child-invocation" })`). From inside a task, the only way to start independent work is `ctx.emit` (event-triggered tasks) or calling the outer `conductor.invoke` inside a `ctx.step`, which loses metadata propagation (item 2) and depends on `dedupe_key` for idempotency.

**Why Fabrial needs it.**

- `ctx.handoff(id, workflow, input)` starts a workflow that takes over the interaction, and the caller then ends.
- `workflow.asTool()` from a Pi agent starts or reconnects to a workflow by a stable key and is awaited later through a separate wake-up.

**Needed.**

```ts
const executionId = await ctx.start("handoff", task, payload, {
	dedupeKey: `${ctx.executionId}:handoff`, // idempotent across retries; defaults from step key
	metadata, // inherits by default (item 2)
});
```

This is a memoized step that creates an independent execution (not cancelled with the parent, not awaited), returns its ID, and is idempotent under retry. Pairs with item 6 (`waitForAny` on `{ execution }`) and a client-side `conductor.waitForResult(executionId)` / result event for awaiting it elsewhere.

**Acceptance.** Retrying the starting step never creates a second execution, the child gets inherited metadata, and cancelling the starter does not cancel the child.

---

## 8. Idempotent `emit` and emit options

**Current.** `conductor.emit(event, payload)` (`src/conductor.ts`) and `ctx.emit(event, payload)` (`src/task-context.ts`) take no options. Each call inserts a new dispatch execution. There is no dedupe key and no way to pass metadata (item 2).

**Why Fabrial needs it.** Slack, GitHub, Linear, and Sentry redeliver webhooks. Ingress turns each delivery into `emit(...)`, so a redelivered approval click or mention must not dispatch twice.

**Needed.**

```ts
await conductor.emit("slack.mentioned", payload, {
	id: `slack:${eventId}`, // repeated id → no-op, returns the original event id
	metadata, // item 2
});
```

Dedup window and scope must be documented: per event name or global, and retention.

**Acceptance.** Emitting the same `id` twice (also concurrently) creates one dispatch, and triggered tasks and waiters see it once.

---

## 9. Node-compatible package

**Current.** Fabrial runs on Node, but the published `pgconductor-js@0.1.0` can't be used there:

- `package.json` has only `"module": "dist/index.js"`. With no `main`/`exports`, Node can't resolve `import "pgconductor-js"`.
- No type declarations are published.
- The entry point (`src/index.ts`) doesn't export documented APIs: `defineTask`, `TaskSchemas`, `EventSchemas`.
- Dependencies use Bun `catalog:` specifiers, so the package also can't be installed from git by pnpm.
- `src/lib/batching-async-queue.ts` uses Bun's global `Timer` type.
- npm is far behind `main`.

The source itself runs fine on Node. Fabrial's smoke test (`packages/conductor/tests/integration/pgconductor.test.ts`) runs a task with a memoized step on Node against Postgres 17.

**What Fabrial does meanwhile** (all marked `SHIM(conductor#9)`): `vendor/postgres-conductor` is a git submodule pinned to a commit and included in Fabrial's pnpm workspace, which supplies the catalog versions. `@fabrial/conductor` imports source subpaths (`pgconductor-js/src/...`), vitest inlines it, and a `.d.ts` declares `Timer`.

**Needed.**

- An ESM build for Node: `exports` map with `types` + `default`, `.d.ts` emitted, `engines.node`.
- Export `defineTask`, `TaskSchemas`, `EventSchemas` (and the other documented APIs) from the entry point.
- Resolve `catalog:` on publish (Bun publish or a release script), and replace `Timer` with `ReturnType<typeof setTimeout>`.
- Run the test suite on Node in CI as well as Bun, or at least the integration smoke tests.
- Publish releases as the items above land, so Fabrial can drop the submodule.

**Acceptance.** `npm i pgconductor-js` in a plain Node 22 ESM project, `import { Conductor, Orchestrator, defineTask, TaskSchemas } from "pgconductor-js"` typechecks and runs.

---

## Not needed from Conductor

- **Repeated step IDs.** Conductor requires unique step names per execution. Fabrial suffixes repeated IDs itself (`processed`, `processed:1`, …), which is deterministic because handlers re-run from the top on every resume.
- **Event filters.** The current filter operators (top-level scalar fields, ≤ 8 fields) are enough for correlating by `interactionId` / `approvalId`.
