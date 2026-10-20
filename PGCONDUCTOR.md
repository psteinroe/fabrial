# PG Conductor changes for Fabrial

This is a handover for work in [`psteinroe/postgres-conductor`](https://github.com/psteinroe/postgres-conductor). [Fabrial](./PLAN.md) builds on Conductor, and its rule is that Conductor gaps are fixed in Conductor rather than worked around in Fabrial. This file lists what Fabrial still needs so that its Conductor adapter (`packages/fabrial/src/runtime/conductor`) uses only public Conductor APIs: no private access, no polling, no `SHIM(conductor#…)` markers.

Checked against Conductor `main` @ `7e6688c`. Items 1, 2, 4, and 5 landed in #84, #85, and #82; the Node package in #83. Re-check each item before starting, because main moves.

## How Fabrial uses Conductor

- `f.defineWorkflow` registers a Conductor task; plugin triggers become typed events and event triggers. Every durable Fabrial operation is a step or wait keyed by an explicit id.
- Fabrial's invocation context (`origin`, `replyTo`, `requestedBy`, the request id) is execution metadata, rebuilt into `ctx.thread`, `ctx.clients`, and `ctx.actor` by middleware on every attempt.
- Each agent Session is hosted by one keyed execution (`signalWithStart`, key = Session key) while Pi has work. Every input (a chat message, a followed PR's review, a subagent report, a cancel) is a signal to it.
- A Session starts workflows as tools and keeps hosting Pi while they run; it needs their outcomes, and they must be cancelled with the run that started them. A workflow can call an agent (`ctx.agent`) or another workflow (`ctx.invoke`), optionally detached or with a timeout.
- Approvals post a card, then wait for the click event.

## Done

| Need                                       | Conductor                                                                                             |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Race-safe waits                            | `ctx.subscribe` (#74); `ctx.waitForAny` over subscriptions (#79)                                      |
| Execution metadata, propagated             | #72                                                                                                   |
| Middleware on every attempt                | #71                                                                                                   |
| Start without waiting, durable and deduped | `ctx.invoke` starts, `ctx.invokeAndWait` waits (#78)                                                  |
| Idempotent emit                            | `dedupe_key` (#77)                                                                                    |
| Execution results                          | `getExecution`, `waitForResult` (#70)                                                                 |
| Stop doesn't hang on queued claims         | `stop()` waits for the running step or handler (#68)                                                  |
| Signals and keyed executions               | `ctx.onSignal`, `signal`, `signalWithStart`, cancel on the worker poll, cancel reason preserved (#80) |
| One owner per Session                      | Keyed executions (#80); Fabrial's Session storage fences writes                                       |
| Node-compatible package                    | #75 (in progress)                                                                                     |

## Still needed

### 1. Wait for a started execution, with its outcome as a value

**Current.** `ctx.invokeAndWait` starts and waits in one call, and a failed child cascade-fails the parent. There is no way to wait for an execution started earlier with `ctx.invoke`, to wait with a timeout without cancelling the child, or to receive a failure or cancellation as a value.

**Needed.** An `{ execution: id }` branch for `ctx.waitForAny` (listed as a follow-up in #79), plus a shorthand:

```ts
const id = await ctx.invoke("start-tool", tool, input);
// … other work …
const outcome = await ctx.waitForResult("tool-result", id, { timeout: "24h" });
// { status: "completed", result } | { status: "failed", error } | { status: "cancelled", reason } | { status: "timeout" }

const winner = await ctx.waitForAny(
	"tool-or-click",
	{ tool: { execution: id }, click },
	{ timeout: "1h" },
);
```

- Settling the execution wakes the waiter, as `invokeAndWait` resumes its parent today; no polling.
- The outcome is a value: a failed or cancelled execution never fails the waiter.
- An execution already settled when the wait registers returns at once. The result is memoized under the step key.

**Acceptance.** Completed, failed (after retries), cancelled-while-pending, and cancelled-while-running executions each wake a waiter with the matching outcome; a timeout returns `{ status: "timeout" }` and leaves the execution running; an execution branch races event branches correctly; replays return the memoized outcome.

### 2. Children linked without being awaited

**Current.** Only `invokeAndWait` children are cancelled with their parent. `ctx.invoke` executions are independent.

**Needed.** `ctx.invoke(key, task, payload, { cancelWithParent: true })`: the parent keeps running, and the child is cancelled (with the parent's reason) when the parent is cancelled or fails permanently, including children of children that opted in. Reuse the existing parent link and cascade.

**Acceptance.** Cancelling or permanently failing the parent cancels linked children in every state (pending, running, suspended); unlinked `invoke` children keep running; a linked child that completed before the parent ended stays completed.

### 3. Notify on settlement (dropped)

Not needed: `ctx.waitForResult` works for any execution id and wakes on every terminal state, so Fabrial reports outcomes with small executions of its own. A supervisor starts a workflow tool, waits for its result, and signals the Session; a watcher waits for a `ctx.agent` caller and signals a cancel when it ends cancelled or failed. Approval cleanup waits on an `{ execution }` branch. Conductor documents this as a recipe.

### 4. Public execution id

**Current.** `TaskContext` doesn't expose the execution's id; Fabrial reads the private `opts.execution`.

**Needed.** `ctx.executionId` (and the queue, if cheap).

### 5. Pending cancels set the cancelled flag

**Current.** `cancel_execution()` sets `failed_at` and `last_error` on pending executions but not `cancelled = true`; running ones get the flag. Fabrial updates the flag itself after the call.

**Needed.** Pending executions (and pending descendants) cancelled by `cancel_execution()` get `cancelled = true` with the reason. In progress.

## What Fabrial removes once these land

- Its own `invoke` and `start` (independent submission, a parent link in its metadata, a polling wait-for-any, output copied into a private step), replaced by native `invoke`, `invokeAndWait`, `waitForResult`, and `waitForAny`.
- The reconcile loop that cancels children of ended parents (item 2) and the settled-execution monitor (item 3).
- Private access to the task context (item 4) and the abort-controller calls used to reschedule (item 1).
- The metadata envelope in payloads and the event-position sequence (native metadata, middleware, subscriptions).
- The vendored submodule, once a version is published.
- The extra `UPDATE` after cancelling (item 5).

## Not needed from Conductor

- **Strict per-key locks.** Keyed executions give one host per Session; Fabrial's Session storage fences writes with its own epoch.
- **Event cursors.** `ctx.subscribe` makes waits race-safe; a wait that sees the same event stream repeatedly (approval clicks) filters on its subscription.
- **Repeated step ids.** Fabrial suffixes repeated ids itself.
- **`cancelWithParent: false` for awaited children (#73).** Fabrial's detached calls use `invoke` plus item 1 instead.
