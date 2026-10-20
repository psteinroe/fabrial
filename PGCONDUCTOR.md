# PG Conductor changes for Fabrial

[Fabrial](./PLAN.md) builds on [`psteinroe/postgres-conductor`](https://github.com/psteinroe/postgres-conductor), and its rule is that Conductor gaps are fixed in Conductor rather than worked around in Fabrial. Everything Fabrial needed has landed: its Conductor adapter (`packages/fabrial/src/runtime/conductor`) uses only public Conductor APIs, with no private access, no polling, and no tables, sequences, or receipts of its own. Nothing is outstanding.

## How Fabrial uses Conductor

- `f.defineWorkflow` registers a Conductor task; plugin triggers become typed events and event triggers. Every durable Fabrial operation is a step or wait keyed by an explicit id.
- Fabrial's invocation context (`origin`, `replyTo`, `requestedBy`, the request id) is execution metadata, rebuilt into `ctx.thread`, `ctx.clients`, and `ctx.actor` by middleware on every attempt.
- Ingress emits one `fabrial.ingress` event per delivery, deduped by `dedupe_key`; one routing workflow handles it.
- Each agent Session is hosted by one keyed execution (`signalWithStart`, key = Session key) while Pi has work. Every input (a chat message, a followed PR's review, a subagent report, a cancel) is a signal to it.
- A Session starts workflows as tools (`ctx.invoke` with `cancelWithParent`) and learns their outcomes through small supervisor executions that `ctx.waitForResult` and signal it. A workflow can call an agent or another workflow, optionally detached or with a timeout.
- Approvals subscribe to the decision event, post cards, and race the decision against the caller's result and a deadline with `ctx.waitForAny`.

## Landed

| Need                                                  | Conductor                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| Execution results                                     | `getExecution`, `waitForResult` (#70)                        |
| Middleware on every attempt                           | #71                                                          |
| Execution metadata, propagated                        | #72                                                          |
| Race-safe waits                                       | `ctx.subscribe` (#74); `ctx.waitForAny` (#79)                |
| Idempotent emit                                       | `dedupe_key` (#77); unambiguous keys (#91)                   |
| Start without waiting                                 | `ctx.invoke` starts, `ctx.invokeAndWait` waits (#78)         |
| Signals and keyed executions                          | `ctx.onSignal`, `signal`, `signalWithStart` (#80)            |
| Pending cancels set the cancelled flag                | #82                                                          |
| Node-compatible package                               | #83                                                          |
| Wait for a started execution, outcome as a value      | `ctx.waitForResult`, `{ execution }` branches (#84)          |
| Public execution id                                   | `ctx.executionId` (#84)                                      |
| Children linked without being awaited                 | `ctx.invoke(…, { cancelWithParent: true })` (#85)            |
| Test clock                                            | `createTestClock` (#86); event waits under a set clock (#98) |
| Claim fencing, attempt lifecycle, shutdown            | #93, #94, #101                                               |
| Replay-safe retried writes                            | #90                                                          |
| Cancellation cascade                                  | #96                                                          |
| Signal branches; metadata on `signalWithStart`/`emit` | #100                                                         |
| Wait deadlines and child timeouts                     | #99                                                          |

## Not needed from Conductor

- **A settlement hook.** `ctx.waitForResult` wakes on every terminal state, so Fabrial reports outcomes with executions of its own.
- **Strict per-key locks.** Keyed executions give one host per Session; Fabrial's Session storage fences writes with its own epoch.
- **Event cursors.** `ctx.subscribe` makes waits race-safe.
- **Repeated step ids.** Fabrial suffixes repeated ids itself.

What remains is publishing a version, so Fabrial can drop the vendored submodule.
