# fabrial core

`createFabrial({ plugins, identity?, logger? })` creates an explicit typed catalog. There is no global `Register` augmentation. Keep it in a module that imports plugins and users only, not workflows, to avoid import cycles.

```ts
// fabrial.ts
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";
export const f = createFabrial({ plugins: [slack({ botToken, signingSecret, workspace })] });

// workflows.ts
import { f } from "./fabrial.ts";
export const hello = f.defineWorkflow({
	name: "hello",
	async run(_input, ctx) {
		const result = await ctx.step("auth", () => ctx.clients.slack.auth.test());
		return { userId: result.user_id ?? null };
	},
});

// app.ts
const app = f.app({ runtime, chat, agents, workflows: [hello] });
// Tests use the same definition-time plugins and spy on live clients:
const testApp = f.app({ runtime: testRuntime, chat: testChat, workflows: [hello] });
vi.spyOn(testApp.host.clients().slack.auth, "test").mockResolvedValue({
	ok: true,
	user_id: "test",
});
```

`f.defineWorkflow` and `f.defineGroup({ id, resolve(ctx) { … } })` infer precise `ctx.clients` from this instance. Definitions remain plain data and can be reused by any app made from `f`; groups can be referenced directly in access/approval options. `f.plugins` is a readonly catalog for introspection. Plugins carry values at definition; factories must stay side-effect free. Credential validation and connections happen at `app.start()`, not import or `f.app()`. `f.app({ runtime, workflows, chat?, agents? })` composes the ports, registers events, and exposes `app.fetch`, `app.start`, `app.stop`, and authenticated adapter ingress through `app.host`.

Durable operations require explicit, nonempty IDs. Repeated IDs are suffixed deterministically on replay. `fabrial:` is reserved for helper receipts. Completed receipts prevent repeated effects on replay; a crash during an external effect can still repeat it. Providers must supply idempotency or reconciliation when exactly-once effects matter.

## Routing and identity

`identity` accepts `readonly UserDefinition[]` only. Define groups in a separate module with `f.defineGroup` for typed `ctx.clients`. Access and approvers resolve group references directly; groups do not require registration, including dynamically resolved groups.

Owner specificity is `trigger.specificity ?? Object.keys(trigger.filter ?? {}).length`. Fields are ANDed, alternatives ORed, with Conductor's scalar, prefix, numeric, exists, and anything-but semantics. Observers run without the owner's thread. Triggers on the same workflow are merged.

Startup rejects equal-specificity owners on the same event whenever their filters may overlap. Literal, prefix, numeric-range, and existence disjointness is checked statically; uncertain combinations are conservatively rejected. Startup cannot know which **different event categories** a future chat message will carry. Such ties are rejected at ingress before emitting anything. Multi-category ingress shares a `dispatchId`: runtimes must dedupe triggered executions by `(dispatchId, workflow)`, not just dedupe each individual event.

Chat integrations must provide trusted installation IDs and authenticated actors. The host ignores messages marked `author.isBot`. Reply buffers and consumed message IDs live in thread routing state; the agent bridge consumes the same buffer while `agentActive`. Cancellation is authorized for the requester or someone who has sent a message in the **current interaction**, not historical thread participants. Invalid cancellation attempts receive an ephemeral notice. Runtime cancellation itself must also be idempotent.

Routing state retains the last 100 accepted ingress dedupe IDs (message, approval action, or cancellation, each with an epoch-ms timestamp), including across settlement. Tombstoned message deliveries return `"ignored"` atomically without reserving a slot or dispatching events. `interactionId: null` explicitly marks a state with no active interaction; adapters must not equate non-null routing state with an active interaction. The optional `ingestedDedupeIds` and `reservedAt` fields must round-trip through the state adapter. Settlement clears interaction-scoped fields but retains tombstones, subject to the 30-day thread-state TTL and the 100-entry bound.

An unbound reservation (`handlerExecutionId: null`) expires at ingress after `reservationTimeout` (default `"10m"`, configurable as a duration string or milliseconds, below 30 days). Fresh messages replace expired reservations without discarding tombstones; replies do not extend the grace. Legacy unbound states without `reservedAt` are immediately considered stale. Bound handlers are not expired by this check. This also limits the impact of redelivery older than the tombstone window or a lost dispatch.

Channel surfaces remain unresolved until a post. The first durable post receipt supplies the real thread reference; every replay rebinds from that receipt. Handoffs and child invocations inherit this real reference, never a provisional channel locator.

## Approvals and terminal cleanup

Requests persist the proposal, assigned approvers, controls, and absolute expiry in a step. Assigned approvers must still belong to an eligible group when they decide. Requester cancellation is separate from approval authority; free text never changes an approval. Reply and approval timeouts must be below the 30-day routing-state TTL.

The runtime's `RuntimeWorkflow.onSettled` hook must deliver success, failure, and cancellation, including cancellation while suspended, and retry failed delivery across restarts. Core emits `fabrial.execution.settled` from this hook. Independent `fabrial.cleanup` executions collect durable `fabrial.presentation` events to clear channel bindings and pending cards even when a parent stops partway through delivery. `fabrial.approval.closed` prevents cleanup from overwriting a resolved decision. These workflows/events are framework internals, not application APIs.

Runtimes must provide race-safe correlated event retention: a click or presentation receipt emitted during the side effect preceding a wait cannot be lost because the worker registers its wait later. This is a required port capability, not a core Conductor shim.

## Testing

```ts
import { createFabrial, trigger } from "fabrial";
import { createTestApp } from "fabrial/testing";

const f = createFabrial({ plugins: [] });
const workflow = f.defineWorkflow({
	name: "hello",
	triggers: [trigger({ event: "demo.hello" })],
	async run(_input, ctx) {
		await ctx.sleep("pause", "1h");
		return "hello";
	},
});
const { app, runtime, chat } = createTestApp(f, {
	now: new Date("2026-01-05T00:00:00Z"),
	workflows: [workflow],
});
await app.start();
await app.emit("demo.hello", {}, { id: "one" });
await runtime.flush(); // runs handlers until all are settled or suspended
await runtime.advanceBy(3_600_000);
console.log(runtime.executions("hello")[0]?.result);
await app.stop();
```

- `createTestApp(f, { workflows, now?, agents? })`: isolated app with a memory runtime, fake chat, and fake agents. Spy on `app.host.clients()` or its client methods for fakes.
- `MemoryRuntime`: `flush`, `advanceBy`, `now`, `executions`, `result`, `stepIds`, and inspectable `emitted`. Replays handlers from the top, memoizes receipts, queues children, propagates structured cancellation, supports detached invokes/independent starts, timers, event races, workflow concurrency, mutexes, emit dedupe, and settlement-hook redelivery. Event retention starts at execution creation; stale events predating the execution are excluded, and each event is delivered at most once per execution. Cron schedules are registered but not automatically driven; invoke scheduled workflows explicitly in tests.
- `FakeChat`: inspect `threads`, their `posts`/`updates`/`messages`/`statuses`, and `ephemeral`. `receive(message, { events, dedupeId })`, `click(receipt, actionId, identity)`, and `cancel(thread, identity)` go through the host. Channel locators are genuinely provisional until posted. State expiry uses the injected clock. Atomic updates and cloned state preserve optional routing fields, idle tombstones, and reservation timestamps just like the live port.
- `fakeAgents({ run?, evaluate?, state?, workflows? })`: injectable ports, default durable echo agent and empty classifier, and schema-checked in-memory state exposed as `values`. No models or database are used.

## Current boundaries

Live plugin clients are built once per app, as specified by `FabrialHost.clients`; they must not capture invocation identity. This differs from PLAN's earlier per-resume wording. Core does not add a reaction acknowledgement to buffered replies because `ChatPort` has no reaction operation. Thread routing state is a read/write port, not a transaction or CAS: ingress for a given thread must be serialized by the integration/state adapter, including across webhook replicas. This package does not emulate missing Conductor capabilities or schedule cron timers in its test runtime.
