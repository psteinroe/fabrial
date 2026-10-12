# fabrial

Durable workflows, AI agents, and human approvals in TypeScript. You install one package, give it a Postgres database and model credentials, and define workflows, agents, and tools on an instance. The durable runtime ([PG Conductor](https://github.com/psteinroe/postgres-conductor)), the agent runtime ([Pi Durable](https://earendil.com/posts/pi-durable/)), and chat ([Chat SDK](https://chat-sdk.dev)) are internal modules, not part of the API.

```ts
// fabrial.ts: imports plugins and users only, never workflows, to avoid import cycles.
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";
export const f = createFabrial({
	database: process.env.DATABASE_URL,
	models: { anthropic: { apiKey: process.env.ANTHROPIC_API_KEY } },
	plugins: [slack({ botToken, signingSecret, workspace })],
	identity: users,
});

// agents.ts
import { z } from "zod";
export const lookup = f.defineTool({
	name: "lookup_order",
	description: "Look up an order",
	input: z.object({ orderId: z.string() }),
	idempotent: true,
	run: async ({ orderId }, ctx) => ctx.clients.shop.order(orderId), // a string or any JSON value
});
export const support = f.defineAgent({
	name: "support",
	model: "anthropic/claude-sonnet-5",
	instructions: "Answer support questions. Return your answer without posting it.",
	tools: [lookup, refundWorkflow], // workflows are tools too
	toolkits: ["slack"],
});

// workflows.ts
export const assistant = f.defineWorkflow({
	name: "assistant",
	triggers: [slack.mentioned()],
	async run(message, ctx) {
		const answer = await ctx.agent<string>("answer", support, { input: message.text });
		await ctx.thread!.post("reply", answer);
	},
});

// app.ts
export const app = f.app({ workflows: [assistant] });
await app.start(); // migrates Postgres, validates credentials, starts workers
// Mount app.fetch in your HTTP server; call app.stop() on shutdown.
```

## The instance and the app

`createFabrial({ database, models, plugins, identity })` creates an explicit, typed instance. There is no global `Register` augmentation. `database` is one Postgres connection string shared by workflows, agent sessions, and chat state. `models` holds credentials per model provider (`{ anthropic: { apiKey } }`); agents and `ctx.evaluate` name models as `"provider/model"` strings.

`f.defineWorkflow`, `f.defineAgent`, `f.defineTool`, `f.defineToolkit`, `f.defineState`, and `f.defineGroup({ id, resolve(ctx) { … } })` infer precise `ctx.clients` and plugin toolkit ids from the instance. Definitions are plain data and can be reused by any app made from `f`; groups can be referenced directly as approvers. `f.plugins` is a readonly catalog for introspection.

`f.app({ workflows, logger? })` builds the app and exposes `app.fetch`, `app.start`, `app.stop`, and authenticated adapter ingress through `app.host`. Plugins carry their values at definition and factories must stay side-effect free: importing the app with unset environment is safe. The database pool opens, credentials are validated, and connections start at `app.start()`, not at import or `f.app()`. Chat-capable plugins are served at `POST /<pluginId>/events`; routes await ingress before responding.

Durable operations require explicit, nonempty IDs. Repeated IDs are suffixed deterministically on replay. `fabrial:` is reserved for helper receipts. Completed receipts prevent repeated effects on replay; a crash during an external effect can still repeat it. Providers must supply idempotency or reconciliation when exactly-once effects matter.

## Agents and tools

- `f.defineAgent({ name, model, thinking?, instructions?, tools?, toolkits?, state? })`. `name` is stable: it is the conversation's continuation key. `instructions` are strings or functions rendered per run with a read-only context (`actor`, `requestedBy`, `clients`, `interaction`, `state(definition).get()`). Define agents before `app.start()`.
- `tools` lists tools and workflows. A workflow tool starts the workflow as a child of the agent run and returns its output; approvals are always workflow-backed.
- `f.defineTool({ name, description, input, idempotent?, run })`. `input` is a Standard Schema with Standard JSON Schema support (e.g. zod ≥ 4.2); the model sees its JSON Schema, and the app fails to start if a schema cannot be converted. Input is validated before `run`: invalid input never reaches `run` and goes back to the model as an error result. `run` returns a string (sent as is) or any JSON value (serialized). Tool `ctx` has `actor`, `requestedBy`, `clients`, `interaction`, `thread` (durable `post`/`update`/`history`, each with an id), `state`, `invoke`, `start`, `evaluate`, and `signal`.
- `idempotent: true` tools run again after a crash interrupts them; other tools give the model an interrupted result instead.
- `f.defineToolkit({ name, tools, instructions })` groups tools and instructions. Agents list app toolkits and plugin ids in `toolkits`.
- `f.defineState({ name, scope, schema, initial, render? })` is state agents see and change. Tools change it with `ctx.state(def).update(fn)`; the changes commit atomically with the tool result. Workflows use `ctx.state(def).get(id)` / `.update(id, fn)`. Agents listing a state in `state` see its `render` output in the prompt.
- `ctx.agent(id, agent, { input, output?, detached? })` runs the agent durably and returns its final answer; it never posts automatically. With `output`, the agent is asked for a single JSON value, which is parsed and validated against the schema in a durable step. Invalid answers fail explicitly; there is no silent coercion or repair loop. `detached: true` keeps the agent run alive when the caller is cancelled.

### `ctx.evaluate`

```ts
const route = await ctx.evaluate("route", {
	model: "typesafe/jev-latest",
	state: { message: message.text },
	questions: { bug: { type: "boolean", instructions: "Is this a bug report?" } },
});
if (route.stopReason === "stop" && route.answers.bug?.value) …
```

Question types are `choice`, `score`, and `boolean`, on Pi AI's `models.classify()`. Choice distributions and confidence, boolean probability, and usage are kept. Score rubrics map to ordered criteria, and the classifier's score is a zero-based rubric index; out-of-range or non-integer scores are errors. Pi exposes no score distribution, so `probabilities` is `{}` for scores rather than invented. Unknown classifiers, provider errors, and aborts return `stopReason: "error"`; workflows fall back explicitly. The result is a memoized step in workflows and a memo in tools.

## Plugins

A plugin carries an `id` and optionally `events`, `routes` (`"METHOD /path"`, served by `app.fetch`), `clients`, `context`, `identity`, `hooks.workflow` middleware, and `init`/`shutdown` for resources it owns, acquired at `app.start()` and never in the factory. For agents it adds:

- `toolkit: (clients) => ({ tools, instructions })` for agents that list the plugin id in `toolkits`. Listing a plugin without a toolkit fails at `app.start()`.
- `hooks.agent` observers for every agent: `onModelResponse({ agent, model, text, toolCalls, stopReason, error?, usage? })` and `onToolCall({ agent, callId, name, arguments, result, isError })` after a tool settles. Observers are best effort: they may repeat on recovery, and a throwing observer is logged without failing the run.
- `chat: chatCapability({ … })` to connect a Chat SDK adapter, the one runtime type plugin authors see:

```ts
import { chatCapability, chatEvents, chatTrigger } from "fabrial";

const plugin = {
	id: "github",
	events: { ...chatEvents() /* provider-specific defineEvent entries */ },
	chat: chatCapability({
		adapter: () => createGitHubAdapter(credentials),
		installationId: configuredInstallation,
		history: { mode: "thread", limit: 50 },
		status: "message", // for platforms without meaningful typing/status
		isNewThread: (thread, message) => isRootComment(thread, message),
	}),
};
const mentioned = (filters = {}) => chatTrigger("github", "mentioned", filters);
```

Adapters must use their plugin id as the thread/channel id prefix. Installation ids are configured, trusted identity namespaces, not display names inferred from messages. Standard payloads include `channelId` (without adapter prefix), `threadId` (full SDK id), `isDM`, `isMention`, and `isNewThread`. Nonstandard root codecs should provide `isNewThread`.

Each inbound message carries all matching categories: channel messages emit `message`, plus `mentioned` when applicable; DMs emit `dm`, plus `mentioned` when applicable (not `message`). Channel `message`/`newThread` observers therefore also see mentions. Core selects one owner across the categories and dedupes each observing workflow across them.

## Routing and identity

`identity` accepts `readonly UserDefinition[]` only. Define groups in a separate module with `f.defineGroup` for typed `ctx.clients`. Approvers resolve group references directly; groups do not require registration, including dynamically resolved groups.

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

`fabrial/testing` has two levels. `createTestApp` runs workflows in memory, without models or a database:

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
- `fakeAgents({ run?, evaluate?, state?, workflows? })`: injectable ports, default durable echo agent and empty classifier, and schema-checked in-memory state exposed as `values`.

`createIntegrationApp(f, { workflows, models, logger? })` runs the real runtime, agents, and chat against the instance's `database` (e.g. Postgres from testcontainers) with scripted models instead of credentials:

```ts
import { createIntegrationApp, reply, scriptedModels } from "fabrial/testing";

const models = scriptedModels();
models
	.model("anthropic/claude-sonnet-5")
	.respond([
		reply.toolCall("lookup_order", { orderId: "o1" }),
		({ messages }) => reply.text(`Seen ${messages.length} messages`),
	]);
models.classifier("typesafe/jev-latest", () => ({ bug: { probability: 0.01 } }));
const app = createIntegrationApp(f, { workflows, models });
```

Each model request consumes the next scripted step; `model.calls` counts requests. Model retries are off by default (`modelRetries`), and worker polling is fast (`pollIntervalMs`, `concurrency`).

## Internals

This section is for contributors. None of it is API.

### Durable runtime (Conductor)

`src/runtime/conductor` adapts PG Conductor to Fabrial's internal `DurableRuntime` port. Registration is side-effect free; `start()` migrates Postgres and starts workers. Handlers replay from the top after suspension. Explicit operation IDs address saved steps and waits; core supplies repeated-ID suffixes. The pool is the instance's; the runtime does not own it. Keep the queue's workflow catalog consistent across instances. Durable wait deadlines use Postgres's clock.

Event-triggered handlers receive the clean event payload. Cron handlers receive `{ name: scheduleName }`, with a generated interaction ID and no thread or actor. Deadlines are absolute epoch milliseconds, fixed once before replayable waits. Core takes a memoized cursor before a side effect that can cause a reply (e.g. posting an approval card), and the following wait passes it as `after`, so retained matching receipts strictly after that position count even if emitted before the wait registered. Without `after`, the shim captures a durable registration boundary on the first wait attempt. Cursors are opaque and database-local.

Core resolves ownership before emitting: only the named workflow's matching owner triggers run; observers run without thread ownership. Overlapping subscriptions on one workflow produce one invocation. `emit(..., { dispatchId })` merges triggered invocations by `(queue, dispatchId, workflow)` across event categories without merging event receipts. `dispatchOwner: { workflow, event }` on every emission of one ingress restricts that workflow to the canonical event; competing observer submissions for it are suppressed before dedupe.

#### Temporary shims

All receipts, waits, outputs, and parent links live in Conductor's tables. No Fabrial-owned tables are added. Direct SQL and private context access are confined to `src/runtime/conductor` and tied to the pinned vendor revision. [PGCONDUCTOR.md](../../PGCONDUCTOR.md) describes the upstream changes that would remove them.

| Item | Adapter behavior / remaining gap                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `#1` | Memoized `cursor` and `after` waits scan retained dispatch receipts and return the consumed event's cursor. A Postgres bigint sequence orders the event stream; a database-wide transaction advisory lock serializes cursor capture, receipt insertion/commit, and observation, preventing concurrent emitters from committing behind a cursor. No JS-clock cursor. Regression coverage includes fast pre-registration clicks, stale exclusion, chained replies, delayed concurrent commit, and restart. Native indexed buffered waits are still wanted.                                                                                                                                                                                                                                                                                                                                                        |
| `#2` | Invocation payload envelope and reserved event fields carry metadata. Invoke/start/emit inherit it; partial overrides are shallow and affect only that operation. Event filter fields stay top-level. Event payload fields `__fabrial` and `__fabrialOwner`, and operation IDs beginning `__fabrial:`, are reserved and rejected.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `#3` | Handler wrapper runs every attempt/resume; native execution identity is read from the private context. The live `TaskContext` stays inside the runtime; apps never see it. Never swallow Conductor's suspension control flow or put durable waits inside `step`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `#4` | Workflow mutex keys and invoke/start mutex overrides become native groups with `groupConcurrency: 1`. **Soft across workers**, scoped to `(queue, task, key)`, not a cross-workflow lock. Only running/claimed attempts exclude each other; suspension releases capacity. Agent sessions add their own fencing (see below).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `#5` | Independent child submissions carry a persisted structured parent link unless detached. Cancel recursively cancels linked calls; start is always independent. Reconciliation also cancels active children of failed/cancelled parents after restart. Cancelled native rows map to a distinct `ExecutionResult`; child failures throw `ExecutionError` with that result. Invoke timeout fixes an absolute database-clock deadline once; it cancels a structured child, but leaves a detached child running. Cancellation reason is saved atomically in the envelope because native running settlement overwrites `last_error`. Native running cancellation signals arrive on the orchestrator heartbeat (up to 30 seconds). Cancellation can settle the attempt before in-flight user code returns; recording that running step's eventual result is **not guaranteed**. `signal.reason` is native control data. |
| `#6` | Native subscriptions wake durable polling over events/executions/absolute timers. No worker remains occupied while suspended. Event branches each honor `after` (or the durable registration boundary); a winner is cached and losing subscriptions removed atomically under the native claim lock. Database timestamps select the earliest ready branch; stream order breaks event ties, then record order breaks remaining ties. Timers use `at` directly on every replay, never add remaining time to the original start. Fractional polling delays are rounded up for native bigint rescheduling. Polling can add up to the configured interval plus worker latency. Receipt scans and serialized observation are a prototype, not an indexed native race API.                                                                                                                                              |
| `#7` | Memoized, independent start returns an execution ID; insert-on-conflict keeps the original execution **even after completion**, unlike today's native deduped invoke (which updates/reinvokes). Default keys derive from execution ID + operation ID; supplied dedupe keys are scoped to queue + workflow. Independent outputs are saved in a private step for invoke/execution branches.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `#8` | Concurrent emit redeliveries dedupe by `(event name, id)`; the first payload/metadata/cursor wins. Dispatch receipts are retained indefinitely on the internal event queue (see policy below). Cross-category `dispatchOwner` suppresses competing observer submissions for the selected owner before invocation insertion; workflow dedupe remains `(queue, dispatchId, workflow)`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `#9` | Source imports and the existing Bun `Timer` type shim allow the pinned vendored engine to run on Node ≥22. No vendor changes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

#### Receipt retention and deployment

All dispatch receipts, including **filter-correlated** events, have immediate deletion disabled and **no TTL**. Thus buffering and emit dedupe cover the maximum approval wait (≤30 days), including restarts. Do not purge receipts while an active workflow can still reference their cursors; any future automatic retention must cover at least 30 days and protect active cursor-based waits. Explicit purging also ends dedupe for those receipts. Native orchestrators sharing `pgconductor.internal` must not re-enable deletion.

The shim adds one sequence, `pgconductor.__fabrial_event_position`, lazily under the stream lock; it adds no tables or columns. The database role needs permission to create/use it. Never reset/drop the sequence independently of receipts/cursors. All emitters must use this adapter for cursor-buffered events: native `emit` and older adapter receipts have no stream position. Drain legacy waits before upgrading; mixed old/new emitters are not a supported race-safe deployment. The advisory lock serializes only short database transactions, not user side effects. Retention is intentionally unbounded until upstream supplies an indexed buffered event API and safe compaction.

#### Terminal notifications

A restart-safe monitor calls `RuntimeWorkflow.onSettled` after terminal completion/failure/cancellation, never on suspension. Delivery is **at least once** (crash between the hook and receipt, or competing monitors); hooks must be idempotent. Core supplies this hook to clear thread routing state and emit `fabrial.execution.settled` with the execution ID as dedupe ID, so repeated hook deliveries still dispatch the event once. Hook failures are logged independently and retried in a cyclic scan; a poison hook does not block other executions, even beyond the first 100-row batch.

Without a hook the monitor emits settlement events for failures/cancellations that a handler wrapper cannot see (retry exhaustion, suspended cancellation). Successful notifications require the hook (core always supplies one); the no-hook fallback does not emit success events, avoiding an unbounded loop of success notifications from raw settlement observers. Reconciliation continues on a new instance with the same queue and registered catalog. Core's process-local cancellation callbacks cannot be reconstructed by this adapter after restart.

#### Contract coordination

The updated port's `cursor`/`after`, absolute `deadline`/`at`, consumed cursors, and canonical `dispatchOwner` are implemented. Core captures cursors before approval/reply side effects and supplies `dispatchOwner` on each ingress category. Tests include a real-Postgres approval through `f.app()` whose authenticated click arrives during card delivery, before the wait registers.

`PGCONDUCTOR.md` describes the upstream gaps: its Findings for **#1** (native wait, unresolved todo) and **cross-category dispatch** (first-submission ownership race) are historical for this adapter. The shims above close those adapter gaps, not the corresponding native Conductor gaps. Strict mutex guarantees still need upstream claims/fencing, not weaker port documentation.

#### Upstream priorities

Keep `#1`, `#2`, `#8`, and `#9` as P0. Native retained, idempotent emit receipts with monotonic positions and ≥30-day correlated buffering are needed to replace the fallback scans/sequence. Canonical owner dispatch precedence should also be native. Add claim-guarded, indexed races (`#6`) and independent start/result retrieval (`#7`) to remove polling and output-step SQL. For `#5`, preserve the reason/distinct terminal status atomically for pending and running cancellation (running settlement currently overwrites the requested reason with the claim's stale error), reduce heartbeat-bound cancellation latency, provide detached calls, and clarify whether a running step finishes and records its result before terminal settlement. Terminal hooks must be restart-safe, at-least-once, and isolate poison deliveries. For `#4`, strict per-key claims and a fencing token remain necessary; soft grouping alone cannot satisfy the port's strict mutex documentation.

### Agent sessions (Pi)

`src/agents` runs agents on Pi Durable. `app.start()` migrates the `fabrial_pi` schema and the `fabrial.inbox` table under transactional advisory locks, builds the registry, and rejects missing live task kinds before admitting work.

**Storage and leases.** A Session is a Postgres commit log plus Pi's native `MemoryStorage` materializer. Pi validates transactions, documents, forks, copies, ancestry, and rewind reads; Postgres atomically persists the detached, validated write batch before adoption. The log stores JSON text, not jsonb, because Pi permits lone-surrogate strings that jsonb cannot represent. Reopening replays the log. There is no log compaction or indexed read path yet; memory and startup cost grow with Session history.

Each acquisition atomically increments a per-Session epoch and assigns an owner UUID. Leases last 30 seconds and renew every 10 seconds on the database clock. Acquisition never steals an unexpired lease. Every commit locks and updates the Session row with matching owner, epoch, and unexpired lease in the same transaction as the write batch, so expired or stale owners cannot write or release a successor's lease. Renewal failures abort the run's Chord Context; uncertain commit outcomes poison the owner and require reopening. Conductor's claim is not the Pi epoch: different executions share a Session, so the Pi lease is independently authoritative. The Conductor Session mutex still avoids paid or externally visible work during stale-worker overlap. Fencing is not an exactly-once guarantee for external effects.

Session keys are `${replyTo.provider}:${replyTo.threadId}`, or the interaction ID without chat. `provider` is the installation-specific plugin id; the Chat SDK thread id is kept verbatim. Unposted channel locators are rejected, never promoted to Session keys: post with `ctx.thread.post` first so core rebinds from the durable receipt. Conversations continue by agent name inside the Session.

**Bridge.** `ctx.agent` invokes the internal `fabrial.pi.run` workflow with the Session mutex. Submission ids derive from the caller's execution and operation id. Per-request identity is a request-keyed `fabrial.invocation` document committed right before submitting, so tools and instructions resolve the active request's actor rather than a conversation-wide current user. Forks have no Fabrial binding and cannot use managed outbound operations.

Workflow tools start children keyed by Pi task id and operation id. When only those children are running, the harness closes, releasing the lease, and the bridge waits on the runtime's execution branches. Results go into Pi documents before reopening; interrupted tools reconnect to the same child. Permissions and input validation use the registered workflow. Unauthorized workflow tools are removed from model discovery. Children inherit the originating request's actor, interaction, origin, and thread, with the driver's trigger and owner markers cleared so core does not treat them as observers.

Cancellation aborts the recorded request and task tree, not a newer run in the conversation. Child start intents and cancellation relationships commit before starting, and lost start receipts reconcile through a Session/task/operation dedupe key. Suspended cancellation uses `RuntimeWorkflow.onSettled` and `runtime.cancel`. Progress is best-effort, throttled, and limited to thinking and tool descriptions, never reasoning or raw stream output. Buffered replies get their own invocation documents, submit with stable ids and `whenBusy: "steer"`, and are consumed from routing state only after durable submission. The driver drains every accepted submission before returning, or persists the request set before releasing the harness to wait for children.

**Tools and state.** Fabrial tools become Pi tools; toolkits and instructions become Pi extensions and prompt sections; observers are Pi hooks. Managed thread posts, updates, and history use stable child executions keyed by Session, Pi task, and operation id. Their step receipts survive driver changes; an external I/O crash before the receipt remains at-least-once. `ctx.evaluate` in tools uses Pi memos. `ctx.start` is independent; `ctx.invoke` is reconnectable workflow work. Plugin clients are not automatically durable: use managed operations or workflow tools for side effects.

State uses Pi documents: an interaction family keyed by interaction id, a thread family, and rewindable, forkable conversation documents. Workflow reads and updates are memoized steps that take a short Session lease; an execution/operation receipt stores the returned value in the same Pi commit as the mutation, so a lost step receipt cannot apply it twice. Agent-scoped state is rejected in workflows because the state port carries no conversation selector. Tool changes are staged, schema-validated, and committed on the Session's serialized commit line together with a tool result receipt; Pi appends its transcript result in its next settlement commit. On replay the receipt returns the saved result without reapplying state. This works around Pi 1.0 having no tool-result transaction callback; it is not an atomic transcript append. Updates must be pure because they may run again against newer state.

**Registry and orphans.** Plugin observers are installed globally; agents select their toolkits explicitly. Installation covers plugin toolkits and the tools, toolkits, and state of agents defined through `f` before start. Discovery is instance-scoped, not exact workflow reachability: apps from the same `f` share definitions, unrelated instances do not install each other's agents, and unused agents of the same instance may still be installed. At start, Fabrial scans the latest task records across Sessions without taking leases and aborts live tasks whose kind is no longer installed (e.g. a toolkit removed from every agent): it acquires only the affected Sessions, uses Pi's abort cascade, and logs a warning per Session and task kind with the count. A Session held by another owner is skipped with a warning; startup does not fail.

**Remaining contract needs.**

- Explicit reachable agents on workflows for precise installation.
- Installation id in `ThreadRef` if literal `slack:<installation>:<thread>` keys are required independently of plugin ids.
- A conversation selector for agent-scoped state from workflows.
- A Pi tool settlement callback for truly atomic state and transcript result.

### Chat (Chat SDK)

`src/chat` connects one Chat SDK bot to all chat-capable plugins, with Postgres state on the instance's database. It supports reconstructed threads, lazy channel roots, DMs, ephemeral messages, text/Markdown/cards, edited messages, best-effort throttled progress, attributed bounded history, and routing state under `thread.state.fabrial`. `ThreadIO.updateState(fn)` is an atomic read-modify-write under the state adapter's token-owned per-thread lock; all routing-state writers use it. Lock contention retries for at most 10 seconds; leases last 60 seconds and are checked and renewed before writing, with release in `finally`. Keep callbacks pure, since a lost lease can retry them. State writes refresh a 30-day TTL. Card action ids and values are forwarded unchanged to core.

Start and stop map to SDK initialize and shutdown. Local SDK concurrency slots serialize arrivals without coalescing. Webhook ingress disables the SDK's pre-dispatch dedupe so failed durable ingress can be retried; core receives a stable platform message or action dedupe id. Channel surfaces stay provisional until the first post and cannot store routing state until then.

Native Slack stop events forward the authenticated stopper, thread, and stable stop receipt to `host.receiveCancellation`; core owns authorization and dedupe. Bare SDK `thread.signal` aborts carry no stopper identity and cannot authorize cancellation. Slack aborts the turn before delivering its native event, so attributing the abort to the message author would bypass the stop event's authorization.

Limitations:

- SDK adapters with a fixed platform name cannot represent a second installation under another plugin id without an id-rewriting adapter. Such aliases fail fast.
- Updaters are synchronous and must be bounded. The state lease is not a database fencing transaction if a process is suspended beyond the lease during a write.
- SDK native stop events have no unique webhook event ID; dedupe uses thread, stopper, and stopped streaming message IDs, scoped by core to the current interaction.
- The SDK Slack adapter swallows assistant status API errors, so missing assistant scopes cannot trigger the edited-message fallback automatically. Providers can opt into `status: "message"`. There is no attachment field in core's normalized message yet.

## Current boundaries

Live plugin clients are built once per app, as specified by `FabrialHost.clients`; they must not capture invocation identity. Core does not add a reaction acknowledgement to buffered replies because `ChatPort` has no reaction operation. Thread routing state is a read/write port, not a transaction or CAS: ingress for a given thread must be serialized by the state adapter, including across webhook replicas. The test runtime does not emulate missing Conductor capabilities or schedule cron timers.

## Send and subscriptions

`ctx.agent` **calls** an agent: it submits and waits for the answer. `ctx.send` **sends**: it submits and returns. The conversation is `[session, agent]`. An idle conversation starts a run. A busy one gets the input by kind and priority.

```ts
await ctx.send("deliver", supportAgent, {
	session: "channel", // "thread" (default) | "channel" | a custom key such as `ticket:${id}`
	input: message, // a ChatMessage, a string, or JSON
	kind: "message", // or "notification" with priority "urgent" | "normal" | "low"
	replyTo: ctx.thread, // default: this execution's thread; null for none
});
```

| Input                 | Rendered as                                                        | Delivery                                     |
| --------------------- | ------------------------------------------------------------------ | -------------------------------------------- |
| `message`             | `<message from="Alice" thread="…" request="…">text</message>`      | steers a busy run, or starts one             |
| `notification` urgent | `<notification source="…" kind="…" priority="urgent" request="…">` | steer                                        |
| `notification` normal | same                                                               | follow-up (next run)                         |
| `notification` low    | same                                                               | written; seen on the next run, does not wake |

Tools have the same `ctx.send`. A tool's send runs in a managed child execution through the same path, and `replyTo` defaults to the tool call's request.

Agents running sent input get a built-in `reply({ text, request? })` tool. It posts to the request's `replyTo`, by default the latest request in the run. The final answer of a sent run is not posted. Each input's request ID comes from the send's operation ID, so retries and redeliveries are submitted once. Sent input waits in `fabrial.inbox` until the conversation's driver admits it to Pi, then the row is deleted. Drivers wake on Postgres `NOTIFY` and poll every few seconds for missed notifications. A redelivery after the row was pruned is deduplicated by Pi's request ID.

Static subscriptions route a trigger's events into a conversation. They compete for trigger ownership like workflows, but never hold the thread, so every message in the channel or thread is delivered:

```ts
export const supportChannel = f.subscribe({
	name: "support-channel",
	source: slack.message({ channel: "C_SUPPORT" }),
	agent: supportAgent,
	session: "channel", // or (event) => `customer:${event.channelId}`
});
export const app = f.app({ workflows, subscriptions: [supportChannel] });
```
