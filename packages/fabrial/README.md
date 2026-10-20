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
	description: "Answers support questions.",
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

`f.app({ workflows, agents?, logger? })` builds the app and exposes `app.fetch`, `app.start`, `app.stop`, and authenticated adapter ingress through `app.host`. Plugins carry their values at definition and factories must stay side-effect free: importing the app with unset environment is safe. The database pool opens, credentials are validated, and connections start at `app.start()`, not at import or `f.app()`. Chat-capable plugins are served at `POST /<pluginId>/events`; routes await ingress before responding.

Durable operations require explicit, nonempty IDs. Repeated IDs are suffixed deterministically on replay. `fabrial:` is reserved for helper receipts. Completed receipts prevent repeated effects on replay; a crash during an external effect can still repeat it. Providers must supply idempotency or reconciliation when exactly-once effects matter.

## Agents and tools

- `f.defineAgent({ name, description, model, thinking?, instructions?, tools?, toolkits?, state?, subagents?, limits? })`. `name` is stable: it is the conversation's continuation key. `description` is required: it says what the agent does and when to use it, and is the tool description when the agent is a subagent. `instructions` are strings or functions rendered per run with a read-only context (`actor`, `requestedBy`, `clients`, `interaction`, `state(definition).get()`). Define agents before `app.start()`.
- `tools` lists tools and workflows. A workflow tool starts the workflow as a child of the agent run and returns its output; approvals are always workflow-backed.
- `f.defineTool({ name, description, input, idempotent?, run })`. `input` is a Standard Schema with Standard JSON Schema support (e.g. zod ≥ 4.2); the model sees its JSON Schema, and the app fails to start if a schema cannot be converted. Input is validated before `run`: invalid input never reaches `run` and goes back to the model as an error result. `run` returns a string (sent as is) or any JSON value (serialized). Tool `ctx` has `actor`, `requestedBy`, `clients`, `interaction`, `thread` (durable `post`/`update`/`history`, each with an id), `state`, `invoke`, `start`, `evaluate`, and `signal`.
- `idempotent: true` tools run again after a crash interrupts them; other tools give the model an interrupted result instead.
- `f.defineToolkit({ name, tools, instructions })` groups tools and instructions. Agents list app toolkits and plugin ids in `toolkits`.
- `f.defineState({ name, scope, schema, initial, render? })` is state agents see and change. Tools change it with `ctx.state(def).update(fn)`; the changes commit atomically with the tool result. Workflows use `ctx.state(def).get(id)` / `.update(id, fn)`. Agents listing a state in `state` see its `render` output in the prompt.
- `ctx.agent(id, agent, { input, output?, detached?, timeout? })` runs the agent durably and returns its final answer; it never posts automatically. With `output`, the agent is asked for a single JSON value, which is parsed and validated against the schema in a durable step. Invalid answers fail explicitly; there is no silent coercion or repair loop. The call is a child execution of its caller: cancelling the caller (or the caller failing permanently) cancels it and the workflows its tools started. `detached: true` keeps the agent run alive when the caller is cancelled. After `timeout` the caller fails; an attached call is cancelled, a detached one keeps running.

### Subagents

```ts
export const researcher = f.defineAgent({
	name: "researcher",
	description: "Investigates a question and returns findings with sources.",
	model: "anthropic/claude-haiku-4-5",
	tools: [search, readDocs],
});

export const lead = f.defineAgent({
	name: "lead",
	description: "Answers support questions in #help.",
	model: "anthropic/claude-sonnet-5",
	subagents: [researcher, { agent: "self", context: "fork" }],
	limits: { depth: 1, concurrent: 5 }, // defaults
});
```

- Entries are an agent, `"self"` (a copy of the lead), or `{ agent, context?: "fresh" | "fork", background?: "allow" | "always" | "never" }`. Names that collide with another subagent, a tool, a workflow, or `no_reply` are rejected at definition (plugin toolkit tools when the app starts).
- The model gets one tool per subagent, named after the agent like workflow tools (`researcher`, `bug-researcher` → `bug_researcher`) and described by its `description`: `researcher({ task, name?, background? })`. A call runs a child conversation in the lead's Session, configured from the subagent's definition (model, instructions, tools, toolkits, state); unset `model` and `thinking` follow the lead. Fresh children see only their task; `context: "fork"` children see the lead's transcript up to the delegating response.
- The call waits and returns the answer as data: `<subagent name="researcher-1" status="done">…escaped…</subagent>`. Pass `name: "researcher-1"` to continue that child with its history. A child that ends without an answer returns `status="failed"` with a generic message and an error id; the provider's error is logged with that id.
- The child is owned by the lead's tool call (Pi ownership): cancelling the lead aborts it, and a restart resumes it without submitting the task twice. Children have no chat binding, post no final replies, and get no `no_reply` tool; their tools and workflows (approvals included) act for the lead's current request: the same actor, thread, and approvers. Their activity shows on the lead's status (`researcher: Thinking…`).
- With `depth: 1` children get no subagent tools; a deeper `depth` passes the remaining levels down. More than `concurrent` children running at once in one conversation is a tool error the model can act on. Running children are counted from durable state (slots held by the call or the reporter task), so the limit holds across driver cycles and restarts. A slot frees in the commit that settles the child's answer, not when its holder task ends. A background child waiting on a workflow (e.g. an approval for days) does not hold the Session: when every live task waits on a workflow, the driver releases the lease and resumes when any of those workflows completes, and new messages and `ctx.agent` calls to the Session are processed meanwhile. A child's line on the lead's status clears as soon as it answers.
- Background: `background: true` (or an `"always"` entry) returns the name at once (`Started researcher-1 in the background…`). The child is owned by a background anchor task, and a reporter task submits the task and later reports `<subagent name="researcher-1" status="done|failed">…</subagent>` to the lead as a `followUp`, once, across restarts. Calling with the name of a background child steers it while it works and gives it new input when idle; each answer is reported once. Background children stay in the background when continued.
- `subagent_stop({ name })` aborts a child's current work (Pi `conversation.abort`); it reports nothing and stays usable. A prompt section lists the lead's children and whether each is working.
- When the run ends: under `ctx.agent` the call settles only after the lead has answered its children's reports, and returns that last answer. Under `ctx.send` and followed resources the driver keeps the Session while children work; reports arrive as follow-ups, and the lead's answer to each posts to the reply thread of the request that started the child. The run ends only once those answers posted. Child activity shows on the lead's status meanwhile.
- Aborts: aborting the lead's Pi run stops at the background boundary. Cancelling the run (its calling workflow, or a stop in the thread) aborts the background children that run started or continued.

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

Startup rejects equal-specificity owners on the same event whenever their filters may overlap. Literal, prefix, numeric-range, and existence disjointness is checked statically; uncertain combinations are conservatively rejected. Startup cannot know which **different event categories** a future chat message will carry. Such ties fail the routing workflow (`fabrial.route`) for that input, which starts nothing.

Chat integrations must provide trusted installation IDs and authenticated actors. The host ignores messages marked `author.isBot`. A stop in a thread (Slack's native stop button) cancels the current run of every conversation following the thread (`AgentPort.stop`: a Pi abort; the Session stays usable). Anyone in a followed thread may stop it, as anyone there may steer it; permissions stay on tools. Runtime cancellation itself must also be idempotent.

Thread state holds Fabrial's routing index: the conversations following the thread (`follows`, `[session, agent]` records, for every session kind). It keeps no ingress receipts: redeliveries are deduped by the `fabrial.ingress` emit's key (about one to two days, see below), and a retried `fabrial.route` resumes from its persisted decision and completed steps. Follows are subject to the 30-day thread-state TTL; a follow idle for longer lapses. There is no thread ownership, interaction, or reply buffer.

An unbound reservation (`handlerExecutionId: null`) expires at ingress after `reservationTimeout` (default `"10m"`, configurable as a duration string or milliseconds, below 30 days). Fresh messages replace expired reservations; replies do not extend the grace. Legacy unbound states without `reservedAt` are immediately considered stale. Bound handlers are not expired by this check. This also limits the impact of a redelivery older than the ingress dedupe window or a lost dispatch.

Channel surfaces remain unresolved until a post. The first durable post receipt supplies the real thread reference; every replay rebinds from that receipt. Handoffs and child invocations inherit this real reference, never a provisional channel locator.

## Approvals and terminal cleanup

Each request is its own execution (`fabrial.approval.request`), whose result stores the proposal, the assigned users and group ids, the requester, controls, and absolute expiry. The routing workflow checks every click against it before the waiting approval sees it: assigned users, and the current members of assigned groups (resolved when the click arrives, by the group defined once with `defineGroup`; an unknown or duplicate group id grants nothing). Unauthorized clicks get an ephemeral notice and never reach the approval. Requester cancellation is separate from approval authority; free text never changes an approval. Reply and approval timeouts must be below the 30-day routing-state TTL.

Each approval also starts a decision execution (`fabrial.approval.decision`). It subscribes to authorized clicks once, then starts one independent `fabrial.cleanup` execution per card: each approver's card, plus the requester's status card. Each cleanup posts its card, waits for the decision's result, and relabels the card without actions. That includes a card posted after the decision. The first authorized click decides. If the caller settles first, the approval is cancelled. At the absolute deadline it expires, and a resume never extends that deadline. A cleanup retries a failed post or update up to three times, then gives up on that card. Undelivered approver cards do not fail the request while another approver's card was delivered. If every approver card fails for good, the approval ends at once instead of at its deadline: the waiting workflow's `wait` throws `Approval failed: No approver could be reached`, and the status card shows that reason. The decision learns this through a `fabrial.approval.delivery` execution, which waits on the approver cleanups' results and settles before the decision only when none of them was delivered. The requester's status card does not count as an approver card. These workflows are framework internals, not application APIs.

The runtime's `RuntimeWorkflow.onSettled` hook must deliver success, failure, and cancellation, including cancellation while suspended, and retry failed delivery across restarts. Core emits `fabrial.execution.settled` from this hook.

Runtimes must provide race-safe correlated event retention: a click or presentation receipt emitted during the side effect preceding a wait cannot be lost because the worker registers its wait later. This is a required port capability, not a core Conductor shim.

## Testing

`fabrial/testing` runs apps on real Conductor; there is no in-memory engine. Tests need a Postgres: start one per test run in Vitest's `globalSetup` (e.g. `new PostgreSqlContainer("postgres:17").start()` from `@testcontainers/postgresql`, passed on with `project.provide`) or point at an existing database URL. Each test app runs on a queue of its own, but ingress is database-wide: give each test file its own database (e.g. `create database` in `beforeAll`), or apps running at once in other files route its events. `createTestApp` runs workflows with fake chat and agents, without models:

```ts
import { createTestApp } from "fabrial/testing";

const { app, chat, runtime, clock, settled, idle, stop } = createTestApp(f, {
	database: process.env.DATABASE_URL!,
	workflows: [hello],
});
await app.start();
const id = await runtime.invoke("hello", {}, { metadata });
console.log(await settled(id)); // { status: "completed", output: … }
await stop();
```

- `createTestApp(f, { database, workflows, agents?, routedAgents?, chat? })`: an app on Conductor with a `FakeChat` and `fakeAgents()`, fast polling, and one connection. `settled(id)` waits for an execution's outcome and `result(id)` reads it; `idle()` waits until every emitted event was dispatched and every execution of the app settled, for asserting that something did not happen. `clock` is Conductor's test clock: `clock.set(date)` freezes the app's time and `clock.advance(ms)` moves it; advance only after the wait it should end is registered. `stop()` stops the app and closes its connection. Spy on `app.host.clients()` or its client methods for fakes.
- `FakeChat`: inspectable threads, posts, DMs, and ephemeral notices, with `receive`, `click`, and `cancel` for ingress.
- `StubRuntime`: a runtime port for unit tests that run no workflow; it records registrations and emitted events.
- `testApp(f, { runtime, chat?, agents?, workflows })`: an app on explicit ports.

**Test reliability.** Ingress events, their dedupe, and routing span the database: an app's router also receives the events of other apps on it, fails on plugin events it doesn't know, and retries with backoff, so a shared database stalls `idle()` and routes events twice. Wait on the concrete effect a test asserts (`settled(id)`, a post, `expect.poll`); `idle()` waits for every event of the database and every execution of the app, so any unrelated retry delays it. Use `idle()` only to assert that something did not happen.

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

Ingress only verifies, normalizes and emits one `fabrial.ingress` event, deduped by the provider's delivery or message id. Dedupe of redelivered webhooks is bounded by Conductor's dispatch retention (about one to two days); a later redelivery routes again. The internal `fabrial.route` workflow, triggered by it and retried by Conductor, routes in durable steps: it resolves the actor, reads the thread's follow records, selects followers, else the most specific trigger (an agent's or a workflow's), and observers; persists that decision; writes follow records and subscribes handled threads; signals agent Sessions; and starts the owner (with the thread) and each observer (without it) with explicit metadata. A message nothing handles is ignored: no state, no subscription.

#### Temporary shims

All receipts, waits, and outputs live in Conductor's tables. No Fabrial-owned tables are added. Direct SQL and private context access are confined to `src/runtime/conductor` and tied to the pinned vendor revision. [PGCONDUCTOR.md](../../PGCONDUCTOR.md) describes the upstream changes that would remove them.

| Item  | Adapter behavior / remaining gap                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `#1`  | Memoized `cursor` and `after` waits scan retained dispatch receipts and return the consumed event's cursor. A Postgres bigint sequence orders the event stream; a database-wide transaction advisory lock serializes cursor capture, receipt insertion/commit, and observation, preventing concurrent emitters from committing behind a cursor. No JS-clock cursor. Regression coverage includes fast pre-registration clicks, stale exclusion, chained replies, delayed concurrent commit, and restart. Native indexed buffered waits are still wanted.                                                                                                                                                                                                           |
| `#2`  | Metadata is native execution metadata: invoke/start/emit pass it explicitly and dispatch passes an event's metadata to the executions it triggers. Partial overrides are shallow and affect only that operation. Event payloads keep a reserved `__fabrial` routing envelope (owner, dispatch ID) until one routing workflow replaces it; event payload fields `__fabrial` and `__fabrialOwner`, and operation IDs beginning `__fabrial:`, are reserved and rejected.                                                                                                                                                                                                                                                                                              |
| `#3`  | Per-attempt hooks (core's context and plugin workflow middleware) run as native Conductor middleware, whose `next` settles when an attempt returns, suspends, or is cancelled. Execution identity and metadata are native (`ctx.executionId`, `ctx.metadata`); only the claim that fences the custom race SQL is read from the private context. The live `TaskContext` stays inside the runtime; apps never see it.                                                                                                                                                                                                                                                                                                                                                |
| `#4`  | Removed: no workflow mutex keys or groups. Agent sessions add their own fencing (see below).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `#5`  | `invoke` is native `ctx.invoke` with `cancelWithParent` unless detached, plus `ctx.waitForResult`; a failed child reaches the caller's `catch` as `ExecutionError`. Cancellation (pending or running) is native and cascades to attached children transitively, also when a parent fails permanently. A native result timeout leaves the child running, so an attached call cancels it explicitly; a detached one keeps running. `start` is a native independent `ctx.invoke`.                                                                                                                                                                                                                                                                                     |
| `#6`  | Native subscriptions wake durable polling over events/executions/absolute timers. No worker remains occupied while suspended. Event branches each honor `after` (or the durable registration boundary); a winner is cached and losing subscriptions removed atomically under the native claim lock. Database timestamps select the earliest ready branch; stream order breaks event ties, then record order breaks remaining ties. Timers use `at` directly on every replay, never add remaining time to the original start. Fractional polling delays are rounded up for native bigint rescheduling. Polling can add up to the configured interval plus worker latency. Receipt scans and serialized observation are a prototype, not an indexed native race API. |
| `#7`  | Client `invoke` inserts on conflict, keeping the original execution **even after completion**, unlike native deduped client invoke (which updates/reinvokes). `ctx.invoke` returns the execution holding a dedupe key without touching it. Independent outputs are saved in a private step for execution branches of the custom race.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `#8`  | Concurrent emit redeliveries dedupe by `(event name, id)`; the first payload/metadata/cursor wins. Ingress emits one `fabrial.ingress` event per delivery; the routing workflow starts each workflow at most once per input.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `#12` | Native signals (PR #80, pinned): `onSignal`, `signal`, and `signalWithStart` map one-to-one. `ctx.signalWithStart` cannot override metadata natively, so a workflow's keyed start runs the client call in a step (at-least-once). Keyed starts are independent of the caller. Signals reach an attempt only on worker polls; a re-queued attempt overwrites the private output step.                                                                                                                                                                                                                                                                                                                                                                               |

#### Receipt retention and deployment

All dispatch receipts, including **filter-correlated** events, have immediate deletion disabled and **no TTL**. Thus buffering and emit dedupe cover the maximum approval wait (≤30 days), including restarts. Do not purge receipts while an active workflow can still reference their cursors; any future automatic retention must cover at least 30 days and protect active cursor-based waits. Explicit purging also ends dedupe for those receipts. Native orchestrators sharing `pgconductor.internal` must not re-enable deletion.

The shim adds one sequence, `pgconductor.__fabrial_event_position`, lazily under the stream lock; it adds no tables or columns. The database role needs permission to create/use it. Never reset/drop the sequence independently of receipts/cursors. All emitters must use this adapter for cursor-buffered events: native `emit` and older adapter receipts have no stream position. Drain legacy waits before upgrading; mixed old/new emitters are not a supported race-safe deployment. The advisory lock serializes only short database transactions, not user side effects. Retention is intentionally unbounded until upstream supplies an indexed buffered event API and safe compaction.

#### Terminal notifications

A restart-safe monitor calls `RuntimeWorkflow.onSettled` after terminal completion/failure/cancellation, never on suspension. Delivery is **at least once** (crash between the hook and receipt, or competing monitors); hooks must be idempotent. Core supplies this hook to clear thread routing state and emit `fabrial.execution.settled` with the execution ID as dedupe ID, so repeated hook deliveries still dispatch the event once. Hook failures are logged independently and retried in a cyclic scan; a poison hook does not block other executions, even beyond the first 100-row batch.

Without a hook the monitor emits settlement events for failures/cancellations that a handler wrapper cannot see (retry exhaustion, suspended cancellation). Successful notifications require the hook (core always supplies one); the no-hook fallback does not emit success events, avoiding an unbounded loop of success notifications from raw settlement observers. Reconciliation continues on a new instance with the same queue and registered catalog. Core's process-local cancellation callbacks cannot be reconstructed by this adapter after restart.

#### Contract coordination

The updated port's `cursor`/`after`, absolute `deadline`/`at`, consumed cursors, are implemented. Core captures cursors before approval/reply side effects. Tests include a real-Postgres approval through `f.app()` whose authenticated click arrives during card delivery, before the wait registers.

`PGCONDUCTOR.md` describes the upstream gaps: its Findings for **#1** (native wait, unresolved todo) and **cross-category dispatch** (first-submission ownership race) are historical for this adapter. The shims above close those adapter gaps, not the corresponding native Conductor gaps.

#### Upstream priorities

Keep `#1`, `#2`, and `#8` as P0. Native retained, idempotent emit receipts with monotonic positions and ≥30-day correlated buffering are needed to replace the fallback scans/sequence. Canonical owner dispatch precedence should also be native. Add claim-guarded, indexed races (`#6`) and independent start/result retrieval (`#7`) to remove polling and output-step SQL. For `#5`, preserve the reason/distinct terminal status atomically for pending and running cancellation (running settlement currently overwrites the requested reason with the claim's stale error), reduce heartbeat-bound cancellation latency, provide detached calls, and clarify whether a running step finishes and records its result before terminal settlement. Terminal hooks must be restart-safe, at-least-once, and isolate poison deliveries.

### Agent sessions (Pi)

`src/agents` runs agents on Pi Durable. `app.start()` migrates the `fabrial_pi` schema under a transactional advisory lock, builds the registry, and rejects missing live task kinds before admitting work.

**Storage and leases.** A Session is a Postgres commit log plus Pi's native `MemoryStorage` materializer. Pi validates transactions, documents, forks, copies, ancestry, and rewind reads; Postgres atomically persists the detached, validated write batch before adoption. The log stores JSON text, not jsonb, because Pi permits lone-surrogate strings that jsonb cannot represent. Reopening replays the log. There is no log compaction or indexed read path yet; memory and startup cost grow with Session history.

Each acquisition atomically increments a per-Session epoch and assigns an owner UUID. Leases last 30 seconds and renew every 10 seconds on the database clock. Acquisition never steals an unexpired lease. Every commit locks and updates the Session row with matching owner, epoch, and unexpired lease in the same transaction as the write batch, so expired or stale owners cannot write or release a successor's lease. Renewal failures abort the run's Chord Context; uncertain commit outcomes poison the owner and require reopening. Conductor's claim is not the Pi epoch: different executions share a Session, so the Pi lease is independently authoritative. The Conductor Session mutex still avoids paid or externally visible work during stale-worker overlap. Fencing is not an exactly-once guarantee for external effects.

Session keys are computed from the event (see [Conversations](#conversations)): `<provider>:<threadId>` for `session: "thread"`, `<provider>:channel:<channelId>` for `"channel"`, `app:<key>` for an app's key, and `<event>:<id>` for thread sessions of input without a thread. A `ctx.agent` call runs in a Session of its own, `run:<executionId>:<operationId>`: it never shares a transcript with a thread's conversation and never follows a thread. Conversations continue by agent name inside the Session. `InvocationMetadata.interactionId` remains the request correlation id (it keys request ids); renaming it is a follow-up.

**Bridge.** One keyed `fabrial.session` execution per Session key hosts the Pi harness while the Session has work. Every input reaches it as an `input` signal through `signalWithStart` (sent input, replies in a thread a call drives, child results, stops); signal ids dedupe redeliveries. A `ctx.agent` call is instead the input of its own Session's execution, a child of the caller (`ctx.invoke` with `cancelWithParent` unless detached, plus `ctx.waitForResult`); that execution returns the call's outcome, also stored on the call's run record. Submission ids derive from the caller's execution and operation id. Per-request identity is a request-keyed `fabrial.invocation` document committed right before submitting, so tools and instructions resolve the active request's actor rather than a conversation-wide current user. Forks have no Fabrial binding and cannot use managed outbound operations.

Workflow tools start children keyed by Pi task id and operation id. A child's settlement is a `child` signal to its Session (only children a Session waits for carry `piChild`), which records the result in a Pi document and resumes the waiting tool in process. When every live task waits on a workflow (an approval, say), the execution closes the harness, releasing the lease, and returns; the result's signal starts the next one. This Pi build has no `Harness.wake()`: idle is `harness.inspect()` plus that release rule, re-checked after every storage commit. Interrupted tools reconnect to the same child. Permissions and input validation use the registered workflow. Unauthorized workflow tools are removed from model discovery. Children inherit the originating request's actor, interaction, origin, and thread, with the driver's trigger and owner markers cleared so core does not treat them as observers.

Cancellation aborts the recorded request and task tree, not a newer run in the conversation. Child start intents and cancellation relationships commit before starting, and lost start receipts reconcile through a Session/task/operation dedupe key. Conductor cancellation is the source of truth for a call: when its Session execution is cancelled while running, the Session aborts the call's Pi run in process (best effort) before closing Pi, so no further model calls happen. Cancelled while suspended, nothing runs: its workflow tools are cancelled natively with it, and the call's Pi Session is left as it is, never reopened (its storage is retained; see Phase 3). Progress is best-effort, throttled, and limited to thinking and tool descriptions, never reasoning or raw stream output. Buffered replies get their own invocation documents, submit with stable ids and `whenBusy: "steer"`, and are consumed from routing state only after durable submission. A call's run drains the buffer once more before it settles.

**Tools and state.** Fabrial tools become Pi tools; toolkits and instructions become Pi extensions and prompt sections; observers are Pi hooks. Managed thread posts, updates, and history use stable child executions keyed by Session, Pi task, and operation id. Their step receipts survive driver changes; an external I/O crash before the receipt remains at-least-once. `ctx.evaluate` in tools uses Pi memos. `ctx.start` is independent; `ctx.invoke` is reconnectable workflow work. Plugin clients are not automatically durable: use managed operations or workflow tools for side effects.

State uses Pi documents: a thread family per Session and rewindable, forkable conversation documents. **Thread-scoped state belongs to the thread's Session**: workflows with a thread and the conversations following it (keyed by that thread) share it. A `ctx.agent` call runs in its own Session, so its tools read and write that call Session's state, not the thread's; pass what the agent needs in `input` and write results back from the workflow. Workflow reads and updates are memoized steps that take a short Session lease; an execution/operation receipt stores the returned value in the same Pi commit as the mutation, so a lost step receipt cannot apply it twice. Workflow state needs a posted thread: an unposted channel locator is rejected (post with `ctx.thread.post` first). Agent-scoped state is rejected in workflows because the state port carries no conversation selector. Tool changes are staged, schema-validated, and committed on the Session's serialized commit line together with a tool result receipt; Pi appends its transcript result in its next settlement commit. On replay the receipt returns the saved result without reapplying state. This works around Pi 1.0 having no tool-result transaction callback; it is not an atomic transcript append. Updates must be pure because they may run again against newer state.

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
- SDK native stop events have no unique webhook event ID; dedupe uses thread, stopper, and stopped streaming message IDs.
- The SDK Slack adapter swallows assistant status API errors, so missing assistant scopes cannot trigger the edited-message fallback automatically. Providers can opt into `status: "message"`. There is no attachment field in core's normalized message yet.

## Current boundaries

Live plugin clients are built once per app, as specified by `FabrialHost.clients`; they must not capture invocation identity. Core does not add a reaction acknowledgement to buffered replies because `ChatPort` has no reaction operation. Thread routing state is a read/write port, not a transaction or CAS: ingress for a given thread must be serialized by the state adapter, including across webhook replicas. The test runtime does not emulate missing Conductor capabilities or schedule cron timers.

## Conversations: triggers, following, and send

Agents receive events, like workflows. A trigger on an agent delivers the event to one of its conversations, keyed by `[session, agent]`; list such agents in `f.app({ workflows, agents })`.

```ts
export const support = f.defineAgent({
	name: "support",
	description: "Answers customer questions in #help.",
	triggers: [slack.mentioned({ channel: "C_HELP" })],
	session: "thread", // default; "channel", or (event) => key (namespaced as `app:<key>`)
	home: slack.channel("C_BUGS"), // for triggers whose events have no thread
});
export const app = f.app({ workflows, agents: [support] });
```

Session keys come from the event: `<provider>:<threadId>` for `"thread"`, `<provider>:channel:<channelId>` for `"channel"`, the app's key otherwise. An event without a thread (a Sentry issue) starts a `"thread"` conversation keyed by the event; its first final reply creates a thread in the `home` channel, which the conversation follows.

An incoming chat message is routed in this order:

1. **Followers.** An agent follows every thread it receives a message from, so every later message there reaches the same conversation, mention or not: it steers a running run or starts the next one. Every followed thread, whatever the session kind, holds a follow record `{ session, agent }` in its Chat SDK state; ingress reads it with the redelivery check, so routing needs no table. A message naming one agent (`@agent-name`) goes only to that agent, which then follows the thread too.
2. **Triggers.** Otherwise the most specific matching trigger wins, an agent's or a workflow's. Equal specificity on overlapping filters fails `app.start()`; `observe: true` workflow triggers run alongside.
3. Otherwise the message is ignored.

An incoming message goes first to the conversations following its thread (a message mentioning one `@agent-name` goes only to that agent, which then follows the thread too), otherwise to the most specific matching trigger, an agent's or a workflow's. Workflows don't receive chat replies: `ctx.thread.waitForReply`, `ask`, `nextReply`, `ctx.handoff`, and the `fabrial.reply` event are removed.

`ctx.agent` **calls** an agent: it submits and waits for the answer. `ctx.send` **sends**: it submits and returns. An idle conversation starts a run. A busy one gets the input by kind and priority. The conversation is found as if the agent's trigger had matched the input: by the input's thread, or `home`; for other input, relative to the sender's thread. The thread is followed afterwards, so a routing workflow picks an agent once and later replies go straight to it.

```ts
await ctx.send("deliver", supportAgent, {
	input: message, // a ChatMessage, a string, or JSON
	home: slack.channel("C_BUGS"), // where the conversation lives when the input has no thread
	// Options: input, home? (where input without a thread lives), session? (the agent's by default), priority? (notifications).
});
```

| Input                 | Rendered as                                            | Delivery                                     |
| --------------------- | ------------------------------------------------------ | -------------------------------------------- |
| `message`             | `<message from="Alice" thread="…">text</message>`      | steers a busy run, or starts one             |
| `notification` urgent | `<notification source="…" kind="…" priority="urgent">` | steer                                        |
| `notification` normal | same                                                   | follow-up (next run)                         |
| `notification` low    | same                                                   | written; seen on the next run, does not wake |

Tools have the same `ctx.send`. A tool's send runs in a managed child execution through the same path, and the reply goes where the tool call's request replies.

The agent's final answer to sent input posts automatically to the input's `replyTo`. When several inputs steer one run, they share its answer, which posts once, to the newest one's thread. The post is a managed effect keyed by the answer, so a restart or redelivery posts it once. Runs that answer input with a reply thread also get a built-in `no_reply({ reason? })` tool: called alone, it ends the run without posting, e.g. for chatter or a notification that needs no answer. The reason stays in the transcript. Input with `replyTo: null` posts nothing. A sent run shows progress ("Thinking…", tool descriptions, or "Waiting for the model provider…" while a model call waits to retry) on the reply thread of its latest request. Each input's request ID comes from the send's operation ID, so retries and redeliveries are submitted once. Sent input is a signal to the Session's execution, which admits it to Pi; a redelivery is deduplicated by the signal id and by Pi's request ID.

**Errors.** Transient model errors are retried durably for about 5 minutes in production (`MODEL_RETRY`: up to 9 retries, 2 s doubling to a 60 s cap). If sent input still ends with a model error, or no model is configured, the driver posts one error reply where its final reply would have gone ("I couldn't reach the model provider…" when the retries ran out, otherwise "I hit an error…"), with an error id that is also logged next to the provider's error text. Redeliveries and restarts don't post it twice. Aborted, reset, or stale input gets no reply. `createIntegrationApp({ modelRetries })` turns retries on: `true` for the production policy, or a policy such as `{ baseDelayMs: 5, maxRetries: 1 }` for fast tests.

### Following other things

`ctx.follow` makes a conversation follow a resource besides its threads, from a workflow or a tool; `ctx.unfollow` stops it. A follow lasts until `unfollow`: merge and close events are delivered like any other, and the agent decides. GitHub provides `github.pullRequest("acme/app#42")` (or `{ repo, number }`):

```ts
await ctx.follow("follow-pr", github.pullRequest("acme/app#42"), {
	priority: { comment: "urgent" }, // per notification kind, over the plugin's default
	events: "all", // also the kinds the plugin leaves out by default (GitHub: labels and CI)
});
await ctx.unfollow("unfollow-pr", github.pullRequest("acme/app#42"));
```

- **Target.** From a tool, or a workflow an agent tool started, the calling agent's conversation follows, and final replies to its notifications go where the current request replies (the conversation's home, e.g. the Slack thread). From other workflows, pass `to: { agent, session, home? }`, or the `conversation` reference a `ctx.send` receipt returns (`to: receipt.conversation`, optionally with `home`); `home` defaults to the workflow's thread. Following again replaces the record's options.
- **Storage.** The follow is a record `{ session, agent, resource, replyTo, priority?, events? }` in `resourceFollows` of the resource thread's Chat SDK state (for GitHub, the PR's thread). There is no Fabrial table; the 30-day state TTL applies, refreshed on every write.
- **Delivery.** When a plugin emits an event, ingress asks the plugin's resource kinds which threads it belongs to, reads their follow records, and delivers a `notification` to each follower with `signalWithStart`: the plugin renders it (and drops untrusted authors), the record's `events` selects kinds, and the priority is the record's override, then the plugin's default, then `normal`. Request ids are `${event id}:${session}:${agent}`, so a redelivered webhook is delivered once. Follows never own threads: triggers on the same events still run.
- **Plugin SPI.** A plugin adds `resources: { [kind]: { key(ref), thread(ref), events, threads(event), notify(event), defaults } }` and a helper built on `resource(pluginId, kind, ref)`.
- **Writing elsewhere** is a plugin tool: `github_comment`, `github_reply_to_review`, `slack_post`. Rendered notifications carry the ids those tools take (`pr="acme/app#42" review="991"`).
