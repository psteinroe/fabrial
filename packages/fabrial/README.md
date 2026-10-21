# fabrial

Durable workflows, AI agents, and human approvals in TypeScript, on one Postgres database. Fabrial is one API over three runtimes that stay internal: [PG Conductor](https://github.com/psteinroe/postgres-conductor) runs workflows, [Pi Durable](https://earendil.com/posts/pi-durable/) runs agents, and [Chat SDK](https://chat-sdk.dev) talks to Slack, GitHub, and Linear. Apps never import or configure them.

Status: preview. The package has three entry points:

| Import            | For                                                                                                     |
| ----------------- | ------------------------------------------------------------------------------------------------------- |
| `fabrial`         | App authors: `createFabrial`, `defineUser`, `defineGroup`, `weeklyRotation`, and the public types.      |
| `fabrial/plugin`  | Plugin authors: `definePlugin`, `defineEvent`, `trigger`, `resource`, the chat helpers, and hook types. |
| `fabrial/testing` | Tests: `createTestApp`, `createIntegrationApp`, `scriptedModels`, `FakeChat`.                           |

The model, in the order this README teaches it: an **instance** holds configuration; on it you define **agents** and **workflows**; **triggers** start them from events. Workflows are also tools for agents, and a workflow can wait for a human **approval**. Sessions, follows, subagents, state, plugins, and testing come after.

Code blocks marked `ts check` in the READMEs are compiled by `examples/acme/tests/unit/readme.test.ts`. Names they use without defining (`f`, `support`, `billing`, `lookupOrder`, `z`, …) come from [`examples/acme/tests/docs/prelude.ts`](../../examples/acme/tests/docs/prelude.ts).

## Hello world

One agent that answers Slack mentions:

```ts check
import { createFabrial } from "fabrial";
import { slack } from "@fabrial/slack";

const f = createFabrial({
	database: process.env.DATABASE_URL, // Postgres: workflows, agent sessions, and chat state
	models: { anthropic: { apiKey: process.env.ANTHROPIC_API_KEY } },
	plugins: [
		slack({
			botToken: process.env.SLACK_BOT_TOKEN!,
			signingSecret: process.env.SLACK_SIGNING_SECRET!,
			workspace: "acme", // identity namespace for Slack users
		}),
	],
});

const hello = f.defineAgent({
	name: "hello",
	description: "Answers questions in Slack.",
	model: "anthropic/claude-sonnet-5",
	instructions: "Answer briefly and kindly.",
	triggers: [slack.mentioned()],
});

export const app = f.app({ workflows: [], agents: [hello] });
await app.start(); // migrates Postgres, validates credentials, starts workers
// Serve app.fetch (Slack posts to /slack/events); call app.stop() on shutdown.
```

Each mention starts or continues the conversation of its thread, and the agent's final answer posts in the thread on its own. Later messages in that thread reach the same conversation, mention or not.

## A workflow with an approval

A refund needs someone from billing. The workflow is a tool of the agent: the agent proposes, a person decides.

```ts check
const refund = f.defineWorkflow({
	name: "refund",
	description: "Refund an order after billing approves it.",
	input: z.object({ orderId: z.string(), amount: z.number().positive(), reason: z.string() }),
	async run(input, ctx) {
		const order = await ctx.step("order", () => ctx.clients.shop.order(input.orderId));
		const decision = await ctx.waitForApproval("approve", {
			title: `Refund ${input.amount} € on ${input.orderId}?`,
			details: { ...input, customer: order.customer }, // shown verbatim; the approval binds to it
			approvers: billing, // users and groups: static, a rotation, or a Slack user group
			timeout: "24h",
		});
		if (!decision.approved) return { status: decision.status, reason: decision.comment ?? null };
		return ctx.step("refund", () => ctx.clients.shop.refund(input.orderId, input.amount));
	},
});

const desk = f.defineAgent({
	name: "desk",
	description: "Handles order questions and refunds in #support.",
	model: "anthropic/claude-sonnet-5",
	tools: [lookupOrder, refund], // a workflow is a tool named after it
	triggers: [slack.message({ channel: "C_SUPPORT" })],
});

export const app = f.app({ workflows: [refund], agents: [desk] });
```

When the agent calls `refund`, each billing member gets a card in their DMs with the proposal and Approve/Reject, and the thread shows a status card with [Cancel request]. The tool call waits durably, across restarts and deploys, and the agent gets the workflow's output.

## 1. The instance

`createFabrial({ database, models, plugins, identity })`:

- `database`: one Postgres connection string. Conductor, Pi, and Chat SDK keep their state there, each in its own schema. **One Fabrial app per database**: ingress events and their dedupe are database-wide, so two different apps on one database route each other's events. Instances of the same app (and versions during a rollout) may share it.
- `models`: credentials per provider (`{ anthropic: { apiKey } }`). Agents and `ctx.evaluate` name models `"provider/model"`.
- `plugins`: `slack(…)`, `github(…)`, `linear(…)`, or your own. **One installation per provider** in this preview: one Slack workspace, one GitHub installation, one Linear organization.
- `identity`: users (`defineUser({ id, name, identities })`), which map chat identities to people. Groups are not listed here: define them with `f.defineGroup({ id, members })` or `f.defineGroup({ id, resolve })` and reference them where they are used.

Keep the instance in a module that imports plugins and users only, never workflows, so every definition file can import `f` without cycles. Plugin factories are side-effect free: nothing connects or validates credentials until `app.start()`, so importing the app with unset environment is safe.

`f.defineAgent`, `f.defineTool`, `f.defineToolkit`, `f.defineWorkflow`, `f.defineState`, and `f.defineGroup` type `ctx.clients` and plugin toolkit ids from the instance's plugins. Definitions are plain data.

`f.app({ workflows, agents?, logger? })` builds the app. `workflows` lists every workflow that runs, including those agents call as tools; `agents` lists the agents with `triggers` (agents only reached by `ctx.agent` or `ctx.send` need no entry). The app has `fetch` (one handler for every plugin route; chat plugins are served at `POST /<pluginId>/events`, and routes respond once the event is stored durably), `start()`, `stop()`, `emit(event, payload, options?)`, and `clients()`.

## 2. Agents and tools

`f.defineAgent({ name, description, model, thinking?, instructions?, tools?, toolkits?, state?, subagents?, limits?, triggers?, session?, home? })`:

- `name` is stable: it keys the agent's conversations. `description` is required: what the agent does and when to use it (the tool description when it is a subagent).
- `instructions`: strings, or functions rendered per run with a read-only context (`actor`, `clients`, `state(definition).get()`).
- `tools`: tools and workflows. `toolkits`: plugin ids (`"slack"`, `"github"`) and app toolkits (`f.defineToolkit({ name, tools, instructions })`). Listing a plugin without a toolkit fails at `app.start()`.
- `triggers`, `session`, `home`: see [Triggers](#4-triggers) and [Sessions and conversations](#sessions-and-conversations).

`f.defineTool({ name, description, input, idempotent?, run })`: `input` is a Standard Schema with JSON Schema support (zod ≥ 4.2). The model sees its JSON Schema; invalid input goes back to the model as an error and never reaches `run`. `run` returns a string (sent as is) or any JSON value. `idempotent: true` tools run again after a crash interrupted them; other tools give the model an interrupted result. The tool `ctx` has `actor`, `clients`, `thread` (durable `post`/`update`/`history`, each with an id), `state`, `invoke`, `start`, `evaluate`, `send`, `follow`, `unfollow`, and `signal`.

A workflow **calls** an agent with `ctx.agent`: it waits for the answer and posts nothing. With `output`, the answer is a JSON value validated against the schema and typed from it:

```ts check
const triage = f.defineWorkflow({
	name: "triage",
	input: z.object({ text: z.string() }),
	async run({ text }, ctx) {
		const verdict = await ctx.agent("classify", support, {
			input: text,
			output: z.object({ urgent: z.boolean(), summary: z.string() }),
		});
		return verdict.urgent ? `Urgent: ${verdict.summary}` : verdict.summary;
	},
});
```

Without `output`, `ctx.agent` returns the answer text. An answer that doesn't match the schema fails the call; there is no repair loop. The call is a child of the workflow: cancelling the workflow cancels it, unless `detached: true`. After `timeout` the call fails (an attached call is cancelled, a detached one keeps running). Each call runs in a Session of its own, never in a thread's conversation.

## 3. Workflows

`f.defineWorkflow({ name, description?, input?, output?, triggers?, cron?, concurrency?, retries?, run })`. `run(input, ctx)` receives the trigger's payload, or the typed `input` when the workflow is invoked, started, or called as a tool.

Workflows replay from the top after a suspension or crash. Every durable operation takes an explicit, nonempty id as its first argument, and a completed operation returns its saved result on replay. Repeated ids in one execution are suffixed (`a`, `a#1`, …); the `fabrial:` prefix is reserved. A renamed id runs again; there is no call-order determinism or version pinning.

| `ctx`                                                                      |                                                                                                                  |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `thread`                                                                   | Where the execution replies (`post`, `update`, `history`); `undefined` for observers and runs without a surface. |
| `clients`, `actor`, `executionId`, `signal`                                | Plugin clients, who asked for the work, diagnostics, cancellation.                                               |
| `step(id, fn)`, `sleep(id, duration)`                                      | A memoized step; a durable timer.                                                                                |
| `agent(id, agent, { input, output?, detached?, timeout? })`                | Call an agent and wait for its answer.                                                                           |
| `send(id, agent, { input, home?, session?, priority? })`                   | Hand input to a conversation and return ([Sessions](#sessions-and-conversations)).                               |
| `follow`, `unfollow`                                                       | Make a conversation follow a resource ([Following](#following-resources)).                                       |
| `evaluate(id, { model, state, questions })`                                | A classifier judgment ([`ctx.evaluate`](#ctxevaluate)).                                                          |
| `state(definition).get(id)`, `.update(id, fn)`                             | Session state ([State](#state)).                                                                                 |
| `waitForApproval`, `requestApproval`, `race`, `timer`                      | [Approvals](#5-workflow-tools-and-approvals).                                                                    |
| `invoke(id, workflow, input, { detached? })`, `start(id, workflow, input)` | Call a child workflow and wait for it, or start an independent one.                                              |
| `isMember(principal, group)`                                               | Current group membership.                                                                                        |

A completed step is not an exactly-once effect: a crash during an external call can repeat it. Use provider idempotency or reconciliation where that matters.

## 4. Triggers

Triggers are plugin helpers on workflows and agents: `slack.mentioned({ channel? })`, `slack.message({ channel })`, `github.pullRequestOpened({ repo })`, `linear.issueCreated({ team })`, …. `cron: [{ schedule, replyTo? }]` schedules a workflow.

- **One owner.** When several triggers match an event, the most specific one receives it, an agent's or a workflow's. Specificity is the trigger's `specificity`, by default the number of filtered fields. Two owners with equal specificity whose filters may overlap make `app.start()` fail; ties startup cannot see (a message in two chat event categories) fail that event's routing instead.
- **Observers.** `observe: true` on a workflow trigger runs it as well, independently and without `ctx.thread`.
- **Chat messages** go first to the conversations following their thread (a message naming one `@agent` goes only to that agent), then to the most specific trigger; otherwise they are ignored. Workflows don't receive replies: replies belong to the conversations following the thread.
- **Events without a thread** (a cron run, a custom event): a workflow trigger's `replyTo` and an agent's `home` say where `ctx.thread` points or where the conversation lives; the thread is created by the first post.

## 5. Workflow tools and approvals

A workflow in an agent's `tools` is a tool named after it (`run-sql` → `run_sql`), described by its `description`, with its `input` schema. A call starts the workflow as a child of the agent's run and returns its output; actor, thread, and approvers are those of the request the agent is answering. Approvals are always workflow code, so the check sits next to the effect it guards.

`ctx.waitForApproval(id, options)` requests and waits. `ctx.requestApproval(id, options)` returns a handle: `approvers`, `decision()` (for `ctx.race`), `wait(id)`, and `cancel(id, reason)`.

| Option              |                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `title`, `details`  | The proposal. `details` (any JSON) is shown verbatim in a code block; the approval binds to it.                         |
| `approvers`         | Users and groups. Group members are resolved when a click arrives.                                                      |
| `timeout`           | A duration (`"24h"`) or ms, below 30 days. At the deadline the approval expires.                                        |
| `card`              | `{ text?, fields? }`, shown after the standard content; it never replaces it.                                           |
| `destination`       | A surface (`slack.channel("C_OPS")`) that gets one approver card instead of each approver's DM. It grants no authority. |
| `requesterControls` | Show [Cancel request] on the requester's status card (default `true`).                                                  |

The decision is `{ status: "approved" | "rejected" | "expired" | "cancelled", approved, decidedBy, comment?, decidedAt }`; `comment` is the approver's rejection reason or the reason given to `cancel`.

- **What a card shows.** Title, details, the verified requester (the authenticated actor, never model text), the source thread (a link where the plugin provides one, as Slack does), and the deadline. A closed card keeps them and adds the outcome, when it closed, and a rejection's reason.
- **Who decides.** Only assigned users and current members of assigned groups. Anyone else who clicks gets an ephemeral notice, and the click never reaches the workflow. The first eligible decision wins: no quorum, no editing (cancel and request again). Free text in the thread never changes an approval.
- **Reject** opens a modal for an optional reason where the provider has modals (Slack); elsewhere it decides at once. Approve carries no text.
- **Failure.** If no approver card can be posted, the approval fails at once (`wait` throws `Approval failed: No approver could be reached`). Cancelling the waiting workflow cancels the approval.

```ts check
const deploy = f.defineWorkflow({
	name: "deploy",
	input: z.object({ service: z.string(), version: z.string() }),
	async run(input, ctx) {
		const approval = await ctx.requestApproval("approve", {
			title: `Deploy ${input.service} ${input.version}?`,
			details: input,
			approvers: billing,
			destination: slack.channel("C_RELEASES"),
			card: { fields: [{ label: "Risk", value: "low" }] },
		});
		const first = await ctx.race("wait", {
			decision: approval.decision(),
			timeout: ctx.timer("1h"),
		});
		if (first.key === "timeout") {
			await approval.cancel("cancel", "No answer within an hour");
			return "timed out";
		}
		return first.value.approved ? "deploying" : `declined: ${first.value.comment ?? "no reason"}`;
	},
});
```

## Advanced

### Sessions and conversations

| Term             | Meaning                                                                                                                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Session          | A Pi Session: a durable log with a key. Agents hold their conversations in Sessions.                                                                                                                                                          |
| `session` option | How an agent picks the Session for an event: `"thread"` (default) is one per chat thread (`<provider>:<threadId>`), `"channel"` one per channel (`<provider>:channel:<channelId>`), and a function `(event) => key` an app key (`app:<key>`). |
| Conversation     | One agent's transcript in a Session, keyed `[session, agent name]`. Several agents can have conversations in one Session.                                                                                                                     |
| Call Session     | Each `ctx.agent` call runs in a Session of its own (`run:<executionId>:<operationId>`), reclaimed 7 days after the call settled.                                                                                                              |
| State scope      | `"session"` state has one value per Session; `"agent"` state one value per conversation ([State](#state)).                                                                                                                                    |

`ctx.send(id, agent, { input, home?, session?, priority? })` delivers input to a conversation as if the agent's trigger had matched it and returns `{ requestId, conversation }`. A chat message is rendered with attribution and steers a busy run or starts one. Any other JSON is a notification: `urgent` steers, `normal` (default) runs next, `low` is written for the next run without waking the agent. `session` overrides the agent's option, so a router can give each thread its own conversation of a channel-session agent. The thread is followed afterwards, so later replies go straight to the agent.

- **Replies.** The final answer posts to the input's thread (or the conversation's home) on its own. Inputs that steer one run share its answer, which posts once, to the newest one's thread. Answers are posted **at least once**: Chat SDK posts carry no idempotency key, so a crash after the provider accepted a post and before its receipt repeats it. A run ends only once its posts settled; a post that fails for good is logged.
- **`no_reply({ reason? })`**: a built-in tool that ends the run without posting (chatter, a notification that needs nothing).
- **Progress** ("Thinking…", tool descriptions, "Waiting for the model provider…") shows on the reply thread.
- **Errors.** Transient model errors are retried durably for about 5 minutes. If the input still fails, one error reply with an error id posts where the answer would have.
- **Stop.** A stop in a thread (Slack's native stop button) aborts the current run of every conversation following it; the conversation stays usable. Anyone in a followed thread may stop or steer it: permissions live on tools.

### Following resources

`ctx.follow(id, resource, options?)` makes a conversation follow something besides its threads; the resource's events arrive as notifications until `ctx.unfollow`. GitHub provides `github.pullRequest("acme/app#42")`:

```ts check
const watch = f.defineTool({
	name: "watch_pr",
	description: "Follow a pull request's reviews in this conversation.",
	input: z.object({ pr: z.string() }),
	async run({ pr }, ctx) {
		await ctx.follow("follow", github.pullRequest(pr), { priority: { comment: "urgent" } });
		return `Following ${pr}.`;
	},
});
```

From a tool (or a workflow an agent's tool started) the calling conversation follows, and answers to notifications post where the current request replies. Other workflows pass `to: { agent, session?, home? }` or the `conversation` of a `ctx.send` receipt. `events: "all"` adds the kinds a plugin leaves out by default (GitHub: labels and CI). A follow is a record in the resource thread's Chat SDK state, which expires 30 days after its last write. Merge and close are delivered like other events; the agent decides when to unfollow.

### Subagents

```ts check
const researcher = f.defineAgent({
	name: "researcher",
	description: "Investigates a question and returns findings.",
	model: "anthropic/claude-haiku-4-5",
	tools: [lookupOrder],
});
const lead = f.defineAgent({
	name: "lead",
	description: "Answers hard order questions.",
	model: "anthropic/claude-sonnet-5",
	subagents: [researcher, { agent: "self", context: "fork" }],
	limits: { depth: 1, concurrent: 5 }, // the defaults
});
```

- Each entry is an agent, `"self"`, or `{ agent, context?: "fresh" | "fork", background?: "allow" | "always" | "never" }`, and becomes one tool named after the agent: `researcher({ task, name?, background? })`. Name collisions with tools fail at definition.
- A call runs a child conversation in the lead's Session and returns `<subagent name="researcher-1" status="done">…</subagent>`; pass `name` to continue that child. `"fresh"` children see only their task, `"fork"` children the lead's transcript so far. Unset `model` and `thinking` follow the lead.
- `background: true` returns at once; the answer reaches the lead later as input, once. `subagent_stop({ name })` aborts a child's current work.
- Children post nothing themselves; their tools act for the lead's current request (same actor, thread, approvers). Cancelling the lead aborts them. More than `concurrent` running children is a tool error the model can act on.

### State

```ts check
const Notes = f.defineState({
	name: "notes",
	scope: "session",
	schema: z.object({ notes: z.array(z.string()) }),
	initial: () => ({ notes: [] }),
	render: (s) => (s.notes.length ? `Known so far:\n- ${s.notes.join("\n- ")}` : undefined),
});
const note = f.defineTool({
	name: "note",
	description: "Remember a finding.",
	input: z.object({ text: z.string() }),
	async run({ text }, ctx) {
		await ctx.state(Notes).update((s) => {
			s.notes.push(text);
		}); // committed with the tool result
		return "noted";
	},
});
const seed = f.defineWorkflow({
	name: "seed",
	async run(_input, ctx) {
		await ctx.state(Notes).update("seed", (s) => ({ notes: [...s.notes, "Seeded."] }));
	},
});
```

- `scope: "session"`: one value per Session, shared by the workflows on that Session's thread and every conversation in it. A `ctx.agent` call's tools see their call Session's value, not the thread's: pass what the agent needs in `input`. Workflows can use state once their thread exists (posted).
- `scope: "agent"`: one value per conversation, for tools only (a type error in workflows).
- Agents listing a state in `state` see its `render` output in their prompt. Tool updates commit with the tool result; workflow updates are optimistic and retry on conflict. Updaters must be pure. Bump `version` and add `migrate` when the shape changes.

### `ctx.evaluate`

```ts check
const route = f.defineWorkflow({
	name: "route",
	triggers: [slack.mentioned({ channel: "C_BUGS" })],
	async run(message, ctx) {
		const result = await ctx.evaluate("route", {
			model: "typesafe/jev-latest",
			state: { message: message.text },
			questions: { bug: { type: "boolean", instructions: "Is this a bug report?" } },
		});
		const bug = result.stopReason === "stop" && result.answers.bug?.value;
		await ctx.send("deliver", support, { input: message, priority: bug ? "urgent" : "normal" });
	},
});
```

Questions are `choice`, `score`, or `boolean`, on Pi AI's `models.classify()`; distributions, confidence, and usage are kept. Unknown classifiers and provider errors return `stopReason: "error"`, so workflows fall back explicitly. The result is a memoized step in workflows and a memo in tools.

### Plugins

A plugin (`definePlugin` from `fabrial/plugin`) has an `id` and optionally `events` (`defineEvent`), `routes` (`"METHOD /path"`, served by `app.fetch`), `clients` (built once per app; `ctx.clients`), `toolkit: (clients) => ({ tools, instructions })`, `context`, `identity.lookup`, `resources` (what `ctx.follow` accepts), `hooks.workflow` middleware, `hooks.agent` observers (`onModelResponse`, `onToolCall`; best effort), and `init`/`shutdown` for resources it owns. Factories must be side-effect free.

```ts check
import { defineEvent, definePlugin, trigger } from "fabrial/plugin";

const issue = z.object({ id: z.string(), title: z.string(), level: z.string() });
export const sentry = definePlugin((options: { secret: string }) => ({
	id: "sentry",
	events: { issueCreated: defineEvent({ payload: issue, filterable: ["level"] }) },
	routes: {
		"POST /sentry/webhook": async (request, ctx) => {
			if (request.headers.get("x-secret") !== options.secret)
				return new Response(null, { status: 401 });
			const body = issue.parse(await request.json());
			await ctx.emit("issueCreated", body, { id: `sentry:${body.id}` }); // the dedupe key
			return new Response(null, { status: 200 });
		},
	},
}));
export const issueCreated = (level?: string) =>
	trigger<z.infer<typeof issue>>({
		event: "sentry.issueCreated",
		filter: level ? { level: [level] } : {},
		replyTo: slack.channel("C_BUGS"),
	});
```

Chat providers pass a Chat SDK adapter in `chat: chatCapability({ adapter, installationId, history?, status?, isNewThread?, permalink? })`, add `events: chatEvents()`, and build triggers with `chatTrigger(id, "mentioned" | "message" | "dm", filters)`. The adapter's name must be the plugin id, which prefixes its thread ids; `installationId` is a configured, trusted identity namespace. Each inbound message carries every matching category (`message` or `dm`, plus `mentioned`). Plugin tools that write elsewhere (`slack_post`, `github_comment`, …) are how agents post outside their conversation.

### Testing

Tests run on real Conductor and need Postgres; there is no in-memory engine. Start one per run in Vitest's `globalSetup` (e.g. `new PostgreSqlContainer("postgres:17").start()`, passed on with `project.provide`), and give each test file its own database: ingress is database-wide, so apps of other files running at once would route its events.

`createTestApp(f, { database, workflows, agents?, routedAgents?, chat? })` runs workflows on Conductor with a `FakeChat` and fake agents, without models:

```ts check
import { createTestApp } from "fabrial/testing";

const hello = f.defineWorkflow({ name: "hello", run: async () => "hi" });
const { app, runtime, settled, stop } = createTestApp(f, {
	database: process.env.DATABASE_URL!,
	workflows: [hello],
});
await app.start();
const id = await runtime.invoke("hello", null, {
	metadata: { requestId: "r1", origin: null, replyTo: null, requestedBy: null },
});
console.log(await settled(id)); // { status: "completed", output: "hi" }
await stop();
```

It also returns `chat` (a `FakeChat` with inspectable posts, DMs, and ephemeral notices, and `receive`, `click`, `cancel` for ingress), `clock` (Conductor's test clock: `set`, `advance`; advance only after the wait it should end is registered), `result(id)`, and `idle()`. `idle()` waits for every event of the database and every execution of the app: use it only to assert that something did not happen, and wait on concrete effects otherwise.

`createIntegrationApp(f, { workflows, agents?, models })` runs the real runtime, agents, and chat on the instance's `database`, with `scriptedModels()` instead of credentials, model retries off, and fast polling:

```ts check
import { createIntegrationApp, reply, scriptedModels } from "fabrial/testing";

const models = scriptedModels();
models
	.model("anthropic/claude-sonnet-5")
	.respond([
		reply.toolCall("lookup_order", { orderId: "o1" }),
		({ messages }) => reply.text(`Seen ${messages.length} messages`),
	]);
models.classifier("typesafe/jev-latest", () => ({ bug: { probability: 0.01 } }));
const app = createIntegrationApp(f, { workflows: [], agents: [support], models });
await app.start();
```

Each model request consumes the next step; `model.calls` counts requests. Spy on `app.host.clients()` (or its client methods) to fake plugin clients. [examples/acme](../../examples/acme) drives signed Slack, GitHub, and Linear webhooks through `app.fetch` this way.

## Guarantees and limits

- **External effects are at-least-once.** Steps, posts, and final replies are memoized, but a crash after an effect and before its receipt repeats it. Approval decisions, sent input, and routing are deduplicated.
- **One app per database; one installation per provider.**
- **Webhook dedupe** lasts as long as Conductor's dispatch retention (about one to two days); an older redelivery routes again.
- **Thread state expires 30 days after its last write**: idle follows lapse, and approval timeouts must stay below 30 days.
- **Permissions live on tools.** Anyone in a thread a conversation follows can steer or stop it; tools that need authority ask approvers.

## Internals

This section is for contributors. None of it is API.

### Durable runtime (Conductor)

`src/runtime/conductor` adapts PG Conductor to Fabrial's internal `DurableRuntime` port. Registration is side-effect free; `start()` migrates Postgres and starts workers. Handlers replay from the top after suspension. Explicit operation IDs address saved steps and waits; core supplies repeated-ID suffixes. The pool is the instance's; the runtime does not own it. Keep the queue's workflow catalog consistent across instances. Durable wait deadlines use Postgres's clock.

Event-triggered handlers receive the clean event payload. Cron handlers receive `{ name: scheduleName }`, with a generated request id and no thread or actor. Deadlines are absolute epoch milliseconds, fixed once before replayable waits. A wait that must not miss an event caused by a preceding side effect (an approval card's click) subscribes before the side effect and waits on the subscription.

Ingress only verifies, normalizes and emits one `fabrial.ingress` event, deduped by the provider's delivery or message id. Dedupe of redelivered webhooks is bounded by Conductor's dispatch retention (about one to two days); a later redelivery routes again. The internal `fabrial.route` workflow, triggered by it and retried by Conductor, routes in durable steps: it resolves the actor, reads the thread's follow records, selects followers, else the most specific trigger (an agent's or a workflow's), and observers; persists that decision; writes follow records and subscribes handled threads; signals agent Sessions; and starts the owner (with the thread) and each observer (without it) with explicit metadata. A message nothing handles is ignored: no state, no subscription.

#### Boundary

Fabrial uses only public APIs of the libraries below: no private access, no polling loops, no Fabrial-owned engine tables. [PGCONDUCTOR.md](../../PGCONDUCTOR.md) lists the Conductor changes this needed; all have landed.

**From Conductor:** tasks and durable steps; `ctx.sleep`; `ctx.subscribe` and `ctx.waitForAny` with event, execution, and signal branches; `ctx.invoke` (with `cancelWithParent` unless detached) and `ctx.waitForResult`, whose failure or cancellation is a value; signals and `signalWithStart` with metadata; `emit` with `dedupe_key`; middleware on every attempt; execution metadata and `ctx.executionId`; cancellation that cascades to linked children; retries; the test clock.

**From Pi Durable:** Sessions and their storage (Fabrial supplies the Postgres log and lease fencing); conversations; admission, ordering, and settlement of submissions (`steer`, `followUp`, `write`, deduped by request id); tasks and tools; ownership and abort cascades; subagents; transcript compaction.

**From Chat SDK:** platform adapters; thread state and its lock; cards and actions; DMs; posting, editing, and ephemeral messages.

**Fabrial owns:** follow records and routing (one routing workflow per input); approval authorization; the Session hosting loop (one keyed `fabrial.session` execution per Session); rendering of answers, progress, and approval cards; plugins.

**One Fabrial app per database.** Ingress events and their dedupe are database-wide: every app on the database routes every `fabrial.ingress` event, and a delivery id dedupes across all of them. Several instances, and versions during a rollout, of the same app may share a database; different apps need their own. Emit dedupe is bounded by Conductor's dispatch retention (about one to two days).

### Approvals

Each request is its own execution (`fabrial.approval.request`), whose result stores the proposal, the assigned users and group ids, the requester, controls, and absolute expiry. The routing workflow checks every click against it before the waiting approval sees it: assigned users, and the current members of assigned groups (resolved when the click arrives, by the group defined with `defineGroup`, whose ids are unique per process: defining an id twice throws; an unknown group id grants nothing). Unauthorized clicks get an ephemeral notice and never reach the approval. Requester cancellation is separate from approval authority; free text never changes an approval. Reply and approval timeouts must be below the 30-day routing-state TTL.

Each approval also starts a decision execution (`fabrial.approval.decision`). It subscribes to authorized clicks once, then starts one independent `fabrial.cleanup` execution per card: each approver's card (or the destination's), plus the requester's status card. Each cleanup posts its card, waits for the decision's result, and closes the card: no actions, with the outcome. That includes a card posted after the decision. The first authorized click decides. If the caller settles first, the approval is cancelled. At the absolute deadline it expires, and a resume never extends that deadline. A cleanup retries a failed post or update up to three times, then gives up on that card. Undelivered approver cards do not fail the request while another approver's card was delivered. If every approver card fails for good, the approval ends at once instead of at its deadline: the waiting workflow's `wait` throws `Approval failed: No approver could be reached`, and the status card shows that reason. The decision learns this through a `fabrial.approval.delivery` execution, which waits on the approver cleanups' results and settles before the decision only when none of them was delivered. The requester's status card does not count as an approver card. These workflows are framework internals, not application APIs.

Authorized decisions are emitted with a dedupe key. The decision execution subscribes to them (`ctx.subscribe`) before any card is posted, so a click that arrives during delivery is not lost.

### Agent sessions (Pi)

`src/agents` runs agents on Pi Durable. `app.start()` migrates the `fabrial_pi` schema under a transactional advisory lock, builds the registry, and rejects missing live task kinds before admitting work.

**Storage and leases.** A Session is a Postgres commit log plus Pi's native `MemoryStorage` materializer. Pi validates transactions, documents, forks, copies, ancestry, and rewind reads; Postgres atomically persists the detached, validated write batch before adoption. The log stores JSON text, not jsonb, because Pi permits lone-surrogate strings that jsonb cannot represent. Reopening replays the log. There is no log compaction or indexed read path yet; memory and startup cost grow with Session history.

Each acquisition atomically increments a per-Session epoch and assigns an owner UUID. Leases last 30 seconds and renew every 10 seconds on the database clock. Acquisition never steals an unexpired lease. Every commit locks and updates the Session row with matching owner, epoch, and unexpired lease in the same transaction as the write batch, so expired or stale owners cannot write or release a successor's lease. Renewal failures abort the run's Chord Context; uncertain commit outcomes poison the owner and require reopening. Conductor's claim is not the Pi epoch: different executions share a Session, so the Pi lease is independently authoritative. The keyed Session execution still avoids paid or externally visible work during stale-worker overlap. Fencing is not an exactly-once guarantee for external effects.

Session keys are computed from the event (see [Conversations](#conversations)): `<provider>:<threadId>` for `session: "thread"`, `<provider>:channel:<channelId>` for `"channel"`, `app:<key>` for an app's key, and `<event>:<id>` for thread sessions of input without a thread. A `ctx.agent` call runs in a Session of its own, `run:<executionId>:<operationId>`: it never shares a transcript with a thread's conversation and never follows a thread. Conversations continue by agent name inside the Session. `InvocationMetadata.requestId` correlates one request and keys its request ids.

**Bridge.** One keyed `fabrial.session` execution per Session key hosts the Pi harness while the Session has work. Every input reaches it as an `input` signal through `signalWithStart` (sent input, stops, workflows' state requests); signal ids dedupe redeliveries. The host commits the request's invocation and its obligation (what Fabrial owes for it: a reply, or a call's outcome), then submits to Pi with the request id; Pi owns admission, ordering, steering, settlement, and recovery, and Fabrial keeps no run records. When every live task waits on a workflow or only reply posts are pending, the host closes Pi and the execution suspends on its input signal, `child` signals, and the execution of each waited-on workflow and pending post; the winner reopens Pi. A `ctx.agent` call is instead the input of its own Session's execution, a child of the caller (`ctx.invoke`, attached unless detached); that execution returns the call's outcome. Request ids derive from the caller's execution and operation id. Per-request identity is a request-keyed `fabrial.invocation` document committed right before submitting, so tools and instructions resolve the active request's actor rather than a conversation-wide current user. Forks have no Fabrial binding and cannot use managed outbound operations.

Workflow tools start children keyed by Pi task id and operation id. An execution a Session waits on sends it a `child` signal with its outcome, which resumes the waiting tool in process; a suspended Session also wakes on the child execution itself. Idle is `harness.inspect()` plus that suspension rule, re-checked after every storage commit. Interrupted tools reconnect to the same child. Permissions and input validation use the registered workflow. Unauthorized workflow tools are removed from model discovery. Children inherit the originating request's actor, request id, origin, and thread, with the Session's trigger and owner markers cleared so core does not treat them as observers.

Cancellation aborts the recorded request and task tree, not a newer run in the conversation. Child start intents and cancellation relationships commit before starting, and lost start receipts reconcile through a Session/task/operation dedupe key. Conductor cancellation is the source of truth for a call: when its Session execution is cancelled while running, the Session aborts the call's Pi run in process (best effort) before closing Pi, so no further model calls happen. Cancelled while suspended, nothing runs: its workflow tools are cancelled natively with it, and the call's Pi Session is left as it is, never reopened; reclaim deletes its storage later (see below). Progress is best-effort, throttled, and limited to thinking and tool descriptions, never reasoning or raw stream output. Final and error replies post through stable child executions; a request's obligation is fulfilled only once its post settled, and a post that fails for good is logged, never answered with another error reply. Posts are at-least-once: a crash after the provider accepted one and before its receipt repeats it.

**Reclaiming call Sessions.** Pi has no whole-storage deletion, so Fabrial, as its storage backend, deletes the storage of finished `ctx.agent` calls. Once a call's outcome is in its caller's result, nothing reopens its `run:` Session: a replayed caller returns the stored outcome without Pi. The internal cron workflow `fabrial.sessions.reclaim` runs daily at 03:30 UTC and deletes the `fabrial_pi.commits` and `fabrial_pi.sessions` rows (one transaction per Session, commits first) of every `run:` Session whose `fabrial.session` execution settled (completed, failed or cancelled, also while suspended) at least `retainCallSessionsDays` ago. Each Session row records the execution that last hosted it; the job reads when that execution settled through Conductor's public `getExecution`, so it reads no Conductor internals. It never touches a thread, channel or app Session, a Session with a live lease, or one whose execution is unknown, and examines `reclaimBatchSize` Sessions per durable step. `fabrial.session` executions are kept forever (no `retentionDays`): callers wait on and replay from their results, and reclaim reads them; a finite retention would have to outlast both. Options of `pi({ ... })`:

| Option                   | Default        |                                                                                |
| ------------------------ | -------------- | ------------------------------------------------------------------------------ |
| `retainCallSessionsDays` | `7`            | Days a call's Session is kept after its Session execution settled.             |
| `reclaimSchedule`        | `"30 3 * * *"` | Cron schedule (UTC) of `fabrial.sessions.reclaim`; `false` to not schedule it. |
| `reclaimBatchSize`       | `100`          | Sessions examined per step.                                                    |

**Tools and state.** Fabrial tools become Pi tools; toolkits and instructions become Pi extensions and prompt sections; observers are Pi hooks. Managed thread posts, updates, and history use stable child executions keyed by Session, Pi task, and operation id. Their step receipts survive Session reopenings; an external I/O crash before the receipt remains at-least-once. `ctx.evaluate` in tools uses Pi memos. `ctx.start` is independent; `ctx.invoke` is reconnectable workflow work. Plugin clients are not automatically durable: use managed operations or workflow tools for side effects.

State uses Pi documents: a thread family per Session and rewindable, forkable conversation documents. **State has two scopes.** `"session"` state belongs to a Session: workflows on that thread's Session and the conversations following it share it, so a channel-session agent sees the same value from every thread of its channel. A `ctx.agent` call runs in its own Session, so its tools read and write that call Session's state, not the thread's; pass what the agent needs in `input` and write results back from the workflow. Workflow reads and updates go through the Session: the workflow signals its execution with a state request and waits for the response event, so it never opens Pi and never waits behind a running agent. Updates are optimistic: the updater runs in the workflow, and the Session applies the new value only if the state's revision is unchanged since the read; otherwise the workflow reads again, up to `stateConflicts` times (default 20), then fails. An operation receipt stores each response in the same Pi commit as the mutation, so a resent or duplicated request gets the same answer and applies once; an unanswered request is resent after `stateTimeoutMs`. Workflow state needs a posted thread: an unposted channel locator is rejected (post with `ctx.thread.post` first). `"agent"` state is per conversation, for tools only: `ctx.state` in a workflow accepts only session-scoped definitions (a type error otherwise, and a runtime error for untyped callers) because the state port carries no conversation selector. Tool changes are staged, schema-validated, and committed on the Session's serialized commit line together with a tool result receipt; Pi appends its transcript result in its next settlement commit. On replay the receipt returns the saved result without reapplying state. This works around Pi 1.0 having no tool-result transaction callback; it is not an atomic transcript append. Updates must be pure because they may run again against newer state.

**Registry and orphans.** Plugin observers are installed globally; agents select their toolkits explicitly. Installation covers plugin toolkits and the tools, toolkits, and state of agents defined through `f` before start. Discovery is instance-scoped, not exact workflow reachability: apps from the same `f` share definitions, unrelated instances do not install each other's agents, and unused agents of the same instance may still be installed. At start, Fabrial finds Sessions whose latest record of a task with an uninstalled kind is not terminal (e.g. a toolkit removed from every agent) from the commit log, without taking leases; it acquires only those Sessions, aborts the live tasks Pi's `inspect()` reports with such a kind through Pi's abort cascade, and logs a warning per Session and task kind with the count. A Session held by another owner is skipped with a warning; startup does not fail.

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

### Current boundaries

Live plugin clients are built once per app; they must not capture invocation identity. Core adds no reaction acknowledgement to replies because `ChatPort` has no reaction operation.
