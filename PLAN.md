# Fabrial

> A TypeScript framework for durable workflows, AI agents, and human approvals.

Fabrial is an open-source framework for making a company AI-native. Events from Slack, GitHub, Linear, Sentry, or schedules start workflows. Workflows can be deterministic, call AI agents, make structured model judgments, and pause for human approval, mainly through Slack. Agents can call workflows as tools.

The name comes from Brandon Sanderson's _Stormlight Archive_: a fabrial is a device that turns extraordinary capabilities into practical tools. (In the books, fabrials trap spren, so avoid branding around captured spirits.)

## Goals

- Wire up integrations (Slack, GitHub, Linear, Sentry, …) and trigger workflows on their events quickly.
- Support plain workflows, AI-assisted workflows, and open-ended agents with one programming model.
- Make approvals first-class: an action can require approval from a group (e.g. this week's triage developer), delivered in Slack, with execution suspended until a decision arrives.
- One Slack bot as the entry point for everyone, with channel-specific behavior where configured.
- Generic and open source; company-specific logic lives in the application, not the framework.

## Non-goals

- Building a coding agent or cloud coding-agent service. Coding work is delegated to existing providers.
- A tracking UI, a "case" model, or application tables that duplicate runtime state. No `workflow_runs`, `agent_sessions`, or approvals tables beside what Conductor and Pi already persist.
- A new workflow engine, agent harness, decision engine, or routing DSL.

## Building blocks

| Component                                            | Role                                                                                                                               |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| [PG Conductor](https://pg-conductor.dev)             | Durable workflow execution: steps, retries, timers, triggers, `waitForEvent`, child invocations, queues, concurrency.              |
| [Pi Durable](https://earendil.com/posts/pi-durable/) | Durable agent harness: conversations, transcripts, compaction, model/tool turns, Pi tasks, documents, extensions.                  |
| `@earendil-works/pi-ai`                              | Model abstraction, including classifier models (TypeSafe Jev) via `models.classify()`.                                             |
| [Chat SDK](https://chat-sdk.dev)                     | Platform adapters: webhooks, threads, messages, cards, modals, streaming, native clients (`WebClient`, `octokit`, `linearClient`). |
| [Executor](https://v2.executor.sh)                   | Not foundational. May become a tool/client plugin later.                                                                           |

These are implementation details. App authors install `fabrial` and provider plugins, configure a database and model credentials, and never import or configure the runtimes (see [Ownership](#ownership)).

Reference for chat UX: Vercel [Eve](https://github.com/vercel/eve). Its persisted thread bindings, thread-handle reconstruction on resume, bounded/attributed history loading, and default status rendering are the patterns to borrow.

## Ownership

> Fabrial owns the API. The runtimes are implementation details.

App authors see one programming model: workflows, agents, tools, toolkits, state, groups, and plugins, all defined on the Fabrial instance. Conductor, Pi, and Chat SDK run underneath and never appear in app code: no runtime packages to install or wire, no runtime types in signatures, and no escape hatch to native handles. While Fabrial ships preview releases, a missing capability becomes a Fabrial field, not a pass-through.

| Concern                                                         | Public API                               | Implemented with                  |
| --------------------------------------------------------------- | ---------------------------------------- | --------------------------------- |
| Event-driven and scheduled workflows, steps, retries, waits     | `f.defineWorkflow`, `ctx.step`, triggers | PG Conductor                      |
| Agent conversations, model execution                            | `f.defineAgent`, `ctx.agent`, `ctx.send` | Pi Durable, Pi AI                 |
| Following threads and resources                                 | `ctx.follow`, `ctx.unfollow`             | Chat SDK thread state             |
| Tools and tool bundles                                          | `f.defineTool`, `f.defineToolkit`        | Pi tools and extensions           |
| Session/conversation state                                      | `f.defineState`                          | Pi documents                      |
| Platform messages, threads, cards                               | `ctx.thread`, `Card`, `ChatMessage`      | Chat SDK, inside the chat plugins |
| Provider events → triggers, context reconstruction, clients     | Plugins (`definePlugin`)                 | Fabrial                           |
| Workflow ↔ agent bridges, approvals, response context, identity | `ctx.*`, `f.defineGroup`                 | Fabrial                           |

Everything lives in the one configured Postgres database, in the runtimes' schemas: Conductor step results, events, and metadata; Pi Sessions and documents; and Chat SDK thread state, which holds follow records and status message refs. Fabrial owns no tables. Conductor's per-Session queue is the only inbox (see [Delivery](#delivery)).

## Public API

All code below is the proposed Fabrial API.

### The instance: `createFabrial`

A Fabrial instance carries the plugin types, so definitions get typed `ctx.clients` without global type augmentation. It holds configuration (database, model credentials, plugins, users) and never workflows, so definition files can import it without cycles. `f.app()` only lists the workflows to run.

Configuration is read from env when the module loads (the Better Auth / Inngest style). Plugin factories are side-effect free: nothing connects or validates credentials until `app.start()`, so unset env at import time is fine. Tests configure the same way, through env set in a test `globalSetup`.

- `database`: one Postgres connection string. Workflows, agent sessions, and chat state share it, each in its own schema. Fabrial owns the pool and closes it on `app.stop()`.
- `models`: credentials per model provider. Agents name models as `"provider/model"` strings (`"anthropic/claude-sonnet-5"`), the AI SDK / Mastra convention. Tests pass scripted models from `fabrial/testing` instead of credentials.

```ts
// fabrial.ts
export const f = createFabrial({
	database: env.DATABASE_URL,
	models: { anthropic: { apiKey: env.ANTHROPIC_API_KEY } },
	plugins: [
		slack({ botToken: env.SLACK_BOT_TOKEN, signingSecret: env.SLACK_SIGNING_SECRET, workspace: "acme" }),
		github({ ... }),
		linear({ ... }),
		database({ url: env.APP_DATABASE_URL }),
	],
	identity: [alice, bob], // users; groups are referenced where they're used
});

// every definition hangs off f: ctx.clients is typed from f's plugins
export const runSql = f.defineWorkflow({ name: "run-sql", async run(input, ctx) { /* ctx.clients.database */ } });
export const supportAgent = f.defineAgent({ name: "support", model: "anthropic/claude-sonnet-5", tools: [runSql] });
export const linearTriage = f.defineGroup({ id: "linear-triage", resolve: (ctx) => ctx.clients.linear.triageResponsibility({ team: "ENG" }) });

// app.ts
export const app = f.app({ workflows: [generalAssistant, runSql] });
export default { fetch: app.fetch };
```

Definitions are plain data, so any app built from `f` can register them. Reusable tools shipped inside plugin packages don't use `f`; they get typed clients through the plugin's own `toolkit: (clients) => …` closure.

### `f.defineWorkflow`

The main abstraction. It registers a durable task, turns plugin triggers into subscriptions, and rebuilds the invocation context on every start or resume. Execution options are Fabrial fields: `concurrency`, `retries`.

```ts
export const repairCustomer = f.defineWorkflow({
	name: "repair-customer",
	triggers: [slack.mentioned({ channel: "C_SUPPORT" })],
	concurrency: 3,

	async run(input, ctx) {
		await ctx.thread.post("ack", "I'll investigate.");

		const proposal = await ctx.agent("investigate", supportAgent, {
			input: input.message.text,
		});

		const decision = await ctx.waitForApproval("approve-repair", {
			title: "Apply this repair?",
			details: proposal,
			approvers: engineeringTriage,
			timeout: "24h",
		});

		if (!decision.approved) return { status: "declined" };

		const result = await ctx.step("apply-repair", () =>
			ctx.clients.customerDb.applyRepair(proposal),
		);
		await ctx.thread.post("result", formatResult(result));
		return result;
	},
});
```

Workflow context:

| API                                             | Behavior                                                                                                       |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `ctx.thread`                                    | The trigger event's thread, rebuilt from persisted references. For posting; conversations belong to agents.    |
| `ctx.clients.*`                                 | Native provider clients from plugins, recreated on resume and never serialized.                                |
| `ctx.thread.post(id, message)`                  | Durable post into the bound thread. Other thread operations follow the same `(id, …)` shape.                   |
| `ctx.agent(id, agent, opts)`                    | Calls an agent and returns its answer; nothing is posted.                                                      |
| `ctx.send(id, agent, { input, home? })`         | Hands an event to an agent's conversation and returns (see [Conversations](#conversations)).                   |
| `ctx.follow(id, ref)` / `ctx.unfollow(id, ref)` | A conversation starts or stops following a thread or resource.                                                 |
| `ctx.state(S).get(id)` / `.update(id, fn)`      | Shared state (`defineState`), durable.                                                                         |
| `ctx.evaluate(id, { model, state, questions })` | Structured judgment (Jev), durably recorded.                                                                   |
| `ctx.waitForApproval(id, opts)`                 | Complete approval protocol over Conductor `waitForEvent`. Sugar for `requestApproval` + awaiting the decision. |
| `ctx.requestApproval(id, opts)`                 | Posts the approval request and returns a handle: `decision()`, `cancel(reason)`.                               |
| `ctx.race(id, { a, b, … })`                     | Durably waits for the first of several awaitables (decision, timer) and returns which one won.                 |
| `ctx.invoke(id, workflow, input)`               | Calls another workflow as a child and returns its result.                                                      |
| `ctx.step(name, fn)`                            | Durable, memoized step for arbitrary code.                                                                     |

### Automatic durability

Intentional framework operations are wrapped in durable operations automatically, so authors never write `ctx.step` around `ctx.thread.post()`.

- Every durable operation takes an **explicit ID as its first argument** (the Inngest model), the same as `ctx.step(name, fn)`. A repeated ID within one execution (e.g. in a loop) is suffixed automatically (`processed`, `processed#1`, `processed#2`, …). The `fabrial:` prefix and the `#` character are reserved. Use a data-derived ID (`processed:${customer.id}`) when the iteration order may change.
- On replay, the saved receipt for that ID is returned and live handles are rebuilt from it.
- Deploys while executions are in flight: an ID with no saved result runs, and a saved result whose ID is gone is ignored. Renaming an ID re-runs that operation. There is no call-order determinism and no version pinning.
- Read-only accessors (`ctx.thread`, `ctx.clients`, `ctx.actor`) are not operations and take no ID.
- Each helper uses the right mechanism. Long waits and agent runs are never one opaque step:

| Operation                      | Mechanism                                              |
| ------------------------------ | ------------------------------------------------------ |
| Post a message, fetch an issue | Memoized Conductor step                                |
| Invoke another workflow        | Durable child invocation                               |
| Run an agent                   | Durable submission + result bridge                     |
| Wait for approval              | Durable request + `waitForEvent`                       |
| `ctx.evaluate`                 | Memoized step (Conductor) or memo/checkpoint (Pi task) |
| Update a Pi document           | Native Pi commit                                       |

Step memoization does not give exactly-once external effects. Integration operations use provider idempotency keys or reconciliation where possible. Fabrial does not promise exactly-once in general.

### Agents

App authors write agents (`f.defineAgent`), tools (`f.defineTool`), toolkits (`f.defineToolkit`), and shared state (`f.defineState`). Everything durable or long-running is a workflow, and a workflow is a tool: pass it in `tools` and agents call it by its `name` and `description`.

```ts
export const bugInvestigator = f.defineAgent({
	name: "bug-investigator",
	description: "Investigates bug reports and finds the root cause and affected customers.",
	model: "anthropic/claude-sonnet-5",
	thinking: "medium", // "off" | "low" | "medium" | "high"
	instructions: [
		"Investigate bug reports. Find root cause and affected customers.",
		(ctx) => `Requested by ${ctx.actor.name}.`, // dynamic, rendered per run
	],
	tools: [readOnlyQuery, addFinding, runSql], // tools and workflows
	toolkits: ["github", "linear", Investigation], // plugin ids (typed) and app toolkits
	state: [Findings], // rendered into the prompt via their `render`
});

const analysis = await ctx.agent("investigate", bugInvestigator, { input });
const plan = await ctx.agent("plan", cleanupPlanner, { input, output: CleanupPlan }); // typed structured result
```

- `name` is stable: it is the continuation key, and labels traces and progress.
- `description` is required on every agent. It says what the agent does and when to use it: it is the tool description when the agent is a subagent, and it labels the agent in traces and listings.
- `ctx.agent` options are `input`, `output` (a Standard Schema; the answer is validated JSON), and `detached`.
- Unset fields follow instance defaults. There is no pass-through to the runtime's agent config; new capabilities (working directory, compaction, …) become fields.

Tools take a Standard Schema `input`, like workflows, and return a string or any JSON value:

```ts
export const addFinding = f.defineTool({
	name: "add_finding",
	description: "Record a finding about this customer issue",
	input: z.object({ text: z.string() }),
	async run({ text }, ctx) {
		ctx.actor; // who sent the message this run answers
		await ctx.state(Findings).update((s) => {
			s.notes.push(text);
		}); // committed with the tool result
		return "recorded";
	},
});

export const Investigation = f.defineToolkit({
	name: "investigation",
	tools: [addFinding, lookupCustomer],
	instructions: (ctx) => `Requested by ${ctx.actor.name}.`,
});
```

- The input schema is converted to JSON Schema for the model and validated before `run`.
- `idempotent: true` lets a call interrupted by a crash run again; otherwise the model sees an interrupted result and decides.
- Tool `ctx` is Fabrial's: `actor`, `requestedBy`, `clients`, `thread`, `state`, `invoke`, `start`, `send`, `follow`, `evaluate`, `signal`.

### State: `defineState`

State that agents see and change and that outlives a single step. One concept and one API in workflows and tools, backed by Pi documents in the thread's Session:

```ts
export const Findings = f.defineState({
	name: "findings",
	scope: "thread", // "thread" | "agent"
	schema: z.object({ customerId: z.string().optional(), notes: z.array(z.string()) }),
	initial: () => ({ notes: [] }),
	render: (s) => (s.notes.length ? `Known so far:\n- ${s.notes.join("\n- ")}` : undefined), // optional: show to agents
});

// workflow: durable ops with IDs
await ctx.state(Findings).update("seed", (s) => {
	s.customerId ??= report.organisationId;
});
const findings = await ctx.state(Findings).get("check");

// tool: no ID, committed atomically with the tool result
await ctx.state(Findings).update((s) => {
	s.notes.push(text);
});
```

| Scope    | Lives for                         | Shared by                                  | Pi document underneath |
| -------- | --------------------------------- | ------------------------------------------ | ---------------------- |
| `thread` | the whole thread, across requests | every workflow and agent in the thread     | session doc            |
| `agent`  | one agent's conversation          | that agent only; forks and rewinds with it | conversation doc       |

- A workflow's own local state is just its variables, made durable through steps.
- Workflow `get`/`update` briefly acquire the thread's Pi Session (strict per-key lock). They can wait behind an agent that is running in the same thread.
- `render` is how state becomes model context. Agents only see the states listed in `defineAgent.state`.
- Schema changes follow Pi document `version` + migration.
- Fabrial ships an internal `fabrial.invocation` document (per request ID: actor, replyTo, origin) and a `ChatBinding` document. Forked conversations get **no** outbound chat binding by default.

### Runtime bridges

| Direction                                           | Behavior                                                                                                                                             |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workflow → agent (`ctx.agent`)                      | Create/find conversation, submit with a stable request ID, persist the reference, suspend, resume with the result.                                   |
| Agent → workflow (workflow in `tools`)              | Start/reconnect to the Conductor execution with a stable invocation key and return its result as the tool result. The tool's own approval guards it. |
| Workflow → workflow                                 | Native Conductor child invocation.                                                                                                                   |
| Managed external ops inside Pi (e.g. `thread.post`) | Routed through the Conductor bridge, keyed by Pi task ID + operation ID, so external effects have one durability implementation.                     |

A workflow called by an agent must not synchronously query that same busy conversation, because that would deadlock. It starts a separate conversation instead.

Approvals are always workflow-backed. A tool that needs approval is a workflow in the agent's `tools`.

### `ctx.evaluate` (Jev)

The shape follows Vercel AI SDK's experimental `evaluate`. It is implemented on Pi AI's native `models.classify()` and needs neither AI SDK nor AI Gateway.

```ts
const result = await ctx.evaluate("route", {
	model: "typesafe/jev-latest",
	state: { message: message.text, channel: ctx.thread.channel },
	questions: {
		intent: {
			type: "choice",
			instructions: "Which workflow should handle this request?",
			criteria: {
				bug: "Investigate broken or unexpected behavior",
				customerOperations: "Perform an operation for a customer",
				general: "Other questions or requests",
			},
		},
	},
});
```

- Question types: `choice`, `score`, `boolean`. The public `boolean` maps to Pi's `bool`.
- The full result (probabilities, confidence, model, usage) is persisted by the current execution owner: a Conductor step in workflows, a Pi memo/checkpoint in Pi tasks. A Pi extension can expose it as a tool.
- `confidence` (distribution statistic) and the selected option's probability are both exposed. Authors pick which one to threshold.
- Questions in one call are evaluated independently. Dependent questions need a follow-up call.
- Provider errors come back as `stopReason: "error"`, and workflows fall back explicitly.
- Evaluation helps with routing only. Permissions and approvals stay deterministic.

### Approvals: `ctx.waitForApproval`

Application code only sees the returned decision. Internal events, correlation IDs, and card plumbing are hidden. Internally the helper:

1. Resolves approvers and persists the request and correlation ID.
2. Registers the durable wait and delivers the card with race-safe ordering (Conductor's `waitForEvent` ignores events that arrive before registration).
3. Receives the click through the plugin's handler.
4. Authenticates the actor, checks eligibility, and correlates the answer to the request. Duplicate clicks and expiry are handled.
5. Emits the internal event that satisfies `waitForEvent`.
6. Updates the card and returns the decision.

`decision.status` is `"approved" | "rejected" | "expired" | "cancelled"`, with `decision.approved` as shorthand. `waitForApproval` is sugar for `ctx.requestApproval(id, opts)`, which returns a handle with `decision()` and `cancel(reason)`, followed by awaiting the decision.

Rules:

- An approval binds to the exact proposal and arguments. A changed proposal needs a new approval.
- By default the requester's status message shows **[Cancel request]**. Free-text messages during the wait reach the conversation that follows the thread (see [open questions](#open-questions)); they never change or cancel the request.
- The approval destination (e.g. the triage developer's DM) is separate from `ctx.thread`, which stays the originating thread.
- Default membership mode: resolve the group when the request is made, persist the assigned approvers, check at decision time that the responder is assigned and still authorized, and reassign explicitly if the rotation changes.
- Agents never decide approvals and never choose `requestedBy`/`decidedBy`. The bridge supplies authenticated context.

### Identity: users and groups

Fabrial does not build a user-management product. It provides a small directory interface:

```ts
interface IdentityDirectory {
  resolveIdentity(identity: ExternalIdentity): Promise<Principal>;
  identitiesFor(userId: string): Promise<ExternalIdentity[]>;
  members(groupId: string): Promise<Principal[]>;
  isMember(userId: string, groupId: string): Promise<boolean>;
}

// External identities are scoped to a provider installation.
{ provider: "slack", installationId: "workspace-acme", subjectId: "U123" }
```

Users are defined in code and registered with `createFabrial({ identity: users })`. Groups are defined with `f.defineGroup` (typed `ctx.clients`) or backed by Slack user groups, and are referenced where they're used (`approvers`); they need no registration:

```ts
// users.ts
export const alice = defineUser({
	id: "alice",
	identities: [
		slack.identity({ workspace: "acme", userId: "U123" }),
		github.identity({ login: "alice" }),
		linear.identity({ userId: "lin_abc" }),
	],
});

// groups.ts: code-defined
export const engineering = f.defineGroup({ id: "engineering", members: [alice, bob] });
export const backendTriage = f.defineGroup({
	id: "backend-triage",
	resolve: () => [weeklyRotation([alice, bob], { start: "2026-01-05" })],
});

// Slack-backed: whoever is in @triage right now; rotate in Slack, no deploy
export const engineeringTriage = slack.userGroup({ id: "engineering-triage", handle: "triage" });

// anything else
export const linearTriage = f.defineGroup({
	id: "linear-triage",
	resolve: (ctx) => ctx.clients.linear.triageResponsibility({ team: "ENG" }),
});
```

- Every invocation has its own identity context: `requestedBy`, `executedBy`, and approval `decidedBy`. There is no mutable conversation-wide "current user", because a thread can have many participants.
- Permissions live on tools. An agent is offered every tool its definition lists, regardless of who is talking to it; a tool that needs permission asks its fixed approvers (see [Permissions](#permissions)). There are no access lists on workflows or triggers. Channel context can help routing but never grants authority.

### Plugins

Plugins are how everything outside the core plugs in: chat providers (Slack via Chat SDK), non-chat sources (Sentry, internal webhooks), clients, tracing, and identity directories. Chat SDK is just one optional capability.

The design borrows from Better Auth (option factory, declarative object, inferred types, plugins extend the core via hooks), Executor (`definePlugin(() => ({ … }))`, one canonical implementation reused by every caller), and Pi (native extension/hook shapes, no side effects in the factory).

`definePlugin` takes a factory (for plugins with options) or a plain object, and infers types for its clients, events, and triggers. Those types reach `ctx.clients` through the `createFabrial` instance the plugin is passed to:

```ts
// @fabrial/sentry: non-chat provider
export const sentry = definePlugin(
	(options: { clientSecret: string; authToken: string; org: string }) => ({
		id: "sentry",

		// typed events, registered as Conductor events "sentry.issueCreated"
		events: { issueCreated: defineEvent({ payload: SentryIssue }) },

		// ingress, served by app.fetch: verify, emit, return fast
		routes: {
			"POST /sentry/webhook": async (req, { emit }) => {
				const body = await req.text();
				if (
					!verifySignature(body, req.headers.get("sentry-hook-signature"), options.clientSecret)
				) {
					return new Response("invalid signature", { status: 401 });
				}
				const payload = JSON.parse(body);
				if (payload.action === "created") {
					await emit("issueCreated", toIssue(payload.data.issue), {
						id: req.headers.get("request-id")!, // ingress dedup
						origin: { provider: "sentry", issueId: payload.data.issue.id },
					});
				}
				return new Response(null, { status: 200 });
			},
		},

		// canonical client: ctx.clients.sentry, rebuilt on every run/resume
		clients: () => ({ sentry: new SentryClient({ token: options.authToken, org: options.org }) }),

		// tools for agents that list this plugin in `toolkits`, built on the same client
		toolkit: ({ sentry }) => ({ tools: [getIssueTool(sentry)] }),

		// model context about the origin, loaded durably when an agent starts from this trigger
		context: async (origin, { sentry }) => formatIssue(await sentry.getIssue(origin.issueId)),
	}),
);

// static trigger helpers (event names don't depend on options)
export const issueCreated = (filter: { project?: string; level?: Level[]; replyTo?: Surface }) =>
	trigger(sentry, "issueCreated", filter);

// @fabrial/slack: chat provider; Chat SDK supplies routes, ctx.thread, cards, history
export const slack = definePlugin((options: SlackOptions) => ({
	id: "slack",
	chat: {
		adapter: () => createSlackAdapter(options),
		history: { mode: "since-last-reply", limit: 50 },
	},
	clients: ({ chat }) => ({ slack: chat.webClient }),
	identity: {
		resolve: (subjectId) => ({ provider: "slack", installationId: options.workspace, subjectId }),
	},
}));

// @fabrial/langfuse: not a provider; global hooks only
export const langfuse = definePlugin((options: LangfuseOptions) => {
	const client = new Langfuse(options);
	return {
		id: "langfuse",
		hooks: {
			workflow: [
				async (execution, ctx, next) =>
					next({ ...ctx, trace: client.trace({ id: execution.rootId }) }),
			],
			agent: { onModelResponse: (response) => client.generation(response) },
		},
		shutdown: () => client.flushAsync(),
	};
});

// app-local plugin without options: plain object
export const triageRotation = definePlugin({
	id: "triage-rotation",
	clients: () => ({ rotations: new RotationService(env.DATABASE_URL) }),
});

// fabrial.ts: the instance carries plugin types for every definition
export const f = createFabrial({
	plugins: [
		slack({
			botToken: env.SLACK_BOT_TOKEN,
			signingSecret: env.SLACK_SIGNING_SECRET,
			workspace: "acme",
		}),
		sentry({ clientSecret: env.SENTRY_CLIENT_SECRET, authToken: env.SENTRY_TOKEN, org: "acme" }),
		langfuse({ publicKey: env.LANGFUSE_PK, secretKey: env.LANGFUSE_SK }),
		triageRotation,
	],
	identity: [alice, bob],
});

// app.ts
export const app = f.app({ workflows: [generalAssistant, bugIntake, runSql] });

export default { fetch: app.fetch }; // one handler for /slack/events, /sentry/webhook, …
await app.start(); // workers and agent runtime
```

A workflow using both providers:

```ts
export const bugIntake = f.defineWorkflow({
	name: "bug-intake",
	input: z.object({ message: SlackMessage }), // #bugs messages arrive via ctx.send from the router
	triggers: [
		sentry.issueCreated({
			project: "app",
			level: ["error", "fatal"],
			replyTo: slack.channel("C_BUGS"),
		}),
	],
	async run(input, ctx) {
		// input: { message } | SentryIssue; ctx.thread is defined because every entry has a surface
		const analysis = await ctx.agent("investigate", bugInvestigator, { input });
		await ctx.thread.post("analysis", analysis.summary);
	},
});
```

Capabilities (all optional except `id`):

| Key                 | Purpose                                                                                                                               |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                | Unique name. Prefixes events (`sentry.issueCreated`). A second installation is a second instance with its own `id`.                   |
| `events`            | Typed event definitions, registered with the workflow runtime.                                                                        |
| `routes`            | Webhook ingress, served by `app.fetch` (mountable in Hono, Next.js, Bun.serve, …).                                                    |
| `chat`              | Chat SDK adapter (the one runtime type plugin authors see). Supplies routes, message events, `ctx.thread`, cards, and history.        |
| `clients`           | Canonical live clients exposed as `ctx.clients.*` and rebuilt on every run/resume. Workflow code and agent tools use the same client. |
| `toolkit`           | Tools and instructions for agents that list this plugin's id in `toolkits`.                                                           |
| `context`           | Model context for a trigger's origin.                                                                                                 |
| `hooks.workflow`    | Middleware around **every** workflow run/resume.                                                                                      |
| `hooks.agent`       | Observers on **every** agent: `onModelResponse`, `onToolCall`.                                                                        |
| `identity`          | External identity → principal.                                                                                                        |
| `init` / `shutdown` | Lifecycle. Factories must have no side effects.                                                                                       |

- Credentials are plugin options. There is no provider/account split, accounts table, or secret store yet.
- Triggers are durable subscriptions in the workflow runtime. Plugins translate provider events into typed events, and the runtime owns matching and starting.
- A non-chat trigger has no thread of its own. `replyTo` says where `ctx.thread` points, and the thread is created lazily on the first post. Without a `replyTo`, `ctx.thread` is absent from the type.
- Polling sources (APIs without webhooks) ship a cron workflow that emits the same events.
- Executions persist only references (installation, thread, message, actor IDs) and event data. Live clients are rebuilt on resume.
- `origin` and `replyTo` are separate. A Sentry-originated workflow can reply in Slack.
- What workflow code can access and what is exposed to the model are separate declarations. A workflow may hold the full GitHub client while an agent only gets three tools.
- Ingress handlers store the event and return quickly. No long work runs inside webhook requests.

### Chat context

A Slack-bound execution gets two guarantees:

| Context   | Supplied                                                                                                                                                                                                                            |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Execution | Thread handle, provider client, installation, actor, reply destination. Automatic.                                                                                                                                                  |
| Model     | Triggering message, attributed history, attachments, linked issue/PR. Eve-like defaults: bounded history, speaker attribution, recorded boundary. Loaded durably so retries see the same context. New replies arrive as new inputs. |

### Response context and progress

> The conversation answers where it lives. The active task supplies activity. The chat integration renders it.

- Each chat-triggered invocation gets a framework-managed **response context**: destination thread, invocation identity, and status/message references.
- `ctx.agent` attaches the agent's activity (derived from Pi's execution stream) to the response context automatically. Statuses look like "Thinking…", "Looking up the customer…", "Waiting for approval". Public progress summaries and tool descriptions are shown, never raw reasoning.
- Nested calls (workflow → agent → tool-workflow → agent) inherit the context. The deepest active child is the foreground progress source, and the parent takes over again when it finishes. Parallel activity is aggregated. A nested agent finishing never closes the outer interaction.
- The renderer is the only writer to the platform.
- By default an agent called as a workflow step shows progress and **returns** its answer without posting it. Publishing an answer is deliberate: `ctx.thread.post`, an explicit streaming/publish option, or an intentional `sendMessage` tool.
- Durability tiers: status/typing updates are throttled, replaceable, and may be lost on crash. Partial streaming updates a single tracked message. Final replies and approval cards are durable operations with stable identities. After a restart, presentation reattaches.

### Conversations

> An event reaches a conversation, and the conversation answers where it lives. Following a thread is automatic; posting anywhere else is a tool.

| Concept          | What it is                                                                  | Backed by                                                       |
| ---------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------- |
| **Conversation** | One agent's transcript, keyed by `[session key, agent name]`                | Pi conversation in a Pi Session (the lock unit)                 |
| **Home**         | The thread whose message started the conversation; final replies post there | Derived from the session key                                    |
| **Follow**       | A thread or resource whose events reach the conversation                    | Home threads: implicit. Others: a record in that thread's state |
| **Input**        | An event delivered to the conversation: a message or a notification         | Pi `submit` (`steer`, `followUp`, `write`)                      |

#### Agents receive events

Agents have triggers, like workflows. A trigger on an agent delivers the event to one of its conversations; a trigger on a workflow starts a run.

```ts
export const support = f.defineAgent({
	name: "support",
	description: "Answers customer questions in #help.",
	model: "anthropic/claude-sonnet-5",
	triggers: [slack.mentioned({ channel: "C_HELP" })],
	session: "thread", // default; "channel", or (event) => key
	tools: [readOnlyQuery, runSql],
});
```

An incoming chat message is routed in this order:

1. **Followers.** If conversations follow the message's thread, it goes to them. A message that mentions one specific agent goes only to that agent, which then follows the thread too.
2. **Triggers.** Otherwise the most specific matching trigger wins, whether it belongs to an agent or a workflow. Ties fail at startup; `observe: true` triggers run alongside without receiving the thread.
3. Otherwise the message is ignored.

Non-chat events (PR opened, Sentry issue, schedules) go through triggers and follows.

Session keys are computed from the event, so routing a message to its conversation needs no stored state:

| `session`      | Key                                 | Use                                                                 |
| -------------- | ----------------------------------- | ------------------------------------------------------------------- |
| `"thread"`     | `<provider>:<threadId>` (default)   | One conversation per thread; threads run in parallel                |
| `"channel"`    | `<provider>:channel:<channelId>`    | One conversation for a whole channel; inputs are processed in order |
| `(event) => …` | The app's key (e.g. `ticket:${id}`) | Anything else, e.g. a conversation per customer                     |

#### Home and following

- **Home.** The thread of the input that created the conversation. A top-level Slack message becomes a thread: the agent replies under it.
- **Following is automatic.** An agent follows every thread it receives a message from: all later messages there reach the same conversation, mention or not. A message during a run steers it; afterwards it starts a new run. For `"thread"` sessions the session key already says so; for other sessions Fabrial writes a follow record on the thread.
- **Everyone in a followed thread steers.** Permissions stay on tools.
- **Triggers without a thread** (a Sentry issue, a schedule) name a home: `home: slack.channel("C_BUGS")`. The first final reply creates the thread, which is followed like any other.

#### Replies

Final replies post automatically:

| Input                                                      | Final reply goes to                            |
| ---------------------------------------------------------- | ---------------------------------------------- |
| A message from a followed thread (including the home)      | That thread                                    |
| A notification from a followed resource (a review on a PR) | The conversation's **home**, not the resource  |
| A background subagent's report                             | The thread of the input that started the child |
| A failed run                                               | The same place, as the error reply             |
| No thread at all (`ctx.agent` from a workflow)             | Nowhere: the workflow receives the answer      |

- When several messages steer one run, the reply goes to the newest one's thread.
- Built-in `no_reply({ reason? })` ends a run without posting (Eve's `no_reply`), for "CI passed" or chatter that is not for the agent. The reason stays in the transcript.
- Progress ("Thinking…", tool activity, "Waiting for the model provider…") shows on the thread the reply will go to.

#### Following other things

```ts
await ctx.follow("watch-pr", github.pullRequest({ repo: "acme/app", number: pr.number }));
await ctx.unfollow("unwatch-pr", github.pullRequest({ repo: "acme/app", number: pr.number }));
```

- From a tool, the calling agent's conversation follows. From a workflow, pass `to: { agent, session }`.
- The follow is a record (`[session, agent]`) in the resource thread's Chat SDK state. A follow lasts until `unfollow`; merge and close events are delivered like any other, and the agent decides. Plugins give agents `github_follow_pull_request` / `github_unfollow_pull_request` in their toolkit.
- Plugins map events to their thread (a review belongs to its PR's thread), decide which events a follow receives by default (GitHub: reviews, comments, new commits, merge and close; `events: "all"` adds labels and CI), and drop untrusted authors (GitHub: author association `OWNER`, `MEMBER`, `COLLABORATOR`, or the app's own bot). The filter is not a permission: it keeps outside input from steering the agent.
- There is no handover: the conversation's home stays where it started.

#### Writing elsewhere

Posting anywhere other than the home thread uses explicit, typed tools from plugin toolkits: `github_comment({ pr, body })`, `github_reply_to_review({ review, body })`, `slack_post({ channel, text })`. Rendered inputs carry the ids those tools need (`<notification source="github" kind="review" pr="acme/app#42" review="991">`).

#### Inputs

| Kind           | Rendered as                                                 | Delivery                                                                                                      |
| -------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `message`      | `<message from="Alice" thread="…">text</message>`           | `steer` when busy, starts a run when idle                                                                     |
| `notification` | `<notification source="github" kind="review" …>summary</…>` | by priority: `urgent` → `steer`, `normal` → `followUp`, `low` → `write` (seen on the next run, does not wake) |

Request ids derive from the source event (Slack event id, GitHub delivery id), so redeliveries are dropped. Plugins set a notification's default priority (GitHub: changes requested is `urgent`); `ctx.follow(…, { priority: { review: "urgent" } })` overrides it per kind.

#### Workflows and agents

- `ctx.agent(id, agent, { input, output? })` calls an agent and returns its answer; nothing is posted. Its conversation is keyed by the workflow run.
- `ctx.send(id, agent, { input, home? })` hands an event to an agent and returns. The conversation is found by session key from the input's thread (or `home`), exactly as if the agent's trigger had matched. This is how a routing workflow picks an agent; it replaces `ctx.handoff`.
- Workflows don't hold conversations in chat: they post (`ctx.thread.post`), send cards, and wait for approvals. `ctx.thread.waitForReply`, `ask`, `nextReply`, and `ctx.handoff` are removed.

```ts
export const router = f.defineWorkflow({
	name: "router",
	triggers: [slack.mentioned()], // less specific than #help's trigger
	async run(msg, ctx) {
		const route = await ctx.evaluate("route", {
			model: "typesafe/jev-latest",
			state: msg,
			questions,
		});
		const agent = route.answers.intent?.choice === "bug" ? bugInvestigator : support;
		await ctx.send("deliver", agent, { input: msg }); // later replies in the thread go straight to that agent
	},
});
```

#### Delivery

Each Session is hosted by one Conductor execution while Pi has work, addressed by the Session key. Every input for a conversation (a trigger match, a followed thread or resource, `ctx.send`, a subagent report, a cancel) is a Conductor signal sent with `signalWithStart` (Conductor item 12): it reaches the running execution in process, or starts one with the signal queued.

```ts
// Every input:
await conductor.signalWithStart({
	task: "fabrial.session",
	key: session,
	signal: { name: "input", id: requestId, payload },
});

// The Session execution:
ctx.onSignal("input", (input) =>
	input.kind === "cancel" ? abortRun(harness, input.run) : submit(harness, input),
);
```

- **Steering is native Pi.** A signal submits to Pi in process (`steer`, `followUp`, or `write`); Pi places it at its next turn boundary. Ownership never moves, and nothing is handed over, polled, or stored outside Pi and Conductor.
- **Cancelling** is a signal: the execution aborts the run in Pi (owned and background work included) and keeps hosting the Session.
- **Ending.** The execution closes the Session when Pi reports no active work (this Pi build has no `Harness.wake()`: `harness.inspect()` plus the release rule, re-checked after every storage commit), including while only waits on workflows such as approvals remain; the workflow's completion is another signal. A short managed effect like a thread post (a final reply) keeps the Session open, and a run ends only after its final replies posted. Conductor re-queues an execution that received signals while finishing, so no input is lost.
- **Dedupe.** Signal ids are request ids derived from the source event; Conductor stores a signal once and Pi submits a request id once.
- **Leases.** The Session's lease and fencing stay. `storage.lost` closes the harness, so a stalled owner whose lease was taken over can't send a duplicate model request.

#### Use cases

1. **Mention in #help.** Alice mentions the bot; the conversation's home is her thread, which it follows. Bob's unmentioned reply steers the run or starts the next one; answers post in the thread.
2. **Every message in a channel** (`slack.message({ channel })`): the agent replies in each message's thread and follows it; irrelevant messages end in `no_reply`.
3. **One conversation for #ops** (`session: "channel"`): messages from many threads reach one conversation; each is answered in its own thread, and each thread is followed.
4. **Slack → PR → review on GitHub.** The agent opens a PR and follows it. A changes-requested review arrives as an urgent notification; the agent pushes a fix, answers on GitHub with `github_reply_to_review`, and its final reply ("Addressed the review on #42") posts in the Slack home. CI passing ends in `no_reply`; after the merge the agent unfollows.
5. **Started on GitHub.** "@bot can you check this?" in a PR comment: the home is the PR, and replies post there.
6. **Sentry issue** with `home: slack.channel("C_BUGS")`: the first reply creates a thread in #bugs, followed from then on.
7. **Background subagent.** The lead replies "on it"; the child's report arrives as input, and the lead's answer posts to the same thread.
8. **Workflow calling an agent.** `ctx.agent` returns the answer; nothing is posted.

#### Subagents

A subagent is another agent the lead delegates to. It runs as a child conversation in the lead's Session, using Pi's ownership model: no Conductor round trip, aborts and idle waits propagate through ownership, and restarts resume children.

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
	subagents: [researcher, reviewer, { agent: "self", context: "fork" }],
	limits: { depth: 1, concurrent: 5 }, // defaults
});
```

Each entry is an agent, `"self"` (a copy of the lead), or `{ agent, context?, background? }`:

| Option       | Values                                     | Meaning                                                                                     |
| ------------ | ------------------------------------------ | ------------------------------------------------------------------------------------------- |
| `context`    | `"fresh"` (default), `"fork"`              | Fresh: the child sees only its task. Fork: the child inherits the lead's transcript so far. |
| `background` | `"allow"` (default), `"always"`, `"never"` | Whether the model may, must, or must not run the child in the background.                   |

**What the model sees.** One tool per subagent, named after the agent and described by its `description`: `researcher({ task, background?, name? })`. A foreground call waits and returns the child's answer as the tool result. A background call returns a name at once (`researcher-1`); the answer arrives later as an input to the lead, `<subagent name="researcher-1" status="done">…</subagent>`. Calling with the `name` of a running child continues it (steering it if busy). `subagent_stop({ name })` stops a child. A short section lists running children and their status.

**How it maps to Pi.**

| Fabrial           | Pi                                                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Foreground call   | Child conversation owned by the tool call (Pi example 22): aborting the lead aborts it                                                                             |
| Background child  | Child owned by a background anchor task, so the lead's Esc and idle waits stop there (Pi example 23)                                                               |
| Background answer | Reporter task that submits the answer to the lead as a `followUp`, with a request ID                                                                               |
| `context: "fork"` | `conversation.fork(entryId)`, owned the same way                                                                                                                   |
| Continue, stop    | A running child: `submit` (`steer` if busy). An idle child: `fork()` at its newest entry, owned by the new call, so aborts still flow through ownership; `abort()` |

**Rules.**

- When the lead's run ends depends on how it was started. Under `ctx.agent` (call), the call settles only after its background children have reported, so a waiting workflow never gets an answer while work is still running. Otherwise reports arrive as follow-ups, and the lead's answer posts to the thread of the input that started the child.
- Only the lead speaks in chat. Children have no chat binding; their activity shows in the lead's status ("researcher: searching docs…").
- Children act under the lead's request context: workflow tools they call (approvals included) see the same actor, thread, and approvers.
- Child output is data: it is escaped inside the `<subagent>` tag. A failed child reports `status="failed"` with a generic message; provider errors go to logs, as in [Errors in sent runs](#decisions).
- Limits: with `depth: 1` (default) children get no subagent tools; reaching `concurrent` returns a tool error the model can act on. A slot frees in the commit that settles the child's answer.
- Lease: a driver keeps the Session while background children work, but releases it whenever every live task (the lead's or a child's) waits on a workflow, and resumes when any of them completes; other input to the Session is processed meanwhile.
- Work that waits for days, such as an approval, is still a workflow in `tools`. Orchestration from code is `ctx.agent`; there is no separate API.
- Background children are owned by tasks in the lead's Session, so their work continues through ordinary deliveries; a report is an input to the lead like any other. Cancelling a run aborts the background children that run started or continued; aborting only the lead's Pi run does not. A stopped child reports nothing. Continuing a background child keeps it in the background, even without `background: true`.

Compared with others: one tool per subagent routed by `description` follows Mastra and Eve; fresh context by default with an opt-in fork follows Claude's Agent SDK and Eve (Mastra forwards the full parent conversation by default); continuing a named child and background reports come from Pi; the call-waits-for-children rule comes from Eve's "a turn can't end while tasks are working".

#### Permissions

Permissions live only on tools. An agent is offered every tool its definition lists, whoever is talking to it, because a conversation can have many participants. A tool that needs permission is a workflow that calls `ctx.waitForApproval` with fixed approvers (`run-sql` always asks `engineeringTriage`), so participants shape what the agent proposes and only the assigned approvers let it act. The approval card shows the exact arguments and the inputs that led to the call, including their source, so the approver can spot a request steered in from a PR comment.

#### Compared with Mastra

Mastra's [signals](https://mastra.ai/docs/harness/signals), [signal providers](https://mastra.ai/docs/harness/signal-providers), and [AgentController](https://mastra.ai/docs/harness/agent-controller) split the problem the same way:

| Fabrial                     | Mastra                                                     | Difference                                                                                 |
| --------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Conversation                | Thread                                                     | Same.                                                                                      |
| Session                     | AgentController Session                                    | Mastra's is live state (mode, model, grants, pending approvals) and is lost on restart.    |
| Input kinds and delivery    | `sendMessage`, `queueMessage`, `sendSignal`, notifications | Mastra's notification inbox (priority, coalescing, summaries) is richer; borrowed in part. |
| Home and automatic follows  | Channels (one chat thread → one session)                   | Fabrial also follows threads and computes session keys, so routing needs no state.         |
| `ctx.follow`                | Signal providers + `github_subscribe_pr`                   | Mastra's registry is in memory, GitHub allows one PR per thread and polls.                 |
| Subagents (fresh, `fork`)   | Subagent definitions, `forked: true`                       | Same modes. Pi adds persistent background subagents.                                       |
| Tool approvals as workflows | Permission policies with `ask`                             | Mastra's approvals live in memory and go stale on restart.                                 |
| Session lease in Postgres   | Leases over pub/sub (Redis Streams across instances)       | No extra infrastructure.                                                                   |

### Coding agents (after v1)

Delegated to an external provider through start / status / cancel / result. The provider owns the checkout, sandbox, session, and logs. The Fabrial workflow owns why the job started, the authorized repo/base revision, links (Sentry, Linear), and what may happen afterward (PR, review, approval).

## Packages

```text
fabrial/
├── package.json  pnpm-workspace.yaml   # workspaces + catalog
├── justfile  .oxlintrc.json  .oxfmtrc.jsonc  cliff.toml  LICENSE
├── PLAN.md
├── packages/
│   ├── fabrial/          # core + runtimes (conductor, pi, chat as internal modules)
│   └── slack/  github/  linear/
└── examples/
    └── acme/         # v1 reference app
```

| Package                                                | Contents                                                                                                                                                                                                                   |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fabrial`                                              | The whole framework: `createFabrial` and every `f.define*`, `f.app`, plugin lifecycle, identity, approvals, response context, and the runtimes as internal modules (Conductor, Pi, Chat SDK). `fabrial/testing` for tests. |
| `@fabrial/slack`, `@fabrial/github`, `@fabrial/linear` | Provider events, clients, auth, context loaders, identities.                                                                                                                                                               |
| `@fabrial/sentry`, `@fabrial/langfuse`                 | After v1.                                                                                                                                                                                                                  |

These are packages, not services. They can all run in one process.

## Decisions

Recorded as open questions are resolved.

- **Conductor gaps are tracked, not worked around up front.** Fabrial assumes the Conductor behavior it needs. Gaps go in [Conductor issues](#conductor-issues) and get fixed in Conductor later, if they still exist by then.
- **Context via execution metadata + middleware (the Temporal/Hatchet/Inngest/Trigger.dev pattern).** Fabrial stores its serializable invocation context (`replyTo`, `origin`, `requestedBy`) as Conductor execution metadata, which propagates event → run → child automatically. Fabrial middleware rebuilds `ctx.thread`, `ctx.clients`, and `ctx.actor` from that metadata on every start and resume. Payloads stay clean, with no Fabrial envelope.
- **Pi Session per thread by default, in Postgres, opened on demand.** Agents choose other Session keys with `session` (see [Conversations](#conversations)). Fabrial ships a Postgres storage backend for Pi (validated with Pi's `registerStorageConformance`) that holds one logical Session per key: `slack:<installationId>:<threadId>` for chat, or the interaction ID for non-chat triggers. Agents in the same thread are separate conversations in that Session, chosen by `[installationId, threadId, agentName]`.
  - `ctx.agent` drives the harness inside a Conductor execution grouped by Session ID under a strict limit of 1 (Conductor issue 4), so only one worker owns a Session at a time. That execution opens the harness, submits with a stable request ID, runs Pi's scheduler until idle or a result arrives, then closes the harness and releases the worker.
  - **Fencing**: each open takes a lease epoch, and every commit checks it, so a stale worker can't commit after a takeover.
  - Long tool calls (e.g. a workflow tool waiting days for approval) close the harness. When the child completes, a Conductor execution reopens the Session, and the `pi.tool` task reconnects through its invocation key.
- **Explicit IDs for every durable operation (Inngest model).** `ctx.thread.post("ack", …)`, `ctx.agent("investigate", …)`, `ctx.evaluate("route", …)`, `ctx.waitForApproval("approve-repair", …)`, and so on. Repeated IDs get automatic suffixes. Deploys are tolerated by ID: new IDs run and missing ones are ignored. There is no call-order determinism or version pinning. This is the most robust option across deploys, at the cost of some verbosity.
- **Cancellation is structured by default, with a per-call `detached` opt-out.** Cancelling an execution cancels every call it made: `ctx.agent` (Pi conversation abort), `ctx.invoke`, and workflows called as tools, recursively. A pending `waitForApproval` updates its card to "Cancelled" and rejects later clicks. Opt out per call with `{ detached: true }`. Cancellation is cooperative: a running step finishes and its result is recorded, and cancellation takes effect at the next durable operation. A "stop" in a thread cancels the current run of the conversation that follows it.
- **`run(input, ctx)` for workflows and tools.** For triggered runs, `input` is the typed trigger event (a union for multiple triggers). For `ctx.invoke` and tool calls it is the workflow's typed input.
- **Plugins: `definePlugin(factory | object)`** with the capability set in [Plugins](#plugins). Chat SDK is just the optional `chat` capability, so non-chat sources like Sentry or internal webhooks are first-class (`events` + `routes`). The shape follows Better Auth and Executor: an option factory, a declarative object, and inferred types. Global hooks are `hooks: { workflow, agent }` in Fabrial shapes: workflow middleware and agent observers. Plugin tools live in `toolkit`. Trigger helpers are static exports.
- **Typed clients come from an instance, not global type augmentation.** `createFabrial({ database, models, plugins, identity })` returns `f`; every `f.define*` gets `ctx.clients` and plugin toolkit ids typed from `f`'s plugins, and a misspelled client is a type error. The instance holds no workflows (no import cycles); `f.app({ workflows })` lists what runs. `identity` lists users only; groups are referenced where they're used. There are no free `defineWorkflow`/`fabrial()` functions and no `declare module` registration. Most backend frameworks do it this way (Conductor, Inngest, tRPC, Hono); global `Register` augmentation is a frontend-library pattern and felt like magic.
- **Plugin values at definition, from env (the Better Auth / Inngest style).** Plugins get their options when passed to `createFabrial`, typically read from env when the module loads. Factories are side-effect free and nothing connects until `app.start()`. Tests set env in a `globalSetup` (e.g. testcontainers URLs, test secrets) before importing the app, and use spies on `app.host.clients()` for fake clients. There is one way to configure plugins: no `f.app()` overrides and no type/value split (the NestJS / Effect style was considered and rejected as heavier).
- **Credentials are plugin options.** Executor's provider/account split (an accounts table, secret storage, slots) is deferred until several accounts per provider or per-person credentials are needed. A second installation is a second plugin instance with its own `id`.
- **`f.defineAgent` with Fabrial fields only.** `name`, `description` (required), `model` (a `"provider/model"` string), `thinking`, `instructions` (strings and per-run functions), `tools` (tools and workflows), `toolkits` (plugin ids and app toolkits), `state`, `subagents`, and `limits`. There is no pass-through to the runtime's agent config and no `configure` escape hatch: a missing capability becomes a field. `ctx.agent` options are `input`, `output` (typed structured result, validated from a JSON answer), and `detached`. No Fabrial-owned agent features such as memory or per-agent permissions.
- **v1 scope: Slack + the SQL approval flow + GitHub and Linear.**
  - Packages: `fabrial`, `@fabrial/slack`, `@fabrial/github`, `@fabrial/linear`.
  - Reference flow: a `generalAssistant` workflow (Slack mention) → a support agent → a `runSql` workflow tool with triage approval in a Slack DM → resume → final reply in the thread. It must survive rejection, expiry, duplicate clicks, a deploy during approval, and a crash right after the DB write.
  - GitHub and Linear prove that plugins, triggers, and `ctx.thread` generalize beyond Slack (PR/issue comments as threads, non-mention events such as PR opened or issue created), with at least one workflow each.
  - Not in v1: Sentry, Executor, coding-agent providers. Next up: Sentry bug intake.
- **Identity in v1: users in code; groups either in code or from Slack user groups.** `defineUser` maps Slack, GitHub, and Linear identities. Groups come in two kinds, both shipped:
  - Code-defined: `defineGroup({ members })` or `defineGroup({ resolve })`, with helpers such as `weeklyRotation([...], { start })`. Versioned in git, but a swap needs a deploy.
  - Slack-backed: `slack.userGroup({ id, handle: "triage" })` resolves the current members of a Slack user group. Rotation happens in Slack (or via an on-call tool that syncs the group) with no deploy.
  - `defineGroup({ resolve })` remains the escape hatch for anything else (Linear triage, PagerDuty, …). No email auto-matching.
- **Package naming: unscoped `fabrial` for the framework + `@fabrial/*` for plugins** (`@fabrial/slack`, `@fabrial/github`, …). `fabrial` on npm was unpublished in 2020 and should be claimable. Confirm the npm scope `@fabrial` before the first publish.
- **Repo and tooling follow postgres-conductor's conventions, with Node instead of Bun.** pnpm workspaces with a catalog, vitest (+ testcontainers Postgres for integration tests), oxlint (type-aware) + oxfmt, a `justfile` (`just ready | lint | format | test`), git-cliff, and the MIT license. Node only (≥ 22), plain ESM.
- **Conversations answer where they live; threads route by session key.** Agents have triggers; an event goes to a conversation keyed by `[session, agent]`, computed from the event (`"thread"` by default, `"channel"`, or a function). Final replies post automatically to the input's thread when it is followed, otherwise to the conversation's home; `no_reply` stays silent; posting anywhere else is a plugin tool. An agent follows every thread it receives a message from, and `ctx.follow` / `ctx.unfollow` add other threads and resources; follows are records in Chat SDK thread state. There is no thread ownership, interaction, reply buffer, or `reply` tool, and workflows don't wait for chat replies (`waitForReply`, `ask`, `nextReply`, and `ctx.handoff` are removed). Modeled on Eve (automatic final reply, `no_reply`) and OpenClaw (session keys computed from the source).
  - Chat SDK thread state has a 30-day TTL refreshed on every write; a follow idle for longer lapses.
  - Chat SDK's `thread.signal` (Slack's native agent stop button) cancels the following conversation's current run.
- **Sessions are hosted by keyed Conductor executions; inputs are signals.** One execution per Session key hosts the Pi Session while Pi has work, and every input reaches it with `signalWithStart` (Conductor item 12), so steering and cancelling are native Pi calls in process. This replaced an inbox table, then hand-overs at Pi's `afterTools` boundary (verified in a spike, but every edge case around closing a Session mid-run had to be handled), and boundary event reads. It follows the hosts other harnesses use: an addressable owner per conversation (Cloudflare Durable Objects, Temporal signals, Eve's run inbox, Mastra's lease-owner routing).
- **Trigger overlap: the most specific trigger receives the event, `observe: true` adds side work, and ties fail at startup.** Agent and workflow triggers compete together for messages that no conversation follows, so exactly one handles an interactive message. The most specific matching trigger wins. Specificity is defined per plugin (Slack: channel+thread > channel > workspace). Two owners with equal specificity on the same event make `app.start()` throw. Triggers marked `observe: true` always run as well, independently and without `ctx.thread`. They can't post into or receive replies from that thread, but can still use clients or a `replyTo` elsewhere.
  ```ts
  export const changelog = defineWorkflow({
  	name: "changelog",
  	triggers: [github.pullRequestOpened({ repo: "acme/app", observe: true })], // prReview owns the PR thread
  	async run(pr, ctx) {
  		/* no ctx.thread */
  	},
  });
  ```
- **Runtimes are hidden behind Fabrial's API** (replaces thin adapters returning native Pi objects). App authors install one `fabrial` package and configure a database and model credentials; Conductor, Pi, and Chat SDK are internal modules. Tools use a Standard Schema `input` (converted to JSON Schema for the model) and return a string or JSON; toolkits replace Pi extensions and sections; hooks are Fabrial observers. There is no escape hatch (`ctx.raw`, `configure`) while Fabrial is in preview: what apps need becomes API. Plugin authors writing a chat provider still hand over a Chat SDK adapter, the one runtime type they see. Internally, per-request context still comes from a `fabrial.invocation` document that `ctx.agent` commits right before `submit()`, keyed by request ID.
- **State is `defineState`, and app authors don't write Pi tasks.** `defineState({ name, scope, schema, initial, render })` with scopes `thread` and `agent`, backed by Pi documents. `ctx.state(S).get/update` works in workflows (durable ops with IDs) and in tools (committed with the tool result). Agents see the states listed in `defineAgent.state` through `render`. Durable or long-running tool work is a workflow in `tools`. Pi tasks and raw documents stay internal / advanced, with no `defineTask` adapter. This replaces exposing raw Pi (`ctx.pi`) and `ctx.agent` document options, both of which made the DX convoluted.
- **The agent registry is derived, and nothing is selected by default.** There is no app-level toolkit list. Installed = Fabrial's internal toolkit + every plugin's `toolkit` + every toolkit referenced by an agent reachable from the registered workflows. Each agent gets exactly what its `toolkits` and `tools` list. On startup, Fabrial aborts pending agent work whose toolkit is no longer installed and logs it.
- **Approval is explicit code only.** A tool that needs approval is a workflow that calls `ctx.waitForApproval`, listed in an agent's `tools`. The approval sits next to the logic, so previews, conditions, and multiple approvals are plain code. Not for now: a declarative `approval` option on `defineWorkflow`, or a `requireApproval` option on `f.defineTool`. Add them later if needed.
- **Orphaned agent work is aborted at startup.** When a deploy removes a toolkit that pending agent work still needs, `app.start()` aborts that work and logs it instead of failing the deploy.
- **Webhooks wait for durable ingress.** Chat and plugin routes respond only after the event is stored. There is no `waitUntil`: ingress is a fast insert, and acknowledging earlier risks losing events.
- **Errors in agent runs (modeled on Eve's `turn.failed` handling).** Pi retries transient model errors (overload, rate limits, 5xx, network) durably. In production the backoff spans about 5 minutes: failed attempt n is retried while n ≤ 9, after min(2 s · 2^(n−1), 60 s), i.e. 2+4+8+16+32+60+60+60+60 = 302 s (`MODEL_RETRY` in `production.ts`; tests keep retries off unless they opt in). Context overflow compacts and retries. While a retry waits, the sent run's status reads "Waiting for the model provider…". When sent input still settles `unanswered`:
  - `model_error` or `no_model`: the driver posts one error reply to the request's `replyTo` through the managed effect path, keyed by request id, so redeliveries and restarts post it once. It reads "I couldn't reach the model provider while handling your request. Please try again in a few minutes." when the error text is transient (the retry budget ran out), and otherwise "I hit an error while handling your request. Please try again, rephrase, or reach out if it keeps failing.", followed by `_Error id: \`<id>\`_`. The provider's text is only logged, with the same generated error id. The progress status is cleared.
  - `aborted`, `reset`, `stale`: the input was ended on purpose; no reply.
  - The conversation stays usable, and later input still drives. `ctx.agent` (call) is unchanged: the calling workflow receives the error.
- **Permissions only on tools.** Agents are offered every tool their definition lists, regardless of who is talking. There are no access lists on workflows, triggers, or tools (`access.invoke` is removed); a tool that needs permission asks its fixed approvers. This keeps shared conversations unambiguous: participants shape proposals, approvers authorize actions.

## Conductor issues

Conductor behaviors Fabrial relies on, checked against Conductor `main` @ `33228e3`. The full handover, with current behavior, proposed APIs, and acceptance criteria, is in [PGCONDUCTOR.md](./PGCONDUCTOR.md).

1. **Race-safe `waitForEvent`** (P0). Events emitted before the wait registers are lost, so a fast approval click can be missed.
2. **Execution metadata** (P0). Propagated event → run → child, generalizing the existing `trace_context` plumbing.
3. **Middleware** (P0). Per-run/resume context transformation. `context` is static today.
4. **Strict per-key mutual exclusion** (P1). `groupConcurrency` is soft, but a Pi Session needs exactly one owner. Expose the claim token for fencing.
5. **Cancellation** (P1). Per-call opt-out (`cancelWithParent: false`) and a terminal status distinct from failure.
6. **Wait for any** (P1). Race events, child results, and timers (`ctx.race`).
7. **Start without waiting** (P1). Durable, deduped `ctx.start` for deliveries and workflow tools.
8. **Idempotent `emit` with options** (P0). Dedup ID and metadata, for webhook redelivery.
9. **Node-compatible package** (P0). Today's build is Bun-only, with no exports or types. Fabrial vendors Conductor as a pinned submodule until this ships.
10. **Terminal-state hooks** (P1). Notify on completion, failure, and cancellation in every state. The adapter polls for settled executions meanwhile.
11. **Stop hangs on queued claims** (P0). Workers enter handlers with an already-aborted signal after stop, so `stop()` can hang forever. The adapter releases those claims meanwhile.
12. **Signals** (P0). `ctx.onSignal` in running handlers, `signalWithStart` for keyed executions, delivery through the worker poll, and re-queueing an execution that received signals while finishing. Agent Sessions are built on it.

## Open questions

- **Messages during a long tool call.** Pi places a steer after the current tool round, so a message sent while a workflow tool waits for an approval reaches the agent only after the approval. Eve aborts waiting tool calls on a steering message. Should a message abort (or answer) a waiting workflow tool?
