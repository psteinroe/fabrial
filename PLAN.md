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

Reference for chat UX: Vercel [Eve](https://github.com/vercel/eve). Its persisted thread bindings, thread-handle reconstruction on resume, bounded/attributed history loading, and default status rendering are the patterns to borrow.

## Ownership

> Native runtimes own execution; Fabrial carries context across them.

| Concern                                                         | Owner                                    |
| --------------------------------------------------------------- | ---------------------------------------- |
| Event-driven and scheduled workflows, steps, retries, waits     | PG Conductor (via `defineWorkflow`)      |
| Agent conversations, model execution, agent tasks               | Pi Durable (native)                      |
| Session/conversation state                                      | Pi documents (native `defineDoc`)        |
| Agent capability bundles                                        | Pi extensions (native `defineExtension`) |
| Platform messages, threads, cards, clients                      | Chat SDK (native)                        |
| Provider events → triggers, context reconstruction, clients     | Fabrial plugins                          |
| Workflow ↔ agent bridges, approvals, response context, identity | Fabrial core                             |
| Users and groups                                                | Fabrial identity definitions/providers   |

Fabrial owns no tables. Its per-thread routing state (active interaction, current handler, response message refs) lives in Chat SDK thread state. Everything else is Conductor step results, events, and metadata, or Pi documents.

## Public API

All code below is the proposed Fabrial API.

### `defineWorkflow`

The main abstraction. It registers a Conductor task, turns plugin triggers into Conductor subscriptions, and rebuilds the invocation context on every start or resume. Conductor execution options (concurrency, retries, …) are passed through.

```ts
export const repairCustomer = defineWorkflow({
	name: "repair-customer",
	triggers: [slack.mentioned({ channel: "C_SUPPORT" })],
	access: { invoke: support },
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

| API                                               | Behavior                                                                                                       |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `ctx.thread`                                      | Chat SDK thread rebuilt from the trigger's persisted references. Survives restarts and multi-day waits.        |
| `ctx.clients.*`                                   | Native provider clients from plugins, recreated on resume and never serialized.                                |
| `ctx.raw.*`                                       | Escape hatch to unwrapped native clients and Conductor/Pi handles.                                             |
| `ctx.thread.post(id, message)`                    | Durable post into the bound thread. Other thread operations follow the same `(id, …)` shape.                   |
| `ctx.agent(id, agent, opts)`                      | Runs a Pi conversation; durable submission and result bridge.                                                  |
| `ctx.state(S).get(id)` / `.update(id, fn)`        | Shared state (`defineState`), durable.                                                                         |
| `ctx.evaluate(id, { model, state, questions })`   | Structured judgment (Jev), durably recorded.                                                                   |
| `ctx.waitForApproval(id, opts)`                   | Complete approval protocol over Conductor `waitForEvent`. Sugar for `requestApproval` + awaiting the decision. |
| `ctx.requestApproval(id, opts)`                   | Posts the approval request and returns a handle: `decision()`, `cancel(reason)`.                               |
| `ctx.thread.waitForReply(id, { from?, timeout })` | Durable wait for the next reply in the interaction's thread.                                                   |
| `ctx.thread.ask(id, text, { from?, timeout })`    | Post + `waitForReply` as one durable op.                                                                       |
| `ctx.thread.nextReply({ from? })`                 | Reply awaitable for use in `ctx.race`.                                                                         |
| `ctx.race(id, { a, b, … })`                       | Durably waits for the first of several awaitables (decision, reply, timer) and returns which one won.          |
| `ctx.invoke(id, workflow, input)`                 | Calls another workflow as a child and returns its result.                                                      |
| `ctx.handoff(id, workflow, input)`                | Transfers responsibility for the interaction to another workflow.                                              |
| `ctx.step(name, fn)`                              | Native Conductor step for arbitrary code.                                                                      |
| `workflow.asTool()`                               | Generates a native Pi tool that starts or reconnects to the workflow.                                          |

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

### Agents (Pi)

Agents run on Pi Durable. App authors write agents (`defineAgent`), tools (`defineTool`, or `workflow.asTool()` for anything durable or long-running), and shared state (`defineState`). Pi tasks and raw Pi documents are Pi's internal machinery and an advanced escape hatch only; app authors don't need them.

`defineAgent` is a thin wrapper over Pi's per-conversation agent config (`conversation.configure()`). It adds a stable `name` (used for the continuation key, tracing, and progress labels), plugin extensions referenced by plugin `id` (they are built at runtime from live clients), and the `state` the agent can see. Every other field passes through, and unset fields follow host defaults as in Pi:

```ts
export const bugInvestigator = defineAgent({
	name: "bug-investigator",
	model: { provider: "anthropic", modelId: "claude-sonnet-5" },
	thinkingLevel: "medium",
	instructions: "Investigate bug reports. Find root cause and affected customers.",
	extensions: ["github", "linear", Investigation], // plugin ids (typed) + native Pi extensions
	tools: [readOnlyQuery, addFinding, runSql.asTool()], // Pi tools, incl. workflows as tools
	state: [Findings], // rendered into the prompt via their `render`
});

const analysis = await ctx.agent("investigate", bugInvestigator, { input });
const plan = await ctx.agent("plan", cleanupPlanner, { input, output: CleanupPlan }); // typed structured result
// native escape hatch: `configure` lives on defineAgent, because the agent runs in its own execution
export const repoAgent = defineAgent({
	name: "repo-agent",
	configure: (conversation) => conversation.configure({ cwd: "/work/repo" }),
});
```

Tools and sections use thin `@fabrial/pi` adapters that return **native Pi objects**. Their handlers receive `ctx` = Pi's `api` plus Fabrial context:

```ts
import { defineTool, section } from "@fabrial/pi";
import { defineExtension } from "@earendil-works/pi-durable";

export const addFinding = defineTool({
	name: "add_finding",
	description: "Record a finding about this customer issue",
	parameters: Type.Object({ text: Type.String() }),
	execute: async ({ text }, ctx) => {
		ctx.actor; // who sent the message this run answers
		await ctx.state(Findings).update((s) => {
			s.notes.push(text);
		}); // committed with the tool result
		return { content: [{ type: "text", text: "recorded" }] };
	},
});

export const Investigation = defineExtension({
	// native
	name: "investigation",
	tools: [addFinding, lookupCustomer],
	sections: [section("requester", (input, ctx) => `Requested by ${ctx.actor.name}.`)],
});
```

### State: `defineState`

State that agents see and change and that outlives a single step. One concept and one API in workflows and tools, backed by Pi documents in the thread's Session:

```ts
export const Findings = defineState({
	name: "findings",
	scope: "thread", // "interaction" | "thread" | "agent"
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

| Scope         | Lives for                         | Shared by                                               | Pi document underneath              |
| ------------- | --------------------------------- | ------------------------------------------------------- | ----------------------------------- |
| `interaction` | one request, across handoffs      | the router, handoff targets, and their agents and tools | session doc keyed by interaction ID |
| `thread`      | the whole thread, across requests | every workflow and agent in the thread                  | session doc                         |
| `agent`       | one agent's conversation          | that agent only; forks and rewinds with it              | conversation doc                    |

- A workflow's own local state is just its variables, made durable through steps. `interaction` scope is the workflow-level state that agents can also see, and it survives handoffs.
- Workflow `get`/`update` briefly acquire the thread's Pi Session (strict per-key lock). They can wait behind an agent that is running in the same thread.
- `render` is how state becomes model context. Agents only see the states listed in `defineAgent.state`.
- Schema changes follow Pi document `version` + migration.
- Fabrial ships an internal `fabrial.invocation` document (per request ID: actor, interaction, replyTo, origin) and a `ChatBinding` document. Forked conversations get **no** outbound chat binding by default.

### Runtime bridges

| Direction                                           | Behavior                                                                                                                               |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Workflow → agent (`ctx.agent`)                      | Create/find conversation, submit with a stable request ID, persist the reference, suspend, resume with the result.                     |
| Agent → workflow (`workflow.asTool()`)              | Start/reconnect to the Conductor execution with a stable invocation key and return its result as the tool result. Access checks apply. |
| Workflow → workflow                                 | Native Conductor child invocation.                                                                                                     |
| Pi task → Pi task / document                        | Native Pi.                                                                                                                             |
| Managed external ops inside Pi (e.g. `thread.post`) | Routed through the Conductor bridge, keyed by Pi task ID + operation ID, so external effects have one durability implementation.       |

A workflow called by an agent must not synchronously query that same busy conversation, because that would deadlock. It starts a separate conversation instead.

Approvals are always Conductor-backed. A Pi tool that needs approval is a workflow exposed with `asTool()`.

### `ctx.evaluate` (Jev)

The shape follows Vercel AI SDK's experimental `evaluate`. It is implemented on Pi AI's native `models.classify()` and needs neither AI SDK nor AI Gateway.

```ts
const result = await ctx.evaluate("route", {
	model: jev,
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
3. Receives the interaction through the plugin's handler.
4. Authenticates the actor, checks eligibility, and correlates the answer to the request. Duplicate clicks and expiry are handled.
5. Emits the internal event that satisfies `waitForEvent`.
6. Updates the card and returns the decision.

`decision.status` is `"approved" | "rejected" | "expired" | "cancelled"`, with `decision.approved` as shorthand. `waitForApproval` is sugar for `ctx.requestApproval(id, opts)`, which returns a handle with `decision()` and `cancel(reason)`, followed by awaiting the decision.

Rules:

- An approval binds to the exact proposal and arguments. A changed proposal needs a new approval.
- By default the requester's status message shows **[Cancel request]**. Free-text replies during the wait are buffered and never change or cancel the request. Apps that want smarter handling combine `requestApproval`, `ctx.race`, and `ctx.thread.nextReply` themselves.
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

Users are defined in code. Groups are defined in code or backed by Slack user groups, and `resolve` handles anything else:

```ts
export const alice = defineUser({
	id: "alice",
	identities: [
		slack.identity({ workspace: "acme", userId: "U123" }),
		github.identity({ login: "alice" }),
		linear.identity({ userId: "lin_abc" }),
	],
});

// code-defined
export const engineering = defineGroup({ id: "engineering", members: [alice, bob] });
export const backendTriage = defineGroup({
	id: "backend-triage",
	resolve: () => [weeklyRotation([alice, bob], { start: "2026-01-05" })],
});

// Slack-backed: whoever is in @triage right now; rotate in Slack, no deploy
export const engineeringTriage = slack.userGroup({ id: "engineering-triage", handle: "triage" });

// anything else
export const linearTriage = defineGroup({
	id: "linear-triage",
	resolve: (ctx) => ctx.clients.linear.triageResponsibility({ team: "ENG" }),
});
```

- Every invocation has its own identity context: `requestedBy`, `executedBy`, and approval `decidedBy`. There is no mutable conversation-wide "current user", because a thread can have many participants.
- Permissions are separate: discovering a workflow (as a tool), invoking it (`access.invoke`), and approving its actions (`approvers`). Channel context can help routing but never grants authority.
- Tools exposed to agents are filtered by the requester's permissions.

### Plugins

Plugins are how everything outside the core plugs in: chat providers (Slack via Chat SDK), non-chat sources (Sentry, internal webhooks), clients, tracing, and identity directories. Chat SDK is just one optional capability.

The design borrows from Better Auth (option factory, declarative object, inferred types, plugins extend the core via hooks), Executor (`definePlugin(() => ({ … }))`, one canonical implementation reused by every caller), and Pi (native extension/hook shapes, no side effects in the factory).

`definePlugin` takes a factory (for plugins with options) or a plain object, and infers types for `ctx.clients`, events, and triggers:

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

		// Pi tools for agents that select this extension, built on the same client
		extension: ({ sentry }) => defineExtension({ name: "sentry", tools: [getIssueTool(sentry)] }),

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
			agent: [hook(GenerationTask, { afterGeneration: (gen) => client.generation(gen) })],
		},
		shutdown: () => client.flushAsync(),
	};
});

// app-local plugin without options: plain object
export const triageRotation = definePlugin({
	id: "triage-rotation",
	clients: () => ({ rotations: new RotationService(env.DATABASE_URL) }),
});

// app
export const app = fabrial({
	conductor,
	pi: { models },
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
	workflows: [generalAssistant, bugIntake, runSql],
	identity: [alice, support, engineeringTriage],
});

export default { fetch: app.fetch }; // one handler for /slack/events, /sentry/webhook, …
await app.start(); // Conductor workers + Pi bridge
```

A workflow using both providers:

```ts
export const bugIntake = defineWorkflow({
	name: "bug-intake",
	input: z.object({ message: SlackMessage }), // #bugs messages arrive via handoff from generalAssistant
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
| `events`            | Typed event definitions, registered as Conductor events.                                                                              |
| `routes`            | Webhook ingress, served by `app.fetch` (mountable in Hono, Next.js, Bun.serve, …).                                                    |
| `chat`              | Chat SDK adapter. Supplies routes, typed message events, `ctx.thread`, cards, status rendering, and history loading.                  |
| `clients`           | Canonical live clients exposed as `ctx.clients.*` and rebuilt on every run/resume. Workflow code and agent tools use the same client. |
| `extension`         | Pi extension (tools, sections, hooks) for agents that select it.                                                                      |
| `context`           | Model context for a trigger's origin.                                                                                                 |
| `hooks.workflow`    | Conductor middleware for **every** workflow run/resume.                                                                               |
| `hooks.agent`       | Native Pi hooks installed on **every** harness. Hooks scoped to particular agents go in `extension`.                                  |
| `identity`          | External identity → principal.                                                                                                        |
| `init` / `shutdown` | Lifecycle. Factories must have no side effects.                                                                                       |

- Credentials are plugin options. There is no provider/account split, accounts table, or secret store yet.
- Triggers are real Conductor triggers. Plugins translate provider events into typed Conductor events, and Conductor owns matching and starting.
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

> The workflow owns the interaction. The active task supplies activity. The chat integration renders it.

- Each chat-triggered invocation gets a framework-managed **response context**: destination thread, invocation identity, and status/message references.
- `ctx.agent` attaches the agent's activity (derived from Pi's execution stream) to the response context automatically. Statuses look like "Thinking…", "Looking up the customer…", "Waiting for approval". Public progress summaries and tool descriptions are shown, never raw reasoning.
- Nested calls (workflow → agent → tool-workflow → agent) inherit the context. The deepest active child is the foreground progress source, and the parent takes over again when it finishes. Parallel activity is aggregated. A nested agent finishing never closes the outer interaction.
- The renderer is the only writer to the platform.
- By default an agent called as a workflow step shows progress and **returns** its answer without posting it. Publishing an answer is deliberate: `ctx.thread.post`, an explicit streaming/publish option, or an intentional `sendMessage` tool.
- Durability tiers: status/typing updates are throttled, replaceable, and may be lost on crash. Partial streaming updates a single tracked message. Final replies and approval cards are durable operations with stable identities. After a restart, presentation reattaches.

### Routing: one bot, many workflows

The single entry point is an ordinary workflow with triggers and plain `if/else`. There is no routing DSL.

```ts
export const generalAssistant = defineWorkflow({
	name: "general-assistant",
	triggers: [slack.mentioned(), slack.newThread({ channel: channels.bugs })],

	async run(message, ctx) {
		if (message.channelId === channels.bugs) {
			return ctx.handoff("bug-intake", bugIntake, { message });
		}

		const result = await ctx.evaluate("route", {
			model: jev,
			state: { message: message.text, channel: ctx.thread.channel },
			questions: routingQuestions,
		});
		const intent = result.answers.intent;

		if (result.stopReason === "stop" && intent?.confidence >= threshold) {
			if (intent.choice === "bug") {
				return ctx.handoff("bug-intake", bugIntake, { message });
			}
			if (intent.choice === "customerOperations") {
				return ctx.handoff("customer-ops", customerOperations, { message });
			}
		}

		const answer = await ctx.agent("assistant", assistantAgent, {
			input: message,
			tools: [investigateCustomer.asTool(), explainDeployment.asTool()],
		});
		await ctx.thread.post("answer", answer);
	},
});
```

- Overlapping triggers on one workflow are merged, so a mention in `#bugs` produces one execution. Ingress deduplicates provider redeliveries.
- Across workflows, the most specific matching trigger owns the thread (e.g. `deployBot` on `slack.mentioned({ channel: "C_DEPLOYS" })` beats `generalAssistant` on `slack.mentioned()`). Ties are a startup error, and `observe: true` triggers run alongside without owning the thread.
- **Call** vs **handoff**: calling a workflow (`asTool`, child invoke) returns a result and posts no final answer. A handoff transfers responsibility for the interaction and its final response, and the router ends.
- Thread replies during an active interaction go to that interaction (clarification, steering), not through trigger matching again. The routing state `{ interactionId, handlerExecutionId, response }` lives in Chat SDK thread state. After completion, a new mention starts a new interaction and gets the thread history as context.
- Agent conversation continuation key: `[installationId, threadId, agentName]`, so two agents in one thread never share a conversation.

### Coding agents (after v1)

Delegated to an external provider through start / status / cancel / result. The provider owns the checkout, sandbox, session, and logs. The Fabrial workflow owns why the job started, the authorized repo/base revision, links (Sentry, Linear), and what may happen afterward (PR, review, approval).

## Packages

```text
fabrial/
├── package.json  pnpm-workspace.yaml   # workspaces + catalog
├── justfile  .oxlintrc.json  .oxfmtrc.jsonc  cliff.toml  LICENSE
├── PLAN.md
├── packages/
│   ├── fabrial/  conductor/  pi/  chat/
│   └── slack/  github/  linear/
└── examples/
    └── acme/         # v1 reference app
```

| Package                                                | Contents                                                                                                                        |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `fabrial`                                              | Core: `defineWorkflow`, plugin lifecycle, invocation-context contract, identity, approvals, response context.                   |
| `@fabrial/conductor`                                   | Event registration, context enrichment, durable dispatch helpers.                                                               |
| `@fabrial/pi`                                          | Pi Postgres storage, Conductor ↔ Pi bridges, `defineAgent`, `defineTool`/`section` adapters, `defineState`, progress rendering. |
| `@fabrial/chat`                                        | Chat SDK ingestion, thread reconstruction, default renderers.                                                                   |
| `@fabrial/slack`, `@fabrial/github`, `@fabrial/linear` | Provider events, clients, auth, context loaders, identities.                                                                    |
| `@fabrial/sentry`, `@fabrial/langfuse`                 | After v1.                                                                                                                       |

These are packages, not services. They can all run in one process.

## Decisions

Recorded as open questions are resolved.

- **Conductor gaps are tracked, not worked around up front.** Fabrial assumes the Conductor behavior it needs. Gaps go in [Conductor issues](#conductor-issues) and get fixed in Conductor later, if they still exist by then.
- **Context via execution metadata + middleware (the Temporal/Hatchet/Inngest/Trigger.dev pattern).** Fabrial stores its serializable invocation context (`replyTo`, `origin`, `requestedBy`, `interactionId`) as Conductor execution metadata, which propagates event → run → child automatically. Fabrial middleware rebuilds `ctx.thread`, `ctx.clients`, and `ctx.actor` from that metadata on every start and resume. Payloads stay clean, with no Fabrial envelope.
- **Pi Session per thread, in Postgres, opened on demand.** `@fabrial/pi` ships a Postgres storage backend (validated with Pi's `registerStorageConformance`) that holds one logical Session per key: `slack:<installationId>:<threadId>` for chat, or the interaction ID for non-chat triggers. Agents in the same thread are separate conversations in that Session, chosen by `[installationId, threadId, agentName]`.
  - `ctx.agent` drives the harness inside a Conductor execution grouped by Session ID under a strict limit of 1 (Conductor issue 4), so only one worker owns a Session at a time. That execution opens the harness, submits with a stable request ID, runs Pi's scheduler until idle or a result arrives, then closes the harness and releases the worker.
  - **Fencing**: each open takes a lease epoch, and every commit checks it, so a stale worker can't commit after a takeover.
  - Long tool calls (e.g. `workflow.asTool()` waiting days for approval) close the harness. When the child completes, a Conductor execution reopens the Session, and the `pi.tool` task reconnects through its invocation key.
- **Explicit IDs for every durable operation (Inngest model).** `ctx.thread.post("ack", …)`, `ctx.agent("investigate", …)`, `ctx.evaluate("route", …)`, `ctx.waitForApproval("approve-repair", …)`, and so on. Repeated IDs get automatic suffixes. Deploys are tolerated by ID: new IDs run and missing ones are ignored. There is no call-order determinism or version pinning. This is the most robust option across deploys, at the cost of some verbosity.
- **Cancellation is structured by default, with a per-call `detached` opt-out.** Cancelling an execution cancels every call it made: `ctx.agent` (Pi conversation abort), `ctx.invoke`, and `asTool()` workflows, recursively. A pending `waitForApproval` updates its card to "Cancelled" and rejects later clicks. Opt out per call with `{ detached: true }`, or per tool with `workflow.asTool({ detached: true })`. Handoffs are always independent, because the receiver owns the interaction. Cancellation is cooperative: a running step finishes and its result is recorded, and cancellation takes effect at the next durable operation. A "stop" in a thread cancels the interaction's current handler.
- **`run(input, ctx)`, matching Conductor's handler shape.** For triggered runs, `input` is the typed trigger event (a union for multiple triggers). For `ctx.invoke`, `ctx.handoff`, and `asTool()` it is the workflow's typed input.
- **Plugins: `definePlugin(factory | object)`** with the capability set in [Plugins](#plugins). Chat SDK is just the optional `chat` capability, so non-chat sources like Sentry or internal webhooks are first-class (`events` + `routes`). The shape follows Better Auth and Executor: an option factory, a declarative object, and inferred types. Global hooks are `hooks: { workflow, agent }` in native Conductor-middleware and Pi-hook shapes. Agent-scoped hooks live in `extension`. Trigger helpers are static exports.
- **Credentials are plugin options.** Executor's provider/account split (an accounts table, secret storage, slots) is deferred until several accounts per provider or per-person credentials are needed. A second installation is a second plugin instance with its own `id`.
- **Thin `defineAgent`.** It holds `name`, plus plugin extensions by `id`. Everything else (`model`, `thinkingLevel`, `instructions`, `extensions`, `tools`, …) passes through to Pi's `configure()`. `ctx.agent` options are `input`, `output` (typed structured result, validated from a JSON answer), and `detached`. The native `configure` escape hatch is a `defineAgent` field, since a function can't cross into the agent's own execution. No Fabrial-owned agent features such as memory or per-agent permissions.
- **v1 scope: Slack + the SQL approval flow + GitHub and Linear.**
  - Packages: `fabrial`, `@fabrial/conductor`, `@fabrial/pi`, `@fabrial/chat`, `@fabrial/slack`, `@fabrial/github`, `@fabrial/linear`.
  - Reference flow: a `generalAssistant` workflow (Slack mention) → a support agent → a `runSql.asTool()` workflow with triage approval in a Slack DM → resume → final reply in the thread. It must survive rejection, expiry, duplicate clicks, a deploy during approval, and a crash right after the DB write.
  - GitHub and Linear prove that plugins, triggers, and `ctx.thread` generalize beyond Slack (PR/issue comments as threads, non-mention events such as PR opened or issue created), with at least one workflow each.
  - Not in v1: Sentry, Executor, coding-agent providers. Next up: Sentry bug intake.
- **Identity in v1: users in code; groups either in code or from Slack user groups.** `defineUser` maps Slack, GitHub, and Linear identities. Groups come in two kinds, both shipped:
  - Code-defined: `defineGroup({ members })` or `defineGroup({ resolve })`, with helpers such as `weeklyRotation([...], { start })`. Versioned in git, but a swap needs a deploy.
  - Slack-backed: `slack.userGroup({ id, handle: "triage" })` resolves the current members of a Slack user group. Rotation happens in Slack (or via an on-call tool that syncs the group) with no deploy.
  - `defineGroup({ resolve })` remains the escape hatch for anything else (Linear triage, PagerDuty, …). No email auto-matching.
- **Package naming: unscoped `fabrial` core + `@fabrial/*` for everything else** (`@fabrial/slack`, `@fabrial/github`, `@fabrial/conductor`, …). `fabrial` on npm was unpublished in 2020 and should be claimable. Confirm the npm scope `@fabrial` before the first publish.
- **Repo and tooling follow postgres-conductor's conventions, with Node instead of Bun.** pnpm workspaces with a catalog, vitest (+ testcontainers Postgres for integration tests), oxlint (type-aware) + oxfmt, a `justfile` (`just ready | lint | format | test`), git-cliff, and the MIT license. Node only (≥ 22), plain ESM.
- **Per-thread routing state lives in Chat SDK thread state** (Postgres state adapter), the same approach Eve takes. Fabrial owns no routing table. Ingress reads `thread.state` on every inbound message. If an interaction is active, the message becomes a correlated reply to it. Otherwise it goes through trigger matching as new work.
  ```ts
  type FabrialThreadState = {
  	interactionId: string;
  	handlerExecutionId: string; // current owner; updated on handoff
  	response: { statusMessageId?: string; finalMessageIds: string[] };
  };
  await thread.setState({ fabrial: { interactionId, handlerExecutionId, response } });
  ```
  - TTL is 30 days and refreshed on every write. An interaction idle for longer loses its binding, and later replies start new work. `waitForApproval` and reply-wait timeouts must stay below 30 days.
  - Chat SDK's `thread.signal` (Slack's native agent stop button) maps to Fabrial cancellation of the interaction's current handler.
- **Workflows receive thread replies via `ctx.thread.waitForReply` / `ctx.thread.ask`.** Replies are durable waits correlated by interaction ID, optionally restricted to `from`. Routing a reply in an active interaction:
  - The handler is running `ctx.agent`: the reply goes to the Pi conversation (follow-up/steering).
  - The handler is waiting for a reply: the reply goes to that wait.
  - Otherwise (e.g. during `waitForApproval`): the reply is **buffered** on the interaction and acknowledged with 👀. A later `waitForReply` consumes buffered replies first, and the next `ctx.agent` receives them as context.
- **Changing your mind during a pending approval uses explicit controls, with no built-in model check.** By default, the requester's status message shows **[Cancel request]**, which cancels the pending request (card → "Cancelled", `decision.status = "cancelled"`). Free-text replies are only buffered. To amend, cancel and start a new request, because an approval never silently moves to a changed proposal. Inside agents this comes from the existing rules: "never mind" steers the agent, which aborts its tool call, and structured cancellation cancels the approval. An amendment means a new tool call with a new approval, and the old card shows "Superseded".
  - Low-level primitives (`ctx.requestApproval` handle, `ctx.race`, `ctx.thread.nextReply`) ship too, so apps can build smarter handling themselves, e.g. classifying replies with their own `ctx.evaluate`:
  ```ts
  const approval = await ctx.requestApproval("approve-sql", {
  	details: proposal,
  	approvers: engineeringTriage,
  });
  const next = await ctx.race("approval-or-reply", {
  	decision: approval.decision(),
  	reply: ctx.thread.nextReply({ from: input.actor }),
  });
  if (next.reply) await approval.cancel("superseded"); // the app decides what the reply means
  ```
- **Trigger overlap: the most specific trigger owns the thread, `observe: true` adds side work, and ties fail at startup.** Each chat thread has one routing slot, so exactly one workflow owns an interactive message or event. The most specific matching trigger wins. Specificity is defined per plugin (Slack: channel+thread > channel > workspace). Two owners with equal specificity on the same event make `app.start()` throw. Triggers marked `observe: true` always run as well, independently and without `ctx.thread`. They can't post into or receive replies from that thread, but can still use clients or a `replyTo` elsewhere.
  ```ts
  export const changelog = defineWorkflow({
  	name: "changelog",
  	triggers: [github.pullRequestOpened({ repo: "acme/app", observe: true })], // prReview owns the PR thread
  	async run(pr, ctx) {
  		/* no ctx.thread */
  	},
  });
  ```
- **Pi code gets Fabrial context through thin `@fabrial/pi` adapters.** `defineTool` and `section` return **native Pi objects** (usable in native `defineExtension`, next to plain Pi tools, with hooks and `wrapTool`). The only change is that handlers receive `ctx` = Pi's `api` **plus** Fabrial fields (`actor`, `requestedBy`, `clients`, `thread`, `state`, `invoke`, `start`, `evaluate`, `interaction`), with Pi's chord `Context` bound automatically. Every other Pi field passes through unchanged. Fabrial's names are reserved and disjoint from Pi's, and a type test fails the build on a collision. Per-request context comes from a `fabrial.invocation` document that `ctx.agent` commits right before `submit()`, keyed by request ID (Pi's `submit()` isn't available on `Tx`, so it can't share the submission's commit). Hooks stay native.
- **State is `defineState`, and app authors don't write Pi tasks.** `defineState({ name, scope, schema, initial, render })` with scopes `interaction` (one request across handoffs), `thread`, and `agent`, backed by Pi documents. `ctx.state(S).get/update` works in workflows (durable ops with IDs) and in tools (committed with the tool result). Agents see the states listed in `defineAgent.state` through `render`. Durable or long-running tool work is a workflow exposed with `asTool()`. Pi tasks and raw documents stay internal / advanced, with no `defineTask` adapter. This replaces exposing raw Pi (`ctx.pi`) and `ctx.agent` document options, both of which made the DX convoluted.
- **The Pi registry is derived, and nothing is selected by default.** There is no app-level extensions list. Installed = Fabrial's internal extension + every plugin's `extension` + every extension referenced by a `defineAgent` reachable from the registered workflows. Harness `settings.extensions` selects only Fabrial's internal extension, so each agent gets exactly what its `defineAgent.extensions` lists. Documents need no registration (`defineDoc` is used directly), so the plugin `docs` key is dropped. On startup, `@fabrial/pi` checks all Sessions for pending or waiting Pi tasks whose kind no installed extension provides, and **fails the deploy** with a fix-it message: keep the extension on an agent until those tasks finish, or run `fabrial pi abort-orphans --kind …`.
- **Approval is explicit code only.** A tool that needs approval is a workflow that calls `ctx.waitForApproval`, exposed to agents with `asTool()`. The approval sits next to the logic, so previews, conditions, and multiple approvals are plain code. Not for now: a declarative `approval` option on `defineWorkflow`, or a `requireApproval` hook for native Pi tools. Add them later if needed.

## Conductor issues

Conductor behaviors Fabrial relies on, checked against Conductor `main` @ `33228e3`. The full handover, with current behavior, proposed APIs, and acceptance criteria, is in [PGCONDUCTOR.md](./PGCONDUCTOR.md).

1. **Race-safe `waitForEvent`** (P0). Events emitted before the wait registers are lost, so a fast approval click can be missed.
2. **Execution metadata** (P0). Propagated event → run → child, generalizing the existing `trace_context` plumbing.
3. **Middleware** (P0). Per-run/resume context transformation. `context` is static today.
4. **Strict per-key mutual exclusion** (P1). `groupConcurrency` is soft, but a Pi Session needs exactly one owner. Expose the claim token for fencing.
5. **Cancellation** (P1). Per-call opt-out (`cancelWithParent: false`) and a terminal status distinct from failure.
6. **Wait for any** (P1). Race events, child results, and timers (`ctx.race`).
7. **Start without waiting** (P1). Durable, deduped `ctx.start` for handoffs and `asTool()`.
8. **Idempotent `emit` with options** (P0). Dedup ID and metadata, for webhook redelivery.
9. **Node-compatible package** (P0). Today's build is Bun-only, with no exports or types. Fabrial vendors Conductor as a pinned submodule until this ships.
10. **Terminal-state hooks** (P1). Notify on completion, failure, and cancellation in every state. The adapter polls for settled executions meanwhile.

## Open questions

None right now. Add new ones here as they come up.
