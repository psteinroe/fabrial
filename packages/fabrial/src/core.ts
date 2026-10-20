import type { App, CoreConfig, FabrialHost, Logger } from "./app.ts";
import { createDirectory } from "./directory.ts";
import { filtersOverlap, matchesFilter } from "./internal.ts";
import { definedAgents, type Agent } from "./agent.ts";
import {
	addFollow,
	INGRESS_EVENT,
	ingressKey,
	ROUTE_WORKFLOW,
	type IngressEvent,
	type RouteDecision,
	type RoutedWorkflow,
} from "./routing.ts";
import {
	conversationSession,
	follow,
	isChatMessage,
	prepareInput,
	threadOf,
	threadSession,
} from "./send.ts";
import type { AnyWorkflow, ExecutionInfo, WorkflowContext } from "./workflow.ts";
import type { Json, JsonObject } from "./json.ts";
import { AGENT_SETTLED_EVENT, EXECUTION_SETTLED_EVENT } from "./runtime.ts";
import type {
	AgentRef,
	DurableExecution,
	ExecutionResult,
	Follow,
	InvocationMetadata,
	RuntimeWorkflow,
} from "./runtime.ts";
import type { ChatMessage, ThreadRef } from "./thread.ts";
import { createContext } from "./context.ts";
import { CLEANUP_ATTEMPTS, CLEANUP_WORKFLOW, cleanup } from "./cleanup.ts";
import type { TriggerSpec } from "./trigger.ts";
import { notifyFollowers } from "./follows.ts";
import {
	APPROVAL_DECIDED_EVENT,
	APPROVAL_DECISION_WORKFLOW,
	APPROVAL_REQUEST_WORKFLOW,
	approvalDecisionWorkflow,
	approvalRequestWorkflow,
	authorized,
	loadRequest,
	type ApprovalAction,
} from "./approval-workflows.ts";

const silentLogger: Logger = { debug() {}, info() {}, warn() {}, error() {} };

/** Compose integrations and register the core workflow/context protocol with the durable runtime. */
export function buildApp(config: CoreConfig): App {
	const runtime = config.runtime;
	const now = () => runtime.now?.() ?? Date.now();
	let chat: ReturnType<NonNullable<CoreConfig["chat"]>["connect"]> | undefined;
	let agents: ReturnType<NonNullable<CoreConfig["agents"]>["connect"]> | undefined;
	let liveClients: Record<string, unknown> | undefined;
	const clients = (): Record<string, unknown> =>
		(liveClients ??= Object.assign(
			{},
			...config.plugins.map((p) => p.clients?.({ chat: chat?.port }) ?? {}),
		));
	const directory = createDirectory(
		config.identity ?? [],
		config.plugins,
		() => host.clients(),
		now,
	);
	/** Workflow and agent triggers compete for the same events: the most specific owner receives each. */
	const triggers: {
		owner: AnyWorkflow | Agent;
		name: string;
		workflow?: AnyWorkflow;
		agent?: Agent;
		spec: TriggerSpec;
	}[] = [
		...config.workflows.flatMap((workflow) =>
			(workflow.definition.triggers ?? []).map((spec: TriggerSpec) => ({
				owner: workflow,
				name: workflow.name,
				workflow,
				spec,
			})),
		),
		...(config.routedAgents ?? []).flatMap((agent) =>
			(agent.definition.triggers ?? []).map((spec: TriggerSpec) => ({
				owner: agent,
				name: `agent ${agent.name}`,
				agent,
				spec,
			})),
		),
	];
	const specificity = (spec: TriggerSpec) =>
		spec.specificity ?? Object.keys(spec.filter ?? {}).length;
	/** The most specific owner trigger of an input's categories (`tied` when two owners compete), and its observers. */
	const select = (events: readonly string[], payload: JsonObject) => {
		const matching = triggers.filter(
			(t) => events.includes(t.spec.event) && matchesFilter(payload, t.spec.filter),
		);
		const owners = matching
			.filter((t) => !t.spec.observe)
			.sort((a, b) => specificity(b.spec) - specificity(a.spec));
		const selected = owners[0];
		const tied =
			!!selected &&
			owners.some(
				(t) => t.owner !== selected.owner && specificity(t.spec) === specificity(selected.spec),
			);
		// One start per observing workflow, on the first category it observes.
		const observers = new Map<string, string>();
		for (const t of matching)
			if (t.spec.observe && !observers.has(t.workflow!.name))
				observers.set(t.workflow!.name, t.spec.event);
		return {
			selected,
			tied,
			observers: [...observers].map(([workflow, event]): RoutedWorkflow => ({ workflow, event })),
		};
	};
	/** Agents a message can name (`@agent-name`): those with triggers and every agent of the instance. */
	const knownAgents = () =>
		new Map(
			[
				...(definedAgents.get(config.definitionScope ?? host) ?? []),
				...(config.routedAgents ?? []),
			].map((agent) => [agent.name, agent]),
		);
	const mentionedAgent = (text: string): Agent | undefined => {
		const named = [...knownAgents().values()].filter((agent) =>
			new RegExp(
				`(^|[^\\w-])@${agent.name.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w-])`,
			).test(text),
		);
		return named.length === 1 ? named[0] : undefined;
	};
	/** Deliver an event to an agent's conversation: one signal to its Session (started when closed). */
	const deliver = async (
		agent: AgentRef,
		conversation: Follow,
		input: import("./json.ts").JsonObject,
		metadata: InvocationMetadata,
	) => {
		const port = agents?.agents;
		if (!port?.deliver) throw new Error("Agents receiving events need the agent integration");
		const request = await prepareInput(host, metadata, {
			agent: agent.name,
			session: conversation.session,
			requestId: `${metadata.interactionId}:${agent.name}`,
			input,
			kind: isChatMessage(input) ? "message" : "notification",
			replyTo: metadata.replyTo,
		});
		await port.deliver(agent, request);
		return request;
	};
	/** Store the request: one `fabrial.ingress` event, deduped by its provider id. `fabrial.route` routes it. */
	const accept = async (ingress: IngressEvent, interactionId: string) => {
		await runtime.emit(INGRESS_EVENT, ingress, {
			id: ingressKey(ingress),
			metadata: { interactionId, origin: ingress.origin, replyTo: null, requestedBy: null },
		});
		return "accepted" as const;
	};
	/**
	 * The routing workflow, run once per ingress event and retried by Conductor. Each effect is a durable
	 * operation: a retry after a partial delivery resumes after the deliveries that completed, and a
	 * delivery that completed without its receipt is deduped by its request id.
	 */
	const route = async (ingress: IngressEvent, execution: DurableExecution): Promise<Json> => {
		const { interactionId } = execution.metadata;
		const actor = (
			ingress.actor
				? await execution.step(
						"actor",
						async () => (await directory.resolveIdentity(ingress.actor!)) as unknown as Json,
					)
				: null
		) as InvocationMetadata["requestedBy"];
		if (ingress.kind === "action") {
			// Only authorized clicks reach the waiting approval, deduped by the click's id.
			const action = ingress.payload as {
				actionId: string;
				value: string | null;
				messageId: string;
			};
			const [, verb, approvalId] = /^fabrial\.approval\.(approve|reject|cancel):(.+)$/.exec(
				action.actionId,
			)!;
			const request = await loadRequest(host, execution, approvalId!);
			const allowed = await execution.step(
				"authorize",
				async () => !!request && (await authorized(host, request, verb as ApprovalAction, actor)),
			);
			if (!allowed) {
				await execution.step("denied", async () => {
					if (ingress.actor && ingress.replyTo)
						await chat?.port.postEphemeral(
							ingress.replyTo as ThreadRef,
							ingress.actor,
							"You are not authorized to decide this approval.",
						);
				});
				return { authorized: false };
			}
			await execution.emit(
				APPROVAL_DECIDED_EVENT,
				{
					approvalId: approvalId!,
					status: verb === "approve" ? "approved" : verb === "reject" ? "rejected" : "cancelled",
					actor: actor as unknown as Json,
					messageId: action.messageId,
					comment: action.value,
				},
				{
					id: ingress.id,
					metadata: {
						interactionId: approvalId!,
						origin: { provider: ingress.actor!.provider },
						replyTo: null,
						requestedBy: actor,
					},
				},
			);
			return { authorized: true };
		}
		if (ingress.kind === "stop") {
			const io = await chat?.port.thread(ingress.replyTo as ThreadRef);
			const follows = (await execution.step(
				"follows",
				async () => ((await io?.getState())?.follows ?? []) as Json,
			)) as Follow[];
			// A stop cancels the current run of each conversation following the thread; anyone there may stop.
			for (const [index, conversation] of follows.entries())
				await execution.step(`stop:${index}`, async () => {
					await agents?.agents.stop?.(conversation, ingress.id, `Stopped by ${actor!.id}`);
				});
			return null;
		}
		const message =
			ingress.kind === "message" ? (ingress.payload as unknown as ChatMessage) : undefined;
		const thread = message && threadOf(message);
		const io = thread && (await chat?.port.thread(thread));
		const decision = await execution.step("route", async (): Promise<RouteDecision> => {
			const { selected, tied, observers } = select(ingress.events, ingress.payload);
			const conversation = (agent: Agent, replyTo: InvocationMetadata["replyTo"]): Follow => ({
				session: conversationSession(
					agent.definition.session,
					ingress.payload,
					replyTo,
					message ? threadSession(message.provider, message.threadId) : interactionId,
				),
				agent: agent.name,
			});
			// 1. Followers: everyone in a followed thread steers. Naming one agent addresses only that agent.
			// Observers still run.
			const followers = (await io?.getState())?.follows ?? [];
			if (message && followers.length) {
				const mentioned = mentionedAgent(message.text);
				const own = mentioned ? followers.filter((f) => f.agent === mentioned.name) : followers;
				return {
					route: "follow",
					conversations: own.length || !mentioned ? own : [conversation(mentioned, thread!)],
					owner: null,
					observers,
					replyTo: thread!,
					event: ingress.events[0]!,
				};
			}
			// 2. Triggers: the most specific one wins, an agent's or a workflow's.
			if (tied) throw new Error(`Ambiguous owner for ${ingress.events.join(", ")}`);
			const replyTo = (thread ??
				ingress.replyTo ??
				selected?.agent?.definition.home ??
				selected?.spec.replyTo ??
				null) as InvocationMetadata["replyTo"];
			const owner = selected?.workflow
				? { workflow: selected.workflow.name, event: selected.spec.event }
				: null;
			const conversations = selected?.agent ? [conversation(selected.agent, replyTo)] : [];
			// The owner runs once, as owner, even when it also observes another category.
			const others = observers.filter((o) => o.workflow !== owner?.workflow);
			return {
				route: owner || conversations.length || others.length ? "new" : "ignored",
				conversations,
				owner,
				observers: others,
				replyTo,
				event: selected?.spec.event ?? ingress.events[0]!,
			};
		});
		// A handled thread stays subscribed; every conversation receiving its messages follows it.
		if (io && decision.route !== "ignored")
			await execution.step("follow", async () => {
				if (decision.conversations.length)
					await io.updateState((state) => decision.conversations.reduce(addFollow, state));
				await io.subscribe();
			});
		const metadata: InvocationMetadata = {
			interactionId,
			origin: (message
				? { provider: message.provider, messageId: message.messageId }
				: (ingress.origin ?? {
						provider: ingress.events[0]!.split(".")[0]!,
					})) as InvocationMetadata["origin"],
			replyTo: decision.replyTo,
			requestedBy: actor,
			ownerWorkflow: decision.owner?.workflow ?? null,
			triggerEvent: decision.event,
		};
		const named = knownAgents();
		for (const [index, conversation] of decision.conversations.entries())
			await execution.step(`deliver:${index}`, async () => {
				const request = await deliver(
					named.get(conversation.agent) ?? { kind: "fabrial.agent", name: conversation.agent },
					conversation,
					ingress.payload,
					{ ...metadata, ownerWorkflow: null },
				);
				// An event's conversation follows the thread its replies go to.
				if (!message) await follow(host, request.follow, conversation);
			});
		if (decision.owner)
			await execution.start("owner", decision.owner.workflow, ingress.payload, { metadata });
		// Observers run without the thread.
		for (const observer of decision.observers)
			await execution.start(`observer:${observer.workflow}`, observer.workflow, ingress.payload, {
				metadata: { ...metadata, replyTo: null, triggerEvent: observer.event },
			});
		// Each category is also emitted once for `ctx.waitForEvent`; no workflow triggers on it.
		if (ingress.kind === "event" || decision.route === "new")
			for (const event of ingress.events)
				await execution.emit(event, ingress.payload, { id: ingress.id, metadata });
		// Conversations following the event's resource threads get a notification; triggers still run.
		if (ingress.kind === "event")
			await execution.step("notify", async () => {
				await notifyFollowers(host, agents?.agents, ingress.events[0]!, ingress.payload, {
					...metadata,
					replyTo: null,
				});
			});
		return decision;
	};
	const host: FabrialHost = {
		definitionScope: config.definitionScope,
		plugins: config.plugins,
		workflows: config.workflows,
		runtime,
		directory,
		logger: config.logger ?? silentLogger,
		clients,
		chat: () => chat?.port,
		resolvePrincipal: (identity) => directory.resolveIdentity(identity),
		ingest: (event, payload, options) => {
			// The routing workflow re-emits ingested events; only Fabrial emits its own.
			if (event.startsWith("fabrial.")) throw new Error(`Reserved event name: ${event}`);
			return accept(
				{
					id: options.id ?? crypto.randomUUID(),
					kind: "event",
					events: [event],
					payload,
					origin: (options.origin ?? null) as IngressEvent["origin"],
					replyTo: (options.replyTo ?? null) as IngressEvent["replyTo"],
					actor: (options.requestedBy ?? null) as IngressEvent["actor"],
				},
				options.interactionId ?? (options.id ? `${event}:${options.id}` : crypto.randomUUID()),
			);
		},
		async receiveMessage(message, options) {
			if (message.author.isBot) return "ignored";
			const events = [...new Set(options.events ?? (options.event ? [options.event] : []))];
			if (!events.length) throw new Error("Inbound messages need at least one event category");
			return accept(
				{
					id: options.dedupeId,
					kind: "message",
					events,
					payload: message,
					origin: { provider: message.provider, messageId: message.messageId },
					replyTo: threadOf(message),
					actor: message.author.identity as IngressEvent["actor"],
				},
				`${message.provider}:${options.dedupeId}`,
			);
		},
		async receiveCancellation(action) {
			await accept(
				{
					id: action.dedupeId,
					kind: "stop",
					events: [],
					payload: {},
					origin: { provider: action.thread.provider },
					replyTo: action.thread as IngressEvent["replyTo"],
					actor: action.actor as IngressEvent["actor"],
				},
				action.dedupeId,
			);
		},
		async receiveAction(action) {
			const match = /^fabrial\.approval\.(approve|reject|cancel):(.+)$/.exec(action.actionId);
			if (!match) return;
			await accept(
				{
					id: action.dedupeId,
					kind: "action",
					events: [],
					payload: {
						actionId: action.actionId,
						value: action.value ?? null,
						messageId: action.messageId,
					},
					origin: { provider: action.actor.provider },
					replyTo: action.thread as IngressEvent["replyTo"],
					actor: action.actor as IngressEvent["actor"],
				},
				match[2]!,
			);
		},
	};
	agents = config.agents?.connect(host);
	async function settled(
		executionId: string,
		metadata: InvocationMetadata,
		result: ExecutionResult,
	) {
		await agents?.agents.settled?.(executionId, metadata, result);
		await runtime.emit(
			EXECUTION_SETTLED_EVENT,
			{ executionId, result: result as unknown as Json },
			{ id: executionId, metadata },
		);
	}
	const middleware = config.plugins.flatMap((p) => p.hooks?.workflow ?? []);
	/** Plugin middleware around one attempt; the innermost `next` hands its context to the handler. */
	const wrap = (
		info: ExecutionInfo,
		context: WorkflowContext,
		next: (context: WorkflowContext) => Promise<void>,
	): Promise<void> => {
		const run = (index: number, ctx: WorkflowContext): Promise<void> =>
			index < middleware.length
				? middleware[index]!(info, ctx, (inner) => run(index + 1, inner))
				: next(ctx);
		return run(0, context);
	};
	const workflows: RuntimeWorkflow[] = config.workflows.map((workflow) => ({
		name: workflow.name,
		// The routing workflow starts app workflows; they have no runtime triggers.
		triggers: [],
		cron: workflow.definition.cron?.map((c) => ({
			schedule: c.schedule,
			name: c.name ?? workflow.name,
			replyTo: c.replyTo,
		})),
		concurrency: workflow.definition.concurrency,
		retries: workflow.definition.retries,
		onSettled: settled,
		async attempt(execution, next) {
			const metadata = execution.metadata;
			const context = await createContext({ host, execution, metadata, agents, now });
			await wrap(
				{ executionId: execution.executionId, workflow: workflow.name, metadata },
				context,
				next,
			);
		},
		handler: (input, _execution, context) =>
			workflow.definition.run(input, context as WorkflowContext),
	}));
	const extraWorkflows = new Map((agents?.agents.workflows() ?? []).map((w) => [w.name, w]));
	for (const workflow of agents?.workflows ?? []) extraWorkflows.set(workflow.name, workflow);
	extraWorkflows.set(APPROVAL_REQUEST_WORKFLOW, approvalRequestWorkflow());
	extraWorkflows.set(APPROVAL_DECISION_WORKFLOW, approvalDecisionWorkflow(now));
	extraWorkflows.set(CLEANUP_WORKFLOW, {
		name: CLEANUP_WORKFLOW,
		triggers: [],
		retries: { maxAttempts: CLEANUP_ATTEMPTS },
		handler: (input, execution) => cleanup(input, execution, host),
	});
	for (const workflow of extraWorkflows.values())
		workflows.push({
			...workflow,
			onSettled: async (id, metadata, result) => {
				await workflow.onSettled?.(id, metadata, result);
				await settled(id, metadata, result);
			},
			attempt: async (execution, next) => {
				const context = await createContext({
					host,
					execution,
					metadata: execution.metadata,
					agents,
					now,
				});
				await wrap(
					{
						executionId: execution.executionId,
						workflow: workflow.name,
						metadata: execution.metadata,
					},
					context,
					next,
				);
			},
			handler: (input, execution) => workflow.handler(input, execution),
		});
	// Routing is plumbing: no workflow context, plugin middleware or settlement event.
	workflows.push({
		name: ROUTE_WORKFLOW,
		triggers: [{ event: INGRESS_EVENT }],
		retries: { maxAttempts: 5 },
		handler: (input, execution) => route(input as IngressEvent, execution),
	});
	runtime.register({
		workflows,
		events: [
			...config.plugins.flatMap((p) =>
				Object.entries(p.events ?? {}).map(([key, event]) => ({
					name: `${p.id}.${key}`,
					filterable: (event as { filterable: string[] }).filterable,
				})),
			),
			{ name: INGRESS_EVENT, filterable: [] },
			{ name: APPROVAL_DECIDED_EVENT, filterable: ["approvalId"] },
			{ name: EXECUTION_SETTLED_EVENT, filterable: ["executionId"] },
			{ name: AGENT_SETTLED_EVENT, filterable: ["requestId"] },
		],
	});
	return {
		host,
		async fetch(request) {
			const key = `${request.method.toUpperCase()} ${new URL(request.url).pathname}`;
			const chatRoute = chat?.routes[key];
			if (chatRoute) return chatRoute(request);
			for (const plugin of config.plugins) {
				const route = plugin.routes?.[key];
				if (route)
					return route(request, {
						clients: host.clients(),
						emit: async (event, payload, options = {}) => {
							await host.ingest(`${plugin.id}.${event}`, payload, options);
						},
					});
			}
			return new Response("Not found", { status: 404 });
		},
		async start() {
			if (new Set(config.plugins.map((p) => p.id)).size !== config.plugins.length)
				throw new Error("Duplicate plugin id");
			if (new Set(workflows.map((w) => w.name)).size !== workflows.length)
				throw new Error("Duplicate workflow name");
			if (config.plugins.some((p) => p.chat) && !config.chat)
				throw new Error("Chat integration required");
			const routed = config.routedAgents ?? [];
			if (new Set(routed.map((a) => a.name)).size !== routed.length)
				throw new Error("Duplicate agent name");
			if (routed.some((a) => a.definition.triggers?.length) && !agents?.agents.deliver)
				throw new Error("Agents with triggers need the agent integration");
			for (let i = 0; i < triggers.length; i++)
				for (const b of triggers.slice(i + 1)) {
					const a = triggers[i]!;
					if (
						!a.spec.observe &&
						!b.spec.observe &&
						a.owner !== b.owner &&
						a.spec.event === b.spec.event &&
						specificity(a.spec) === specificity(b.spec) &&
						filtersOverlap(a.spec.filter, b.spec.filter)
					)
						throw new Error(`Ambiguous trigger owners: ${a.name}, ${b.name} on ${a.spec.event}`);
				}
			for (const plugin of config.plugins) await plugin.init?.();
			await config.resources?.open();
			chat = config.chat?.connect(host);
			await chat?.start?.();
			await agents?.start();
			await runtime.start();
		},
		async stop() {
			await runtime.stop();
			await agents?.stop();
			await chat?.stop?.();
			for (const plugin of [...config.plugins].reverse()) await plugin.shutdown?.();
			await config.resources?.close();
			liveClients = undefined;
		},
		async emit(event, payload, options = {}) {
			await host.ingest(event, payload, options);
		},
	};
}
