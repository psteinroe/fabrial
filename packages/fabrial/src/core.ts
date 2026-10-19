import type { App, CoreConfig, FabrialHost, Logger } from "./app.ts";
import { createDirectory, identityKey } from "./directory.ts";
import { boundedTimeout, filtersOverlap, matchesFilter } from "./internal.ts";
import { definedAgents, type Agent } from "./agent.ts";
import { addFollow, clearInteraction, hasIngress, rememberIngress } from "./routing.ts";
import {
	conversationSession,
	follow,
	isChatMessage,
	prepareInput,
	threadSession,
	type ConversationKey,
} from "./send.ts";
import type { AnyWorkflow } from "./workflow.ts";
import type { Json } from "./json.ts";
import { AGENT_SETTLED_EVENT, EXECUTION_SETTLED_EVENT } from "./runtime.ts";
import type {
	AgentRef,
	ExecutionResult,
	InvocationMetadata,
	RuntimeWorkflow,
	ThreadRoutingState,
} from "./runtime.ts";
import { createContext } from "./context.ts";
import { APPROVAL_CLOSED_EVENT, CLEANUP_WORKFLOW, PRESENTATION_EVENT, cleanup } from "./cleanup.ts";
import type { TriggerSpec } from "./trigger.ts";
import { deliveryWorkflow } from "./subscriptions.ts";

const silentLogger: Logger = { debug() {}, info() {}, warn() {}, error() {} };

/** Compose integrations and register the core workflow/context protocol with the durable runtime. */
export function buildApp(config: CoreConfig): App {
	const runtime = config.runtime;
	const now = () => runtime.now?.() ?? Date.now();
	const reservationTimeout = boundedTimeout(config.reservationTimeout ?? "10m");
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
	const select = (events: readonly string[], payload: import("./json.ts").JsonObject) => {
		const matching = triggers.filter(
			(t) => events.includes(t.spec.event) && matchesFilter(payload, t.spec.filter),
		);
		const owners = matching
			.filter((t) => !t.spec.observe)
			.sort((a, b) => specificity(b.spec) - specificity(a.spec));
		const selected = owners[0];
		if (
			selected &&
			owners.some(
				(t) => t.owner !== selected.owner && specificity(t.spec) === specificity(selected.spec),
			)
		)
			throw new Error(`Ambiguous owner for ${events.join(", ")}`);
		return {
			selected,
			observers: [...new Set(matching.filter((t) => t.spec.observe).map((t) => t.workflow!.name))],
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
		conversation: ConversationKey,
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
	const host: FabrialHost = {
		definitionScope: config.definitionScope,
		plugins: config.plugins,
		workflows: config.workflows,
		runtime,
		directory,
		logger: config.logger ?? silentLogger,
		clients,
		subscriptions: config.subscriptionStore,
		chat: () => chat?.port,
		resolvePrincipal: (identity) => directory.resolveIdentity(identity),
		async ingest(event, payload, options) {
			const { selected, observers } = select([event], payload);
			if (selected?.agent) {
				// An agent's trigger: the event goes to a conversation; observers still run.
				const metadata: InvocationMetadata = {
					interactionId:
						options.interactionId ?? (options.id ? `${event}:${options.id}` : crypto.randomUUID()),
					origin: (options.origin ?? {
						provider: event.split(".")[0]!,
					}) as InvocationMetadata["origin"],
					replyTo: (options.replyTo ??
						selected.agent.definition.home ??
						selected.spec.replyTo ??
						null) as InvocationMetadata["replyTo"],
					requestedBy: options.requestedBy
						? ((await directory.resolveIdentity(
								options.requestedBy,
							)) as InvocationMetadata["requestedBy"])
						: null,
					ownsThread: false,
					ownerWorkflow: null,
					triggerEvent: event,
				};
				if (observers.length)
					await runtime.emit(event, payload, { id: options.id, metadata, owner: undefined });
				const session = conversationSession(
					selected.agent.definition.session,
					payload,
					metadata.replyTo,
					metadata.interactionId,
				);
				const request = await deliver(
					selected.agent,
					{ session, agent: selected.agent.name },
					payload,
					metadata,
				);
				await follow(host, request.follow, { session, agent: selected.agent.name });
				return { owner: `agent:${selected.agent.name}`, observers };
			}
			const metadata: InvocationMetadata = {
				interactionId:
					options.interactionId ?? (options.id ? `${event}:${options.id}` : crypto.randomUUID()),
				origin: (options.origin ?? {
					provider: event.split(".")[0]!,
				}) as InvocationMetadata["origin"],
				replyTo: (options.replyTo ??
					selected?.spec.replyTo ??
					null) as InvocationMetadata["replyTo"],
				requestedBy: options.requestedBy
					? ((await directory.resolveIdentity(
							options.requestedBy,
						)) as InvocationMetadata["requestedBy"])
					: null,
				ownsThread: !!selected && !selected.workflow!.subscription,
				ownerWorkflow: selected?.workflow!.name ?? null,
				triggerEvent: event,
			};
			await runtime.emit(event, payload, {
				id: options.id,
				metadata,
				owner: selected?.workflow!.name,
				dispatchOwner: selected
					? { workflow: selected.workflow!.name, event: selected.spec.event }
					: undefined,
			});
			return { owner: selected?.workflow!.name, observers };
		},
		async receiveMessage(message, options) {
			if (message.author.isBot) return "ignored";
			const ref = {
				kind: "thread" as const,
				provider: message.provider,
				threadId: message.threadId,
			};
			const io = await chat?.port.thread(ref);
			const actor = await directory.resolveIdentity(message.author.identity);
			const events = [...new Set(options.events ?? (options.event ? [options.event] : []))];
			if (!events.length) throw new Error("Inbound messages need at least one event category");
			const interactionId = `${message.provider}:${options.dedupeId}`;
			const receivedAt = now();
			const threadKey = threadSession(message.provider, message.threadId);
			// Conversations keyed by this thread follow it implicitly; others leave a record in its state.
			const implicit = (await agents?.agents.conversations?.(threadKey)) ?? [];
			const mentioned = mentionedAgent(message.text);
			const decision: {
				route: "new" | "reply" | "follow" | "ignored";
				selected: ReturnType<typeof select>["selected"];
				conversations: ConversationKey[];
			} = { route: "new", selected: undefined, conversations: [] };
			const followed = (state: ThreadRoutingState | null, targets: ConversationKey[]) =>
				rememberIngress(
					targets.reduce<ThreadRoutingState | null>(
						(next, target) => (target.session === threadKey ? next : addFollow(next, target)),
						state,
					),
					"message",
					options.dedupeId,
					receivedAt,
				);
			// Recompute the local decision on every lock retry; no dispatch/I/O occurs in the updater.
			const decide = (state: ThreadRoutingState | null): ThreadRoutingState | null => {
				decision.selected = undefined;
				decision.conversations = [];
				if (hasIngress(state, "message", options.dedupeId)) {
					decision.route = "ignored";
					return state;
				}
				const stale =
					state?.handlerExecutionId === null &&
					(state.reservedAt === undefined || receivedAt - state.reservedAt >= reservationTimeout);
				if (state?.interactionId != null && !stale) {
					decision.route = "reply";
					return rememberIngress(
						{
							...state,
							participantIds: [...new Set([...(state.participantIds ?? []), actor.id])],
							bufferedReplies:
								state.bufferedReplies.some((m) => m.messageId === message.messageId) ||
								state.consumedReplyIds?.includes(message.messageId)
									? state.bufferedReplies
									: [...state.bufferedReplies, message],
						},
						"message",
						options.dedupeId,
						receivedAt,
					);
				}
				// 1. Followers: everyone in a followed thread steers. Naming one agent addresses only that agent.
				const followers = [
					...(state?.follows ?? []),
					...implicit.map((agent) => ({ session: threadKey, agent })),
				].filter(
					(f, i, all) => all.findIndex((o) => o.session === f.session && o.agent === f.agent) === i,
				);
				if (followers.length) {
					decision.route = "follow";
					const own = mentioned ? followers.filter((f) => f.agent === mentioned.name) : followers;
					decision.conversations =
						own.length || !mentioned
							? own
							: [
									{
										session: conversationSession(
											mentioned.definition.session,
											message,
											ref,
											threadKey,
										),
										agent: mentioned.name,
									},
								];
					return followed(state, decision.conversations);
				}
				// 2. Triggers: the most specific one wins, an agent's or a workflow's.
				decision.route = "new";
				decision.selected = select(events, message).selected;
				const idle = clearInteraction(state);
				const agent = decision.selected?.agent;
				if (agent) {
					decision.conversations = [
						{
							session: conversationSession(agent.definition.session, message, ref, threadKey),
							agent: agent.name,
						},
					];
					return followed(idle, decision.conversations);
				}
				// A subscription delivers the message and returns at once; it never holds the routing slot,
				// so the next message in this thread matches its trigger again instead of being buffered.
				return rememberIngress(
					decision.selected && !decision.selected.workflow!.subscription
						? {
								...idle,
								interactionId,
								handlerExecutionId: null,
								reservedAt: receivedAt,
								agentActive: false,
								statusMessageId: null,
								bufferedReplies: [],
								consumedReplyIds: [message.messageId],
								requesterId: actor.id,
								participantIds: [actor.id],
							}
						: idle,
					"message",
					options.dedupeId,
					receivedAt,
				);
			};
			const state = io ? await io.updateState(decide) : decide(null);
			if (decision.route === "ignored") return "ignored";
			const { selected } = decision;
			if (decision.route === "follow" || selected?.agent) {
				const named = knownAgents();
				const metadata: InvocationMetadata = {
					interactionId,
					origin: { provider: message.provider, messageId: message.messageId },
					replyTo: ref as InvocationMetadata["replyTo"],
					requestedBy: actor as InvocationMetadata["requestedBy"],
					ownsThread: false,
					ownerWorkflow: null,
					triggerEvent: selected?.spec.event ?? events[0]!,
				};
				for (const conversation of decision.conversations)
					await deliver(
						named.get(conversation.agent) ?? { kind: "fabrial.agent", name: conversation.agent },
						conversation,
						message,
						metadata,
					);
				if (decision.route === "follow") return "reply";
				// Observers of the message still run, without the thread.
				for (const event of events)
					await runtime.emit(event, message, {
						id: options.dedupeId,
						metadata: { ...metadata, ownerWorkflow: null },
						owner: undefined,
						dispatchId: interactionId,
					});
				return "new";
			}
			if (decision.route === "reply" && state?.interactionId != null) {
				await runtime.emit(
					"fabrial.reply",
					{
						interactionId: state.interactionId,
						message,
						from: actor.id,
						messageId: message.messageId,
					},
					{
						id: options.dedupeId,
						metadata: {
							interactionId: state.interactionId,
							origin: { provider: message.provider },
							replyTo: ref as InvocationMetadata["replyTo"],
							requestedBy: actor as InvocationMetadata["requestedBy"],
							ownsThread: false,
						},
					},
				);
				// A reply in a thread an agent call drives steers that call's run in its Session.
				if (state.agentActive)
					await agents?.agents.reply?.(ref, state.interactionId, options.dedupeId);
				return "reply";
			}
			const invocation: InvocationMetadata = {
				interactionId,
				origin: { provider: message.provider, messageId: message.messageId },
				replyTo: ref as InvocationMetadata["replyTo"],
				requestedBy: actor as InvocationMetadata["requestedBy"],
				ownsThread: !!selected && !selected.workflow!.subscription,
				ownerWorkflow: selected?.workflow!.name ?? null,
				triggerEvent: selected?.spec.event ?? events[0]!,
				initialMessageId: message.messageId,
			};
			// Only the winning event dispatches an owner; the shared key merges overlapping observers.
			for (const event of new Set([...(selected ? [selected.spec.event] : []), ...events]))
				await runtime.emit(event, message, {
					id: options.dedupeId,
					metadata: invocation,
					owner: event === selected?.spec.event ? selected.workflow!.name : undefined,
					dispatchId: interactionId,
					dispatchOwner: selected
						? { workflow: selected.workflow!.name, event: selected.spec.event }
						: undefined,
				});
			return "new";
		},
		async receiveCancellation(action) {
			const io = await chat?.port.thread(action.thread);
			const state = await io?.getState();
			if (
				!state?.handlerExecutionId ||
				hasIngress(state, "cancellation", action.dedupeId) ||
				state.cancellationIds?.includes(action.dedupeId)
			)
				return;
			const actor = await directory.resolveIdentity(action.actor);
			const participants = [
				...new Set([
					...(state.participantIds ?? []),
					...(state.requesterId ? [state.requesterId] : []),
				]),
			];
			const aliases = await Promise.all(participants.map((id) => directory.identitiesFor(id)));
			if (
				!participants.includes(actor.id) &&
				!aliases.flat().some((identity) => identityKey(identity) === identityKey(action.actor))
			) {
				await chat!.port.postEphemeral(
					action.thread,
					action.actor,
					"Only participants in this interaction may stop it.",
				);
				return;
			}
			await runtime.cancel(state.handlerExecutionId, `Stopped by ${actor.id}`);
			const receivedAt = now();
			await io!.updateState((latest) =>
				latest?.interactionId === state.interactionId &&
				latest.handlerExecutionId === state.handlerExecutionId
					? rememberIngress(
							{
								...latest,
								cancellationIds: [...new Set([...(latest.cancellationIds ?? []), action.dedupeId])],
							},
							"cancellation",
							action.dedupeId,
							receivedAt,
						)
					: latest,
			);
		},
		async receiveAction(action) {
			const match = /^fabrial\.approval\.(approve|reject|cancel):(.+)$/.exec(action.actionId);
			if (!match) return;
			const actor = await directory.resolveIdentity(action.actor);
			const io = await chat?.port.thread(action.thread);
			const receivedAt = now();
			let duplicate = false;
			await io?.updateState((state) => {
				duplicate = hasIngress(state, "action", action.dedupeId);
				return duplicate ? state : rememberIngress(state, "action", action.dedupeId, receivedAt);
			});
			if (duplicate) return;
			await runtime.emit(
				"fabrial.approval.decided",
				{
					approvalId: match[2]!,
					status:
						match[1] === "approve" ? "approved" : match[1] === "reject" ? "rejected" : "cancelled",
					actor: actor as unknown as Json,
					identity: action.actor as unknown as Json,
					thread: action.thread as unknown as Json,
					messageId: action.messageId,
					comment: action.value ?? null,
					dedupeId: action.dedupeId,
				},
				{
					id: action.dedupeId,
					metadata: {
						interactionId: match[2]!,
						origin: { provider: action.actor.provider },
						replyTo: null,
						requestedBy: actor as InvocationMetadata["requestedBy"],
						ownsThread: false,
					},
				},
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
		if (metadata.ownsThread && metadata.replyTo?.kind === "thread" && chat) {
			const io = await chat.port.thread(metadata.replyTo);
			const state = await io.getState();
			if (
				state?.interactionId === metadata.interactionId &&
				state.handlerExecutionId === executionId
			) {
				try {
					await io.setStatus(null);
				} catch (error) {
					host.logger.warn("Could not clear thread status", { error });
				}
				await io.updateState((latest) =>
					latest?.interactionId === metadata.interactionId &&
					latest.handlerExecutionId === executionId
						? clearInteraction(latest)
						: latest,
				);
			}
		}
		await runtime.emit(
			EXECUTION_SETTLED_EVENT,
			{ executionId, result: result as unknown as Json },
			{ id: executionId, metadata, owner: undefined },
		);
	}
	const middleware = config.plugins.flatMap((p) => p.hooks?.workflow ?? []);
	const workflows: RuntimeWorkflow[] = config.workflows.map((workflow) => ({
		name: workflow.name,
		triggers: (workflow.definition.triggers ?? []).map((t: TriggerSpec) => ({
			event: t.event,
			filter: t.filter,
			role: t.observe ? "observer" : "owner",
		})),
		cron: workflow.definition.cron?.map((c) => ({
			schedule: c.schedule,
			name: c.name ?? workflow.name,
			replyTo: c.replyTo,
		})),
		concurrency: workflow.definition.concurrency,
		retries: workflow.definition.retries,
		onSettled: settled,
		async handler(input, execution) {
			const observer =
				!!execution.metadata.triggerEvent && execution.metadata.ownerWorkflow !== workflow.name;
			const metadata = observer
				? { ...execution.metadata, replyTo: null, ownsThread: false }
				: execution.metadata;
			const context = await createContext({ host, execution, metadata, agents, now });
			const run = (index: number, ctx: typeof context): Promise<Json | void> =>
				index < middleware.length
					? middleware[index]!(
							{ executionId: execution.executionId, workflow: workflow.name, metadata },
							ctx,
							(next) => run(index + 1, next),
						)
					: workflow.definition.run(input, ctx);
			return run(0, context);
		},
	}));
	const extraWorkflows = new Map((agents?.agents.workflows() ?? []).map((w) => [w.name, w]));
	for (const workflow of agents?.workflows ?? []) extraWorkflows.set(workflow.name, workflow);
	const delivery = deliveryWorkflow(host, () => agents?.agents);
	if (delivery) extraWorkflows.set(delivery.name, delivery);
	extraWorkflows.set(CLEANUP_WORKFLOW, {
		name: CLEANUP_WORKFLOW,
		triggers: [],
		retries: { maxAttempts: 3 },
		handler: (input, execution) => cleanup(input, execution, host),
	});
	for (const workflow of extraWorkflows.values())
		workflows.push({
			...workflow,
			onSettled: async (id, metadata, result) => {
				await workflow.onSettled?.(id, metadata, result);
				await settled(id, metadata, result);
			},
			handler: async (input, execution) => {
				const context = await createContext({
					host,
					execution,
					metadata: execution.metadata,
					agents,
					now,
				});
				const run = (index: number, ctx: typeof context): Promise<Json | void> =>
					index < middleware.length
						? middleware[index]!(
								{
									executionId: execution.executionId,
									workflow: workflow.name,
									metadata: execution.metadata,
								},
								ctx,
								(next) => run(index + 1, next),
							)
						: workflow.handler(input, execution);
				return run(0, context);
			},
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
			{ name: "fabrial.reply", filterable: ["interactionId", "from", "messageId"] },
			{ name: "fabrial.approval.decided", filterable: ["approvalId"] },
			{ name: APPROVAL_CLOSED_EVENT, filterable: ["approvalId"] },
			{ name: PRESENTATION_EVENT, filterable: ["presentationId"] },
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
			await config.subscriptionStore?.start?.();
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
