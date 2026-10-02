import type { App, FabrialConfig, FabrialHost, Logger } from "./app.ts";
import { createDirectory, identityKey } from "./directory.ts";
import { boundedTimeout, filtersOverlap, matchesFilter } from "./internal.ts";
import { clearInteraction, hasIngress, rememberIngress } from "./routing.ts";
import type { Json } from "./json.ts";
import type { Clients } from "./register.ts";
import { EXECUTION_SETTLED_EVENT } from "./runtime.ts";
import type { ExecutionResult, InvocationMetadata, RuntimeWorkflow } from "./runtime.ts";
import { createContext } from "./context.ts";
import { APPROVAL_CLOSED_EVENT, CLEANUP_WORKFLOW, PRESENTATION_EVENT, cleanup } from "./cleanup.ts";
import type { TriggerSpec } from "./trigger.ts";

const silentLogger: Logger = { debug() {}, info() {}, warn() {}, error() {} };

/** Compose integrations and register the core workflow/context protocol with the durable runtime. */
export function fabrial(config: FabrialConfig): App {
	const runtime = config.runtime;
	const now = () => runtime.now?.() ?? Date.now();
	const reservationTimeout = boundedTimeout(config.reservationTimeout ?? "10m");
	let chat: ReturnType<NonNullable<FabrialConfig["chat"]>["connect"]> | undefined;
	let agents: ReturnType<NonNullable<FabrialConfig["agents"]>["connect"]> | undefined;
	let liveClients: Clients | undefined;
	const clients = (): Clients =>
		(liveClients ??= Object.assign(
			{},
			...config.plugins.map((p) => p.clients?.({ chat: chat?.port }) ?? {}),
		));
	const directory = createDirectory(config.identity ?? [], config.plugins, clients, now);
	const triggers = config.workflows.flatMap((workflow) =>
		(workflow.definition.triggers ?? []).map((spec: TriggerSpec) => ({ workflow, spec })),
	);
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
				(t) =>
					t.workflow.name !== selected.workflow.name &&
					specificity(t.spec) === specificity(selected.spec),
			)
		)
			throw new Error(`Ambiguous owner for ${events.join(", ")}`);
		return {
			selected,
			observers: [...new Set(matching.filter((t) => t.spec.observe).map((t) => t.workflow.name))],
		};
	};
	const host: FabrialHost = {
		plugins: config.plugins,
		workflows: config.workflows,
		runtime,
		directory,
		logger: config.logger ?? silentLogger,
		clients,
		chat: () => chat?.port,
		resolvePrincipal: (identity) => directory.resolveIdentity(identity),
		async ingest(event, payload, options) {
			const { selected, observers } = select([event], payload);
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
				ownsThread: !!selected,
				ownerWorkflow: selected?.workflow.name ?? null,
				triggerEvent: event,
			};
			await runtime.emit(event, payload, {
				id: options.id,
				metadata,
				owner: selected?.workflow.name,
				dispatchOwner: selected
					? { workflow: selected.workflow.name, event: selected.spec.event }
					: undefined,
			});
			return { owner: selected?.workflow.name, observers };
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
			const decision: {
				route: "new" | "reply" | "ignored";
				selected: ReturnType<typeof select>["selected"];
			} = { route: "new", selected: undefined };
			// Recompute the local decision on every lock retry; no dispatch/I/O occurs in the updater.
			const state = await io?.updateState((state) => {
				decision.selected = undefined;
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
				decision.route = "new";
				decision.selected = select(events, message).selected;
				const idle = clearInteraction(state);
				return rememberIngress(
					decision.selected
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
			});
			if (decision.route === "ignored") return "ignored";
			if (!io) decision.selected = select(events, message).selected;
			const { selected } = decision;
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
				return "reply";
			}
			const invocation: InvocationMetadata = {
				interactionId,
				origin: { provider: message.provider, messageId: message.messageId },
				replyTo: ref as InvocationMetadata["replyTo"],
				requestedBy: actor as InvocationMetadata["requestedBy"],
				ownsThread: !!selected,
				ownerWorkflow: selected?.workflow.name ?? null,
				triggerEvent: selected?.spec.event ?? events[0]!,
				initialMessageId: message.messageId,
			};
			// Only the winning event dispatches an owner; the shared key merges overlapping observers.
			for (const event of new Set([...(selected ? [selected.spec.event] : []), ...events]))
				await runtime.emit(event, message, {
					id: options.dedupeId,
					metadata: invocation,
					owner: event === selected?.spec.event ? selected.workflow.name : undefined,
					dispatchId: interactionId,
					dispatchOwner: selected
						? { workflow: selected.workflow.name, event: selected.spec.event }
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
	chat = config.chat?.connect(host);
	agents = config.agents?.connect(host);
	async function settled(
		executionId: string,
		metadata: InvocationMetadata,
		result: ExecutionResult,
	) {
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
						clients: clients(),
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
			if (config.plugins.some((p) => p.chat) && !chat) throw new Error("Chat integration required");
			for (let i = 0; i < triggers.length; i++)
				for (const b of triggers.slice(i + 1)) {
					const a = triggers[i]!;
					if (
						!a.spec.observe &&
						!b.spec.observe &&
						a.workflow !== b.workflow &&
						a.spec.event === b.spec.event &&
						specificity(a.spec) === specificity(b.spec) &&
						filtersOverlap(a.spec.filter, b.spec.filter)
					)
						throw new Error(
							`Ambiguous trigger owners: ${a.workflow.name}, ${b.workflow.name} on ${a.spec.event}`,
						);
				}
			for (const plugin of config.plugins) await plugin.init?.();
			await chat?.start?.();
			await agents?.start();
			await runtime.start();
		},
		async stop() {
			await runtime.stop();
			await agents?.stop();
			await chat?.stop?.();
			for (const plugin of [...config.plugins].reverse()) await plugin.shutdown?.();
		},
		async emit(event, payload, options = {}) {
			await host.ingest(event, payload, options);
		},
	};
}
