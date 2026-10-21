// The app: its configuration, the host its modules share, task registration, HTTP dispatch, start and stop, and the
// production wiring of Conductor, Pi and Chat SDK on the instance's database.
import type { Conductor, Orchestrator } from "pgconductor-js";
import type { MutableModels } from "@earendil-works/pi-ai";
import type { ConversationRetryPolicy } from "@earendil-works/pi-durable";
import postgres, { type Sql } from "postgres";
import { createPostgresState } from "@chat-adapter/state-pg";
import type { AnyPlugin } from "./plugin.ts";
import { type AnyWorkflow, workflowTask } from "./workflows/definition.ts";
import {
	createDirectory,
	type ExternalIdentity,
	type IdentityDirectory,
	type Principal,
	type UserDefinition,
} from "./identity.ts";
import { type ChatConnection, type ChatOptions, connectChat } from "./chat/connection.ts";
import type { JsonObject } from "./schema.ts";
import type { EmitOptions, Origin } from "./routing/events.ts";
import type { ChatMessage, Surface, ThreadRef } from "./chat/messages.ts";
import type { Agent } from "./agents/definition.ts";
import { type ConductorConfig, createConductor, type WorkflowTask } from "./conductor.ts";
import { connectAgents, type PiOptions } from "./sessions/host.ts";
import { accept, createRouter, INGRESS_EVENT, type IngressEvent } from "./routing/route.ts";
import { threadOf } from "./sessions/delivery.ts";
import { attemptHook, pluginMiddleware } from "./workflows/context.ts";
import { APPROVAL_REQUEST_WORKFLOW, approvalRequestWorkflow } from "./approvals/request.ts";
import {
	APPROVAL_DECIDED_EVENT,
	APPROVAL_DECISION_WORKFLOW,
	approvalDecisionWorkflow,
} from "./approvals/decision.ts";
import {
	APPROVAL_DELIVERY_WORKFLOW,
	approvalDeliveryWorkflow,
	CARD_WORKFLOW,
	cardWorkflow,
} from "./approvals/delivery.ts";
import { sealDefinitions } from "./state.ts";
import { createModels, type ModelCredentials } from "./models.ts";

export interface Logger {
	debug(message: string, data?: Record<string, unknown>): void;
	info(message: string, data?: Record<string, unknown>): void;
	warn(message: string, data?: Record<string, unknown>): void;
	error(message: string, data?: Record<string, unknown>): void;
}

/** What core exposes to its runtime modules and to tests (`HostedApp.host`). Internal. */
export interface FabrialHost<TClients = Record<string, unknown>> {
	/** Definition catalog identity, used by adapters to isolate instance-local definitions. */
	readonly definitionScope?: object;
	readonly plugins: readonly AnyPlugin[];
	readonly workflows: readonly AnyWorkflow[];
	/** The app's Conductor client, once app.start() created its tasks. */
	readonly conductor: Conductor;
	/** Runs the app's tasks; started and stopped with the app. */
	readonly orchestrator: Orchestrator;
	/** The Conductor queue of the app's tasks. */
	readonly queue: string;
	/** Conductor's clock (epoch ms): the test clock in tests. */
	now(): number;
	readonly directory: IdentityDirectory;
	readonly logger: Logger;
	/** Live clients of all plugins (built once per app; plugin `clients()` must be cheap and stateless). */
	clients(): TClients;
	/** The app's Chat SDK bot, after app.start() connects chat. */
	chat(): ChatConnection | undefined;

	/**
	 * New work from a plugin: emits one `fabrial.ingress` event (deduped by `id`). The routing workflow
	 * starts the most specific owner (a workflow, or an agent's conversation) and all observers, and
	 * notifies conversations following the event's resources.
	 */
	ingest(
		event: string,
		payload: JsonObject,
		options: EmitOptions & {
			origin?: Origin;
			replyTo?: Surface;
			requestedBy?: ExternalIdentity;
			requestId?: string;
		},
	): Promise<"accepted">;

	/**
	 * Inbound chat message: emits one `fabrial.ingress` event (deduped by `dedupeId`) unless a bot sent it.
	 * The routing workflow delivers it to the conversations following its thread, else to the most specific
	 * matching trigger, and to observers.
	 */
	receiveMessage(
		message: ChatMessage,
		options: {
			events?: readonly string[];
			/** @deprecated Use events. */ event?: string;
			dedupeId: string;
		},
	): Promise<"accepted" | "ignored">;

	/** Inbound card action (approval buttons, [Cancel request]). */
	receiveAction(action: {
		actionId: string;
		value: string | undefined;
		actor: ExternalIdentity;
		thread: ThreadRef;
		messageId: string;
		dedupeId: string;
	}): Promise<void>;

	/** A stop in a thread cancels the current run of every conversation following it. */
	receiveCancellation(action: {
		actor: ExternalIdentity;
		thread: ThreadRef;
		dedupeId: string;
	}): Promise<void>;

	resolvePrincipal(identity: ExternalIdentity): Promise<Principal>;
}

export interface CoreConfig<P extends readonly AnyPlugin[] = readonly AnyPlugin[]> {
	definitionScope?: object;
	/** Conductor: core creates its client, tasks and Orchestrator when the app starts. */
	conductor: ConductorConfig;
	/** Chat SDK options. Required when any plugin has a `chat` capability. */
	chat?: ChatOptions;
	/** Pi: agents, state, and evaluation (`ctx.agent`, `ctx.send`, `ctx.state`, `ctx.evaluate`). */
	agents: PiOptions;
	/** Owned resources (database pools): opened after plugin `init`, closed last on stop. */
	resources?: { open(): Promise<void> | void; close(): Promise<void> };
	plugins: P;
	workflows: readonly AnyWorkflow[];
	/** Agents whose triggers receive events. */
	routedAgents?: readonly Agent[];
	/** Known users only; groups are resolved directly by reference in access/approval options. */
	identity?: readonly UserDefinition[];
	logger?: Logger;
}

export interface App<TClients = Record<string, unknown>> {
	/** One fetch handler for all plugin routes and chat webhooks. */
	fetch(request: Request): Promise<Response>;
	/** Validates (trigger ties, …), aborts orphaned agent work, runs plugin `init`, starts Conductor. */
	start(): Promise<void>;
	stop(): Promise<void>;
	/** Emit an event from application code. */
	emit(
		event: string,
		payload: JsonObject,
		options?: EmitOptions & { replyTo?: Surface },
	): Promise<void>;
	/** Live clients of the app's plugins (`ctx.clients` outside a run). */
	clients(): TClients;
}

/** An app with its host, for Fabrial's runtime modules and tests. Internal. */
export interface HostedApp<TClients = Record<string, unknown>> extends App<TClients> {
	readonly host: FabrialHost<TClients>;
}

const silentLogger: Logger = { debug() {}, info() {}, warn() {}, error() {} };

/** Compose integrations and register the core workflows as Conductor tasks on the app's queue. */
export function buildApp(config: CoreConfig): HostedApp {
	const queue = config.conductor.queue ?? "default";
	const now = () => config.conductor.clock?.now().getTime() ?? Date.now();
	/** The app's Conductor client and Orchestrator, once `start` registered its tasks. */
	let tasks: { conductor: Conductor; orchestrator: Orchestrator } | undefined;
	let running = false;
	const started = () => {
		if (!tasks) throw new Error("The app is not started");
		return tasks;
	};
	const conductor = () => started().conductor;
	let chat: ChatConnection | undefined;
	let liveClients: Record<string, unknown> | undefined;
	const clients = (): Record<string, unknown> =>
		(liveClients ??= Object.assign({}, ...config.plugins.map((p) => p.clients?.() ?? {})));
	const directory = createDirectory(
		config.identity ?? [],
		config.plugins,
		() => host.clients(),
		now,
	);
	const host: FabrialHost = {
		definitionScope: config.definitionScope,
		plugins: config.plugins,
		workflows: config.workflows,
		get conductor() {
			return conductor();
		},
		get orchestrator() {
			return started().orchestrator;
		},
		queue,
		now,
		directory,
		logger: config.logger ?? silentLogger,
		clients,
		chat: () => chat,
		resolvePrincipal: (identity) => directory.resolveIdentity(identity),
		ingest: (event, payload, options) => {
			// The routing workflow re-emits ingested events; only Fabrial emits its own.
			if (event.startsWith("fabrial.")) throw new Error(`Reserved event name: ${event}`);
			return accept(
				host,
				{
					id: options.id ?? crypto.randomUUID(),
					kind: "event",
					events: [event],
					payload,
					origin: (options.origin ?? null) as IngressEvent["origin"],
					replyTo: (options.replyTo ?? null) as IngressEvent["replyTo"],
					actor: (options.requestedBy ?? null) as IngressEvent["actor"],
				},
				options.requestId ?? (options.id ? `${event}:${options.id}` : crypto.randomUUID()),
			);
		},
		async receiveMessage(message, options) {
			if (message.author.isBot) return "ignored";
			const events = [...new Set(options.events ?? (options.event ? [options.event] : []))];
			if (!events.length) throw new Error("Inbound messages need at least one event category");
			return accept(
				host,
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
				host,
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
				host,
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
	const agents = connectAgents(host, config.agents);
	const router = createRouter(config, host, agents);
	const middleware = pluginMiddleware(config.plugins);
	const attempt = attemptHook(host, agents);
	const workflows: WorkflowTask[] = config.workflows.map((workflow) =>
		workflowTask(workflow, attempt),
	);
	/** Create every workflow's task, once all definitions exist: Pi's only if the instance uses it. */
	let registered = false;
	const register = () => {
		if (registered) return;
		registered = true;
		const extraWorkflows = new Map(agents.workflows.map((w) => [w.name, w]));
		extraWorkflows.set(APPROVAL_REQUEST_WORKFLOW, approvalRequestWorkflow());
		extraWorkflows.set(APPROVAL_DECISION_WORKFLOW, approvalDecisionWorkflow(now));
		extraWorkflows.set(APPROVAL_DELIVERY_WORKFLOW, approvalDeliveryWorkflow());
		extraWorkflows.set(CARD_WORKFLOW, cardWorkflow(host));
		for (const workflow of extraWorkflows.values()) workflows.push({ ...workflow, attempt });
		workflows.push(router.workflow);
		tasks = createConductor(config.conductor, {
			workflows,
			middleware,
			events: [
				...config.plugins.flatMap((p) =>
					Object.entries(p.events ?? {}).map(([key, event]) => ({
						name: `${p.id}.${key}`,
						filterable: (event as { filterable: string[] }).filterable,
					})),
				),
				{ name: INGRESS_EVENT, filterable: [] },
				{ name: APPROVAL_DECIDED_EVENT, filterable: ["approvalId"] },
			],
		});
	};
	let unseal: (() => void) | undefined;
	return {
		host,
		clients: () => host.clients(),
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
			// Agents and state are complete now: whether the app runs Pi is decided here, for good.
			unseal ??= sealDefinitions(config.definitionScope ?? host);
			register();
			if (new Set(workflows.map((w) => w.name)).size !== workflows.length)
				throw new Error("Duplicate workflow name");
			if (config.plugins.some((p) => p.chat) && !config.chat)
				throw new Error("Chat integration required");
			const routed = config.routedAgents ?? [];
			if (new Set(routed.map((a) => a.name)).size !== routed.length)
				throw new Error("Duplicate agent name");
			router.check();
			for (const plugin of config.plugins) await plugin.init?.();
			await config.resources?.open();
			chat = config.chat && connectChat(config.chat, host);
			await chat?.start();
			await agents.start();
			if (!running) {
				// Resumed handlers may emit while worker startup is still completing.
				running = true;
				try {
					await tasks!.orchestrator.start();
				} catch (error) {
					running = false;
					throw error;
				}
			}
		},
		async stop() {
			if (running && tasks) {
				running = false;
				await tasks.orchestrator.stop();
				await tasks.orchestrator.stopped;
			}
			await agents.stop();
			await chat?.stop();
			for (const plugin of [...config.plugins].reverse()) await plugin.shutdown?.();
			await config.resources?.close();
			liveClients = undefined;
			unseal?.();
			unseal = undefined;
		},
		async emit(event, payload, options = {}) {
			await host.ingest(event, payload, options);
		},
	};
}

/** Settings only `fabrial/testing` may change. */
export interface ProductionOverrides {
	models?: MutableModels;
	agentSettings?: PiOptions["settings"];
	conductor?: Omit<ConductorConfig, "sql">;
}

/**
 * Durable retries of transient model errors (overload, rate limits, 5xx, network). Pi retries failed attempt
 * n while n <= maxRetries, after min(baseDelayMs * 2^(n-1), maxAgentDelayMs): 2+4+8+16+32+60+60+60+60 s, so
 * about 5 minutes of backoff before a sent request gets the "couldn't reach the model provider" reply.
 */
const MODEL_RETRY: ConversationRetryPolicy = {
	enabled: true,
	maxRetries: 9,
	baseDelayMs: 2_000,
	maxAgentDelayMs: 60_000,
};

/** One lazily opened pool. Its identity is stable, so Conductor can hold it before app.start(). */
function lazyPool(url: string | undefined) {
	let pool: Sql | undefined;
	const current = (): Sql => {
		if (!pool) throw new Error("The Fabrial database is not open; call app.start() first");
		return pool;
	};
	const sql = new Proxy(function () {} as unknown as Sql, {
		apply: (_target, _this, args: unknown[]) => Reflect.apply(current(), undefined, args),
		get: (_target, key) => Reflect.get(current(), key),
	});
	return {
		sql,
		url,
		open() {
			if (!url) throw new Error("createFabrial requires a database connection string");
			pool ??= postgres(url, { max: 20, onnotice: () => {} });
		},
		async close() {
			const open = pool;
			pool = undefined;
			await open?.end();
		},
	};
}

/** Conductor, Pi, and Chat SDK on the instance's database. Side-effect free until app.start(). */
export function production(
	config: { database: string | undefined; models: ModelCredentials },
	overrides: ProductionOverrides = {},
): Pick<CoreConfig, "conductor" | "chat" | "agents" | "resources"> {
	const pool = lazyPool(config.database);
	const credentials = createModels(config.models);
	let state: ReturnType<typeof createPostgresState> | undefined;
	return {
		conductor: { ...overrides.conductor, sql: pool.sql },
		// Opened when the app starts.
		chat: { state: () => (state = createPostgresState({ url: pool.url! })) },
		agents: {
			models: overrides.models ?? credentials.models,
			sql: pool.sql,
			settings: {
				...overrides.agentSettings,
				retry: { ...MODEL_RETRY, ...overrides.agentSettings?.retry },
			},
		},
		resources: {
			open() {
				if (!overrides.models) credentials.validate();
				pool.open();
			},
			async close() {
				const open = state;
				state = undefined;
				try {
					await open?.disconnect();
				} finally {
					await pool.close();
				}
			},
		},
	};
}
