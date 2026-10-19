import type { EmitOptions, Origin } from "./events.ts";
import type { Evaluator } from "./evaluate.ts";
import type { ExternalIdentity, IdentityDirectory, Principal, UserDefinition } from "./identity.ts";
import type { JsonObject } from "./json.ts";
import type { AnyPlugin } from "./plugin.ts";
import type { AgentPort, ChatPort, DurableRuntime, RuntimeWorkflow, StatePort } from "./runtime.ts";
import type { ChatMessage, Surface, ThreadRef } from "./thread.ts";
import type { AnyWorkflow } from "./workflow.ts";

export interface Logger {
	debug(message: string, data?: Record<string, unknown>): void;
	info(message: string, data?: Record<string, unknown>): void;
	warn(message: string, data?: Record<string, unknown>): void;
	error(message: string, data?: Record<string, unknown>): void;
}

/** What core exposes to its runtime modules and to tests (`app.host`). */
export interface FabrialHost<TClients = Record<string, unknown>> {
	/** Definition catalog identity, used by adapters to isolate instance-local definitions. */
	readonly definitionScope?: object;
	readonly plugins: readonly AnyPlugin[];
	readonly workflows: readonly AnyWorkflow[];
	readonly runtime: DurableRuntime;
	readonly directory: IdentityDirectory;
	readonly logger: Logger;
	/** Live clients of all plugins (built once per app; plugin `clients()` must be cheap and stateless). */
	clients(): TClients;
	/** Dynamic subscriptions (`fabrial.subscriptions`); absent without a store. */
	readonly subscriptions?: import("./subscriptions.ts").SubscriptionStore;
	/** Chat port, after app.start() connects chat. */
	chat(): ChatPort | undefined;

	/**
	 * New work from a plugin: resolves trigger ownership (most specific owner + all observers) and
	 * emits through the runtime, or delivers to an agent's conversation. Returns the owning workflow name, if any.
	 */
	ingest(
		event: string,
		payload: JsonObject,
		options: EmitOptions & {
			origin?: Origin;
			replyTo?: Surface;
			requestedBy?: ExternalIdentity;
			interactionId?: string;
		},
	): Promise<{ owner: string | undefined; observers: string[] }>;

	/**
	 * Inbound chat message. Goes to the conversations following its thread, else to the most specific
	 * matching trigger. Returns how it was routed.
	 */
	receiveMessage(
		message: ChatMessage,
		options: {
			events?: readonly string[];
			/** @deprecated Use events. */ event?: string;
			dedupeId: string;
		},
	): Promise<"follow" | "new" | "ignored">;

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

/** A runtime module that plugs into the host (chat, agents). Internal. */
export interface HostIntegration<T> {
	readonly kind: string;
	connect(host: FabrialHost): T;
}

export interface ChatIntegration extends HostIntegration<{
	port: ChatPort;
	routes: Record<string, (request: Request) => Promise<Response>>;
	start?(): Promise<void>;
	stop?(): Promise<void>;
}> {
	readonly kind: "fabrial.chat";
}

export interface AgentIntegration extends HostIntegration<{
	agents: AgentPort;
	state: StatePort;
	evaluator: Evaluator;
	workflows: RuntimeWorkflow[];
	start(): Promise<void>;
	stop(): Promise<void>;
}> {
	readonly kind: "fabrial.agents";
}

export interface CoreConfig<P extends readonly AnyPlugin[] = readonly AnyPlugin[]> {
	definitionScope?: object;
	/** Durable workflow runtime (Conductor, or in memory for tests). */
	runtime: DurableRuntime;
	/** Chat SDK integration. Required when any plugin has a `chat` capability. */
	chat?: ChatIntegration;
	/** Agents, state, and evaluation. Required for `ctx.agent`, `ctx.state`, `ctx.evaluate`. */
	agents?: AgentIntegration;
	/** Store of dynamic subscriptions; started after `resources` open. */
	subscriptionStore?: import("./subscriptions.ts").SubscriptionStore;
	/** Owned resources (database pools): opened after plugin `init`, closed last on stop. */
	resources?: { open(): Promise<void> | void; close(): Promise<void> };
	plugins: P;
	workflows: readonly AnyWorkflow[];
	/** Agents whose triggers receive events. */
	routedAgents?: readonly import("./agent.ts").Agent[];
	/** Known users only; groups are resolved directly by reference in access/approval options. */
	identity?: readonly UserDefinition[];
	logger?: Logger;
}

export interface App<TClients = Record<string, unknown>> {
	/** One fetch handler for all plugin routes and chat webhooks. */
	fetch(request: Request): Promise<Response>;
	/** Validates (trigger ties, …), aborts orphaned agent work, runs plugin `init`, starts the runtime. */
	start(): Promise<void>;
	stop(): Promise<void>;
	/** Emit an event from application code. */
	emit(
		event: string,
		payload: JsonObject,
		options?: EmitOptions & { replyTo?: Surface },
	): Promise<void>;
	readonly host: FabrialHost<TClients>;
}
