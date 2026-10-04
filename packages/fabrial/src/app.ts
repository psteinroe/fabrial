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

/** What core exposes to adapter packages (`@fabrial/chat`, `@fabrial/pi`). Not for app code. */
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
	/** Chat port, after app.start() connects `@fabrial/chat`. */
	chat(): ChatPort | undefined;

	/**
	 * New work from a plugin: resolves trigger ownership (most specific owner + all observers), creates
	 * the interaction, and emits through the runtime. Returns the owning workflow name, if any.
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
	 * Inbound chat message. Routes to the thread's active interaction (agent steering, a waiting
	 * `waitForReply`, or the buffer) or, if none, through `ingest` as new work. Returns how it was routed.
	 */
	receiveMessage(
		message: ChatMessage,
		options: {
			events?: readonly string[];
			/** @deprecated Use events. */ event?: string;
			dedupeId: string;
		},
	): Promise<"reply" | "new" | "ignored">;

	/** Inbound card action (approval buttons, [Cancel request]). */
	receiveAction(action: {
		actionId: string;
		value: string | undefined;
		actor: ExternalIdentity;
		thread: ThreadRef;
		messageId: string;
		dedupeId: string;
	}): Promise<void>;

	/** Requesters and participants in the current interaction may stop its handler. */
	receiveCancellation(action: {
		actor: ExternalIdentity;
		thread: ThreadRef;
		dedupeId: string;
	}): Promise<void>;

	resolvePrincipal(identity: ExternalIdentity): Promise<Principal>;
}

/** An adapter that plugs into the host (`chat(...)`, `pi(...)`). */
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
	/** Durable workflow runtime, e.g. `conductor({ sql })` from `@fabrial/conductor`. */
	runtime: DurableRuntime;
	/** Chat SDK integration from `@fabrial/chat`. Required when any plugin has a `chat` capability. */
	chat?: ChatIntegration;
	/** Pi Durable integration from `@fabrial/pi`. Required for `ctx.agent`, `ctx.state`, `ctx.evaluate`. */
	agents?: AgentIntegration;
	plugins: P;
	workflows: readonly AnyWorkflow[];
	/** Known users only; groups are resolved directly by reference in access/approval options. */
	identity?: readonly UserDefinition[];
	logger?: Logger;
	/** Grace period for an ingress reservation with no bound handler. Default: 10m; must be below 30 days. */
	reservationTimeout?: import("./runtime.ts").DurationInput;
}

export interface App<TClients = Record<string, unknown>> {
	/** One fetch handler for all plugin routes and chat webhooks. */
	fetch(request: Request): Promise<Response>;
	/** Validates (trigger ties, orphaned Pi tasks, …), runs plugin `init`, starts the runtime. */
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
