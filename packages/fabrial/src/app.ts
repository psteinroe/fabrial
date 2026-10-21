import type { EmitOptions, Origin } from "./events.ts";
import type { ExternalIdentity, IdentityDirectory, Principal, UserDefinition } from "./identity.ts";
import type { JsonObject } from "./json.ts";
import type { AnyPlugin } from "./plugin.ts";
import type { ChatConnection, ChatOptions } from "./chat/index.ts";
import type { PiOptions } from "./agents/pi.ts";
import type { Conductor, Orchestrator } from "pgconductor-js";
import type { ConductorConfig } from "./conductor.ts";
import type { ChatMessage, Surface, ThreadRef } from "./thread.ts";
import type { AnyWorkflow } from "./workflow.ts";

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
	routedAgents?: readonly import("./agent.ts").Agent[];
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
