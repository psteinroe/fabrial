// The app's Chat SDK bot and what Fabrial keeps beside it: provider capabilities, statuses, and pending state writes.
import {
	type Adapter,
	Chat,
	type Logger as ChatLogger,
	type StateAdapter,
	type Thread,
} from "chat";
import type { AnyPlugin, ChatPluginCapability } from "../plugin.ts";
import type { ChatMessage, ThreadRef } from "./messages.ts";
import type { FabrialHost, Logger } from "../app.ts";
import { listen } from "./ingress.ts";
import type { ThreadRoutingState } from "../routing/follows.ts";
import { StatusThrottle } from "./status.ts";
import type { ExternalIdentity } from "../identity.ts";

export interface ChatOptions {
	/** A factory runs when the app starts (production opens its Postgres state then). */
	state: StateAdapter | (() => StateAdapter);
	userName?: string;
	/** Routes served besides the providers' webhooks. */
	routes?: Record<string, (request: Request) => Promise<Response>>;
	/** Internal (FakeChat): the chat capability each plugin runs with, instead of its own. */
	capability?(plugin: AnyPlugin): ChatPluginCapability | undefined;
	/** Internal (FakeChat): how core took an inbound message. */
	received?(message: ChatMessage, result: "accepted" | "ignored"): void;
	/** Internal (FakeChat): the connection, once connected. */
	connected?(connection: ChatConnection): void;
}

/** One Chat SDK bot for every chat plugin of the app, dispatching into core. Internal. */
export function connectChat(options: ChatOptions, host: FabrialHost): ChatConnection {
	const adapters: Record<string, Adapter> = {};
	const capabilities = new Map<string, ChatPluginCapability>();
	for (const plugin of host.plugins) {
		const config = options.capability
			? options.capability(plugin)
			: (plugin.chat as ChatPluginCapability | undefined);
		if (!config) continue;
		if (!config.installationId)
			throw new Error(`Chat plugin "${plugin.id}" must declare an installationId`);
		if (capabilities.has(plugin.id)) throw new Error(`Duplicate chat plugin "${plugin.id}"`);
		const adapter = config.adapter();
		if (adapter.name !== plugin.id)
			throw new Error(
				`Chat adapter "${adapter.name}" must encode ids with its plugin id "${plugin.id}"`,
			);
		adapters[plugin.id] = adapter;
		capabilities.set(plugin.id, config);
	}
	const logger: ChatLogger = {
		debug: (message, ...args) => host.logger.debug(message, { args }),
		info: (message, ...args) => host.logger.info(message, { args }),
		warn: (message, ...args) => host.logger.warn(message, { args }),
		error: (message, ...args) => host.logger.error(message, { args }),
		child: () => logger,
	};
	const bot = new Chat<Record<string, Adapter>, SDKState>({
		adapters,
		state: typeof options.state === "function" ? options.state() : options.state,
		userName: options.userName ?? "fabrial",
		// Queue/debounce strategies coalesce messages; every message is an input of its own.
		concurrency: { strategy: "concurrent", maxConcurrent: 1 },
		logger,
	});
	const routes = listen(bot, host, options, adapters, capabilities);
	const connection = new ChatConnection(bot, capabilities, host.logger, routes);
	options.connected?.(connection);
	return connection;
}

export type SDKState = { fabrial?: ThreadRoutingState | null } & Record<string, unknown>;
export type SDKThread = Thread<SDKState>;
export type Bot = Chat<Record<string, Adapter>, SDKState>;

/** The provider of a thread: its adapter's name, which is its plugin's id. */
export const providerOf = (thread: SDKThread) => thread.adapter.name;

export function threadRef(thread: SDKThread): ThreadRef {
	return { kind: "thread", provider: providerOf(thread), threadId: thread.id };
}

/** The thread's channel, without the provider. */
export function channelOf(thread: SDKThread): string {
	const prefix = `${providerOf(thread)}:`;
	return thread.channelId.startsWith(prefix)
		? thread.channelId.slice(prefix.length)
		: thread.channelId;
}

/**
 * The connected Chat SDK bot, with what Fabrial keeps beside it: each provider's capability, the
 * status of each thread, and the routing-state writes still running (stopping drains them before the
 * state adapter disconnects).
 */
export class ChatConnection {
	private closing = false;
	private readonly pending = new Set<Promise<unknown>>();
	private readonly statuses = new Map<string, StatusThrottle>();

	constructor(
		readonly bot: Bot,
		private readonly capabilities: Map<string, ChatPluginCapability>,
		readonly logger: Logger,
		/** The providers' webhook routes (and, for FakeChat, routes of its own). */
		readonly routes: Record<string, (request: Request) => Promise<Response>>,
	) {}

	capability(provider: string): ChatPluginCapability {
		const value = this.capabilities.get(provider);
		if (!value) throw new Error(`No chat provider "${provider}"`);
		return value;
	}

	private checkIdentity(identity: ExternalIdentity) {
		if (identity.installationId !== this.capability(identity.provider).installationId)
			throw new Error("Identity belongs to another installation");
	}

	async start() {
		await this.bot.initialize();
	}

	async stop() {
		this.closing = true;
		for (const status of this.statuses.values()) status.drop();
		// Native workers can finish in-flight state operations after their abort race settles.
		// Keep the adapter connected through releaseLock.
		await Promise.allSettled(this.pending);
		await this.bot.shutdown();
	}

	/** The SDK thread of `ref`, on an initialized bot. */
	async thread(ref: ThreadRef): Promise<SDKThread> {
		this.capability(ref.provider);
		if (!ref.threadId.startsWith(`${ref.provider}:`))
			throw new Error("Thread belongs to another provider");
		await this.bot.initialize();
		return this.bot.thread(ref.threadId);
	}

	async openDM(identity: ExternalIdentity): Promise<SDKThread> {
		this.checkIdentity(identity);
		await this.bot.initialize();
		const adapter = this.bot.getAdapter(identity.provider);
		if (!adapter.openDM) throw new Error(`Provider "${identity.provider}" does not support DMs`);
		const threadId = await adapter.openDM(identity.subjectId);
		return this.thread({ kind: "thread", provider: identity.provider, threadId });
	}

	/** A link to the thread, when its provider has one. */
	async permalink(ref: ThreadRef): Promise<string | null> {
		return (await this.capability(ref.provider).permalink?.(ref)) ?? null;
	}

	/** Short-lived message visible only to one user (e.g. "you are not an approver"). */
	async postEphemeral(ref: ThreadRef, identity: ExternalIdentity, text: string): Promise<void> {
		this.checkIdentity(identity);
		if (identity.provider !== ref.provider) throw new Error("Identity belongs to another provider");
		const thread = await this.thread(ref);
		await thread.postEphemeral(identity.subjectId, text, { fallbackToDM: true });
	}

	/** Replaceable progress status ("Thinking…"); `null` clears it. Best effort, never durable. */
	setStatus(thread: SDKThread, text: string | null): Promise<void> {
		let status = this.statuses.get(thread.id);
		if (!status) {
			status = new StatusThrottle(thread, this);
			// Bounded; a forgotten thread's fallback status message is found again in its state.
			if (this.statuses.size >= 1000) {
				const [oldest, value] = this.statuses.entries().next().value!;
				value.drop();
				this.statuses.delete(oldest);
			}
			this.statuses.set(thread.id, status);
		}
		return status.set(text);
	}

	/** Runs a state operation the stop waits for; none start once stopping. */
	track<T>(operation: () => Promise<T>): Promise<T> {
		if (this.closing) return Promise.reject(new Error("Chat integration is stopped"));
		const running = operation().finally(() => this.pending.delete(running));
		this.pending.add(running);
		return running;
	}

	get closed() {
		return this.closing;
	}
}
