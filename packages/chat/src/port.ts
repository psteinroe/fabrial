import {
	Actions,
	Button,
	Card as SDKCard,
	CardText,
	Field,
	Fields,
	type Adapter,
	type StateAdapter,
	type AdapterPostableMessage,
	type ChatElement,
	toCardElement,
	type Chat,
	type Thread as SDKThread,
} from "chat";
import type {
	ChatPort,
	ExternalIdentity,
	FabrialHost,
	MessageRef,
	OutboundMessage,
	Surface,
	ThreadIO,
	ThreadRef,
	ThreadRoutingState,
} from "fabrial";
import { normalizeMessage, type ChatPluginCapability } from "./events.ts";

export function renderMessage(message: OutboundMessage): AdapterPostableMessage {
	if (typeof message === "string" || "markdown" in message) return message;
	const card = message.card;
	const children: ChatElement[] = [];
	if (card.text) children.push(CardText(card.text));
	if (card.fields?.length) children.push(Fields(card.fields.map((field) => Field(field))));
	if (card.code) {
		const fence = "`".repeat(
			Math.max(3, ...[...card.code.content.matchAll(/`+/g)].map((m) => m[0].length + 1)),
		);
		children.push(CardText(`${fence}${card.code.language ?? ""}\n${card.code.content}\n${fence}`));
	}
	if (card.actions?.length) children.push(Actions(card.actions.map((action) => Button(action))));
	const rendered = toCardElement(SDKCard({ title: card.title, children }));
	if (!rendered) throw new Error("Invalid card");
	return rendered;
}

export type SDKState = { fabrial?: ThreadRoutingState | null } & Record<string, unknown>;
type Bot = Chat<Record<string, Adapter>, SDKState>;

class LiveThread implements ThreadIO {
	private lastStatusAt = 0;
	private statusText: string | null = null;
	private statusMessageId: string | null = null;
	private fallbackStatus = false;

	constructor(
		private readonly sdk: SDKThread<SDKState>,
		private readonly provider: string,
		private readonly capability: ChatPluginCapability,
		private readonly host: FabrialHost,
		private readonly stateAdapter: StateAdapter,
	) {
		this.fallbackStatus = capability.status === "message";
	}

	get ref(): ThreadRef {
		return { kind: "thread", provider: this.provider, threadId: this.sdk.id };
	}
	get channelId() {
		const prefix = `${this.provider}:`;
		return this.sdk.channelId.startsWith(prefix)
			? this.sdk.channelId.slice(prefix.length)
			: this.sdk.channelId;
	}
	get isDM() {
		return this.sdk.isDM;
	}

	async post(message: OutboundMessage): Promise<MessageRef> {
		const sent = await this.sdk.post(renderMessage(message));
		return { provider: this.provider, threadId: this.sdk.id, messageId: sent.id };
	}

	async update(message: MessageRef, content: OutboundMessage): Promise<void> {
		if (message.provider !== this.provider || message.threadId !== this.sdk.id) {
			throw new Error("Message does not belong to this thread");
		}
		await this.sdk.adapter.editMessage(this.sdk.id, message.messageId, renderMessage(content));
	}

	async setStatus(text: string | null): Promise<void> {
		const now = Date.now();
		if (text !== null && now - this.lastStatusAt < (text === this.statusText ? 5000 : 1000)) return;
		this.lastStatusAt = now;
		this.statusText = text;
		try {
			if (!this.fallbackStatus) {
				try {
					// Slack's adapter maps this to assistant.threads.setStatus (empty clears).
					await this.sdk.startTyping(text === null ? "" : text.slice(0, 80));
					if (text === null) await this.sdk.adapter.endTyping?.(this.sdk.id);
					return;
				} catch {
					this.fallbackStatus = true;
				}
			}
			const state = await this.getState();
			this.statusMessageId ??= state?.statusMessageId ?? null;
			if (text === null) {
				if (this.statusMessageId)
					await this.sdk.adapter.deleteMessage(this.sdk.id, this.statusMessageId);
				this.statusMessageId = null;
			} else if (this.statusMessageId) {
				await this.sdk.adapter.editMessage(this.sdk.id, this.statusMessageId, { markdown: text });
			} else {
				this.statusMessageId = (await this.sdk.post({ markdown: text })).id;
			}
			await this.updateState((current) =>
				current ? { ...current, statusMessageId: this.statusMessageId } : null,
			);
		} catch (error) {
			this.host.logger.debug("Chat status update failed", { threadId: this.sdk.id, error });
		}
	}

	async history(options: { limit?: number; sinceLastBotReply?: boolean }) {
		const defaults = this.capability.history;
		const requestedLimit = options.limit ?? defaults?.limit ?? 50;
		const limit = Number.isNaN(requestedLimit)
			? 50
			: Math.max(0, Math.min(100, Math.floor(requestedLimit)));
		if (!limit) return [];
		const { messages } = await this.sdk.adapter.fetchMessages(this.sdk.id, {
			limit,
			direction: "backward",
		});
		const sinceLastReply = options.sinceLastBotReply ?? defaults?.mode === "since-last-reply";
		const boundary = sinceLastReply ? messages.findLastIndex((m) => m.author.isMe) : -1;
		return messages
			.slice(boundary + 1)
			.slice(-limit)
			.map((m) => normalizeMessage(this.provider, this.capability, this.sdk, m));
	}

	async getState(): Promise<ThreadRoutingState | null> {
		return structuredClone((await this.sdk.state)?.fabrial ?? null);
	}

	async updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null> {
		// Separate from SDK ingress locks: a handler may update state while holding those.
		// StateAdapter locks are token-owned and shared by all replicas (Postgres/Redis).
		const key = `fabrial:routing:${this.sdk.id}`;
		const deadline = Date.now() + 10_000;
		const leaseMs = 60_000;
		while (Date.now() < deadline) {
			const lock = await this.stateAdapter.acquireLock(key, leaseMs);
			if (!lock) {
				await new Promise((resolve) => setTimeout(resolve, 25));
				continue;
			}
			try {
				const next = structuredClone(fn(await this.getState()));
				// A slow updater may outlive its lease. Never knowingly commit a stale read;
				// reacquire and recompute instead (callbacks must therefore be pure).
				if (!(await this.stateAdapter.extendLock(lock, leaseMs))) continue;
				// SDK merges unrelated keys and refreshes the 30-day TTL, including clears.
				await this.sdk.setState({ fabrial: next });
				if (next) await this.sdk.subscribe();
				return structuredClone(next);
			} finally {
				await this.stateAdapter.releaseLock(lock);
			}
		}
		throw new Error(`Timed out acquiring routing state lock for ${this.sdk.id}`);
	}
}

/** A channel surface does not create a platform message until the first intentional post. */
class LazyThread implements ThreadIO {
	private live: ThreadIO | undefined;
	private firstPost: Promise<MessageRef> | undefined;
	constructor(
		private readonly surface: Extract<Surface, { kind: "channel" }>,
		private readonly bot: Bot,
		private readonly port: ChatPort,
	) {}
	get ref(): ThreadRef {
		return (
			this.live?.ref ?? {
				kind: "thread",
				provider: this.surface.provider,
				threadId: `provisional:${this.surface.provider}:${this.surface.channelId}`,
			}
		);
	}
	get channelId() {
		return this.surface.channelId;
	}
	get isDM() {
		return this.live?.isDM ?? false;
	}
	async post(message: OutboundMessage): Promise<MessageRef> {
		if (this.firstPost) await this.firstPost;
		if (this.live) return this.live.post(message);
		const create = async () => {
			const sent = await this.bot
				.channel(`${this.surface.provider}:${this.surface.channelId}`)
				.post(renderMessage(message));
			this.live = await this.port.thread({
				kind: "thread",
				provider: this.surface.provider,
				threadId: sent.threadId,
			});
			return { provider: this.surface.provider, threadId: sent.threadId, messageId: sent.id };
		};
		this.firstPost = create();
		try {
			return await this.firstPost;
		} finally {
			this.firstPost = undefined;
		}
	}
	async update(message: MessageRef, content: OutboundMessage) {
		if (message.provider !== this.surface.provider)
			throw new Error("Message belongs to another provider");
		const live =
			this.live ??
			(await this.port.thread({
				kind: "thread",
				provider: message.provider,
				threadId: message.threadId,
			}));
		if (live.channelId !== this.surface.channelId)
			throw new Error("Message belongs to another channel");
		this.live = live;
		await live.update(message, content);
	}
	async setStatus(text: string | null) {
		await this.live?.setStatus(text);
	}
	async history(options: { limit?: number; sinceLastBotReply?: boolean }) {
		return this.live ? this.live.history(options) : [];
	}
	async getState() {
		return this.live ? this.live.getState() : null;
	}
	async updateState(fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null) {
		if (!this.live)
			throw new Error("Cannot persist routing state for a provisional channel thread");
		return this.live.updateState(fn);
	}
}

export function createChatPort(
	bot: Bot,
	capabilities: Map<string, ChatPluginCapability>,
	host: FabrialHost,
): ChatPort & { stop(): Promise<void> } {
	const pending = new Set<Promise<unknown>>();
	let closing = false;
	const threads = new Map<string, ThreadIO>();
	const capability = (provider: string) => {
		const value = capabilities.get(provider);
		if (!value) throw new Error(`No chat provider "${provider}"`);
		return value;
	};
	const checkIdentity = (identity: ExternalIdentity) => {
		const value = capability(identity.provider);
		if (identity.installationId !== value.installationId)
			throw new Error("Identity belongs to another installation");
		return value;
	};
	const port: ChatPort & { stop(): Promise<void> } = {
		async stop() {
			closing = true;
			// Native workers can finish in-flight state operations after their abort
			// race settles. Keep the adapter connected through releaseLock.
			await Promise.allSettled(pending);
		},
		async thread(ref) {
			const config = capability(ref.provider);
			if (!ref.threadId.startsWith(`${ref.provider}:`))
				throw new Error("Thread belongs to another provider");
			await bot.initialize();
			let io = threads.get(ref.threadId);
			if (!io) {
				io = new LiveThread(bot.thread(ref.threadId), ref.provider, config, host, bot.getState());
				const update = io.updateState.bind(io);
				io.updateState = (change) => {
					if (closing) return Promise.reject(new Error("Chat integration is stopped"));
					const operation = update(change).finally(() => pending.delete(operation));
					pending.add(operation);
					return operation;
				};
				// Presentation caching is bounded; durable state remains in the SDK adapter.
				if (threads.size >= 1000) threads.delete(threads.keys().next().value!);
				threads.set(ref.threadId, io);
			}
			return io;
		},
		async resolve(surface) {
			if (surface.kind === "thread") return port.thread(surface);
			capability(surface.provider);
			await bot.initialize();
			return new LazyThread(surface, bot, port);
		},
		async openDM(identity) {
			checkIdentity(identity);
			await bot.initialize();
			const adapter = bot.getAdapter(identity.provider);
			if (!adapter.openDM) throw new Error(`Provider "${identity.provider}" does not support DMs`);
			const threadId = await adapter.openDM(identity.subjectId);
			return port.thread({ kind: "thread", provider: identity.provider, threadId });
		},
		async postEphemeral(ref, identity, text) {
			checkIdentity(identity);
			if (identity.provider !== ref.provider)
				throw new Error("Identity belongs to another provider");
			await port.thread(ref);
			await bot
				.thread(ref.threadId)
				.postEphemeral(identity.subjectId, text, { fallbackToDM: true });
		},
	};
	return port;
}
