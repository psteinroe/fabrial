// Fabrial's semantics on Chat SDK threads: posting with receipts, lazy channel posts, history, the
// throttled status, and atomic routing state. Callers hold the SDK's own `Chat` and `Thread`.
import type { Adapter, Chat, Thread } from "chat";
import type { Logger } from "../app.ts";
import type { ExternalIdentity } from "../identity.ts";
import type { ThreadRoutingState } from "../routing.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface, ThreadRef } from "../thread.ts";
import { normalizeMessage, type ChatPluginCapability } from "./events.ts";
import { renderMessage } from "./render.ts";

export type SDKState = { fabrial?: ThreadRoutingState | null } & Record<string, unknown>;
export type SDKThread = Thread<SDKState>;
export type Bot = Chat<Record<string, Adapter>, SDKState>;

/** The provider of a thread: its adapter's name, which is its plugin's id. */
const providerOf = (thread: SDKThread) => thread.adapter.name;

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

/** Posts to the thread; the receipt is serializable (never the SDK's `SentMessage`). */
export async function postMessage(
	thread: SDKThread,
	message: OutboundMessage,
): Promise<MessageRef> {
	const sent = await thread.post(renderMessage(message));
	return { provider: providerOf(thread), threadId: thread.id, messageId: sent.id };
}

/**
 * Posts to a surface. A channel stays lazy until its first post, which creates the thread its receipt
 * names; later posts go to that thread (callers rebind to the receipt, also on replay).
 */
export async function postTo(
	chat: ChatConnection,
	surface: Surface,
	message: OutboundMessage,
): Promise<MessageRef> {
	if (surface.kind === "thread") return postMessage(await chat.thread(surface), message);
	chat.capability(surface.provider);
	await chat.bot.initialize();
	const sent = await chat.bot
		.channel(`${surface.provider}:${surface.channelId}`)
		.post(renderMessage(message));
	return { provider: surface.provider, threadId: sent.threadId, messageId: sent.id };
}

/** The channel of a surface, without the provider. */
export async function channelOfSurface(chat: ChatConnection, surface: Surface): Promise<string> {
	return surface.kind === "channel" ? surface.channelId : channelOf(await chat.thread(surface));
}

/** Edits a posted message; `within` (the surface it was posted to) must contain it. */
export async function editMessage(
	chat: ChatConnection,
	message: MessageRef,
	content: OutboundMessage,
	within?: Surface,
): Promise<void> {
	if (within && message.provider !== within.provider)
		throw new Error("Message belongs to another provider");
	const thread = await chat.thread({
		kind: "thread",
		provider: message.provider,
		threadId: message.threadId,
	});
	if (within?.kind === "thread" && within.threadId !== thread.id)
		throw new Error("Message does not belong to this thread");
	if (within?.kind === "channel" && channelOf(thread) !== within.channelId)
		throw new Error("Message belongs to another channel");
	await thread.adapter.editMessage(thread.id, message.messageId, renderMessage(content));
}

/** Messages for model context: bounded (provider default, at most 100) and attributed. */
export async function fetchHistory(
	chat: ChatConnection,
	thread: SDKThread,
	options: { limit?: number; sinceLastBotReply?: boolean },
): Promise<ChatMessage[]> {
	const provider = providerOf(thread);
	const capability = chat.capability(provider);
	const defaults = capability.history;
	const requestedLimit = options.limit ?? defaults?.limit ?? 50;
	const limit = Number.isNaN(requestedLimit)
		? 50
		: Math.max(0, Math.min(100, Math.floor(requestedLimit)));
	if (!limit) return [];
	const { messages } = await thread.adapter.fetchMessages(thread.id, {
		limit,
		direction: "backward",
	});
	const sinceLastReply = options.sinceLastBotReply ?? defaults?.mode === "since-last-reply";
	const boundary = sinceLastReply ? messages.findLastIndex((m) => m.author.isMe) : -1;
	return messages
		.slice(boundary + 1)
		.slice(-limit)
		.map((m) => normalizeMessage(provider, capability, thread, m));
}

/** Fabrial's routing state in Chat SDK thread state (30-day TTL, refreshed on write). */
export async function routingState(thread: SDKThread): Promise<ThreadRoutingState | null> {
	return structuredClone((await thread.state)?.fabrial ?? null);
}

/**
 * Atomic read-modify-write of the routing state under a per-thread lock (Chat SDK state adapter lock),
 * across processes; the SDK's `setState` alone is read/merge/write. `fn` may run more than once; keep
 * it pure. Returning `null` clears the state; a state subscribes the thread.
 */
export function updateRoutingState(
	chat: ChatConnection,
	thread: SDKThread,
	fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
): Promise<ThreadRoutingState | null> {
	return chat.track(async () => {
		// Separate from SDK ingress locks: a handler may update state while holding those.
		// StateAdapter locks are token-owned and shared by all replicas (Postgres/Redis).
		const state = chat.bot.getState();
		const key = `fabrial:routing:${thread.id}`;
		const deadline = Date.now() + 10_000;
		const leaseMs = 60_000;
		while (Date.now() < deadline) {
			const lock = await state.acquireLock(key, leaseMs);
			if (!lock) {
				await new Promise((resolve) => setTimeout(resolve, 25));
				continue;
			}
			try {
				const next = structuredClone(fn(await routingState(thread)));
				// A slow updater may outlive its lease. Never knowingly commit a stale read;
				// reacquire and recompute instead (callbacks must therefore be pure).
				if (!(await state.extendLock(lock, leaseMs))) continue;
				// SDK merges unrelated keys and refreshes the 30-day TTL, including clears.
				await thread.setState({ fabrial: next });
				if (next) await thread.subscribe();
				return structuredClone(next);
			} finally {
				await state.releaseLock(lock);
			}
		}
		throw new Error(`Timed out acquiring routing state lock for ${thread.id}`);
	});
}

/**
 * The status of one thread: at most one change per second, and a repeat of the shown status at most
 * every 5 s. A change inside the window shows when it ends (the latest one wins); clearing is
 * immediate. Providers without a native status get one edited status message.
 */
class StatusThrottle {
	private lastStatusAt = 0;
	private statusText: string | null = null;
	private statusMessageId: string | null = null;
	private fallbackStatus: boolean;
	private pending: { text: string; timer: ReturnType<typeof setTimeout> } | undefined;
	private chain = Promise.resolve();

	constructor(
		private readonly thread: SDKThread,
		private readonly chat: ChatConnection,
	) {
		this.fallbackStatus = chat.capability(providerOf(thread)).status === "message";
	}

	async set(text: string | null): Promise<void> {
		const elapsed = Date.now() - this.lastStatusAt;
		if (text !== null && elapsed < (text === this.statusText ? 5000 : 1000)) {
			if (text === this.statusText) this.drop();
			else if (this.pending) this.pending.text = text;
			else if (!this.chat.closed) {
				const timer = setTimeout(() => {
					const pending = this.pending;
					this.drop();
					if (pending) void this.show(pending.text);
				}, 1000 - elapsed);
				timer.unref?.();
				this.pending = { text, timer };
			}
			return;
		}
		this.drop();
		await this.show(text);
	}

	/** Forgets a status waiting for the throttle window to end. */
	drop() {
		if (!this.pending) return;
		clearTimeout(this.pending.timer);
		this.pending = undefined;
	}

	private show(text: string | null): Promise<void> {
		// Clearing doesn't throttle the next status: a new run's "Thinking…" right after a clear shows.
		this.lastStatusAt = text === null ? 0 : Date.now();
		this.statusText = text;
		// In order, so a trailing status and a clear right after it don't race on the fallback post.
		const shown = this.chain.then(() => this.write(text));
		this.chain = shown;
		return shown;
	}

	private async write(text: string | null): Promise<void> {
		const { thread } = this;
		try {
			if (!this.fallbackStatus) {
				try {
					// Slack's adapter maps this to assistant.threads.setStatus (empty clears).
					await thread.startTyping(text === null ? "" : text.slice(0, 80));
					if (text === null) await thread.adapter.endTyping?.(thread.id);
					return;
				} catch {
					this.fallbackStatus = true;
				}
			}
			this.statusMessageId ??= (await routingState(thread))?.statusMessageId ?? null;
			if (text === null) {
				if (this.statusMessageId)
					await thread.adapter.deleteMessage(thread.id, this.statusMessageId);
				this.statusMessageId = null;
			} else if (this.statusMessageId) {
				await thread.adapter.editMessage(thread.id, this.statusMessageId, { markdown: text });
			} else {
				this.statusMessageId = (await thread.post({ markdown: text })).id;
			}
			await updateRoutingState(this.chat, thread, (current) =>
				current ? { ...current, statusMessageId: this.statusMessageId } : null,
			);
		} catch (error) {
			this.chat.logger.debug("Chat status update failed", { threadId: thread.id, error });
		}
	}
}
