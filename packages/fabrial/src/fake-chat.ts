// `FakeChat`: the chat integration `fabrial/testing` apps use by default.
import type { ChatIntegration, FabrialHost } from "./app.ts";
import { identityKey } from "./directory.ts";
import type { ExternalIdentity } from "./identity.ts";
import { THREAD_TTL } from "./internal.ts";
import type { ChatPort, ThreadIO, ThreadRoutingState } from "./runtime.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface, ThreadRef } from "./thread.ts";

const clone = <T>(value: T): T => structuredClone(value);

function messageText(content: OutboundMessage): string {
	return typeof content === "string"
		? content
		: "markdown" in content
			? content.markdown
			: [content.card.title, content.card.text].filter(Boolean).join("\n");
}

export interface FakePost {
	ref: MessageRef;
	content: OutboundMessage;
	updates: OutboundMessage[];
}
class FakeThread implements ThreadIO {
	readonly posts: FakePost[] = [];
	readonly messages: ChatMessage[] = [];
	/** The statuses shown, in order (`null` clears); setting the shown status again, as a chat thread does, adds none. */
	readonly statuses: (string | null)[] = [];
	/** Whether the thread is subscribed (Chat SDK delivers its messages as subscribed messages). */
	subscribed = false;
	private state: ThreadRoutingState | null = null;
	private expires = 0;
	constructor(
		readonly ref: ThreadRef,
		readonly channelId: string,
		readonly isDM: boolean,
		private readonly owner: FakeChat,
	) {}
	async post(content: OutboundMessage): Promise<MessageRef> {
		const ref = {
			provider: this.ref.provider,
			threadId: this.ref.threadId,
			messageId: `message-${this.owner.nextMessageId++}`,
		};
		this.posts.push({ ref, content: clone(content), updates: [] });
		this.messages.push({
			provider: ref.provider,
			threadId: ref.threadId,
			channelId: this.channelId,
			messageId: ref.messageId,
			text: messageText(content),
			author: {
				identity: { provider: ref.provider, installationId: "testing", subjectId: "bot" },
				name: "Fabrial",
				isBot: true,
			},
			isMention: false,
			isDM: this.isDM,
			sentAt: new Date(this.owner.now()).toISOString(),
		});
		return ref;
	}
	async update(ref: MessageRef, content: OutboundMessage): Promise<void> {
		const post = this.posts.find((p) => p.ref.messageId === ref.messageId);
		if (!post) throw new Error(`Unknown message ${ref.messageId}`);
		post.content = clone(content);
		post.updates.push(clone(content));
		const message = this.messages.find((message) => message.messageId === ref.messageId);
		if (message) message.text = messageText(content);
	}
	async setStatus(text: string | null): Promise<void> {
		if (this.statuses.at(-1) !== text) this.statuses.push(text);
	}
	async history(options: { limit?: number; sinceLastBotReply?: boolean }): Promise<ChatMessage[]> {
		const lastBot = options.sinceLastBotReply
			? this.messages.findLastIndex((m) => m.author.isBot)
			: -1;
		return clone(this.messages.slice(lastBot + 1).slice(-(options.limit ?? 50)));
	}
	async subscribe(): Promise<void> {
		this.subscribed = true;
	}
	async getState(): Promise<ThreadRoutingState | null> {
		return this.owner.now() >= this.expires ? null : clone(this.state);
	}
	async updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null> {
		// No await between read and write: atomic within this in-memory process.
		this.state = clone(fn(this.owner.now() >= this.expires ? null : clone(this.state)));
		this.expires = this.owner.now() + THREAD_TTL;
		// Like Chat SDK state writes, a stored state subscribes the thread.
		if (this.state) this.subscribed = true;
		return clone(this.state);
	}
}

class FakeChannelThread implements ThreadIO {
	private actual?: FakeThread;
	constructor(
		private readonly surface: Extract<Surface, { kind: "channel" }>,
		private readonly owner: FakeChat,
	) {}
	get ref(): ThreadRef {
		return (
			this.actual?.ref ?? {
				kind: "thread",
				provider: this.surface.provider,
				threadId: `provisional:${this.surface.channelId}`,
			}
		);
	}
	get channelId(): string {
		return this.surface.channelId;
	}
	get isDM(): boolean {
		return false;
	}
	async post(content: OutboundMessage): Promise<MessageRef> {
		this.actual ??= await this.owner.thread({
			kind: "thread",
			provider: this.surface.provider,
			threadId: `channel:${this.surface.channelId}:root-${this.owner.nextThreadId++}`,
		});
		return this.actual.post(content);
	}
	async update(ref: MessageRef, content: OutboundMessage): Promise<void> {
		await (
			await this.owner.thread({ kind: "thread", provider: ref.provider, threadId: ref.threadId })
		).update(ref, content);
	}
	async setStatus(text: string | null): Promise<void> {
		await this.actual?.setStatus(text);
	}
	async history(options: { limit?: number; sinceLastBotReply?: boolean }): Promise<ChatMessage[]> {
		return this.actual ? this.actual.history(options) : [];
	}
	async subscribe(): Promise<void> {
		if (!this.actual) throw new Error("Cannot subscribe a provisional channel thread");
		await this.actual.subscribe();
	}
	async getState(): Promise<ThreadRoutingState | null> {
		return this.actual ? this.actual.getState() : null;
	}
	async updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null> {
		if (!this.actual)
			throw new Error("Cannot persist routing state for a provisional channel thread");
		return this.actual.updateState(fn);
	}
}

/** Fake chat integration with inspectable posts/DMs/ephemeral notices and authenticated ingress helpers. */
export class FakeChat implements ChatPort, ChatIntegration {
	readonly kind = "fabrial.chat" as const;
	readonly threads = new Map<string, FakeThread>();
	readonly ephemeral: { thread: ThreadRef; identity: ExternalIdentity; text: string }[] = [];
	readonly routes: Record<string, (request: Request) => Promise<Response>> = {};
	nextMessageId = 1;
	nextThreadId = 1;
	private host?: FabrialHost;
	constructor(readonly now: () => number = Date.now) {}
	connect(host: FabrialHost): ReturnType<ChatIntegration["connect"]> {
		this.host = host;
		return { port: this, routes: this.routes };
	}
	async thread(ref: ThreadRef): Promise<FakeThread> {
		const key = `${ref.provider}:${ref.threadId}`;
		let thread = this.threads.get(key);
		if (!thread) {
			thread = new FakeThread(
				clone(ref),
				ref.threadId.split(":")[1] ?? ref.threadId,
				ref.threadId.startsWith("dm:"),
				this,
			);
			this.threads.set(key, thread);
		}
		return thread;
	}
	async resolve(surface: Surface): Promise<ThreadIO> {
		return surface.kind === "thread" ? this.thread(surface) : new FakeChannelThread(surface, this);
	}
	async openDM(identity: ExternalIdentity): Promise<FakeThread> {
		return this.thread({
			kind: "thread",
			provider: identity.provider,
			threadId: `dm:${identityKey(identity)}`,
		});
	}
	async permalink(thread: ThreadRef): Promise<string | null> {
		return `https://chat.test/${thread.provider}/${thread.threadId}`;
	}
	async postEphemeral(thread: ThreadRef, identity: ExternalIdentity, text: string): Promise<void> {
		this.ephemeral.push(clone({ thread, identity, text }));
	}
	async receive(
		message: ChatMessage,
		options: { events?: readonly string[]; event?: string; dedupeId?: string } = {},
	): Promise<"accepted" | "ignored"> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		const thread = await this.thread({
			kind: "thread",
			provider: message.provider,
			threadId: message.threadId,
		});
		if (!thread.messages.some((m) => m.messageId === message.messageId))
			thread.messages.push(clone(message));
		return this.host.receiveMessage(message, {
			events: options.events ?? [options.event ?? `${message.provider}.message`],
			dedupeId: options.dedupeId ?? message.messageId,
		});
	}
	async cancel(
		thread: ThreadRef,
		actor: ExternalIdentity,
		dedupeId: string = crypto.randomUUID(),
	): Promise<void> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		await this.host.receiveCancellation({ thread, actor, dedupeId });
	}
	async click(
		ref: MessageRef,
		actionId: string,
		actor: ExternalIdentity,
		options: { value?: string; dedupeId?: string } = {},
	): Promise<void> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		await this.host.receiveAction({
			actionId,
			value: options.value,
			actor,
			thread: { kind: "thread", provider: ref.provider, threadId: ref.threadId },
			messageId: ref.messageId,
			dedupeId: options.dedupeId ?? crypto.randomUUID(),
		});
	}
}
