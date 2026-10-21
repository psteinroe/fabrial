// `FakeChat`: the chat `fabrial/testing` apps use by default. It is the production chat integration on the
// real Chat SDK and memory state, with a recording adapter standing in for each provider's platform.
import { createMemoryState } from "@chat-adapter/state-memory";
import {
	Message,
	parseMarkdown,
	stringifyMarkdown,
	type Adapter,
	type AdapterPostableMessage,
	type Author,
	type ModalElement,
	type ModalResponse,
	type StateAdapter,
	type WebhookOptions,
} from "chat";
import {
	ingressOptions,
	literalCard,
	routingState,
	type ChatConnection,
	type ChatOptions,
	type ChatPluginCapability,
} from "./chat/index.ts";
import type { ExternalIdentity } from "./identity.ts";
import type { AnyPlugin } from "./plugin.ts";
import type { ThreadRoutingState } from "./routing.ts";
import type { ChatMessage, MessageRef, OutboundMessage, ThreadRef } from "./thread.ts";

const clone = <T>(value: T): T => structuredClone(value);

/** What the platform shows for a message Fabrial sent. */
function recorded(message: AdapterPostableMessage): OutboundMessage {
	const card = literalCard(message);
	if (card) return { card: clone(card) };
	if (typeof message === "string") return message;
	if ("markdown" in message) return { markdown: message.markdown };
	throw new Error("FakeChat records only messages Fabrial renders");
}

function messageText(content: OutboundMessage): string {
	return typeof content === "string"
		? content
		: "markdown" in content
			? content.markdown
			: [content.card.title, content.card.text].filter(Boolean).join("\n");
}

const BOT: Author = {
	userId: "bot",
	userName: "fabrial",
	fullName: "Fabrial",
	isBot: true,
	isMe: true,
};

export interface FakePost {
	ref: MessageRef;
	content: OutboundMessage;
	updates: OutboundMessage[];
}

/** A modal Fabrial opened, until it is submitted. */
export interface FakeModal {
	viewId: string;
	modal: ModalElement;
	contextId?: string;
}

/**
 * What a thread shows: Fabrial's posts and the statuses it set. `post` and `update` are the platform
 * receiving them; wrap them to intercept a delivery.
 */
export class FakeThread {
	readonly posts: FakePost[] = [];
	/** The statuses shown, in order (`null` clears); setting the shown status again, as a chat thread does, adds none. */
	readonly statuses: (string | null)[] = [];
	/** The thread's messages, as the adapter returns them for history. */
	readonly history: Message[] = [];
	constructor(
		readonly ref: ThreadRef,
		private readonly owner: FakeChat,
	) {}
	/** The channel, without the provider. */
	get channelId(): string {
		return this.ref.threadId.split(":")[1] ?? "";
	}
	async post(content: OutboundMessage): Promise<MessageRef> {
		const ref = { ...this.message(), messageId: `message-${this.owner.nextMessageId++}` };
		this.posts.push({ ref, content: clone(content), updates: [] });
		const text = messageText(content);
		this.history.push(
			new Message({
				id: ref.messageId,
				threadId: ref.threadId,
				text,
				formatted: parseMarkdown(text),
				raw: null,
				author: BOT,
				metadata: { dateSent: new Date(), edited: false },
				attachments: [],
			}),
		);
		return ref;
	}
	async update(ref: MessageRef, content: OutboundMessage): Promise<void> {
		const post = this.posts.find((p) => p.ref.messageId === ref.messageId);
		if (!post) throw new Error(`Unknown message ${ref.messageId}`);
		post.content = clone(content);
		post.updates.push(clone(content));
		const sent = this.history.find((m) => m.id === ref.messageId);
		if (sent) {
			sent.text = messageText(content);
			sent.formatted = parseMarkdown(sent.text);
		}
	}
	/** Whether the SDK subscribed the thread (its messages then arrive as subscribed messages). */
	async isSubscribed(): Promise<boolean> {
		return this.owner.connection().bot.getState().isSubscribed(this.ref.threadId);
	}
	/** Fabrial's routing state of the thread, read through the connected app. */
	async getState(): Promise<ThreadRoutingState | null> {
		return routingState(await this.owner.connection().thread(this.ref));
	}
	setStatus(text: string | null) {
		if (this.statuses.at(-1) !== text) this.statuses.push(text);
	}
	private message() {
		return { provider: this.ref.provider, threadId: this.ref.threadId };
	}
}

/** The platform behind one provider: thread ids are `provider:channel:root`, DMs `provider:dm-user`. */
class RecordingAdapter implements Adapter {
	readonly userName = BOT.userName;
	readonly botUserId = BOT.userId;
	constructor(
		readonly name: string,
		private readonly chat: FakeChat,
	) {}
	async initialize() {}
	encodeThreadId(data: { channel: string; root?: string }) {
		return [this.name, data.channel, data.root].filter(Boolean).join(":");
	}
	decodeThreadId(threadId: string) {
		const [, channel = "", root] = threadId.split(":");
		return { channel, root };
	}
	channelIdFromThreadId(threadId: string) {
		return threadId.split(":").slice(0, 2).join(":");
	}
	isDM(threadId: string) {
		return this.decodeThreadId(threadId).channel.startsWith("dm-");
	}
	async fetchThread(threadId: string) {
		return {
			id: threadId,
			channelId: this.channelIdFromThreadId(threadId),
			isDM: this.isDM(threadId),
			metadata: {},
		};
	}
	async openDM(userId: string) {
		const identity = {
			provider: this.name,
			installationId: this.chat.installationOf(this.name),
			subjectId: userId,
		};
		return (await this.chat.openDM(identity)).ref.threadId;
	}
	async postMessage(threadId: string, message: AdapterPostableMessage) {
		const ref = await this.chat.record(threadId).post(recorded(message));
		return { id: ref.messageId, threadId, raw: null };
	}
	/** A channel post starts a thread rooted at it. */
	async postChannelMessage(channelId: string, message: AdapterPostableMessage) {
		return this.postMessage(`${channelId}:message-${this.chat.nextMessageId}`, message);
	}
	async editMessage(threadId: string, messageId: string, message: AdapterPostableMessage) {
		await this.chat
			.record(threadId)
			.update({ provider: this.name, threadId, messageId }, recorded(message));
		return { id: messageId, threadId, raw: null };
	}
	async deleteMessage(threadId: string, messageId: string) {
		const thread = this.chat.record(threadId);
		const post = thread.posts.findIndex((p) => p.ref.messageId === messageId);
		if (post >= 0) thread.posts.splice(post, 1);
		const sent = thread.history.findIndex((m) => m.id === messageId);
		if (sent >= 0) thread.history.splice(sent, 1);
	}
	async fetchMessages(threadId: string, options: { limit?: number } = {}) {
		return { messages: this.chat.record(threadId).history.slice(-(options.limit ?? 50)) };
	}
	async startTyping(threadId: string, status?: string) {
		this.chat.record(threadId).setStatus(status || null);
	}
	async endTyping(threadId: string) {
		this.chat.record(threadId).setStatus(null);
	}
	async postEphemeral(threadId: string, userId: string, message: AdapterPostableMessage) {
		await this.chat.postEphemeral(
			{ kind: "thread", provider: this.name, threadId },
			{
				provider: this.name,
				installationId: this.chat.installationOf(this.name),
				subjectId: userId,
			},
			messageText(recorded(message)),
		);
		return {
			id: `ephemeral-${this.chat.nextMessageId++}`,
			threadId,
			usedFallback: false,
			raw: null,
		};
	}
	async openModal(_triggerId: string, modal: ModalElement, contextId?: string) {
		const viewId = `view-${this.chat.nextMessageId++}`;
		this.chat.modals.push({ viewId, modal: clone(modal), contextId });
		return { viewId };
	}
	parseMessage(raw: unknown): Message {
		return raw as Message;
	}
	renderFormatted(content: Parameters<Adapter["renderFormatted"]>[0]) {
		return stringifyMarkdown(content);
	}
	async addReaction() {}
	async removeReaction() {}
	async handleWebhook() {
		return new Response("FakeChat has no webhooks", { status: 404 });
	}
}

/**
 * Fake chat: the real chat integration and Chat SDK, with inspectable posts, statuses, DMs, ephemeral
 * notices and modals, and inbound messages, clicks, modal submits and stops dispatched by the SDK. Every
 * plugin is a provider; plugins without a chat capability belong to installation `installationId`.
 * Its state is in memory and outlives app restarts, as a database would.
 */
export class FakeChat implements ChatOptions {
	readonly threads = new Map<string, FakeThread>();
	readonly ephemeral: { thread: ThreadRef; identity: ExternalIdentity; text: string }[] = [];
	readonly modals: FakeModal[] = [];
	/** Routes served besides the providers' webhooks. */
	readonly routes: Record<string, (request: Request) => Promise<Response>> = {};
	readonly installation: string;
	nextMessageId = 1;
	private readonly installations = new Map<string, string>();
	readonly state: StateAdapter;
	private connection_?: ChatConnection;
	private readonly results = new Map<string, "accepted" | "ignored">();
	constructor(options: { installationId?: string } = {}) {
		this.installation = options.installationId ?? "workspace";
		const memory = createMemoryState();
		// The app disconnects its state on stop; a restarted app finds it again.
		this.state = new Proxy(memory, {
			get: (target, key) =>
				key === "disconnect"
					? async () => {}
					: typeof target[key as keyof StateAdapter] === "function"
						? (target[key as keyof StateAdapter] as (...args: unknown[]) => unknown).bind(target)
						: target[key as keyof StateAdapter],
		});
	}
	/** Every plugin is a provider, on a recording adapter. */
	capability(plugin: AnyPlugin): ChatPluginCapability {
		const adapter = new RecordingAdapter(plugin.id, this);
		const chat: ChatPluginCapability = {
			installationId: this.installation,
			permalink: async (thread) => `https://chat.test/${thread.provider}/${thread.threadId}`,
			...(plugin.chat as ChatPluginCapability | undefined),
			adapter: () => adapter,
		};
		this.installations.set(plugin.id, chat.installationId);
		return chat;
	}
	received(message: ChatMessage, result: "accepted" | "ignored") {
		this.results.set(`${message.threadId} ${message.messageId}`, result);
	}
	connected(connection: ChatConnection) {
		this.connection_ = connection;
	}
	/** The connected app's chat. */
	connection(): ChatConnection {
		if (!this.connection_) throw new Error("FakeChat must be connected to an app");
		return this.connection_;
	}
	/** What `threadId` shows. */
	record(threadId: string): FakeThread {
		let thread = this.threads.get(threadId);
		if (!thread) {
			const provider = threadId.split(":")[0]!;
			thread = new FakeThread({ kind: "thread", provider, threadId }, this);
			this.threads.set(threadId, thread);
		}
		return thread;
	}
	async thread(ref: ThreadRef): Promise<FakeThread> {
		if (!ref.threadId.startsWith(`${ref.provider}:`))
			throw new Error(`Thread ids of ${ref.provider} start with "${ref.provider}:"`);
		return this.record(ref.threadId);
	}
	/** The platform showing a notice only `identity` sees; wrap it to intercept one. */
	async postEphemeral(thread: ThreadRef, identity: ExternalIdentity, text: string): Promise<void> {
		this.ephemeral.push(clone({ thread, identity, text }));
	}
	/** The platform opening the DM with `identity` (`provider:dm-subject`); wrap it to intercept one. */
	async openDM(identity: ExternalIdentity): Promise<FakeThread> {
		if (identity.installationId !== this.installationOf(identity.provider))
			throw new Error(`${identity.subjectId} belongs to another ${identity.provider} installation`);
		return this.record(`${identity.provider}:dm-${identity.subjectId}`);
	}
	/**
	 * A message from someone, dispatched by the SDK: ingress events are `provider.message` (`.dm` in a
	 * DM) and, when it mentions the bot, `provider.mentioned`. Resolves with whether core accepted it.
	 */
	async receive(message: ChatMessage): Promise<"accepted" | "ignored"> {
		const { bot, adapter } = this.provider(message.author.identity, message.provider);
		const channelId = adapter
			.channelIdFromThreadId(message.threadId)
			.slice(adapter.name.length + 1);
		if (channelId !== message.channelId || adapter.isDM(message.threadId) !== message.isDM)
			throw new Error(
				`Message ${message.messageId} (channel ${message.channelId}, isDM ${message.isDM}) does not fit thread ${message.threadId} (channel ${channelId}, isDM ${adapter.isDM(message.threadId)})`,
			);
		const sdk = new Message({
			id: message.messageId,
			threadId: message.threadId,
			text: message.text,
			formatted: parseMarkdown(message.text),
			raw: clone(message),
			author: this.author(message.author.identity, message.author),
			metadata: { dateSent: new Date(message.sentAt), edited: false },
			attachments: [],
			isMention: message.isMention,
		});
		const history = this.record(message.threadId).history;
		if (!history.some((m) => m.id === message.messageId)) history.push(sdk);
		await this.dispatch((options) => bot.processMessage(adapter, message.threadId, sdk, options));
		return this.results.get(`${message.threadId} ${message.messageId}`) ?? "ignored";
	}
	/** A stop of the thread's agent session by `actor`; one `dedupeId` is one stop. */
	async cancel(
		thread: ThreadRef,
		actor: ExternalIdentity,
		dedupeId: string = crypto.randomUUID(),
	): Promise<void> {
		const { bot, adapter } = this.provider(actor, thread.provider);
		await this.dispatch((options) =>
			bot.processAgentSessionStopped(
				{
					adapter,
					channelId: adapter.channelIdFromThreadId(thread.threadId),
					threadId: thread.threadId,
					threadTs: "",
					userId: actor.subjectId,
					streamingMessageTs: [dedupeId],
				},
				options,
			),
		);
	}
	/**
	 * A click on a card action. `dedupeId` is the platform's receipt; with a `triggerId`, as platforms
	 * with modals send, an action may open a modal (see `modals` and `submit`).
	 */
	async click(
		ref: MessageRef,
		actionId: string,
		actor: ExternalIdentity,
		options: { value?: string; dedupeId?: string; triggerId?: string } = {},
	): Promise<void> {
		const { bot, adapter } = this.provider(actor, ref.provider);
		await this.dispatch((webhook) =>
			bot.processAction(
				{
					adapter,
					actionId,
					value: options.value,
					threadId: ref.threadId,
					messageId: ref.messageId,
					triggerId: options.triggerId,
					user: this.author(actor),
					raw: { id: options.dedupeId ?? crypto.randomUUID() },
				},
				webhook,
			),
		);
	}
	/** Submit an open modal as `actor`; resolves with the app's response (`undefined` closes it). */
	async submit(
		viewId: string,
		values: Record<string, string>,
		actor: ExternalIdentity,
	): Promise<ModalResponse | undefined> {
		const index = this.modals.findIndex((m) => m.viewId === viewId);
		if (index < 0) throw new Error(`No open modal ${viewId}`);
		const { modal, contextId } = this.modals[index]!;
		const { bot, adapter } = this.provider(actor);
		const response = await bot.processModalSubmit(
			{
				adapter,
				callbackId: modal.callbackId,
				privateMetadata: modal.privateMetadata,
				viewId,
				values,
				user: this.author(actor),
				raw: null,
			},
			contextId,
		);
		if (!response || response.action !== "errors") this.modals.splice(index, 1);
		return response;
	}
	private provider(identity: ExternalIdentity, provider = identity.provider) {
		const { bot } = this.connection();
		if (identity.provider !== provider)
			throw new Error(`${identity.subjectId} is not a ${provider} user`);
		const adapter = bot.getAdapter(provider) as RecordingAdapter | undefined;
		if (!adapter) throw new Error(`No chat provider "${provider}"`);
		const installation = this.installationOf(provider);
		if (identity.installationId !== installation)
			throw new Error(
				`${identity.subjectId} belongs to installation ${identity.installationId}, not ${provider}'s ${installation}`,
			);
		return { bot, adapter };
	}
	/** The installation of `provider`'s chat capability, or `installationId` for plugins without one. */
	installationOf(provider: string): string {
		return this.installations.get(provider) ?? this.installation;
	}
	private author(identity: ExternalIdentity, author?: ChatMessage["author"]): Author {
		return {
			userId: identity.subjectId,
			userName: author?.name ?? identity.subjectId,
			fullName: author?.name ?? "",
			isBot: author?.isBot ?? false,
			isMe: false,
		};
	}
	private async dispatch(run: (options: WebhookOptions) => unknown): Promise<void> {
		const tasks: Promise<unknown>[] = [];
		await run(ingressOptions(tasks));
		await Promise.all(tasks);
	}
}
