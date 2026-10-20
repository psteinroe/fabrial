import type { FabrialHost } from "./app.ts";
import type { Json, JsonObject } from "./json.ts";
import { addFollow } from "./routing.ts";
import type { AgentRef, InvocationMetadata } from "./runtime.ts";
import type { ChatMessage, Surface, Thread, ThreadRef } from "./thread.ts";

/** Which Session (and with the agent name, which conversation) an input goes to. */
export type SessionOption = "thread" | "channel" | (string & {});
export type InputKind = "message" | "notification";
export type InputPriority = "urgent" | "normal" | "low";
/** How Pi admits an input: join the running work, start the next run, or append without waking. */
export type Delivery = "steer" | "followUp" | "write";

/**
 * How an agent keys its conversations, computed from each event: one per thread (default), one per channel,
 * or the app's key (namespaced as `app:<key>`).
 */
export type AgentSession<TEvent = any> = "thread" | "channel" | ((event: TEvent) => string);

/** A conversation: the agent's transcript in one Session. */
export interface ConversationKey extends JsonObject {
	session: string;
	agent: string;
}

export interface SendOptions {
	/**
	 * A ChatMessage (rendered with attribution, delivered as a message) or any other JSON value (delivered as a
	 * notification), as if the agent's trigger had matched it.
	 */
	input: Json;
	/**
	 * Where the conversation lives when the input has no thread (e.g. `slack.channel("C_BUGS")`): the first final
	 * reply creates the thread, which the conversation follows. Default: this execution's thread.
	 */
	home?: Thread | Surface;
	/**
	 * Overrides the agent's `session` (`"thread"`, `"channel"`, or an app key such as `ops`). Needed for input
	 * without a thread: without it, each such send would start a conversation of its own.
	 */
	session?: SessionOption;
	/** Notifications only (default `"normal"`): `urgent` steers, `normal` follows up, `low` is written unread. */
	priority?: InputPriority;
}

/**
 * An opaque reference to a conversation, returned by `ctx.send`. `ctx.follow(…, { to })` takes it as is, so a sent
 * conversation can follow a resource.
 */
export interface ConversationReference extends JsonObject {
	kind: "fabrial.conversation";
	session: string;
	agent: string;
}

export const conversationReference = (agent: string, session: string): ConversationReference => ({
	kind: "fabrial.conversation",
	session,
	agent,
});

export interface SendReceipt extends JsonObject {
	requestId: string;
	conversation: ConversationReference;
}

/** Context of a request delivered by `ctx.send`, kept in its `fabrial.invocation` document. */
export interface SentRequest extends JsonObject {
	kind: InputKind;
	priority: InputPriority;
}

/** What core hands the agent integration: a rendered input for one conversation. */
export interface SendRequest extends JsonObject {
	session: string;
	requestId: string;
	content: string;
	delivery: Delivery;
	/** The request's own context: actor, replyTo, origin, kind. */
	metadata: InvocationMetadata;
	/** The thread the conversation follows after this delivery. */
	follow: (ThreadRef & JsonObject) | null;
}

export function deliveryFor(kind: InputKind, priority: InputPriority = "normal"): Delivery {
	if (kind === "message") return "steer";
	return priority === "urgent" ? "steer" : priority === "low" ? "write" : "followUp";
}

/** Session key of a thread. The default Session, as for `ctx.agent`. */
export const threadSession = (provider: string, threadId: string) => `${provider}:${threadId}`;
export const channelSession = (provider: string, channelId: string) =>
	`${provider}:channel:${channelId}`;
/** Namespaced so an app key never collides with a chat Session. */
export const customSession = (key: string) => `app:${key}`;

const XML: Record<string, string> = {
	"&": "&amp;",
	"<": "&lt;",
	">": "&gt;",
	'"': "&quot;",
	"'": "&apos;",
};
export const escapeXml = (value: string) => value.replaceAll(/[&<>"']/g, (c) => XML[c]!);

export function isChatMessage(value: unknown): value is ChatMessage {
	const message = value as Partial<ChatMessage> | null;
	return (
		typeof message === "object" &&
		message !== null &&
		typeof message.text === "string" &&
		typeof message.messageId === "string" &&
		typeof message.threadId === "string" &&
		typeof message.author?.identity === "object"
	);
}

const attributes = (values: Record<string, string | null | undefined>) =>
	Object.entries(values)
		.filter((entry): entry is [string, string] => typeof entry[1] === "string")
		.map(([key, value]) => ` ${key}="${escapeXml(value)}"`)
		.join("");
const text = (input: Json) =>
	isChatMessage(input) ? input.text : typeof input === "string" ? input : JSON.stringify(input);

/** Render an input for the model, with attribution. */
export function renderInput(options: {
	input: Json;
	kind: InputKind;
	priority?: InputPriority;
	/** Fallback author for non-message input, e.g. the workflow's actor. */
	from?: string | null;
	/** Fallback thread for non-message input. */
	thread?: string | null;
	source?: string | null;
	event?: string | null;
	/** Extra notification attributes, e.g. the resource and the event's details. */
	attributes?: Record<string, string | number | boolean | null>;
}): string {
	const { input, kind } = options;
	if (kind === "message") {
		const message = isChatMessage(input) ? input : undefined;
		return `<message${attributes({
			from: message ? (message.author.name ?? message.author.identity.subjectId) : options.from,
			thread: message?.threadId ?? options.thread,
		})}>${escapeXml(text(input))}</message>`;
	}
	return `<notification${attributes({
		source: options.source,
		kind: options.event,
		priority: options.priority ?? "normal",
		...Object.fromEntries(
			Object.entries(options.attributes ?? {})
				.filter(([key]) => !["source", "kind", "priority"].includes(key))
				.map(([key, value]) => [key, value === null ? null : String(value)]),
		),
	})}>${escapeXml(text(input))}</notification>`;
}

/** The Session key of a session option, relative to the caller's chat surface. */
export async function resolveSession(
	option: SessionOption,
	surface: { current: InvocationMetadata["replyTo"]; channelId: () => Promise<string> },
): Promise<string> {
	if (option !== "thread" && option !== "channel") return customSession(option);
	const current = surface.current;
	if (!current) throw new Error(`Session "${option}" needs an execution with a chat surface`);
	if (option === "channel")
		return channelSession(
			current.provider,
			current.kind === "channel" ? current.channelId : await surface.channelId(),
		);
	if (current.kind !== "thread")
		throw new Error(
			"Post to the channel with ctx.thread.post before sending; a channel is not a thread",
		);
	return threadSession(current.provider, current.threadId);
}

/** A reply surface option (`Thread`, `Surface`, or `null`) as stored metadata. */
export function replySurface(
	option: Thread | Surface | null | undefined,
	current: InvocationMetadata["replyTo"],
): InvocationMetadata["replyTo"] {
	if (option === undefined) return current;
	if (option === null) return null;
	return ("post" in option ? option.ref : option) as InvocationMetadata["replyTo"];
}

/** The thread of a chat message. */
export const threadOf = (message: ChatMessage): ThreadRef & JsonObject => ({
	kind: "thread",
	provider: message.provider,
	threadId: message.threadId,
});

/**
 * The Session key of an agent's conversation for an event, as its trigger would compute it: from the message's
 * thread or channel, else from the reply surface (the trigger's thread or the agent's home). `fallback` keys a
 * `"thread"` conversation whose event has no thread yet (its home is a channel).
 */
export function conversationSession(
	option: AgentSession | SessionOption | undefined,
	input: Json,
	replyTo: InvocationMetadata["replyTo"],
	fallback: string,
): string {
	if (typeof option === "function") return customSession(option(input));
	const kind = option ?? "thread";
	if (kind !== "thread" && kind !== "channel") return customSession(kind);
	const message = isChatMessage(input) ? input : undefined;
	if (kind === "channel") {
		if (message) return channelSession(message.provider, message.channelId);
		if (replyTo?.kind === "channel") return channelSession(replyTo.provider, replyTo.channelId);
		throw new Error('Session "channel" needs a chat message or a channel home');
	}
	if (message) return threadSession(message.provider, message.threadId);
	if (replyTo?.kind === "thread") return threadSession(replyTo.provider, replyTo.threadId);
	return fallback;
}

/**
 * The thread a delivery makes the conversation follow: its reply thread, whatever the session kind (follow records
 * are the routing index). A channel home is followed once the first final reply creates its thread
 * (`metadata.follow`).
 */
export function followedThread(
	replyTo: InvocationMetadata["replyTo"],
): (ThreadRef & JsonObject) | null {
	if (replyTo?.kind !== "thread") return null;
	return { kind: "thread", provider: replyTo.provider, threadId: replyTo.threadId };
}

/** Render an input for one conversation, with its actor, origin, and plugin context. */
export async function prepareInput(
	host: FabrialHost,
	metadata: InvocationMetadata,
	options: {
		agent: string;
		session: string;
		requestId: string;
		input: Json;
		kind?: InputKind;
		priority?: InputPriority;
		replyTo: InvocationMetadata["replyTo"];
	},
): Promise<SendRequest> {
	const kind = options.kind ?? "message";
	const priority = options.priority ?? "normal";
	const { session, replyTo } = options;
	const message = isChatMessage(options.input) ? options.input : undefined;
	const actor = message
		? await host.resolvePrincipal(message.author.identity)
		: metadata.requestedBy;
	const origin = message
		? { provider: message.provider, threadId: message.threadId, messageId: message.messageId }
		: metadata.origin;
	const context = origin
		? (
				await Promise.all(
					host.plugins.map(async (plugin) => plugin.context?.(origin, host.clients())),
				)
			).filter(Boolean)
		: [];
	const event = typeof metadata.triggerEvent === "string" ? metadata.triggerEvent : null;
	const rendered = renderInput({
		input: options.input,
		kind,
		priority,
		from: actor?.name ?? actor?.id,
		thread: replyTo?.kind === "thread" ? replyTo.threadId : null,
		source: metadata.origin?.provider ?? event?.split(".")[0],
		event,
	});
	const conversation: ConversationKey = { session, agent: options.agent };
	return {
		session,
		requestId: options.requestId,
		content: [...context, rendered].join("\n\n"),
		delivery: deliveryFor(kind, priority),
		follow: followedThread(replyTo),
		metadata: {
			...metadata,
			requestedBy: actor as InvocationMetadata["requestedBy"],
			replyTo,
			origin: origin as InvocationMetadata["origin"],
			ownerWorkflow: null,
			triggerEvent: null,
			send: { kind, priority } satisfies SentRequest,
			// A channel home: the thread the first final reply creates is followed.
			...(replyTo?.kind === "channel" ? { follow: conversation } : {}),
		},
	};
}

/**
 * Resolve the Session, actor, and reply surface of a send, and render its input. Shared by workflow and
 * tool `ctx.send`; `surface.current` is the sender's chat surface. The conversation is found as if the
 * agent's trigger had matched the input: by its thread, or `home`; otherwise relative to the sender's surface.
 */
export async function prepareSend(
	host: FabrialHost,
	metadata: InvocationMetadata,
	surface: { current: InvocationMetadata["replyTo"]; channelId: () => Promise<string> },
	requestId: string,
	agent: AgentRef & { definition?: { session?: AgentSession } },
	opts: SendOptions,
): Promise<SendRequest> {
	const message = isChatMessage(opts.input) ? opts.input : undefined;
	const home = opts.home ? replySurface(opts.home, null) : undefined;
	const replyTo = message ? threadOf(message) : (home ?? surface.current);
	const option = opts.session ?? agent.definition?.session ?? "thread";
	const session =
		message || home || typeof option === "function" || (option !== "thread" && option !== "channel")
			? conversationSession(option, opts.input, replyTo, `send:${requestId}`)
			: await resolveSession(option, surface);
	return prepareInput(host, metadata, {
		agent: agent.name,
		session,
		requestId,
		input: opts.input,
		kind: message ? "message" : "notification",
		priority: opts.priority,
		replyTo,
	});
}

/** Write a follow record on a thread (when its session key doesn't already say so). */
export async function follow(
	host: FabrialHost,
	thread: ThreadRef | null | undefined,
	conversation: ConversationKey,
): Promise<void> {
	const chat = host.chat();
	if (!thread || !chat) return;
	await (await chat.thread(thread)).updateState((state) => addFollow(state, conversation));
}
