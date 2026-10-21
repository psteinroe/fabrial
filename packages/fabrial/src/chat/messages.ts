// Serializable surfaces and messages, and posting, editing and history on Chat SDK threads.
import {
	Actions,
	type AdapterPostableMessage,
	Button,
	CardText,
	type ChatElement,
	Field,
	Fields,
	Card as SDKCard,
	toCardElement,
} from "chat";
import { channelOf, ChatConnection, providerOf, type SDKThread } from "./connection.ts";
import { normalizeMessage } from "./ingress.ts";
import type { JsonObject } from "../schema.ts";
import type { ExternalIdentity } from "../identity.ts";

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

/** Keyed globally, so a plugin with its own copy of Fabrial still finds it. */
const LITERAL_CARD = Symbol.for("fabrial.card");

/**
 * The Fabrial card a rendered message came from. Adapters that would transform card text (emoji,
 * markdown, mentions) render this instead, so what it shows stays literal.
 */
export function literalCard(message: unknown): Card | undefined {
	if (!message || typeof message !== "object") return undefined;
	const card = (message as { [LITERAL_CARD]?: Card })[LITERAL_CARD];
	if (card) return card;
	const inner = (message as { card?: unknown }).card;
	return inner && typeof inner === "object" ? literalCard(inner) : undefined;
}

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
	if (card.section) {
		children.push(CardText(card.section.title, { style: "bold" }));
		if (card.section.text) children.push(CardText(card.section.text));
		if (card.section.fields?.length)
			children.push(Fields(card.section.fields.map((field) => Field(field))));
	}
	if (card.footer) children.push(CardText(card.footer));
	if (card.actions?.length) children.push(Actions(card.actions.map((action) => Button(action))));
	const rendered = toCardElement(SDKCard({ title: card.title, children }));
	if (!rendered) throw new Error("Invalid card");
	// Enumerable, so the SDK's copies keep it; a symbol, so it never serializes.
	return Object.assign(rendered, { [LITERAL_CARD]: card });
}

/** Serializable reference to a chat thread. `threadId` is Chat SDK's full id (`adapter:channel:thread`). */
export interface ThreadRef {
	readonly kind: "thread";
	/** Plugin id that owns the chat adapter, e.g. "slack". */
	readonly provider: string;
	readonly threadId: string;
}

/** A place to reply into: an existing thread, or a channel where a new thread is created lazily on first post. */
export type Surface =
	| ThreadRef
	| { readonly kind: "channel"; readonly provider: string; readonly channelId: string };

export interface MessageRef {
	readonly provider: string;
	readonly threadId: string;
	readonly messageId: string;
}

/** A normalized inbound chat message. */
export interface ChatMessage extends JsonObject {
	provider: string;
	threadId: string;
	channelId: string;
	messageId: string;
	text: string;
	author: { identity: ExternalIdentity & JsonObject; name: string | null; isBot: boolean };
	isMention: boolean;
	isDM: boolean;
	sentAt: string;
}

/** Provider-neutral card, rendered by the Chat SDK integration to Block Kit / Markdown / … */
export interface Card {
	title: string;
	text?: string;
	fields?: { label: string; value: string }[];
	/** Pre-formatted block (e.g. SQL). */
	code?: { language?: string; content: string };
	/** A titled section after the code block, kept apart from `fields` (e.g. an approval's app content). */
	section?: { title: string; text?: string; fields?: { label: string; value: string }[] };
	/** Text after the other content, before the actions. */
	footer?: string;
	actions?: CardAction[];
}

export interface CardAction {
	/** Routed back to Fabrial; keep it opaque. */
	id: string;
	label: string;
	style?: "primary" | "danger" | "default";
	value?: string;
}

export type OutboundMessage = string | { markdown: string } | { card: Card };

/**
 * A live thread handle in workflow code. Every intentional operation is durable and takes an explicit id
 * (first argument), memoized in the current execution.
 */
export interface Thread {
	readonly ref: ThreadRef;
	readonly channelId: string;
	readonly isDM: boolean;

	post(id: string, message: OutboundMessage): Promise<MessageRef>;
	update(id: string, message: MessageRef, content: OutboundMessage): Promise<void>;

	/** Messages for model context (bounded, attributed). Durable: loaded once per id. */
	history(id: string, options?: { limit?: number }): Promise<ChatMessage[]>;
}
