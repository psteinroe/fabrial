import type { ExternalIdentity, Principal } from "./identity.ts";
import type { Json, JsonObject } from "./json.ts";

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

/** Provider-neutral card, rendered by `@fabrial/chat` to Block Kit / Markdown / … */
export interface Card {
	title: string;
	text?: string;
	fields?: { label: string; value: string }[];
	/** Pre-formatted block (e.g. SQL). */
	code?: { language?: string; content: string };
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

	/** Durable wait for the next reply in this interaction's thread; buffered replies are consumed first. */
	waitForReply(
		id: string,
		options?: { from?: Principal; timeout?: string | number },
	): Promise<ChatMessage | null>;
	/** `post` + `waitForReply` as one durable operation. */
	ask(
		id: string,
		message: OutboundMessage,
		options?: { from?: Principal; timeout?: string | number },
	): Promise<ChatMessage | null>;
	/** Reply awaitable for `ctx.race`. */
	nextReply(options?: { from?: Principal }): Awaitable<ChatMessage>;

	/** Messages for model context (bounded, attributed). Durable: loaded once per id. */
	history(id: string, options?: { limit?: number }): Promise<ChatMessage[]>;
}

/** Something `ctx.race` can wait on. Produced by Fabrial helpers; opaque to apps. */
export interface Awaitable<T extends Json = Json> {
	readonly kind: "fabrial.awaitable";
	/** @internal */
	readonly __type?: T;
}
