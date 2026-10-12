import type { Json, JsonObject } from "./json.ts";
import type { InvocationMetadata } from "./runtime.ts";
import type { ChatMessage, Surface, Thread } from "./thread.ts";

/** Which Session (and with the agent name, which conversation) an input goes to. */
export type SessionOption = "thread" | "channel" | (string & {});
export type InputKind = "message" | "notification";
export type InputPriority = "urgent" | "normal" | "low";
/** How Pi admits an input: join the running work, start the next run, or append without waking. */
export type Delivery = "steer" | "followUp" | "write";

export interface SendOptions {
	/** `"thread"` (default), `"channel"`, or an app-chosen key such as `ticket:${id}`. */
	session?: SessionOption;
	/** A ChatMessage (rendered with attribution), a string, or any JSON value. */
	input: Json;
	/** Default `"message"`. */
	kind?: InputKind;
	/** Notifications only; default `"normal"`. */
	priority?: InputPriority;
	/** Where the agent's `reply` goes. Defaults to this execution's thread; `null` for none. */
	replyTo?: Thread | Surface | null;
}

export interface SendReceipt extends JsonObject {
	requestId: string;
	session: string;
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

/** Render an input for the model, with attribution and the request id `reply` can target. */
export function renderInput(options: {
	input: Json;
	kind: InputKind;
	priority?: InputPriority;
	requestId: string;
	/** Fallback author for non-message input, e.g. the workflow's actor. */
	from?: string | null;
	/** Fallback thread for non-message input. */
	thread?: string | null;
	source?: string | null;
	event?: string | null;
}): string {
	const { input, kind, requestId } = options;
	if (kind === "message") {
		const message = isChatMessage(input) ? input : undefined;
		return `<message${attributes({
			from: message ? (message.author.name ?? message.author.identity.subjectId) : options.from,
			thread: message?.threadId ?? options.thread,
			request: requestId,
		})}>${escapeXml(text(input))}</message>`;
	}
	return `<notification${attributes({
		source: options.source,
		kind: options.event,
		priority: options.priority ?? "normal",
		request: requestId,
	})}>${escapeXml(text(input))}</notification>`;
}
