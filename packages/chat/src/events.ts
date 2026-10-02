/**
 * Chat plugins declare `events: chatEvents()` and `chat: chatCapability({ adapter,
 * installationId })`. Use chatTrigger(id, event, filters) for static helpers.
 * GitHub/Linear may add provider events/routes alongside these standard events;
 * their adapters must encode thread/channel ids with the plugin id as prefix.
 */
import type { Adapter, Message, Thread } from "chat";
import {
	defineEvent,
	trigger,
	type ChatCapability,
	type ChatMessage,
	type EventFilter,
	type ExternalIdentity,
} from "fabrial";

export interface ChatPluginCapability extends ChatCapability {
	adapter: () => Adapter;
	/** Override root detection for adapters whose thread ids do not end in the root message id. */
	isNewThread?: (thread: Thread, message: Message) => boolean;
	/** Use an edited status post when the platform has no meaningful typing/status API. */
	status?: "typing" | "message";
}

export function chatCapability(options: ChatPluginCapability): ChatPluginCapability {
	return options;
}

export type ChatEventPayload = ChatMessage & { isNewThread: boolean };
export type ChatEventName = "mentioned" | "message" | "dm";

/** Standard Schema without requiring a particular schema library in provider plugins. */
const messageSchema = {
	"~standard": {
		version: 1 as const,
		vendor: "fabrial.chat",
		validate(value: unknown) {
			if (typeof value !== "object" || value === null) {
				return { issues: [{ message: "Expected a chat message" }] };
			}
			const message = value as Record<string, unknown>;
			const strings = ["provider", "threadId", "channelId", "messageId", "text", "sentAt"];
			const author = message.author as ChatMessage["author"] | undefined;
			if (
				strings.some((key) => typeof message[key] !== "string") ||
				["isMention", "isDM", "isNewThread"].some((key) => typeof message[key] !== "boolean") ||
				!author ||
				typeof author.isBot !== "boolean" ||
				(author.name !== null && typeof author.name !== "string") ||
				!author.identity ||
				["provider", "installationId", "subjectId"].some(
					(key) => typeof author.identity[key] !== "string",
				)
			) {
				return { issues: [{ message: "Invalid normalized chat message" }] };
			}
			return { value: value as ChatEventPayload };
		},
	},
};

export function chatEvents() {
	const event = defineEvent<ChatEventPayload>({
		payload: messageSchema,
		filterable: ["channelId", "isDM", "isMention", "threadId", "isNewThread"],
	});
	// Keep filter keys literal: core's generic EventDefinition constraint otherwise inverts keyof variance.
	const standard = {
		...event,
		filterable: ["channelId", "isDM", "isMention", "threadId", "isNewThread"] as const,
	};
	return { mentioned: standard, message: standard, dm: standard };
}

export function chatTrigger(
	provider: string,
	event: ChatEventName,
	filters: { channel?: string; thread?: string; isNewThread?: boolean; observe?: boolean } = {},
) {
	const filter: EventFilter<ChatEventPayload> = {};
	if (filters.channel !== undefined) filter.channelId = [filters.channel];
	if (filters.thread !== undefined) filter.threadId = [filters.thread];
	if (filters.isNewThread !== undefined) filter.isNewThread = [filters.isNewThread];
	return trigger<ChatEventPayload>({
		event: `${provider}.${event}`,
		filter,
		observe: filters.observe,
		specificity: filters.thread ? 2 : filters.channel ? 1 : 0,
	});
}

export function chatIdentity(
	provider: string,
	installationId: string,
	subjectId: string,
): ExternalIdentity {
	return { provider, installationId, subjectId };
}

export function normalizeMessage(
	provider: string,
	capability: ChatPluginCapability,
	thread: Thread,
	message: Message,
): ChatEventPayload {
	const channelPrefix = `${provider}:`;
	return {
		provider,
		threadId: thread.id,
		channelId: thread.channelId.startsWith(channelPrefix)
			? thread.channelId.slice(channelPrefix.length)
			: thread.channelId,
		messageId: message.id,
		text: message.text,
		author: {
			identity: { ...chatIdentity(provider, capability.installationId, message.author.userId) },
			name: message.author.fullName || message.author.userName || null,
			isBot: message.author.isBot === true,
		},
		isMention: message.isMention === true,
		isDM: thread.isDM,
		isNewThread: capability.isNewThread?.(thread, message) ?? thread.id.endsWith(`:${message.id}`),
		sentAt: message.metadata.dateSent.toISOString(),
	};
}
