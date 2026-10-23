// Chat ingress: messages, stops and clicks, normalized and stored durably before the webhook is acknowledged.
import type { ActionEvent, Adapter, Message, Thread, WebhookOptions } from "chat";
import type { Bot, ChatOptions } from "./connection.ts";
import type { FabrialHost } from "../app.ts";
import type { ChatPluginCapability } from "../plugin.ts";
import { chatIdentity, type ExternalIdentity } from "../identity.ts";
import { REJECT_MODAL, type RejectContext, rejectModal } from "../approvals/authorization.ts";
import type { ChatEventPayload } from "../routing/events.ts";

/** Platform action receipts are not exposed uniformly by Chat SDK; prefer native receipt ids. */
export function actionDedupeId(
	provider: string,
	action: Pick<
		ActionEvent,
		"raw" | "triggerId" | "threadId" | "messageId" | "actionId" | "user" | "value"
	>,
): string {
	const raw = action.raw as {
		event_id?: string;
		id?: string;
		actions?: { action_ts?: string }[];
	} | null;
	const receipt = raw?.event_id ?? raw?.id ?? action.triggerId ?? raw?.actions?.[0]?.action_ts;
	return JSON.stringify([
		provider,
		action.threadId,
		action.messageId,
		action.actionId,
		action.user.userId,
		receipt ?? action.value ?? "",
	]);
}

/**
 * How chat ingress is processed: done once its handlers stored it (acknowledging earlier risks losing the
 * delivery), with handler errors propagated. Core dedupes by the ingress emit's key; the SDK dedupes
 * before dispatch and can lose retries.
 */
export function ingressOptions(tasks: Promise<unknown>[]): WebhookOptions {
	return {
		waitUntil: (task) => {
			tasks.push(task);
		},
		deduplicate: false,
		propagateHandlerErrors: true,
	};
}

/** Dispatches the bot's messages, stops and clicks into core; returns the webhook routes. */
export function listen(
	bot: Bot,
	host: FabrialHost,
	options: ChatOptions,
	adapters: Record<string, Adapter>,
	capabilities: Map<string, ChatPluginCapability>,
): Record<string, (request: Request) => Promise<Response>> {
	const providerFor = (adapter: Adapter) => {
		const provider = Object.keys(adapters).find((id) => adapters[id] === adapter);
		if (!provider) throw new Error("Unknown chat adapter");
		return provider;
	};
	const receive = async (thread: Thread, message: Message) => {
		if (message.author.isMe) return;
		const provider = providerFor(thread.adapter);
		const normalized = normalizeMessage(provider, capabilities.get(provider)!, thread, message);
		const events = [`${provider}.${normalized.isDM ? "dm" : "message"}`];
		if (normalized.isMention) events.push(`${provider}.mentioned`);
		const dedupeId = JSON.stringify([provider, normalized.channelId, message.id]);
		// The routing workflow subscribes the thread when something handles the message.
		const result = await host.receiveMessage(normalized, { events, dedupeId });
		options.received?.(normalized, result);
		if (thread.signal.aborted) {
			// A bare SDK abort carries no stopper identity. Never attribute it to the message
			// author: Slack aborts the turn BEFORE delivering its actor-bearing stop event.
			// Applications can supply an authenticated actor in the abort reason.
			const reason: unknown = thread.signal.reason;
			if (reason && typeof reason === "object" && "actor" in reason && "dedupeId" in reason) {
				const actor = reason.actor as Partial<ExternalIdentity> | null;
				if (
					actor?.provider === provider &&
					actor.installationId === capabilities.get(provider)!.installationId &&
					typeof actor.subjectId === "string" &&
					typeof reason.dedupeId === "string"
				)
					await host.receiveCancellation({
						actor: actor as ExternalIdentity,
						thread: { kind: "thread", provider, threadId: thread.id },
						dedupeId: reason.dedupeId,
					});
			}
		}
	};
	bot.onNewMention(receive);
	bot.onSubscribedMessage(receive);
	bot.onDirectMessage(receive);
	bot.onNewMessage(/[\s\S]*/, receive);
	bot.onAction(async (action) => {
		if (!action.thread || action.user.isMe) return;
		const provider = providerFor(action.adapter);
		// Reject asks for a reason first where the provider has modals; its submit is the decision,
		// authorized like any click. Without modals, Reject decides at once, without a reason.
		if (action.actionId.startsWith("fabrial.approval.reject:")) {
			const context: RejectContext = {
				actionId: action.actionId,
				threadId: action.threadId,
				messageId: action.messageId,
			};
			const opened = await action.openModal(rejectModal(context)).catch((error: unknown) => {
				host.logger.warn("Reject modal failed to open", { error });
				return undefined;
			});
			if (opened) return;
		}
		await host.receiveAction({
			actionId: action.actionId,
			value: action.value,
			actor: chatIdentity(provider, capabilities.get(provider)!.installationId, action.user.userId),
			thread: { kind: "thread", provider, threadId: action.threadId },
			messageId: action.messageId,
			dedupeId: actionDedupeId(provider, action),
		});
	});
	bot.onModalSubmit(REJECT_MODAL, async (event) => {
		const provider = providerFor(event.adapter);
		const context = JSON.parse(event.privateMetadata ?? "null") as RejectContext | null;
		if (!context?.actionId.startsWith("fabrial.approval.reject:")) return undefined;
		try {
			await host.receiveAction({
				actionId: context.actionId,
				value: event.values.reason || undefined,
				actor: chatIdentity(
					provider,
					capabilities.get(provider)!.installationId,
					event.user.userId,
				),
				thread: { kind: "thread", provider, threadId: context.threadId },
				messageId: context.messageId,
				dedupeId: JSON.stringify([provider, "modal", event.viewId, event.user.userId]),
			});
			return undefined;
		} catch (error) {
			// The SDK swallows handler errors: keep the modal open so the approver can retry.
			host.logger.warn("Reject submit failed", { error });
			return { action: "errors", errors: { reason: "Could not reject. Try again." } };
		}
	});
	bot.onAgentSessionStopped(async (event) => {
		const provider = providerFor(event.adapter);
		await host.receiveCancellation({
			actor: chatIdentity(provider, capabilities.get(provider)!.installationId, event.userId),
			thread: { kind: "thread", provider, threadId: event.threadId },
			dedupeId: JSON.stringify([
				provider,
				"agent-session-stopped",
				event.threadId,
				event.userId,
				event.streamingMessageTs,
			]),
		});
	});
	const routes = options.routes ?? {};
	for (const provider of capabilities.keys()) {
		routes[`POST /${provider}/events`] = async (request) => {
			const tasks: Promise<unknown>[] = [];
			const response = await bot.webhooks[provider]!(request, ingressOptions(tasks));
			await Promise.all(tasks);
			return response;
		};
	}
	return routes;
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
