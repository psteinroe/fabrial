import {
	Chat,
	type ActionEvent,
	type Adapter,
	type StateAdapter,
	type Thread,
	type Message,
	type Logger,
} from "chat";
import type { ChatIntegration, ExternalIdentity, FabrialHost } from "fabrial";
import { chatIdentity, normalizeMessage, type ChatPluginCapability } from "./events.ts";
import { createChatPort, type SDKState } from "./port.ts";

export * from "./events.ts";
export { renderMessage } from "./port.ts";

export interface ChatOptions {
	state: StateAdapter;
	userName?: string;
	/** Supply the deployment's background-task hook. Without it routes await ingress tasks. */
	waitUntil?: (task: Promise<unknown>) => void;
}

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

export function chat(options: ChatOptions): ChatIntegration {
	return {
		kind: "fabrial.chat",
		connect(host: FabrialHost) {
			const adapters: Record<string, Adapter> = {};
			const capabilities = new Map<string, ChatPluginCapability>();
			for (const plugin of host.plugins) {
				if (!plugin.chat) continue;
				const config = plugin.chat as ChatPluginCapability;
				if (!config.installationId)
					throw new Error(`Chat plugin "${plugin.id}" must declare an installationId`);
				if (capabilities.has(plugin.id)) throw new Error(`Duplicate chat plugin "${plugin.id}"`);
				const adapter = config.adapter();
				if (adapter.name !== plugin.id)
					throw new Error(
						`Chat adapter "${adapter.name}" must encode ids with its plugin id "${plugin.id}"`,
					);
				adapters[plugin.id] = adapter;
				capabilities.set(plugin.id, config);
			}
			const logger: Logger = {
				debug: (message, ...args) => host.logger.debug(message, { args }),
				info: (message, ...args) => host.logger.info(message, { args }),
				warn: (message, ...args) => host.logger.warn(message, { args }),
				error: (message, ...args) => host.logger.error(message, { args }),
				child: () => logger,
			};
			const bot = new Chat<Record<string, Adapter>, SDKState>({
				adapters,
				state: options.state,
				userName: options.userName ?? "fabrial",
				// Queue/debounce strategies coalesce messages, which would lose buffered replies.
				concurrency: { strategy: "concurrent", maxConcurrent: 1 },
				logger,
			});
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
				const routed = await host.receiveMessage(normalized, { events, dedupeId });
				if (routed === "new") await thread.subscribe();
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
				await host.receiveAction({
					actionId: action.actionId,
					value: action.value,
					actor: chatIdentity(
						provider,
						capabilities.get(provider)!.installationId,
						action.user.userId,
					),
					thread: { kind: "thread", provider, threadId: action.threadId },
					messageId: action.messageId,
					dedupeId: actionDedupeId(provider, action),
				});
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
			const port = createChatPort(bot, capabilities, host);
			const routes: Record<string, (request: Request) => Promise<Response>> = {};
			for (const provider of capabilities.keys()) {
				routes[`POST /${provider}/events`] = async (request) => {
					const tasks: Promise<unknown>[] = [];
					const response = await bot.webhooks[provider]!(request, {
						waitUntil:
							options.waitUntil ??
							((task) => {
								tasks.push(task);
							}),
						// Core dedupes after durable ingress; SDK dedupes before dispatch and can lose retries.
						deduplicate: false,
						propagateHandlerErrors: true,
					});
					if (!options.waitUntil) await Promise.all(tasks);
					return response;
				};
			}
			return {
				port,
				routes,
				start: () => bot.initialize(),
				async stop() {
					await port.stop();
					await bot.shutdown();
				},
			};
		},
	};
}
