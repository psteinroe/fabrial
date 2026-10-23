// Progress in chat: the throttled status of a thread, and the progress of an agent's conversation.
import { type Conversation, Harness, watchEvents } from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import type { FabrialHost } from "../app.ts";
import { ChatConnection, providerOf, type SDKThread } from "./connection.ts";
import { Invocation } from "../sessions/obligations.ts";
import { routingState, updateRoutingState } from "../routing/follows.ts";

/**
 * The status a batch of a conversation's Pi events shows: the newest started tool's description, or "Thinking…"; none
 * for only detail updates.
 */
export function activityOf(
	batch: readonly { type: string }[],
	tools: readonly { name: string; description?: string }[],
): string | undefined {
	if (batch.every((event) => event.type === "tool_execution_update")) return undefined;
	const tool = batch.findLast((event) => event.type === "tool_execution_start") as
		| { toolName: string }
		| undefined;
	if (!tool) return "Thinking…";
	return tools.find((each) => each.name === tool.toolName)?.description ?? "Using a tool…";
}

/**
 * Progress of a conversation: on the thread a call owns (`io`), or on the reply thread of the latest sent request it
 * follows. Shows "Thinking…", tool activity, and "Waiting for the model provider…" while a
 * generation backs off.
 */
export async function progressOf(
	host: FabrialHost,
	conversation: Conversation,
	io: SDKThread | undefined,
	harness: Harness,
	context: Context,
) {
	let progress = io;
	let progressRequest: string | undefined;
	let waiting = false;
	const shown = new Map<string, SDKThread>();
	const setStatus = async (thread: SDKThread | undefined, text: string | null) => {
		if (thread)
			await host
				.chat()
				?.setStatus(thread, text)
				.catch(() => {});
	};
	const showStatus = async (text: string) => {
		if (!progress) return;
		shown.set(progress.id, progress);
		await setStatus(progress, text);
	};
	const thinking = () => showStatus(waiting ? WAITING_STATUS : "Thinking…");
	const events = await watchEvents(harness, conversation.id, context);
	waiting = !!events.snapshot.generation?.retry;
	events.start(async (batch) => {
		const retry = batch.findLast(
			(event) => event.type === "auto_retry_start" || event.type === "auto_retry_end",
		);
		if (retry) {
			waiting = retry.type === "auto_retry_start";
			await thinking();
			return;
		}
		// The chat thread throttles statuses.
		if (waiting || !progress) return;
		const text = activityOf(batch, (await conversation.agent(context)).tools);
		if (text) await showStatus(text);
	});
	await thinking();
	return {
		io,
		async follow(requestId: string) {
			const replyTo = (await harness.snapshot(Invocation, requestId, context))?.replyTo;
			if (replyTo?.kind !== "thread" || !host.chat()) return;
			progress = await host.chat()!.thread(replyTo);
			progressRequest = requestId;
			await thinking();
		},
		async unfollow(requestId: string) {
			if (progressRequest !== requestId) return;
			await setStatus(progress, null);
			progress = undefined;
		},
		async stop() {
			await events.stop();
			for (const [id, surface] of shown) if (id !== io?.id) await setStatus(surface, null);
			shown.clear();
		},
	};
}

export type Progress = Awaited<ReturnType<typeof progressOf>>;

/** Status of a sent run whose generation is backing off before a durable retry. */
export const WAITING_STATUS = "Waiting for the model provider…";

/**
 * The status of one thread: at most one change per second, and a repeat of the shown status at most
 * every 5 s. A change inside the window shows when it ends (the latest one wins); clearing is
 * immediate. Providers without a native status get one edited status message.
 */
export class StatusThrottle {
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
