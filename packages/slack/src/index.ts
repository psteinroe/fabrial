import { cardToBlockKit, SlackAdapter, type SlackBlock } from "@chat-adapter/slack";
import { WebClient } from "@slack/web-api";
import {
	defineGroup,
	type Card,
	type ExternalIdentity,
	type GroupResolveContext,
	type Surface,
	type ToolDefinition,
} from "fabrial";
import {
	chatCapability,
	chatEvents,
	chatIdentity,
	chatTrigger,
	definePlugin,
	literalCard,
} from "fabrial/plugin";
import { z } from "zod";

export interface SlackOptions {
	botToken: string;
	signingSecret: string;
	workspace: string;
	/** Native Slack team ID. If omitted, resolved from the bot token via auth.test at start. */
	teamId?: string;
}

/** Fixed-installation HTTP adapter; signature verification remains in the SDK. */
class ScopedSlackAdapter extends SlackAdapter {
	private teamId: string | undefined;
	constructor(options: SlackOptions) {
		super({ botToken: options.botToken, signingSecret: options.signingSecret });
		this.teamId = options.teamId;
	}
	override async initialize(...args: Parameters<SlackAdapter["initialize"]>) {
		if (!this.teamId) {
			const auth = await this.client.auth.test();
			if (!auth.team_id) throw new Error("Slack auth.test returned no team ID");
			this.teamId = auth.team_id;
		}
		await super.initialize(...args);
	}

	// SHIM: @chat-adapter/slack marks receipts BEFORE dispatch, even with deduplicate:false.
	// Disable that independent pre-ingress cache (including markers from old deployments).
	// Core alone dedupes after durable ingress; failed ingress must remain retryable.
	protected override markEventDelivered(): void {}
	protected override isDuplicateEventDelivery(): Promise<boolean> {
		return Promise.resolve(false);
	}

	// Fabrial cards render literally (see literalBlocks); everything else goes through the SDK.
	override async postMessage(...args: Parameters<SlackAdapter["postMessage"]>) {
		const [threadId, message] = args;
		const card = literalCard(message);
		if (!card) return super.postMessage(...args);
		const { channel, threadTs } = this.decodeThreadId(threadId);
		try {
			const result = await this._client.chat.postMessage(
				await this.withToken({
					channel,
					thread_ts: threadTs || undefined,
					text: escape(card.title),
					blocks: literalBlocks(card, sdkCard(message)),
					unfurl_links: false,
					unfurl_media: false,
				}),
			);
			return { id: result.ts!, threadId, raw: result };
		} catch (error) {
			return this.handleSlackError(error);
		}
	}

	override async editMessage(...args: Parameters<SlackAdapter["editMessage"]>) {
		const [threadId, messageId, message] = args;
		const card = literalCard(message);
		if (!card || messageId.startsWith("ephemeral:")) return super.editMessage(...args);
		const { channel } = this.decodeThreadId(threadId);
		try {
			const result = await this._client.chat.update(
				await this.withToken({
					channel,
					ts: messageId,
					text: escape(card.title),
					blocks: literalBlocks(card, sdkCard(message)),
				}),
			);
			return { id: result.ts!, threadId, raw: result };
		} catch (error) {
			return this.handleSlackError(error);
		}
	}

	override async handleWebhook(...args: Parameters<SlackAdapter["handleWebhook"]>) {
		const [request] = args;
		const body = await request.clone().text();
		let payload: { type?: string; team_id?: string; team?: { id?: string } };
		try {
			if (request.headers.get("content-type")?.includes("application/x-www-form-urlencoded")) {
				const form = new URLSearchParams(body);
				payload = form.has("payload")
					? (JSON.parse(form.get("payload")!) as typeof payload)
					: { team_id: form.get("team_id") ?? undefined };
			} else {
				payload = JSON.parse(body) as typeof payload;
			}
			if (!payload || typeof payload !== "object") throw new Error("Invalid payload");
		} catch {
			return new Response("Invalid payload", { status: 400 });
		}
		// URL challenges contain no team and do not dispatch. SDK still checks their signature.
		if (payload.type !== "url_verification") {
			const teams = [payload.team_id, payload.team?.id].filter((id) => id !== undefined);
			if (!this.teamId || !teams.length || teams.some((id) => id !== this.teamId))
				return new Response("Wrong Slack workspace", { status: 403 });
		}
		return super.handleWebhook(...args);
	}
}

/** Shown as typed: no emoji aliases, markdown, mentions or entities. */
const plain = (text: string) => ({ type: "plain_text", text, emoji: false });
/** Notification text is mrkdwn: escape it so a title cannot mention anyone. */
const escape = (text: string) =>
	text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function fieldBlocks(fields: { label: string; value: string }[]): SlackBlock[] {
	const blocks: SlackBlock[] = [];
	// Slack allows ten fields per section.
	for (let i = 0; i < fields.length; i += 10)
		blocks.push({
			type: "section",
			fields: fields.slice(i, i + 10).map((field) => plain(`${field.label}\n${field.value}`)),
		});
	return blocks;
}

type SDKCard = Parameters<typeof cardToBlockKit>[0];

/**
 * A Fabrial card as literal Block Kit. The SDK's card renderer runs every text through emoji and
 * markdown conversion into mrkdwn, so an approval could show something other than what was proposed.
 * Buttons are the SDK's own, from the card it rendered, so clicks route as before.
 */
export function literalBlocks(card: Card, rendered: SDKCard): SlackBlock[] {
	const blocks: SlackBlock[] = [{ type: "header", text: plain(card.title) }];
	if (card.text) blocks.push({ type: "section", text: plain(card.text) });
	if (card.fields?.length) blocks.push(...fieldBlocks(card.fields));
	if (card.code?.content)
		blocks.push({
			type: "rich_text",
			elements: [
				{ type: "rich_text_preformatted", elements: [{ type: "text", text: card.code.content }] },
			],
		});
	if (card.section) {
		blocks.push({ type: "divider" }, { type: "header", text: plain(card.section.title) });
		if (card.section.text) blocks.push({ type: "section", text: plain(card.section.text) });
		if (card.section.fields?.length) blocks.push(...fieldBlocks(card.section.fields));
	}
	if (card.footer) blocks.push({ type: "context", elements: [plain(card.footer)] });
	const actions = rendered.children.filter((child) => child.type === "actions");
	if (actions.length) blocks.push(...cardToBlockKit({ type: "card", children: actions }));
	return blocks;
}

function sdkCard(message: unknown): SDKCard {
	const card =
		(message as { type?: string; card?: SDKCard }).type === "card"
			? message
			: (message as { card?: SDKCard }).card;
	if (!card) throw new Error("Fabrial card without a rendered card");
	return card as SDKCard;
}

const workspaces = new WeakMap<WebClient, string>();

/** Post outside the conversation's home thread; final replies post there on their own. */
const postTool: ToolDefinition<{ channel: string; text: string }, { slack: WebClient }> = {
	name: "slack_post",
	description:
		"Post a message to a Slack channel (a channel id such as C0123, not #name). Your final answer already posts in this conversation's thread: use this only to write somewhere else.",
	input: z.object({
		channel: z.string().min(1).describe("Slack channel id, e.g. C0123"),
		text: z.string().min(1).describe("Slack mrkdwn"),
	}),
	async run({ channel, text }, ctx) {
		const posted = await ctx.clients.slack.chat.postMessage({ channel, text });
		if (!posted.ok || !posted.ts) throw new Error(`Slack did not post: ${posted.error ?? "no ts"}`);
		return `Posted to ${posted.channel ?? channel} (ts ${posted.ts}).`;
	},
};

const createPlugin = definePlugin<
	[SlackOptions],
	"slack",
	ReturnType<typeof chatEvents>,
	{ slack: WebClient }
>((options) => {
	let permalinks: WebClient | undefined;
	return {
		id: "slack",
		init() {
			if (!options.workspace) throw new Error("Slack requires workspace");
			if (!options.botToken) throw new Error("Slack requires botToken");
			if (!options.signingSecret) throw new Error("Slack requires signingSecret");
		},
		events: chatEvents(),
		chat: chatCapability({
			adapter: () => new ScopedSlackAdapter(options),
			installationId: options.workspace,
			history: { mode: "since-last-reply", limit: 50 },
			permalink: async (thread) => {
				// Thread ids are `slack:<channel>:<root ts>`.
				const [, channel, ts] = thread.threadId.split(":");
				if (!channel || !ts) return null;
				permalinks ??= new WebClient(options.botToken);
				const { permalink } = await permalinks.chat.getPermalink({ channel, message_ts: ts });
				return permalink ?? null;
			},
		}),
		toolkit: () => ({ tools: [postTool] }),
		clients: () => {
			const client = new WebClient(options.botToken);
			workspaces.set(client, options.workspace);
			return { slack: client };
		},
		identity: {
			async lookup(identity: ExternalIdentity, clients: { slack: WebClient }) {
				if (identity.provider !== "slack" || identity.installationId !== options.workspace)
					return undefined;
				const { user } = await clients.slack.users.info({ user: identity.subjectId });
				if (!user) return undefined;
				return {
					name:
						user.profile?.display_name || user.profile?.real_name || user.real_name || user.name,
				};
			},
		},
	};
});

export interface SlackFilter {
	channel?: string;
	thread?: string;
	observe?: boolean;
}

export function mentioned(filter: SlackFilter = {}) {
	return chatTrigger("slack", "mentioned", filter);
}

export function message(filter: SlackFilter & { channel: string }) {
	return chatTrigger("slack", "message", filter);
}

export function newThread(filter: SlackFilter & { channel: string }) {
	return chatTrigger("slack", "message", { ...filter, isNewThread: true });
}

export function dm(filter: Pick<SlackFilter, "thread" | "observe"> = {}) {
	return chatTrigger("slack", "dm", filter);
}

export function channel(id: string): Surface {
	return { kind: "channel", provider: "slack", channelId: id };
}

export function identity(options: { workspace: string; userId: string }): ExternalIdentity {
	return chatIdentity("slack", options.workspace, options.userId);
}

/** Resolve live membership; never cache approval authority in the plugin. */
export function userGroup(options: { id: string; handle: string }) {
	return defineGroup({
		id: options.id,
		async resolve(ctx: GroupResolveContext<{ slack: WebClient }>) {
			const client = ctx.clients.slack;
			if (!client) throw new Error("Slack user group resolution requires clients.slack");
			const { usergroups } = await client.usergroups.list({ include_disabled: false });
			const group = usergroups?.find(
				(candidate) => candidate.handle === options.handle.replace(/^@/, ""),
			);
			const workspace = workspaces.get(client) ?? group?.team_id;
			if (!group?.id || !workspace)
				throw new Error(`Slack user group "${options.handle}" not found or missing workspace`);
			const { users } = await client.usergroups.users.list({ usergroup: group.id });
			return (users ?? []).map((userId) => identity({ workspace, userId }));
		},
	});
}

/** Factory and static helpers work both as `slack.mentioned()` and named imports. */
export const slack = Object.assign(createPlugin, {
	mentioned,
	message,
	newThread,
	dm,
	channel,
	identity,
	userGroup,
});
