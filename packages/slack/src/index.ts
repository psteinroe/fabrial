import { SlackAdapter } from "@chat-adapter/slack";
import { chatCapability, chatEvents, chatIdentity, chatTrigger } from "@fabrial/chat";
import { WebClient } from "@slack/web-api";
import { defineGroup, definePlugin, type ExternalIdentity, type Surface } from "fabrial";

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

const workspaces = new WeakMap<WebClient, string>();

const createPlugin = definePlugin<
	[SlackOptions],
	"slack",
	ReturnType<typeof chatEvents>,
	{ slack: WebClient }
>((options) => ({
	id: "slack",
	events: chatEvents(),
	chat: chatCapability({
		adapter: () => new ScopedSlackAdapter(options),
		installationId: options.workspace,
		history: { mode: "since-last-reply", limit: 50 },
	}),
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
				name: user.profile?.display_name || user.profile?.real_name || user.real_name || user.name,
			};
		},
	},
}));

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
		async resolve(ctx) {
			const client = ctx.clients.slack as WebClient | undefined;
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
