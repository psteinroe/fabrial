import { createSlackAdapter } from "@chat-adapter/slack";
import { chatCapability, chatEvents, chatIdentity, chatTrigger } from "@fabrial/chat";
import { WebClient } from "@slack/web-api";
import { defineGroup, definePlugin, type ExternalIdentity, type Surface } from "fabrial";

export interface SlackOptions {
	botToken: string;
	signingSecret: string;
	workspace: string;
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
		adapter: () =>
			createSlackAdapter({ botToken: options.botToken, signingSecret: options.signingSecret }),
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
