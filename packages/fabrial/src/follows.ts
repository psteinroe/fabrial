/**
 * Following other things: `ctx.follow` writes a record in a resource thread's Chat SDK state (a pull request's
 * thread). When a plugin emits an event that belongs to that thread, ingress reads the records and sends each
 * following conversation a notification. The records are the only routing index; Fabrial owns no tables.
 */
import type { FabrialHost } from "./app.ts";
import type { JsonObject } from "./json.ts";
import type { AnyPlugin, ResourceKind, ResourceRef } from "./plugin.ts";
import type {
	AgentPort,
	AgentRef,
	InvocationMetadata,
	ResourceFollow,
	ThreadRoutingState,
} from "./runtime.ts";
import {
	deliveryFor,
	renderInput,
	replySurface,
	resolveSession,
	type ConversationReference,
	type InputPriority,
	type SentRequest,
	type SessionOption,
} from "./send.ts";
import type { Surface, Thread, ThreadRef } from "./thread.ts";

export interface FollowOptions {
	/** Priority per notification kind, overriding the plugin's default: `{ review: "urgent" }`. */
	priority?: Partial<Record<string, InputPriority>>;
	/** `"all"` also receives the kinds the plugin leaves out by default (GitHub: labels and CI). */
	events?: "default" | "all";
	/**
	 * The conversation that follows. Default: the calling agent's (from a tool, or a workflow an agent tool
	 * started). Required from other workflows. `home` is where final replies to its notifications go
	 * (default: this execution's thread).
	 */
	to?:
		| { agent: AgentRef; session?: SessionOption; home?: Thread | Surface }
		| (ConversationReference & { home?: Thread | Surface });
}

/** `FollowOptions` with references instead of definitions, for durable operations. */
export type StoredFollowOptions = JsonObject & {
	priority?: Partial<Record<string, InputPriority>>;
	events?: "default" | "all";
	to?: {
		/** Set when `session` is a resolved key, from a `ConversationReference`. */
		kind?: "fabrial.conversation";
		agent: string;
		session?: string;
		home?: (Surface & JsonObject) | null;
	};
};

export function storedFollowOptions(options: FollowOptions = {}): StoredFollowOptions {
	const { to, ...rest } = options;
	return JSON.parse(
		JSON.stringify({
			...rest,
			to: to && {
				...("kind" in to ? { kind: to.kind } : {}),
				agent: typeof to.agent === "string" ? to.agent : to.agent.name,
				session: to.session,
				home: to.home && ("post" in to.home ? to.home.ref : to.home),
			},
		}),
	) as StoredFollowOptions;
}

/** The conversation an agent run belongs to; set on the metadata its tools and their workflows see. */
export interface ConversationRef extends JsonObject {
	session: string;
	agent: string;
}

export interface FollowReceipt extends JsonObject {
	/** Resource key, e.g. `github:acme/app#42`. */
	resource: string;
	session: string;
	agent: string;
}

export interface UnfollowReceipt extends FollowReceipt {
	removed: boolean;
}

type ResourceRefLike = ResourceRef<any> | { provider: string; type: string; ref: JsonObject };

/** The resource kind behind a reference, its prefixed key, and its thread. */
export function resolveResource(
	plugins: readonly AnyPlugin[],
	ref: ResourceRefLike,
): { kind: ResourceKind<any>; key: string; thread: ThreadRef } {
	const plugin = plugins.find((candidate) => candidate.id === ref.provider);
	if (!plugin) throw new Error(`No plugin "${ref.provider}" for resource ${ref.type}`);
	const kind = (plugin.resources as Record<string, ResourceKind<any>> | undefined)?.[ref.type];
	if (!kind) throw new Error(`Plugin "${ref.provider}" has no resource kind "${ref.type}"`);
	return { kind, key: `${plugin.id}:${kind.key(ref.ref)}`, thread: kind.thread(ref.ref) };
}

type Surfaces = { current: InvocationMetadata["replyTo"]; channelId: () => Promise<string> };

async function target(
	metadata: InvocationMetadata,
	surface: Surfaces,
	to: StoredFollowOptions["to"],
): Promise<ConversationRef> {
	// A reference from `ctx.send` names its conversation already; resolving it again would re-prefix its key.
	if (to?.kind === "fabrial.conversation" && to.session)
		return { agent: to.agent, session: to.session };
	if (to)
		return { agent: to.agent, session: await resolveSession(to.session ?? "thread", surface) };
	const conversation = metadata.conversation as ConversationRef | undefined;
	if (!conversation)
		throw new Error(
			"Pass `to: { agent, session }`: this execution belongs to no agent conversation",
		);
	return { session: conversation.session, agent: conversation.agent };
}

const same = (a: ConversationRef, b: ConversationRef) =>
	a.session === b.session && a.agent === b.agent;

/** Add or replace a conversation's follow record of a resource thread. */
export function addResourceFollow(
	state: ThreadRoutingState | null,
	follow: ResourceFollow,
): ThreadRoutingState {
	return {
		...state,
		resourceFollows: [
			...(state?.resourceFollows ?? []).filter((each) => !same(each, follow)),
			follow,
		],
	};
}

function threadOf(host: FabrialHost, thread: ThreadRef) {
	const chat = host.chat();
	if (!chat) throw new Error("Following a resource needs the chat integration (its thread state)");
	return chat.thread(thread);
}

/** Write a follow record on the resource's thread. Shared by workflow and tool `ctx.follow`. */
export async function followResource(
	host: FabrialHost,
	metadata: InvocationMetadata,
	surface: Surfaces,
	ref: ResourceRefLike,
	options: StoredFollowOptions,
): Promise<FollowReceipt> {
	const { key, thread } = resolveResource(host.plugins, ref);
	const conversation = await target(metadata, surface, options.to);
	const priority = Object.fromEntries(
		Object.entries(options.priority ?? {}).filter(([, value]) => value !== undefined),
	) as Record<string, InputPriority>;
	const record: ResourceFollow = {
		...conversation,
		resource: key,
		// Notifications reply to the conversation's home: the request's reply thread, or `to.home`.
		replyTo: replySurface(options.to?.home ?? undefined, surface.current),
		...(Object.keys(priority).length ? { priority } : {}),
		...(options.events === "all" ? { events: "all" as const } : {}),
	};
	await (await threadOf(host, thread)).updateState((state) => addResourceFollow(state, record));
	return { resource: key, ...conversation };
}

export async function unfollowResource(
	host: FabrialHost,
	metadata: InvocationMetadata,
	surface: Surfaces,
	ref: ResourceRefLike,
	options: Pick<StoredFollowOptions, "to">,
): Promise<UnfollowReceipt> {
	const { key, thread } = resolveResource(host.plugins, ref);
	const conversation = await target(metadata, surface, options.to);
	let removed = false;
	await (
		await threadOf(host, thread)
	).updateState((state) => {
		removed = !!state?.resourceFollows?.some((each) => same(each, conversation));
		return removed
			? {
					...state,
					resourceFollows: state!.resourceFollows!.filter((each) => !same(each, conversation)),
				}
			: state;
	});
	return { removed, resource: key, ...conversation };
}

/**
 * Deliver a plugin event to the conversations following its resource threads, as notifications: the
 * plugin renders it (and drops untrusted authors), the follow's `events` selects kinds, its `priority`
 * overrides the plugin's default. Request ids derive from the event and the conversation, so a redelivery
 * is dropped. Returns the number of deliveries.
 */
export async function notifyFollowers(
	host: FabrialHost,
	port: AgentPort | undefined,
	event: string,
	payload: JsonObject,
	metadata: InvocationMetadata,
): Promise<number> {
	const dot = event.indexOf(".");
	const plugin = host.plugins.find((candidate) => candidate.id === event.slice(0, dot));
	const chat = host.chat();
	if (!plugin?.resources || !chat) return 0;
	const name = event.slice(dot + 1);
	const delivered = new Set<string>();
	for (const kind of Object.values(plugin.resources as Record<string, ResourceKind<any>>)) {
		if (!kind.events.includes(name)) continue;
		const notification = kind.notify({ name, payload });
		if (!notification) continue;
		const threads = new Map(kind.threads({ name, payload }).map((t) => [t.threadId, t]));
		for (const thread of threads.values()) {
			const state = await (await chat.thread(thread)).getState();
			for (const follow of state?.resourceFollows ?? []) {
				if (follow.events !== "all" && !kind.defaults.includes(notification.kind)) continue;
				const requestId = `${metadata.requestId}:${follow.session}:${follow.agent}`;
				if (delivered.has(requestId)) continue;
				delivered.add(requestId);
				if (!port?.deliver) throw new Error("Following resources needs the agent integration");
				const priority = follow.priority?.[notification.kind] ?? notification.priority ?? "normal";
				const agent: AgentRef = { kind: "fabrial.agent", name: follow.agent };
				await port.deliver(agent, {
					session: follow.session,
					requestId,
					content: renderInput({
						input: notification.summary,
						kind: "notification",
						priority,
						source: plugin.id,
						event: notification.kind,
						attributes: notification.attributes,
					}),
					delivery: deliveryFor("notification", priority),
					follow: null,
					metadata: {
						requestId,
						origin: metadata.origin,
						replyTo: follow.replyTo,
						requestedBy: metadata.requestedBy,
						ownerWorkflow: null,
						triggerEvent: null,
						send: { kind: "notification", priority } satisfies SentRequest,
					},
				});
			}
		}
	}
	return delivered.size;
}
