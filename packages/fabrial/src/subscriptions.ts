/**
 * Dynamic subscriptions: `ctx.subscribe` stores a record in `fabrial.subscriptions`; an internal observer
 * workflow on every resource event finds the records for the event's resources and sends each matching
 * conversation a notification. Agent triggers replace static subscriptions.
 */
import type { Sql } from "postgres";
import type { FabrialHost, Logger } from "./app.ts";
import type { Json, JsonObject } from "./json.ts";
import type {
	AnyPlugin,
	NotificationAttributes,
	ResourceEvent,
	ResourceKind,
	ResourceNotification,
	ResourceRef,
} from "./plugin.ts";
import type { AgentPort, AgentRef, InvocationMetadata, RuntimeWorkflow } from "./runtime.ts";
import {
	deliveryFor,
	renderInput,
	replySurface,
	resolveSession,
	type InputPriority,
	type SentRequest,
	type SessionOption,
} from "./send.ts";
import type { Surface, Thread } from "./thread.ts";

/**
 * App functions a subscription applies to its events. Functions can't be stored, so a subscription stores
 * the policy's name and delivery looks it up among the instance's policies. A deploy that removes a policy
 * delivers with the plugin's defaults and logs a warning.
 */
export interface SubscriptionPolicyDefinition {
	/** Stable: stored with every subscription that uses it. */
	name: string;
	/** Which events count, after the plugin's own filter. */
	filter?: (event: ResourceEvent) => boolean;
	/** Priority of an event; `undefined` falls back to the subscription's `priority` and the plugin's. */
	priority?: (event: ResourceEvent) => InputPriority | undefined;
}

export interface SubscriptionPolicy extends SubscriptionPolicyDefinition {
	readonly kind: "fabrial.subscription-policy";
}

/** Registry of policies per instance, like agents. */
export const definedPolicies = new WeakMap<object, Map<string, SubscriptionPolicy>>();

export function defineSubscriptionPolicy(
	definition: SubscriptionPolicyDefinition,
	scope: object,
): SubscriptionPolicy {
	if (!definition.name) throw new Error("Subscription policy name must not be empty");
	const policy: SubscriptionPolicy = { kind: "fabrial.subscription-policy", ...definition };
	let policies = definedPolicies.get(scope);
	if (!policies) definedPolicies.set(scope, (policies = new Map()));
	if (policies.has(definition.name))
		throw new Error(`Duplicate subscription policy name: ${definition.name}`);
	policies.set(definition.name, policy);
	return policy;
}

/** A priority for every event, or one per notification kind (`{ review: "urgent" }`). */
export type PriorityOption = InputPriority | Partial<Record<string, InputPriority>>;

export interface SubscribeOptions {
	/** A preset of the resource kind (GitHub: `"review"`, `"working"`). Default: the kind's first. */
	preset?: string;
	/** Declarative priority; a policy's `priority` function wins, the plugin's default comes last. */
	priority?: PriorityOption;
	/** App filter and priority functions, from `f.defineSubscriptionPolicy`. */
	policy?: SubscriptionPolicy;
	/** An end condition of the resource kind (GitHub: `"merged-or-closed"`). */
	until?: string;
	/**
	 * The conversation to deliver to. Default: the calling agent's conversation (from a tool, or a
	 * workflow an agent tool started). Required from other workflows.
	 */
	to?: { agent: AgentRef; session?: SessionOption };
	/** Where the agent's final reply to a notification goes. Default: the current request's reply surface. */
	replyTo?: Thread | Surface | null;
}

/** `SubscribeOptions` with references instead of definitions, for durable operations. */
export type StoredSubscribeOptions = JsonObject & {
	preset?: string;
	priority?: Json;
	policy?: string;
	until?: string;
	to?: { agent: string; session?: string };
	replyTo?: (Surface & JsonObject) | null;
};

export function storedOptions(options: SubscribeOptions = {}): StoredSubscribeOptions {
	const { policy, to, replyTo, ...rest } = options;
	return JSON.parse(
		JSON.stringify({
			...rest,
			policy: policy?.name,
			to: to && { agent: to.agent.name, session: to.session },
			replyTo: replyTo && "post" in replyTo ? replyTo.ref : replyTo,
		}),
	) as StoredSubscribeOptions;
}

/** The conversation an agent run belongs to; set on the metadata its tools and their workflows see. */
export interface ConversationRef extends JsonObject {
	session: string;
	agent: string;
}

export interface SubscriptionReceipt extends JsonObject {
	subscriptionId: string;
	/** Resource key, e.g. `github:acme/app#123`. */
	resource: string;
	session: string;
	agent: string;
}

export interface UnsubscribeReceipt extends JsonObject {
	removed: boolean;
	resource: string;
	session: string;
	agent: string;
}

/** A row of `fabrial.subscriptions`. */
export interface SubscriptionRecord extends JsonObject {
	id: string;
	resource: string;
	provider: string;
	type: string;
	session: string;
	agent: string;
	replyTo: (Surface & JsonObject) | null;
	preset: string;
	until: string | null;
	priority: Json;
	policy: string | null;
}

/** Storage of dynamic subscriptions. Postgres in production, in memory for `fabrial/testing`. */
export interface SubscriptionStore {
	start?(): Promise<void>;
	/** Insert, or update the record of the same resource and conversation (keeping its id). */
	upsert(record: SubscriptionRecord): Promise<SubscriptionRecord>;
	remove(target: { resource: string; session: string; agent: string }): Promise<boolean>;
	delete(id: string): Promise<void>;
	find(resources: readonly string[]): Promise<SubscriptionRecord[]>;
}

export class MemorySubscriptions implements SubscriptionStore {
	readonly records = new Map<string, SubscriptionRecord>();
	private key = (r: { resource: string; session: string; agent: string }) =>
		JSON.stringify([r.resource, r.session, r.agent]);
	async upsert(record: SubscriptionRecord): Promise<SubscriptionRecord> {
		const existing = this.records.get(this.key(record));
		const saved = { ...structuredClone(record), id: existing?.id ?? record.id };
		this.records.set(this.key(record), saved);
		return structuredClone(saved);
	}
	async remove(target: { resource: string; session: string; agent: string }): Promise<boolean> {
		return this.records.delete(this.key(target));
	}
	async delete(id: string): Promise<void> {
		for (const [key, record] of this.records) if (record.id === id) this.records.delete(key);
	}
	async find(resources: readonly string[]): Promise<SubscriptionRecord[]> {
		return [...this.records.values()]
			.filter((record) => resources.includes(record.resource))
			.map((record) => structuredClone(record));
	}
}

interface SubscriptionRow {
	id: string;
	resource_key: string;
	provider: string;
	resource_type: string;
	session_key: string;
	agent: string;
	reply_to: (Surface & JsonObject) | null;
	preset: string;
	until: string | null;
	priority: Json;
	policy: string | null;
}
const fromRow = (row: SubscriptionRow): SubscriptionRecord => ({
	id: row.id,
	resource: row.resource_key,
	provider: row.provider,
	type: row.resource_type,
	session: row.session_key,
	agent: row.agent,
	replyTo: row.reply_to,
	preset: row.preset,
	until: row.until,
	priority: row.priority,
	policy: row.policy,
});

/** `fabrial.subscriptions`, looked up by resource key. Created idempotently at app.start(). */
export class PostgresSubscriptions implements SubscriptionStore {
	constructor(private readonly sql: Sql) {}

	static async migrate(sql: Sql): Promise<void> {
		await sql.begin(async (sql) => {
			await sql`SELECT pg_advisory_xact_lock(739410813)`;
			await sql`CREATE SCHEMA IF NOT EXISTS fabrial`;
			// The unique index leads with resource_key, so it also serves lookups by resource.
			await sql`CREATE TABLE IF NOT EXISTS fabrial.subscriptions (
				id text PRIMARY KEY, resource_key text NOT NULL, provider text NOT NULL,
				resource_type text NOT NULL, session_key text NOT NULL, agent text NOT NULL, reply_to jsonb,
				preset text NOT NULL, until text, priority jsonb, policy text,
				created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
				UNIQUE (resource_key, session_key, agent)
			)`;
		});
	}

	async start(): Promise<void> {
		await PostgresSubscriptions.migrate(this.sql);
	}

	async upsert(record: SubscriptionRecord): Promise<SubscriptionRecord> {
		const json = (value: Json) => (value === null ? null : this.sql.json(value as never));
		const [row] = await this.sql<SubscriptionRow[]>`INSERT INTO fabrial.subscriptions
			(id, resource_key, provider, resource_type, session_key, agent, reply_to, preset, until, priority, policy)
			VALUES (${record.id}, ${record.resource}, ${record.provider}, ${record.type}, ${record.session},
				${record.agent}, ${json(record.replyTo)}, ${record.preset}, ${record.until},
				${json(record.priority)}, ${record.policy})
			ON CONFLICT (resource_key, session_key, agent) DO UPDATE SET
				reply_to = EXCLUDED.reply_to, preset = EXCLUDED.preset, until = EXCLUDED.until,
				priority = EXCLUDED.priority, policy = EXCLUDED.policy, updated_at = now()
			RETURNING *`;
		return fromRow(row!);
	}

	async remove(target: { resource: string; session: string; agent: string }): Promise<boolean> {
		const rows = await this
			.sql`DELETE FROM fabrial.subscriptions WHERE resource_key = ${target.resource}
			AND session_key = ${target.session} AND agent = ${target.agent} RETURNING id`;
		return rows.length > 0;
	}

	async delete(id: string): Promise<void> {
		await this.sql`DELETE FROM fabrial.subscriptions WHERE id = ${id}`;
	}

	async find(resources: readonly string[]): Promise<SubscriptionRecord[]> {
		if (!resources.length) return [];
		const rows = await this.sql<SubscriptionRow[]>`SELECT * FROM fabrial.subscriptions
			WHERE resource_key IN ${this.sql(resources)} ORDER BY created_at, id`;
		return rows.map(fromRow);
	}
}

/** The resource kind behind a reference, and its prefixed key. */
export function resolveResource(
	plugins: readonly AnyPlugin[],
	ref: ResourceRef<any> | { provider: string; type: string; ref: Json },
): { kind: ResourceKind<any>; key: string } {
	const plugin = plugins.find((candidate) => candidate.id === ref.provider);
	if (!plugin) throw new Error(`No plugin "${ref.provider}" for resource ${ref.type}`);
	const kind = (plugin.resources as Record<string, ResourceKind<any>> | undefined)?.[ref.type];
	if (!kind) throw new Error(`Plugin "${ref.provider}" has no resource kind "${ref.type}"`);
	return { kind, key: `${plugin.id}:${kind.key(ref.ref as JsonObject)}` };
}

type Surfaces = { current: InvocationMetadata["replyTo"]; channelId: () => Promise<string> };

async function target(
	metadata: InvocationMetadata,
	surface: Surfaces,
	to: StoredSubscribeOptions["to"],
): Promise<{ session: string; agent: string }> {
	if (to)
		return { agent: to.agent, session: await resolveSession(to.session ?? "thread", surface) };
	const conversation = metadata.conversation as ConversationRef | undefined;
	if (!conversation)
		throw new Error(
			"Pass `to: { agent, session }`: this execution belongs to no agent conversation",
		);
	return { session: conversation.session, agent: conversation.agent };
}

/** Resolve a subscribe call to the record it stores. Shared by workflow and tool `ctx.subscribe`. */
export async function prepareSubscribe(
	host: FabrialHost,
	metadata: InvocationMetadata,
	surface: Surfaces,
	id: string,
	ref: ResourceRef<any> | { provider: string; type: string; ref: Json },
	options: StoredSubscribeOptions,
): Promise<SubscriptionRecord> {
	const { kind, key } = resolveResource(host.plugins, ref);
	const presets = Object.keys(kind.presets);
	const preset = options.preset ?? presets[0];
	if (!preset || !presets.includes(preset))
		throw new Error(`Unknown preset "${options.preset}" for ${ref.provider} ${ref.type}`);
	if (options.until !== undefined && !kind.until?.[options.until])
		throw new Error(`Unknown end condition "${options.until}" for ${ref.provider} ${ref.type}`);
	if (
		options.policy !== undefined &&
		!definedPolicies.get(host.definitionScope ?? host)?.has(options.policy)
	)
		throw new Error(`Unknown subscription policy "${options.policy}"`);
	return {
		id,
		resource: key,
		provider: ref.provider,
		type: ref.type,
		...(await target(metadata, surface, options.to)),
		replyTo: replySurface(options.replyTo, surface.current),
		preset,
		until: options.until ?? null,
		priority: options.priority ?? null,
		policy: options.policy ?? null,
	};
}

export async function prepareUnsubscribe(
	host: FabrialHost,
	metadata: InvocationMetadata,
	surface: Surfaces,
	ref: ResourceRef<any> | { provider: string; type: string; ref: Json },
	options: Pick<StoredSubscribeOptions, "to">,
): Promise<{ resource: string; session: string; agent: string }> {
	return {
		resource: resolveResource(host.plugins, ref).key,
		...(await target(metadata, surface, options.to)),
	};
}

/** A resource kind an event belongs to, with the event's resources and notification. */
export interface ResourceMatch {
	provider: string;
	type: string;
	kind: ResourceKind<any>;
	resources: string[];
	notification: ResourceNotification;
}

/** Resource kinds and keys a plugin event belongs to. `event` is the full name (`github.pullRequestReviewed`). */
export function matchResources(
	plugins: readonly AnyPlugin[],
	event: string,
	payload: JsonObject,
): ResourceMatch[] {
	const dot = event.indexOf(".");
	const plugin = plugins.find((candidate) => candidate.id === event.slice(0, dot));
	const name = event.slice(dot + 1);
	const matches: ResourceMatch[] = [];
	for (const [type, kind] of Object.entries(
		(plugin?.resources ?? {}) as Record<string, ResourceKind<any>>,
	)) {
		if (!kind.events.includes(name)) continue;
		const resources = [...new Set(kind.match({ name, payload }))].map(
			(key) => `${plugin!.id}:${key}`,
		);
		const notification = resources.length ? kind.notify({ name, payload }) : undefined;
		if (notification) matches.push({ provider: plugin!.id, type, kind, resources, notification });
	}
	return matches;
}

export interface Delivery {
	subscription: SubscriptionRecord;
	event: ResourceEvent;
	priority: InputPriority;
	/** The event meets the subscription's end condition: deliver it, then remove the record. */
	ends: boolean;
}

function declaredPriority(option: Json, kind: string): InputPriority | undefined {
	if (typeof option === "string") return option as InputPriority;
	if (option && typeof option === "object" && !Array.isArray(option))
		return (option as Partial<Record<string, InputPriority>>)[kind];
	return undefined;
}

/**
 * Which subscriptions receive an event, at which priority: the preset selects notification kinds, the
 * plugin's filter and then the policy's filter keep events out, and the priority is the policy's, the
 * subscription's declared one, the plugin's default, or `normal`, in that order.
 */
export function selectDeliveries(
	event: string,
	payload: JsonObject,
	matches: readonly ResourceMatch[],
	records: readonly SubscriptionRecord[],
	policies: ReadonlyMap<string, SubscriptionPolicy>,
	logger?: Logger,
): Delivery[] {
	const deliveries: Delivery[] = [];
	for (const subscription of records) {
		const match = matches.find(
			(m) =>
				m.provider === subscription.provider &&
				m.type === subscription.type &&
				m.resources.includes(subscription.resource),
		);
		if (!match) continue;
		const { kind, notification } = match;
		const kinds = kind.presets[subscription.preset];
		if (!kinds) {
			logger?.warn("Subscription preset no longer exists", {
				subscription: subscription.id,
				preset: subscription.preset,
			});
			continue;
		}
		if (!kinds.includes(notification.kind)) continue;
		const resourceEvent: ResourceEvent = {
			...notification,
			attributes: (notification.attributes ?? {}) as NotificationAttributes,
			event,
			resource: subscription.resource,
			payload,
		};
		if (kind.filter && !kind.filter(resourceEvent)) continue;
		const policy = subscription.policy ? policies.get(subscription.policy) : undefined;
		if (subscription.policy && !policy)
			logger?.warn("Subscription policy no longer exists; delivering with plugin defaults", {
				subscription: subscription.id,
				policy: subscription.policy,
			});
		if (policy?.filter && !policy.filter(resourceEvent)) continue;
		const until = subscription.until ? kind.until?.[subscription.until] : undefined;
		deliveries.push({
			subscription,
			event: resourceEvent,
			priority:
				policy?.priority?.(resourceEvent) ??
				declaredPriority(subscription.priority, notification.kind) ??
				notification.priority ??
				"normal",
			ends: !!until?.(resourceEvent),
		});
	}
	return deliveries;
}

export const DELIVER_WORKFLOW = "fabrial.subscriptions.deliver";

/**
 * Observer of every resource event: never owns a thread, so triggers on the same events still run.
 * Ingress dedup (provider delivery id) keeps a redelivered event from running it twice; the request id
 * `${event id}:${subscription id}` makes a replayed send a no-op.
 */
export function deliveryWorkflow(
	host: FabrialHost,
	agents: () => AgentPort | undefined,
): RuntimeWorkflow | undefined {
	const events = host.plugins.flatMap((plugin) =>
		Object.values((plugin.resources ?? {}) as Record<string, ResourceKind<any>>).flatMap((kind) =>
			kind.events.map((event) => `${plugin.id}.${event}`),
		),
	);
	if (!events.length) return undefined;
	return {
		name: DELIVER_WORKFLOW,
		triggers: [...new Set(events)].map((event) => ({ event, role: "observer" as const })),
		retries: { maxAttempts: 5 },
		async handler(payload, execution) {
			const event = execution.metadata.triggerEvent;
			const store = host.subscriptions;
			if (typeof event !== "string" || !store) return;
			const planned = await execution.step("fabrial:subscriptions:plan", async () => {
				const matches = matchResources(host.plugins, event, payload as JsonObject);
				const records = await store.find([...new Set(matches.flatMap((m) => m.resources))]);
				const policies = definedPolicies.get(host.definitionScope ?? host) ?? new Map();
				return selectDeliveries(
					event,
					payload as JsonObject,
					matches,
					records,
					policies,
					host.logger,
				).map(({ subscription, event: notification, priority, ends }) => {
					const requestId = `${execution.metadata.interactionId}:${subscription.id}`;
					return {
						subscription,
						ends,
						request: {
							session: subscription.session,
							requestId,
							content: renderInput({
								input: notification.summary,
								kind: "notification",
								priority,
								source: subscription.provider,
								event: notification.kind,
								attributes: { resource: notification.resource, ...notification.attributes },
							}),
							delivery: deliveryFor("notification", priority),
							follow: null,
							metadata: {
								interactionId: requestId,
								origin: execution.metadata.origin,
								replyTo: subscription.replyTo,
								requestedBy: execution.metadata.requestedBy,
								ownerWorkflow: null,
								triggerEvent: null,
								send: { kind: "notification", priority } satisfies SentRequest,
								subscription: { id: subscription.id, resource: subscription.resource },
							} as InvocationMetadata,
						},
					};
				});
			});
			const port = agents();
			if (planned.length && !port) throw new Error("Subscriptions need the agent integration");
			for (const { subscription, ends, request } of planned) {
				const agent: AgentRef = { kind: "fabrial.agent", name: subscription.agent };
				await port!.send(execution, `subscription:${subscription.id}`, agent, request);
				if (ends)
					await execution.step(`fabrial:subscriptions:end:${subscription.id}`, () =>
						store.delete(subscription.id),
					);
			}
		},
	};
}
