// Ingress routing: every input is one `fabrial.ingress` event, which the `fabrial.route` workflow routes to followers,
// the most specific owner, and observers.
import type { TaskContext } from "pgconductor-js";
import type { CoreConfig, FabrialHost } from "../app.ts";
import type { Json, JsonObject } from "../schema.ts";
import type { Agents } from "../sessions/host.ts";
import type { AnyWorkflow } from "../workflows/definition.ts";
import { type Agent, type AgentRef, definedAgents } from "../agents/definition.ts";
import { filtersOverlap, matchesFilter, type Origin, type TriggerSpec } from "./events.ts";
import {
	addFollow,
	follow,
	type Follow,
	notifyFollowers,
	routingState,
	updateRoutingState,
} from "./follows.ts";
import { childMetadata, type InvocationMetadata, metadataOf } from "../workflows/metadata.ts";
import {
	conversationSession,
	isChatMessage,
	prepareInput,
	threadOf,
	threadSession,
} from "../sessions/delivery.ts";
import { nativeMethods } from "../execution.ts";
import { decideAction } from "../approvals/authorization.ts";
import type { ChatMessage, Surface, ThreadRef } from "../chat/messages.ts";
import type { WorkflowTask } from "../conductor.ts";
import type { ExternalIdentity } from "../identity.ts";

/** Store the request: one `fabrial.ingress` event, deduped by its provider id. `fabrial.route` routes it. */
export const accept = async (host: FabrialHost, ingress: IngressEvent, requestId: string) => {
	await host.conductor.emit(INGRESS_EVENT as never, ingress as never, {
		dedupe_key: ingressKey(ingress),
		metadata: { requestId, origin: ingress.origin, replyTo: null, requestedBy: null },
	});
	return "accepted" as const;
};

/** The router of an app's ingress: the most specific owner of each event, its followers, and its observers. */
export function createRouter(config: CoreConfig, host: FabrialHost, agents: Agents) {
	/** Workflow and agent triggers compete for the same events: the most specific owner receives each. */
	const triggers: {
		owner: AnyWorkflow | Agent;
		name: string;
		workflow?: AnyWorkflow;
		agent?: Agent;
		spec: TriggerSpec;
	}[] = [
		...config.workflows.flatMap((workflow) =>
			(workflow.definition.triggers ?? []).map((spec: TriggerSpec) => ({
				owner: workflow,
				name: workflow.name,
				workflow,
				spec,
			})),
		),
		...(config.routedAgents ?? []).flatMap((agent) =>
			(agent.definition.triggers ?? []).map((spec: TriggerSpec) => ({
				owner: agent,
				name: `agent ${agent.name}`,
				agent,
				spec,
			})),
		),
	];
	const specificity = (spec: TriggerSpec) =>
		spec.specificity ?? Object.keys(spec.filter ?? {}).length;
	/** The most specific owner trigger of an input's categories (`tied` when two owners compete), and its observers. */
	const select = (events: readonly string[], payload: JsonObject) => {
		const matching = triggers.filter(
			(t) => events.includes(t.spec.event) && matchesFilter(payload, t.spec.filter),
		);
		const owners = matching
			.filter((t) => !t.spec.observe)
			.sort((a, b) => specificity(b.spec) - specificity(a.spec));
		const selected = owners[0];
		const tied =
			!!selected &&
			owners.some(
				(t) => t.owner !== selected.owner && specificity(t.spec) === specificity(selected.spec),
			);
		// One start per observing workflow, on the first category it observes.
		const observers = new Map<string, string>();
		for (const t of matching)
			if (t.spec.observe && !observers.has(t.workflow!.name))
				observers.set(t.workflow!.name, t.spec.event);
		return {
			selected,
			tied,
			observers: [...observers].map(([workflow, event]): RoutedWorkflow => ({ workflow, event })),
		};
	};
	/** Agents a message can name (`@agent-name`): those with triggers and every agent of the instance. */
	const knownAgents = () =>
		new Map(
			[
				...(definedAgents.get(config.definitionScope ?? host) ?? []),
				...(config.routedAgents ?? []),
			].map((agent) => [agent.name, agent]),
		);
	const mentionedAgent = (text: string): Agent | undefined => {
		const named = [...knownAgents().values()].filter((agent) =>
			new RegExp(
				`(^|[^\\w-])@${agent.name.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w-])`,
			).test(text),
		);
		return named.length === 1 ? named[0] : undefined;
	};
	/** Deliver an event to an agent's conversation: one signal to its Session (started when closed). */
	const deliver = async (
		agent: AgentRef,
		conversation: Follow,
		input: JsonObject,
		metadata: InvocationMetadata,
	) => {
		const request = await prepareInput(host, metadata, {
			agent: agent.name,
			session: conversation.session,
			requestId: `${metadata.requestId}:${agent.name}`,
			input,
			kind: isChatMessage(input) ? "message" : "notification",
			replyTo: metadata.replyTo,
		});
		await agents.deliver(agent, request);
		return request;
	};
	/**
	 * The routing workflow, run once per ingress event and retried by Conductor. Each effect is a durable
	 * operation: a retry after a partial delivery resumes after the deliveries that completed, and a
	 * delivery that completed without its receipt is deduped by its request id.
	 */
	const route = async (ingress: IngressEvent, ctx: TaskContext): Promise<Json> => {
		const native = nativeMethods(ctx);
		const { requestId } = metadataOf(ctx);
		const chat = host.chat();
		const actor = (
			ingress.actor
				? await native.step(
						"actor",
						async () => (await host.directory.resolveIdentity(ingress.actor!)) as unknown as Json,
					)
				: null
		) as InvocationMetadata["requestedBy"];
		if (ingress.kind === "action") return decideAction(host, ctx, ingress, actor);
		if (ingress.kind === "stop") {
			const sdk = await chat?.thread(ingress.replyTo as ThreadRef);
			const follows = (await native.step(
				"follows",
				async () => ((sdk && (await routingState(sdk)))?.follows ?? []) as Json,
			)) as Follow[];
			// A stop cancels the current run of each conversation following the thread; anyone there may stop.
			for (const [index, conversation] of follows.entries())
				await native.step(`stop:${index}`, async () => {
					await agents.stopConversation(conversation, ingress.id, `Stopped by ${actor!.id}`);
				});
			return null;
		}
		const message =
			ingress.kind === "message" ? (ingress.payload as unknown as ChatMessage) : undefined;
		const thread = message && threadOf(message);
		const sdk = thread && (await chat?.thread(thread));
		const decision = await native.step("route", async (): Promise<RouteDecision> => {
			const { selected, tied, observers } = select(ingress.events, ingress.payload);
			const conversation = (agent: Agent, replyTo: InvocationMetadata["replyTo"]): Follow => ({
				session: conversationSession(
					agent.definition.session,
					ingress.payload,
					replyTo,
					message ? threadSession(message.provider, message.threadId) : requestId,
				),
				agent: agent.name,
			});
			// 1. Followers: everyone in a followed thread steers. Naming one agent addresses only that agent.
			// Observers still run.
			const followers = (sdk && (await routingState(sdk)))?.follows ?? [];
			if (message && followers.length) {
				const mentioned = mentionedAgent(message.text);
				const own = mentioned ? followers.filter((f) => f.agent === mentioned.name) : followers;
				return {
					route: "follow",
					conversations: own.length || !mentioned ? own : [conversation(mentioned, thread!)],
					owner: null,
					observers,
					replyTo: thread!,
					event: ingress.events[0]!,
				};
			}
			// 2. Triggers: the most specific one wins, an agent's or a workflow's.
			if (tied) throw new Error(`Ambiguous owner for ${ingress.events.join(", ")}`);
			const replyTo = (thread ??
				ingress.replyTo ??
				selected?.agent?.definition.home ??
				selected?.spec.replyTo ??
				null) as InvocationMetadata["replyTo"];
			const owner = selected?.workflow
				? { workflow: selected.workflow.name, event: selected.spec.event }
				: null;
			const conversations = selected?.agent ? [conversation(selected.agent, replyTo)] : [];
			// The owner runs once, as owner, even when it also observes another category.
			const others = observers.filter((o) => o.workflow !== owner?.workflow);
			return {
				route: owner || conversations.length || others.length ? "new" : "ignored",
				conversations,
				owner,
				observers: others,
				replyTo,
				event: selected?.spec.event ?? ingress.events[0]!,
			};
		});
		// A handled thread stays subscribed; every conversation receiving its messages follows it.
		if (chat && sdk && decision.route !== "ignored")
			await native.step("follow", async () => {
				if (decision.conversations.length)
					await updateRoutingState(chat!, sdk, (state) =>
						decision.conversations.reduce(addFollow, state),
					);
				await sdk.subscribe();
			});
		const metadata: InvocationMetadata = {
			requestId,
			origin: (message
				? { provider: message.provider, messageId: message.messageId }
				: (ingress.origin ?? {
						provider: ingress.events[0]!.split(".")[0]!,
					})) as InvocationMetadata["origin"],
			replyTo: decision.replyTo,
			requestedBy: actor,
			ownerWorkflow: decision.owner?.workflow ?? null,
			triggerEvent: decision.event,
		};
		const named = knownAgents();
		for (const [index, conversation] of decision.conversations.entries())
			await native.step(`deliver:${index}`, async () => {
				const request = await deliver(
					named.get(conversation.agent) ?? { kind: "fabrial.agent", name: conversation.agent },
					conversation,
					ingress.payload,
					{ ...metadata, ownerWorkflow: null },
				);
				// An event's conversation follows the thread its replies go to.
				if (!message) await follow(host, request.follow, conversation);
			});
		const start = (key: string, workflow: string, metadata: Partial<InvocationMetadata>) =>
			native.invoke(
				key,
				{ name: workflow, queue: ctx.queue },
				ingress.payload as never,
				{
					metadata: childMetadata(ctx, metadata),
				} as never,
			);
		if (decision.owner) await start("owner", decision.owner.workflow, metadata);
		// Observers run without the thread.
		for (const observer of decision.observers)
			await start(`observer:${observer.workflow}`, observer.workflow, {
				...metadata,
				replyTo: null,
				triggerEvent: observer.event,
			});
		// Each category is also emitted once for `ctx.waitForEvent`; no workflow triggers on it.
		if (ingress.kind === "event" || decision.route === "new")
			for (const event of ingress.events)
				await native.emit(
					event as never,
					ingress.payload as never,
					{
						dedupe_key: ingress.id,
						metadata: childMetadata(ctx, metadata),
					} as never,
				);
		// Conversations following the event's resource threads get a notification; triggers still run.
		if (ingress.kind === "event")
			await native.step("notify", async () => {
				await notifyFollowers(host, agents, ingress.events[0]!, ingress.payload, {
					...metadata,
					replyTo: null,
				});
			});
		return decision;
	};
	/** Two owners of equal specificity whose filters may overlap make an event's owner ambiguous. */
	const check = () => {
		for (let i = 0; i < triggers.length; i++)
			for (const b of triggers.slice(i + 1)) {
				const a = triggers[i]!;
				if (
					!a.spec.observe &&
					!b.spec.observe &&
					a.owner !== b.owner &&
					a.spec.event === b.spec.event &&
					specificity(a.spec) === specificity(b.spec) &&
					filtersOverlap(a.spec.filter, b.spec.filter)
				)
					throw new Error(`Ambiguous trigger owners: ${a.name}, ${b.name} on ${a.spec.event}`);
			}
	};
	// Routing is plumbing: no workflow context or plugin middleware.
	const workflow: WorkflowTask = {
		name: ROUTE_WORKFLOW,
		triggers: [{ event: INGRESS_EVENT }],
		retries: { maxAttempts: 5 },
		handler: (input, ctx) => route(input as IngressEvent, ctx),
	};
	return { check, workflow };
}

/** The one event ingress emits; `fabrial.route` triggers on it. */
export const INGRESS_EVENT = "fabrial.ingress";
/** The internal workflow that routes every ingress event. */
const ROUTE_WORKFLOW = "fabrial.route";

/**
 * Verified, normalized input from a webhook or chat, as `fabrial.ingress` carries it. Ingress emits it once
 * per provider delivery: the emit dedupes by `kind` and `id` (and the event name of plugin events), for as
 * long as Conductor retains the event's dispatch (about one to two days). A redelivery after that routes
 * again.
 */
export interface IngressEvent extends JsonObject {
	/** The provider's delivery or message id. */
	id: string;
	/** A plugin event, a chat message, a card action (approval click), or a stop in a thread. */
	kind: "event" | "message" | "action" | "stop";
	/** Event categories: the plugin event's name, or a message's (`slack.message`, `slack.mentioned`). */
	events: string[];
	/** The plugin event's payload, the `ChatMessage`, or the action (`actionId`, `value`, `messageId`). */
	payload: JsonObject;
	origin: (Origin & JsonObject) | null;
	/** Where replies go: the event's surface, or the thread of a message, action or stop. */
	replyTo: (Surface & JsonObject) | null;
	/** The authenticated sender, resolved to a principal by the router. */
	actor: (ExternalIdentity & JsonObject) | null;
}

/** The emit's dedupe key of an ingress event. */
const ingressKey = (ingress: IngressEvent): string =>
	JSON.stringify(
		ingress.kind === "event"
			? [ingress.kind, ingress.events[0], ingress.id]
			: [ingress.kind, ingress.id],
	);

/** A workflow the router starts, and the event category it starts on. */
interface RoutedWorkflow extends JsonObject {
	workflow: string;
	event: string;
}

/** How the router routed one ingress event, persisted in its `route` step. `ignored` leaves no follow or subscription. */
interface RouteDecision extends JsonObject {
	route: "follow" | "new" | "ignored";
	conversations: Follow[];
	owner: RoutedWorkflow | null;
	observers: RoutedWorkflow[];
	/** Where the owner's (or the agent's) replies go. */
	replyTo: (Surface & JsonObject) | null;
	/** The winning trigger's event category, else the input's first. */
	event: string;
}
