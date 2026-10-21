import type { FabrialHost } from "./app.ts";
import type { Agents } from "./agents/pi.ts";
import {
	channelOf,
	channelOfSurface,
	editMessage,
	fetchHistory,
	postMessage,
	postTo,
	threadRef,
	type SDKThread,
} from "./chat/threads.ts";
import { createApproval } from "./protocol.ts";
import { duration } from "./internal.ts";
import type { Json, JsonObject } from "./json.ts";
import { bindMetadata, nativeMethods, type InvocationMetadata } from "./execution.ts";
import { parseSchema } from "./schema.ts";
import { follow, prepareSend } from "./send.ts";
import { followResource, storedFollowOptions, unfollowResource } from "./follows.ts";
import type { MessageRef, OutboundMessage, Thread, Surface } from "./thread.ts";
import { workflowObject, type AnyWorkflow } from "./workflow.ts";
import type { TaskContext } from "pgconductor-js";
import { CHILD_SIGNAL } from "./agents/documents.ts";
import type { AgentRef, AgentRunOptions } from "./agent.ts";
import type { SendOptions } from "./send.ts";
import type { ResourceRef } from "./plugin.ts";
import type { FollowOptions } from "./follows.ts";
import type { EvaluateRequest } from "./evaluate.ts";
import type { StateDefinition } from "./state.ts";
import type { ApprovalOptions } from "./approval.ts";
import type { GroupDefinition, Principal } from "./identity.ts";

export interface ContextOptions {
	host: FabrialHost;
	/** Conductor's context of the attempt: Fabrial's fields are merged onto it. */
	raw: TaskContext;
	metadata: InvocationMetadata;
	agents: Agents;
	now: () => number;
}

export { bindMetadata, metadataOf, nativeMethods } from "./execution.ts";

/** Operation keys beginning with `fabrial:` are Fabrial's; Conductor derives its own keys from a key and a suffix. */
export function userKey(key: string): string {
	if (!key || typeof key !== "string")
		throw new Error("Durable operations require a non-empty explicit key");
	if (key.startsWith("fabrial:"))
		throw new Error("Operation keys beginning with fabrial: are reserved");
	return key;
}

/** Signals Fabrial's internal executions listen for: user code cannot send them. */
const INTERNAL_SIGNALS = new Set(["input", CHILD_SIGNAL]);

/**
 * The context of one attempt: Conductor's `TaskContext` plus Fabrial's fields, merged onto it by the attempt
 * middleware (`next(additions)`). Native methods are wrapped for Fabrial's policy (operation keys, metadata,
 * reserved destinations); `fabrial.native` keeps the originals for Fabrial's own helpers.
 */
export async function createContext(options: ContextOptions) {
	const { host, raw, metadata, agents } = options;
	const native = nativeMethods(raw);
	const op = userKey;
	const chat = host.chat();
	// The thread replies go to. A channel destination stays lazy until its first post, whose receipt
	// (also on replay) names the thread later posts and child executions use.
	let bound: SDKThread | undefined =
		metadata.replyTo?.kind === "thread" && chat ? await chat.thread(metadata.replyTo) : undefined;
	let firstPost: Promise<unknown> | undefined;
	const post = async (id: string, message: OutboundMessage): Promise<MessageRef> => {
		while (firstPost) await firstPost.catch(() => {});
		const posting = native.step(id, async () => {
			if (!metadata.replyTo || !chat) throw new Error("This execution has no chat surface");
			return (await (bound
				? postMessage(bound, message)
				: postTo(chat, metadata.replyTo, message))) as unknown as JsonObject;
		}) as unknown as Promise<MessageRef>;
		if (!bound) firstPost = posting;
		try {
			const ref = await posting;
			bound = await chat!.thread({
				kind: "thread",
				provider: ref.provider,
				threadId: ref.threadId,
			});
			metadata.replyTo = threadRef(bound) as InvocationMetadata["replyTo"];
			return ref;
		} finally {
			if (firstPost === posting) firstPost = undefined;
		}
	};

	const surface = metadata.replyTo as Surface | null;
	const thread: Thread | undefined =
		surface && chat
			? {
					get ref() {
						return bound
							? threadRef(bound)
							: surface.kind === "thread"
								? surface
								: {
										kind: "thread" as const,
										provider: surface.provider,
										threadId: surface.channelId,
									};
					},
					get channelId() {
						return bound
							? channelOf(bound)
							: surface.kind === "channel"
								? surface.channelId
								: surface.threadId;
					},
					get isDM() {
						return bound?.isDM ?? false;
					},
					post: (id, message) => post(op(id), message),
					update: async (id, message, content) =>
						native.step(op(id), () => editMessage(chat, message, content)),
					history: (id, opts = {}) =>
						native.step(op(id), async () => (bound ? fetchHistory(chat, bound, opts) : [])),
				}
			: undefined;
	/** An app workflow: internal tasks (routing, approvals, sessions, …) are not destinations for user code. */
	const target = (workflow: AnyWorkflow | { name: string } | string) => {
		const name = typeof workflow === "string" ? workflow : workflow.name;
		const registered = host.workflows.find((w) => w.name === name);
		if (!registered) throw new Error(`Unknown or reserved workflow: ${name}`);
		return { registered, task: { name, queue: raw.queue } };
	};
	/** A workflow's input is the JSON object Conductor stores: checked by its schema once, before the start. */
	const input = async (key: string, workflow: AnyWorkflow, value?: Json): Promise<JsonObject> => {
		const { registered } = target(workflow);
		return (await native.step(`fabrial:${key}:input`, async () =>
			workflowObject(
				registered.definition.input
					? await parseSchema(registered.definition.input, value, `${workflow.name} input`)
					: (value ?? {}),
				`${workflow.name} input`,
			),
		)) as JsonObject;
	};
	// Children inherit this execution's metadata (after a lazy post, `replyTo` is the thread it created),
	// minus routing-only fields; an explicit override merges onto it.
	const childMetadata = (override?: unknown) => (): InvocationMetadata => {
		const base: InvocationMetadata = { ...metadata, ownerWorkflow: null, triggerEvent: null };
		const extra =
			typeof override === "function" ? override(base) : (override as Partial<InvocationMetadata>);
		return { ...base, ...extra };
	};
	const event = (name: string) => {
		if (name.startsWith("fabrial.")) throw new Error(`Reserved event name: ${name}`);
		return name;
	};
	const eventOf = (definition: { event: string | { name: string } }) => ({
		...definition,
		event: {
			name: event(typeof definition.event === "string" ? definition.event : definition.event.name),
		},
	});
	const surfaces = () => ({
		current: metadata.replyTo,
		channelId: async () => {
			if (!metadata.replyTo || !chat) throw new Error("This execution has no chat surface");
			return channelOfSurface(chat, metadata.replyTo);
		},
	});
	const additions = {
		thread,
		clients: host.clients(),
		actor: metadata.requestedBy,
		step: (key: string, fn: () => Promise<Json | void> | Json | void) => native.step(op(key), fn),
		sleep: (key: string, value: string | number) => native.sleep(op(key), duration(value)),
		subscribe: (key: string, options: { event: string | { name: string }; filter?: object }) =>
			native.subscribe(op(key), eventOf(options) as never),
		waitForEvent: (
			key: string,
			options: { event: string | { name: string }; filter?: object; timeout?: number | string },
		) => native.waitForEvent(op(key), eventOf(options) as never),
		// Not native.waitForResult: Conductor's calls ctx.waitForAny, which plugin middleware may wrap.
		waitForResult: async (key: string, executionId: string, opts?: object) => {
			const winner = await native.waitForAny(
				op(key),
				{ result: { execution: executionId } },
				opts as never,
			);
			return winner.key === "timeout" ? { status: "timeout" } : winner.outcome;
		},
		invoke: async (
			key: string,
			workflow: AnyWorkflow,
			value?: Json,
			opts?: { metadata?: unknown },
		) =>
			native.invoke(op(key), target(workflow).task, await input(key, workflow, value), {
				...opts,
				metadata: childMetadata(opts?.metadata),
			} as never),
		invokeAndWait: async (
			key: string,
			workflow: AnyWorkflow,
			value?: Json,
			opts?: { metadata?: unknown },
		) =>
			native.invokeAndWait(op(key), target(workflow).task, await input(key, workflow, value), {
				...opts,
				metadata: childMetadata(opts?.metadata),
			} as never),
		schedule: async (workflow: AnyWorkflow, name: string, opts: { cron: string }, value?: Json) =>
			native.schedule(
				target(workflow).task,
				name,
				opts as never,
				workflowObject(value ?? {}, `${name} input`) as never,
			),
		unschedule: (workflow: AnyWorkflow, name: string) =>
			native.unschedule(target(workflow).task, name),
		sendSignal: (executionId: string, signal: { name: string; id: string; payload?: Json }) => {
			if (signal.name.startsWith("fabrial.") || INTERNAL_SIGNALS.has(signal.name))
				throw new Error(`Reserved signal name: ${signal.name}`);
			return native.sendSignal(executionId, signal as never);
		},
		signalWithStart: (
			key: string,
			opts: {
				task: AnyWorkflow | string;
				key: string;
				signal: { name: string; id: string; payload?: Json };
				input?: Json;
				metadata?: unknown;
			},
		) =>
			native.signalWithStart(op(key), {
				...opts,
				task: target(opts.task).task.name,
				queue: raw.queue,
				input: workflowObject(opts.input ?? {}, `${opts.key} input`),
				metadata: childMetadata(opts.metadata),
			} as never),
		emit: (name: string, payload: JsonObject, opts?: { dedupe_key?: string; metadata?: unknown }) =>
			native.emit(
				event(name) as never,
				payload as never,
				{
					...opts,
					metadata: childMetadata(opts?.metadata),
				} as never,
			),
		agent: async (id: string, agent: AgentRef, opts: AgentRunOptions) =>
			agents.run(raw, op(id), agent, opts),
		send: async (id: string, agent: AgentRef, opts: SendOptions) => {
			const key = op(id);
			const request = await native.step(`fabrial:${key}:send`, async () => {
				const prepared = await prepareSend(
					host,
					metadata,
					surfaces(),
					`${raw.executionId}:${key}`,
					agent,
					opts,
				);
				// Later messages in the thread reach the conversation directly.
				await follow(host, prepared.follow, { session: prepared.session, agent: agent.name });
				return prepared;
			});
			return agents.send(raw, key, agent, request);
		},
		follow: (id: string, resource: ResourceRef<any>, opts?: FollowOptions) =>
			native.step(op(id), () =>
				followResource(host, metadata, surfaces(), resource, storedFollowOptions(opts)),
			),
		unfollow: (id: string, resource: ResourceRef<any>, opts?: Pick<FollowOptions, "to">) =>
			native.step(op(id), () =>
				unfollowResource(host, metadata, surfaces(), resource, storedFollowOptions(opts)),
			),
		evaluate: async (id: string, request: EvaluateRequest<any>) =>
			native.step(
				op(id),
				async () => (await agents.evaluator.evaluate(request, raw.signal)) as unknown as Json,
			),
		state: (state: StateDefinition<any, "session">) => ({
			get: (id: string) => agents.state.get(raw, op(id), state),
			update: (id: string, fn: never) => agents.state.update(raw, op(id), state, fn),
		}),
		requestApproval: async (id: string, opts: ApprovalOptions) =>
			createApproval(op(id), opts, { ...options, native, op }),
		waitForApproval: async (id: string, opts: ApprovalOptions) => {
			const key = op(id);
			const approval = await createApproval(key, opts, { ...options, native, op });
			return approval.wait(`${key}:decision`);
		},
		isMember: (principal: Principal, group: GroupDefinition) =>
			host.directory.isMember(principal, group),
		fabrial: { native, metadata },
	};
	bindMetadata(raw, metadata);
	return additions;
}
