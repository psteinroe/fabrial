// A workflow's `ctx`: Conductor's context of each attempt, with Fabrial's fields merged on and native methods wrapped
// for policy.
import type { Middleware, TaskContext } from "pgconductor-js";
import type { AnyPlugin, ResourceRef } from "../plugin.ts";
import { bindMetadata, type InvocationMetadata, metadataOf } from "./metadata.ts";
import { type AnyWorkflow, type WorkflowContext, workflowObject } from "./definition.ts";
import type { FabrialHost } from "../app.ts";
import type { Agents } from "../sessions/host.ts";
import type { WorkflowTask } from "../conductor.ts";
import { CHILD_SIGNAL } from "../sessions/workflows.ts";
import { duration, nativeMethods } from "../execution.ts";
import { channelOf, type SDKThread, threadRef } from "../chat/connection.ts";
import {
	channelOfSurface,
	editMessage,
	fetchHistory,
	type MessageRef,
	type OutboundMessage,
	postMessage,
	postTo,
	type Surface,
	type Thread,
} from "../chat/messages.ts";
import { type Json, type JsonObject, parseSchema } from "../schema.ts";
import type { AgentRef, AgentRunOptions } from "../agents/definition.ts";
import { prepareSend, type SendOptions } from "../sessions/delivery.ts";
import {
	follow,
	type FollowOptions,
	followResource,
	storedFollowOptions,
	unfollowResource,
} from "../routing/follows.ts";
import { evaluate, type EvaluateRequest } from "../models.ts";
import { type ApprovalOptions, createApproval } from "../approvals/request.ts";
import type { GroupDefinition, Principal } from "../identity.ts";

/**
 * Plugin workflow middleware, as Conductor middleware: it runs per attempt around executions with a
 * Fabrial context (not routing), after the attempt hook merged Fabrial's fields onto it.
 */
export function pluginMiddleware(plugins: readonly AnyPlugin[]): Middleware<TaskContext>[] {
	return plugins
		.flatMap((p) => p.hooks?.workflow ?? [])
		.map((hook) => async ({ execution, ctx }, next) => {
			if (!("fabrial" in ctx)) return next({});
			const metadata = metadataOf(ctx);
			// A hook that doesn't call `next` still runs the attempt; the result itself may be null.
			let ran = false;
			let result: Awaited<ReturnType<typeof next>> | undefined;
			await hook(
				{
					executionId: execution.id,
					workflow: execution.task_key,
					actor: metadata.requestedBy,
					origin: metadata.origin,
				},
				ctx as unknown as WorkflowContext,
				async () => {
					ran = true;
					result = await next({});
				},
			);
			return ran ? result! : next({});
		});
}

/** Merges Fabrial's fields onto Conductor's context of every attempt. */
export function attemptHook(host: FabrialHost, agents: Agents): WorkflowTask["attempt"] {
	return async (raw, next) =>
		next(
			await createContext({ host, raw, metadata: metadataOf(raw), agents, now: () => host.now() }),
		);
}

export interface ContextOptions {
	host: FabrialHost;
	/** Conductor's context of the attempt: Fabrial's fields are merged onto it. */
	raw: TaskContext;
	metadata: InvocationMetadata;
	agents: Agents;
	now: () => number;
}

/**
 * A user operation key, as stored. Fabrial and Conductor derive keys by appending `:<suffix>` (an approval's
 * wait, race branches, a child's input), and Fabrial's own keys begin with `fabrial:`. A stored user key
 * contains no `:` (`%` and `:` are escaped), so no derived or internal key equals one.
 */
function userKey(key: string): string {
	if (!key || typeof key !== "string")
		throw new Error("Durable operations require a non-empty explicit key");
	if (key.startsWith("fabrial:"))
		throw new Error("Operation keys beginning with fabrial: are reserved");
	return key.replaceAll("%", "%25").replaceAll(":", "%3A");
}

/** Signals Fabrial's internal executions listen for: user code cannot send them. */
const INTERNAL_SIGNALS = new Set(["input", CHILD_SIGNAL]);

/**
 * The context of one attempt: Conductor's `TaskContext` plus Fabrial's fields, merged onto it by the attempt
 * middleware (`next(additions)`). Native methods are wrapped for Fabrial's policy (operation keys, metadata,
 * reserved destinations); `fabrial.native` keeps the originals for Fabrial's own helpers.
 */
async function createContext(options: ContextOptions) {
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

	const threadOf = (surface: Surface | null): Thread | undefined =>
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
	// Where a static cron tick replies: its trigger's destination, bound before the workflow runs.
	const replyTo = async (surface: Surface) => {
		metadata.replyTo = surface as InvocationMetadata["replyTo"];
		bound = surface.kind === "thread" && chat ? await chat.thread(surface) : undefined;
		Object.assign(raw, { thread: threadOf(surface) });
	};
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
		thread: threadOf(metadata.replyTo as Surface | null),
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
		waitForResult: (key: string, executionId: string, opts?: { timeout?: number | string }) =>
			native.waitForResult(op(key), executionId, opts as never),
		waitForAny: (key: string, branches: object, opts?: object) =>
			native.waitForAny(op(key), branches as never, opts as never),
		invoke: async (
			key: string,
			workflow: AnyWorkflow,
			value?: Json,
			opts?: { metadata?: unknown },
		) =>
			native.invoke(op(key), target(workflow).task, await input(op(key), workflow, value), {
				...opts,
				metadata: childMetadata(opts?.metadata),
			} as never),
		invokeAndWait: async (
			key: string,
			workflow: AnyWorkflow,
			value?: Json,
			opts?: { metadata?: unknown },
		) =>
			native.invokeAndWait(op(key), target(workflow).task, await input(op(key), workflow, value), {
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
		evaluate: async (id: string, request: EvaluateRequest) =>
			native.step(
				op(id),
				async () => (await evaluate(agents.models, request, raw.signal)) as unknown as Json,
			),
		requestApproval: async (id: string, opts: ApprovalOptions) => {
			const approval = await createApproval(op(id), opts, { ...options, native, op });
			return {
				approvalId: approval.approvalId,
				approvers: approval.approvers,
				branch: approval.branch,
				wait: (waitId: string) => approval.wait(waitId),
				cancel: (cancelId: string, reason: string) => approval.cancel(cancelId, reason),
			};
		},
		waitForApproval: async (id: string, opts: ApprovalOptions) => {
			const key = op(id);
			const approval = await createApproval(key, opts, { ...options, native, op });
			return approval.waitAt(`fabrial:${key}:wait`);
		},
		isMember: (principal: Principal, group: GroupDefinition) =>
			host.directory.isMember(principal, group),
		fabrial: { native, metadata, replyTo },
	};
	bindMetadata(raw, metadata);
	return additions;
}
