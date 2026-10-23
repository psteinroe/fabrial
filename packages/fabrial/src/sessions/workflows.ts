// Children of a Session: the workflows and effects its tools start, the reporter that signals their outcomes, and
// their cancellation links.
import { defineDoc, defineDocFamily } from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import type { TaskContext } from "pgconductor-js";
import { cloneJson, type Json, type JsonObject } from "../schema.ts";
import { childMetadata, type InvocationMetadata, metadataOf } from "../workflows/metadata.ts";
import type { BridgeFrame } from "../agents/tool-context.ts";
import { Obligations } from "./obligations.ts";
import { executionResult, nativeMethods } from "../execution.ts";
import type { FabrialHost } from "../app.ts";
import {
	channelOfSurface,
	editMessage,
	fetchHistory,
	type MessageRef,
	type OutboundMessage,
	postTo,
} from "../chat/messages.ts";
import type { WorkflowTask } from "../conductor.ts";
import type { Agent } from "../agents/definition.ts";
import { prepareSend, type SendOptions, type SessionDelivery } from "./delivery.ts";
import type { ResourceRefJson } from "../plugin.ts";
import {
	type ConversationKey,
	follow,
	followResource,
	type StoredFollowOptions,
	unfollowResource,
} from "../routing/follows.ts";

export const ChildResults = defineDoc({
	kind: "fabrial.children",
	scope: "session",
	version: 1,
	initial: (): { results: Record<string, JsonObject> } => ({ results: {} }),
});
interface ChildIntent extends JsonObject {
	workflow: string;
	input: Json;
	metadata: InvocationMetadata;
	dedupeKey: string;
	detached: boolean;
	executionId: string | null;
	/** Conductor confirmed a stop's cancel: a redelivered stop skips it. */
	cancelled: boolean;
}
export const ChildIntents = defineDocFamily({
	family: true,
	kind: "fabrial.child-intent",
	scope: "session",
	version: 1,
	initial: (intent: ChildIntent): ChildIntent => intent,
});
export const EFFECT_WORKFLOW = "fabrial.pi.effect";
/** Runs a child a Session waits on and reports its outcome once committed (see `startChild`). */
const REPORT_WORKFLOW = "fabrial.pi.report";
/** Name of the signal a reporter sends the Session's execution, id `child:<child execution id>`. */
export const CHILD_SIGNAL = "child";
/** Payload of a `child` signal: the reporter's execution id, and the committed outcome of its child. */
export interface ChildSignal extends JsonObject {
	executionId: string;
	result: JsonObject;
}

/** A reporter's result is `{ child, result }`, the committed outcome of the child it ran; else it never reported. */
export function childOutcome(reporter: JsonObject): JsonObject {
	return reporter.status === "completed"
		? ((reporter.output as { result: JsonObject }).result ?? reporter)
		: reporter;
}

/**
 * Persist the child's intent BEFORE starting, linked to the request it acts for (none for a reply's post), so stopping
 * the request cancels the child. Attached children are cancelled with the Session's execution; a lost start receipt
 * reconciles by global key.
 *
 * A child the Session waits on (`waited`) runs behind a reporter: the reporter runs it as its own attached child,
 * waits until Conductor committed its outcome, and only then signals this Session's execution. The intent, and the
 * returned id, are the reporter's.
 */
export async function startChild(
	bridge: BridgeFrame,
	request: string | null,
	key: string,
	workflow: string,
	input: Json,
	metadata: InvocationMetadata,
	options: { detached: boolean; waited: boolean },
	context: Context,
): Promise<string> {
	const { detached, waited } = options;
	const dedupeKey = JSON.stringify(["fabrial.pi.child", bridge.session, key]);
	const intent = await bridge.harness.commit(async (tx) => {
		const obligation = request === null ? undefined : (await tx.doc(Obligations)).requests[request];
		if (request !== null && !obligation)
			throw new Error(`Missing Fabrial obligation for request ${request}`);
		// A stop cancels what its requests' tools started; nothing starts for them afterwards.
		if (obligation?.stopped) throw new Error(`Request ${request} was stopped`);
		const saved = await tx.doc(ChildIntents, key, {
			workflow: waited ? REPORT_WORKFLOW : workflow,
			input: waited
				? { workflow, input: cloneJson(input ?? null), session: bridge.ctx.executionId }
				: cloneJson(input ?? null),
			metadata: { ...cloneJson(metadata), triggerEvent: null, ownerWorkflow: null },
			dedupeKey,
			detached,
			executionId: null,
			cancelled: false,
		});
		if (obligation) {
			obligation.intents ??= [];
			if (!obligation.intents.includes(key)) obligation.intents.push(key);
		}
		return cloneJson(saved) as ChildIntent;
	}, context);
	return intent.executionId ?? launch(bridge, key, intent, context);
}

/**
 * Starts an intent's execution, or gets the one already started for it: the operation id and the global dedupe key
 * are the intent's. Saves its execution id.
 */
async function launch(
	bridge: Pick<BridgeFrame, "harness" | "ctx">,
	key: string,
	intent: ChildIntent,
	context: Context,
): Promise<string> {
	const executionId = await nativeMethods(bridge.ctx).invoke(
		key,
		{ name: intent.workflow, queue: bridge.ctx.queue },
		intent.input as never,
		{
			metadata: childMetadata(bridge.ctx, intent.metadata),
			dedupe_key: intent.dedupeKey,
			...(intent.detached ? {} : { cancelWithParent: true }),
		} as never,
	);
	await bridge.harness.commit(async (tx) => {
		(await tx.doc(ChildIntents, key, intent)).executionId = executionId;
	}, context);
	return executionId;
}

/**
 * Cancels the workflows and effects the tools of `requests` started or wait on, except detached ones. Call it once the
 * requests are `stopped`: an intent still starting (no execution id yet) is started or found by its start key, then
 * cancelled, and no later start joins them. Each intent is marked cancelled once Conductor confirmed it, so a stop
 * that failed or crashed partway is completed by its redelivery.
 */
export async function cancelChildren(
	bridge: Pick<BridgeFrame, "harness" | "ctx">,
	cancel: (executionId: string, reason: string) => Promise<unknown>,
	requests: Iterable<string>,
	reason: string,
	context: Context,
): Promise<void> {
	const { harness } = bridge;
	const obligations = (await harness.snapshot(Obligations, context))?.requests ?? {};
	for (const request of requests) {
		const obligation = obligations[request];
		if (!obligation) continue;
		for (const key of obligation.intents ?? []) {
			const intent = await harness.snapshot(ChildIntents, key, context);
			if (!intent || intent.detached || intent.cancelled) continue;
			await cancel(intent.executionId ?? (await launch(bridge, key, intent, context)), reason);
			await harness.commit(async (tx) => {
				(await tx.doc(ChildIntents, key, intent)).cancelled = true;
			}, context);
		}
	}
}

/** The channel of an execution's reply surface, without the provider. */
async function channelOfExecution(
	host: FabrialHost,
	ctx: TaskContext,
	what: string,
): Promise<string> {
	const chat = host.chat();
	const { replyTo } = metadataOf(ctx);
	if (!replyTo || !chat) throw new Error(`Managed Pi ${what} has no chat surface`);
	return channelOfSurface(chat, replyTo);
}

/**
 * Runs a child a Session waits on (a workflow tool, an effect) as its own attached child and signals the Session's
 * execution once Conductor committed the child's outcome, cancellations included: a retried, failed or interrupted
 * attempt never reaches the Session. Returns that outcome. Kept forever, like the executions Sessions wait on.
 */
export const reportWorkflow: WorkflowTask = {
	name: REPORT_WORKFLOW,
	triggers: [],
	handler: async (input, ctx) => {
		const native = nativeMethods(ctx);
		const {
			workflow,
			input: data,
			session,
		} = input as {
			workflow: string;
			input: Json;
			session: string;
		};
		const child = await native.invoke(
			"child",
			{ name: workflow, queue: ctx.queue },
			data as never,
			{ metadata: childMetadata(ctx), cancelWithParent: true } as never,
		);
		const result = executionResult(await native.waitForResult("result", child)) as JsonObject;
		await native.step("report", async () => {
			await native.sendSignal(session, {
				name: CHILD_SIGNAL,
				id: `child:${child}`,
				payload: { executionId: ctx.executionId, result } satisfies ChildSignal,
			});
		});
		return { child, result };
	},
};

export function effectWorkflow(
	host: FabrialHost,
	agents: Map<string, Agent>,
	delivery: SessionDelivery,
): WorkflowTask {
	// Effects run in a stable child execution, never in the Session's replaceable execution.
	const effect: WorkflowTask = {
		name: EFFECT_WORKFLOW,
		triggers: [],
		handler: async (input, ctx) => {
			const native = nativeMethods(ctx);
			const metadata = metadataOf(ctx);
			const { method, args } = input as { method: string; args: Json[] };
			if (method === "follow" || method === "unfollow") {
				const [resource, options] = args as [ResourceRefJson, StoredFollowOptions];
				const surface = {
					current: metadata.replyTo,
					channelId: () => channelOfExecution(host, ctx, method),
				};
				const apply = method === "follow" ? followResource : unfollowResource;
				return native.step(`fabrial:pi:${method}`, () =>
					apply(host, metadata, surface, resource as never, options),
				);
			}
			if (method === "send") {
				const [agent, options] = args as [string, SendOptions];
				const ref = agents.get(agent) ?? { kind: "fabrial.agent" as const, name: agent };
				const request = await native.step("fabrial:pi:send", async () => {
					const prepared = await prepareSend(
						host,
						metadata,
						{
							current: metadata.replyTo,
							channelId: () => channelOfExecution(host, ctx, "send"),
						},
						`${ctx.executionId}:send`,
						ref,
						options,
					);
					await follow(host, prepared.follow, { session: prepared.session, agent });
					return prepared;
				});
				return delivery.send(ctx, "send", ref, request);
			}
			return native.step("fabrial:pi:effect", async () => {
				const chat = host.chat();
				const replyTo = metadata.replyTo;
				if (!replyTo || !chat) throw new Error("Managed Pi thread operation has no thread");
				if (method === "post") {
					// To a channel home, each post starts a thread of its own.
					const posted = await postTo(chat, replyTo, args[0] as OutboundMessage);
					// A reply to a channel home created the conversation's thread: follow it.
					const conversation = metadata.follow as ConversationKey | undefined;
					if (conversation && metadata.replyTo?.kind === "channel")
						await follow(
							host,
							{ kind: "thread", provider: posted.provider, threadId: posted.threadId },
							conversation,
						);
					return cloneJson(posted) as unknown as Json;
				}
				if (method === "update") {
					await editMessage(
						chat,
						args[0] as unknown as MessageRef,
						args[1] as OutboundMessage,
						replyTo,
					);
					return null;
				}
				if (method === "history")
					return replyTo.kind === "thread"
						? cloneJson(
								await fetchHistory(
									chat,
									await chat.thread(replyTo),
									args[0] as { limit?: number; sinceLastBotReply?: boolean },
								),
							)
						: [];
				throw new Error(`Unknown Pi effect ${method}`);
			});
		},
	};
	return effect;
}
