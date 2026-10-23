// What Fabrial owes for each request submitted to Pi: replies, call outcomes, stops, and error replies.
import {
	type Conversation,
	type ConversationId,
	defineDoc,
	defineDocFamily,
	type EntryId,
	Harness,
	LiveDoc,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { TaskContext } from "pgconductor-js";
import type { Context } from "@earendil-works/chord";
import { randomBytes } from "node:crypto";
import { type AssistantMessage, isRetryableAssistantError } from "@earendil-works/pi-ai";
import type { InvocationMetadata } from "../workflows/metadata.ts";
import { cloneJson, type Json, type JsonObject } from "../schema.ts";
import type { FabrialHost } from "../app.ts";
import type { BridgeFrame } from "../agents/tool-context.ts";
import { PostgresStorage } from "./storage.ts";
import type { Progress } from "../chat/status.ts";
import { Conversations, type Hosted } from "./host.ts";
import {
	cancelChildren,
	ChildIntents,
	childOutcome,
	ChildResults,
	type ChildSignal,
	EFFECT_WORKFLOW,
	startChild,
} from "./workflows.ts";
import { type ExecutionResult, settlement } from "../execution.ts";

export const Invocation = defineDocFamily({
	family: true,
	kind: "fabrial.invocation",
	scope: "session",
	version: 1,
	initial: (metadata: InvocationMetadata) => ({ ...metadata, actor: metadata.requestedBy }),
});
export const Binding = defineDoc({
	kind: "fabrial.binding",
	scope: "conversation",
	history: "latest",
	fork: "initial",
	version: 1,
	initial: (): { requestId: string | null } => ({ requestId: null }),
});

/** A `ctx.agent` call's Session execution returns its outcome: the answer, or why there is none. */
export type CallOutcome =
	| { status: "completed"; output: Json }
	| { status: "failed"; error: string; executionId: string };

/**
 * What Fabrial owes for one request submitted to Pi, beyond what Pi tracks: committed before the request is submitted,
 * so a crash in between leaves the obligation for its redelivery. Pi owns the submission and its lifecycle (looked up
 * by request id); this holds only what Pi cannot know.
 */
export type Obligation = {
	/** The conversation the request is submitted to. */
	conversationId: number;
	/**
	 * `reply`: its answer posts to the reply thread of its invocation (sent input, or a background child's report).
	 * `call`: its answer is a `ctx.agent` call's outcome. `task`: nothing; a background child's task acts for `origin`.
	 */
	owes: "reply" | "call" | "task";
	/** The request whose invocation it acts for (follows, reply thread), when not its own: a background child's. */
	origin?: string;
	/** Nothing more is owed: its answer or error reply is posting or skipped, its call settled, or it was stopped. */
	fulfilled?: true;
	/** A stop cancelled the workflows its tools started; its tools start none afterwards. */
	stopped?: true;
	/** A settled call's outcome: its Session execution's result. */
	outcome?: CallOutcome;
	/** The error id of its error reply; assigned once. */
	errorId?: string;
	/**
	 * Final and error replies posted for it, by their effect's intent key: the run settles only once each post's outcome
	 * arrived. A post that failed for good stays `failed` (logged, never retried or answered with an error reply).
	 */
	deliveries?: Record<
		string,
		{ executionId: string; status: "pending" | "posted" | "failed"; error?: string }
	>;
	/** Workflows and effects its tools started, by intent key: stopping the request cancels the attached ones. */
	intents?: string[];
};
export const Obligations = defineDoc({
	kind: "fabrial.obligations",
	scope: "session",
	version: 1,
	initial: (): { requests: Record<string, Obligation> } => ({ requests: {} }),
});

/** Whether something is still owed for an obligation, on `conversationId` if given; a child's task owes nothing. */
export const outstanding = (obligation: Obligation, conversationId?: number) =>
	!obligation.fulfilled &&
	obligation.owes !== "task" &&
	(conversationId === undefined || obligation.conversationId === conversationId);

/** How many settled obligations a Session keeps for a redelivered request's dedupe: older ones are pruned on open. */
export const retention = { settled: 100 };

/**
 * Obligations beyond the newest `keep` settled ones, in admission order: fulfilled, no post pending, not a call (its
 * outcome is its execution's result), and none acting for them. A redelivered request pruned meanwhile is not
 * submitted again (Pi dedupes by request id), and its posts reuse their effects (their intents stay).
 */
export function prunable(requests: Record<string, Obligation>, keep: number): string[] {
	const origins = new Set(Object.values(requests).map((each) => each.origin));
	const settled = Object.keys(requests).filter((requestId) => {
		const each = requests[requestId]!;
		return (
			each.fulfilled &&
			each.owes !== "call" &&
			!origins.has(requestId) &&
			!Object.values(each.deliveries ?? {}).some((delivery) => delivery.status === "pending")
		);
	});
	return settled.slice(0, Math.max(0, settled.length - keep));
}

/** Prunes settled obligations past the retention, with the results their tools' workflows left. */
export async function prune(harness: Harness, context: Context): Promise<void> {
	const requests = (await harness.snapshot(Obligations, context))?.requests ?? {};
	const pruned = prunable(requests, retention.settled);
	if (!pruned.length) return;
	const executions = new Set<string>();
	for (const requestId of pruned)
		for (const key of requests[requestId]!.intents ?? []) {
			const intent = await harness.snapshot(ChildIntents, key, context);
			if (intent?.executionId) executions.add(intent.executionId);
		}
	await harness.commit(async (tx) => {
		const owed = (await tx.doc(Obligations)).requests;
		for (const requestId of pruned) delete owed[requestId];
		const results = (await tx.doc(ChildResults)).results;
		for (const executionId of executions) delete results[executionId];
	}, context);
}

/** What an opened Session's obligation handling works with: its host loop's state, read and changed in place. */
interface SessionScope {
	host: FabrialHost;
	ctx: TaskContext;
	harness: Harness;
	context: Context;
	frame: BridgeFrame;
	storage: PostgresStorage;
	/** Progress of the conversations with outstanding obligations, by conversation id. */
	views: Map<number, Progress>;
	/** Outcomes of the calls this Session finished, by request id. */
	outcomes: Map<string, CallOutcome>;
	reportOf: (executionId: string) => PromiseWithResolvers<JsonObject>;
	/** Schedules a reconciliation pass. */
	schedule: () => void;
	hostedOf: (conversationId: number) => Promise<Hosted>;
	closeView: (conversationId: number) => Promise<void>;
	noReply: ToolRegistration;
}

/** What an opened Session owes each request beyond what Pi tracks: replies posted once, calls settled, stops. */
export function sessionObligations(scope: SessionScope) {
	const {
		host,
		ctx,
		harness,
		context,
		frame,
		storage,
		views,
		outcomes,
		reportOf,
		schedule,
		hostedOf,
		closeView,
		noReply,
	} = scope;
	/**
	 * A stop in a followed thread aborts the agent's conversation through Pi: its queued inputs are withdrawn and its
	 * live task tree is aborted. Its requests are marked stopped, answered or not, and the workflows their tools
	 * started are cancelled: a background child's tools act for the lead's request, so its workflows go too, also
	 * after the lead's answer posted. The abort reaches past the background boundary: the conversation's background
	 * children and their reports stop too. Nothing more is owed for its requests, nor for reports on them. Calls have their own Session and
	 * belong to their workflow. A redelivered stop cancels what an earlier delivery marked stopped but did not cancel.
	 */
	async function stop(agentName: string, reason: string) {
		const lead = (await harness.snapshot(Conversations, context))?.ids[agentName];
		const conversation =
			lead === undefined ? undefined : await harness.conversation(lead as ConversationId, context);
		if (!conversation) return;
		const owed = await obligations();
		const requests = new Set(
			Object.keys(owed).filter(
				(id) => owed[id]!.conversationId === lead && owed[id]!.owes === "reply",
			),
		);
		// Stopped first, so a tool starting a workflow for them now either is refused or left an intent to cancel.
		const stopped = await harness.commit(async (tx) => {
			const ids: string[] = [];
			for (const [requestId, obligation] of Object.entries((await tx.doc(Obligations)).requests))
				if (requests.has(requestId) || requests.has(obligation.origin ?? "")) {
					obligation.stopped = true;
					ids.push(requestId);
				}
			return ids;
		}, context);
		await cancelChildren(
			frame,
			(executionId, why) => host.conductor.cancel(executionId, { reason: why }),
			stopped,
			reason,
			context,
		);
		// Past the background boundary: the lead's background children and their reports stop too.
		await conversation.abort(context, { background: true });
		await harness.commit(async (tx) => {
			for (const [requestId, obligation] of Object.entries((await tx.doc(Obligations)).requests))
				if (requests.has(requestId) || requests.has(obligation.origin ?? ""))
					obligation.fulfilled = true;
		}, context);
		await closeView(conversation.id);
	}

	const obligations = async () => (await harness.snapshot(Obligations, context))?.requests ?? {};
	/** Pi's record of a request submitted to a conversation. */
	const submissionOf = (conversationId: ConversationId, requestId: string) =>
		storage.submissionByRequest(conversationId, requestId, context);
	/** Requests with their reply thread's invocation, or null for none. */
	const threads = new Map<string, InvocationMetadata | null>();
	/** The invocation of sent input or a report on it, when its answer posts to a reply thread. */
	async function replyThread(requestId: string) {
		if (!threads.has(requestId)) {
			const obligation = (await obligations())[requestId];
			const invocation = (await harness.snapshot(
				Invocation,
				obligation?.origin ?? requestId,
				context,
			)) as InvocationMetadata | undefined;
			threads.set(
				requestId,
				obligation?.owes === "reply" && invocation?.send && invocation.replyTo ? invocation : null,
			);
		}
		return threads.get(requestId) ?? undefined;
	}

	/** Text of an answer entry, or undefined when it ends in `no_reply`. */
	async function answerOf(conversation: Conversation, answer: EntryId) {
		const messages = (await conversation.commit((tx) => tx.entry(answer), context))?.model ?? [];
		const assistant = messages.flatMap((message) =>
			message.role === "assistant" ? message.content : [],
		);
		if (assistant.some((block) => block.type === "toolCall" && block.name === noReply.name))
			return undefined;
		return assistant.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("\n");
	}

	/**
	 * Post a reply through an attached effect keyed by its intent, to `requestId`'s reply thread, and fulfil the
	 * obligations of `covered` with it; a call settles only once the post did. A redelivered or restarted Session
	 * reuses the effect; the post is at-least-once: a crash after the provider accepted it and before its receipt
	 * repeats it.
	 */
	async function deliver(key: string, text: string, requestId: string, covered: string[]) {
		const executionId = await startChild(
			frame,
			null,
			key,
			EFFECT_WORKFLOW,
			{ method: "post", args: [text] },
			(await replyThread(requestId))!,
			{ detached: false, waited: true },
			context,
		);
		const delivery = await harness.commit(async (tx) => {
			const requests = (await tx.doc(Obligations)).requests;
			const obligation = requests[requestId]!;
			obligation.deliveries ??= {};
			obligation.deliveries[key] ??= { executionId, status: "pending" };
			for (const each of covered) if (requests[each]) requests[each].fulfilled = true;
			return { ...obligation.deliveries[key], conversationId: obligation.conversationId };
		}, context);
		if (delivery.status === "pending")
			track(delivery.executionId, requestId, key, delivery.conversationId);
	}

	/** Fulfils obligations that need no post. */
	async function fulfil(requestIds: string[]) {
		if (!requestIds.length) return;
		await harness.commit(async (tx) => {
			const requests = (await tx.doc(Obligations)).requests;
			for (const requestId of requestIds)
				if (requests[requestId]) requests[requestId].fulfilled = true;
		}, context);
	}

	async function postAnswer(
		conversation: Conversation,
		answer: EntryId,
		requestId: string,
		covered: string[],
	) {
		const text = await answerOf(conversation, answer);
		if (!text?.trim()) return fulfil(covered);
		await deliver(`fabrial:pi:answer:${conversation.id}:${answer}`, text, requestId, covered);
		host.logger.debug("Posting final reply", { requestId, answer });
		await views.get(conversation.id)?.unfollow(requestId);
	}

	/**
	 * Posts whose outcome is not recorded yet, by execution id. A post's outcome is read once as it is tracked (on
	 * reopening, or as it starts) and again only once its `child` signal arrived, which also re-evaluates its
	 * conversation. One that settles without a signal wakes the parked Session through its execution's wait branch.
	 */
	const posts = new Map<
		string,
		{ requestId: string; key: string; conversationId: number; stale: boolean }
	>();
	function track(executionId: string, requestId: string, key: string, conversationId: number) {
		if (posts.has(executionId)) return;
		posts.set(executionId, { requestId, key, conversationId, stale: true });
		reportOf(executionId).promise.then(() => {
			const post = posts.get(executionId);
			if (post) post.stale = true;
			schedule();
		});
	}
	const reportedOutcome = (executionId: string) =>
		Promise.race([reportOf(executionId).promise, Promise.resolve(undefined)]);

	/**
	 * Record the outcomes of the posts that settled, of one conversation's obligations or all; returns the posts
	 * still pending. A post that failed for good is logged and marked failed: a call settles without it, and nothing
	 * reruns the model or posts an error reply for it.
	 */
	async function deliveriesPending(conversationId?: ConversationId): Promise<string[]> {
		const pending: string[] = [];
		for (const [executionId, post] of posts) {
			if (conversationId !== undefined && post.conversationId !== conversationId) continue;
			const reported = post.stale
				? (((await reportedOutcome(executionId)) as ChildSignal["result"] | undefined) ??
					(await host.conductor.getExecution(executionId).then((info) => info && settlement(info))))
				: undefined;
			const result = reported && (childOutcome(reported as JsonObject) as ExecutionResult);
			post.stale = false;
			if (!result) {
				pending.push(executionId);
				continue;
			}
			posts.delete(executionId);
			const { requestId, key } = post;
			const failed = result.status !== "completed";
			const error =
				result.status === "failed"
					? result.error
					: result.status === "cancelled"
						? result.reason
						: undefined;
			const changed = await harness.commit(async (tx) => {
				const entry = (await tx.doc(Obligations)).requests[requestId]!.deliveries![key]!;
				if (entry.status !== "pending") return false;
				entry.status = failed ? "failed" : "posted";
				if (error !== undefined) entry.error = error;
				return true;
			}, context);
			if (changed && failed)
				host.logger.error("Reply post failed", { requestId, delivery: key, executionId, error });
		}
		return pending;
	}

	// Input Pi gave up on: one error reply for a model error or no model, in the newest input's reply thread.
	async function settleUnanswered(
		conversation: Conversation,
		failures: { requestId: string; id: number; reason: string; detail: Json }[],
	) {
		for (const { requestId } of failures) await views.get(conversation.id)?.unfollow(requestId);
		const newest = failures.reduce((a, b) => (b.id > a.id ? b : a));
		const { requestId, reason, detail } = newest;
		const covered = failures.map((failure) => failure.requestId);
		const error =
			typeof detail === "string" ? detail : detail === null ? undefined : JSON.stringify(detail);
		if (!sentErrorReply(reason, error, "")) {
			host.logger.info("Sent input unanswered", { requestId, reason });
			return fulfil(covered);
		}
		const fresh = newErrorId();
		const errorId = await harness.commit(
			async (tx) => ((await tx.doc(Obligations)).requests[requestId]!.errorId ??= fresh),
			context,
		);
		// A stable intent and global dedupe key: a redelivered or restarted Session reuses the post.
		await deliver(
			`fabrial:pi:error:${requestId}`,
			sentErrorReply(reason, error, errorId)!,
			requestId,
			covered,
		);
		if (errorId === fresh)
			host.logger.error("Sent input failed", { requestId, reason, errorId, error });
	}

	/**
	 * Post each answer Pi settled once, to the thread of the newest input it answers (steering inputs share their run's
	 * answer), or the error reply there. A report its reporter has not submitted yet waits.
	 */
	async function postAnswers(
		conversation: Conversation,
	): Promise<{ settled: boolean; queued: boolean }> {
		let settled = true;
		let queued = false;
		const answered = new Map<EntryId, { requestId: string; id: number; covered: string[] }>();
		const failures: { requestId: string; id: number; reason: string; detail: Json }[] = [];
		const skipped: string[] = [];
		for (const [requestId, obligation] of Object.entries(await obligations())) {
			if (
				obligation.owes !== "reply" ||
				obligation.fulfilled ||
				obligation.conversationId !== conversation.id
			)
				continue;
			const record = await submissionOf(conversation.id, requestId);
			if (!record) continue;
			if (record.status !== "done" && record.status !== "unanswered") {
				settled = false;
				queued ||= record.status === "queued";
				continue;
			}
			if (record.status === "unanswered") {
				if (await replyThread(requestId))
					failures.push({
						requestId,
						id: record.id,
						reason: record.reason,
						detail: (record.detail ?? null) as Json,
					});
				else {
					host.logger.info("Sent input unanswered", { requestId, reason: record.reason });
					skipped.push(requestId);
				}
				continue;
			}
			if (record.type !== "input" || !(await replyThread(requestId))) {
				skipped.push(requestId);
				continue;
			}
			const newest = answered.get(record.answer);
			const covered = [...(newest?.covered ?? []), requestId];
			answered.set(
				record.answer,
				!newest || record.id > newest.id
					? { requestId, id: record.id, covered }
					: { ...newest, covered },
			);
		}
		await fulfil(skipped);
		for (const [answer, { requestId, covered }] of answered)
			await postAnswer(conversation, answer, requestId, covered);
		if (failures.length) await settleUnanswered(conversation, failures);
		return { settled, queued };
	}

	/** Settle a call: its outcome becomes its Session execution's result. */
	async function finishCall(conversationId: number, requestId: string, outcome: CallOutcome) {
		// A call that failed still settles its execution: the caller throws, and nothing retries the run.
		await harness.commit(async (tx) => {
			const obligation = (await tx.doc(Obligations)).requests[requestId]!;
			obligation.outcome = cloneJson(outcome);
			obligation.fulfilled = true;
		}, context);
		outcomes.set(requestId, outcome);
		const io = views.get(conversationId)?.io;
		await closeView(conversationId);
		if (io)
			await host
				.chat()
				?.setStatus(io, null)
				.catch(() => {});
	}

	/** Post what Pi answered; a call settles with the lead's answer only once every post of the conversation settled. */
	async function evaluate(conversationId: ConversationId) {
		const { conversation } = await hostedOf(conversationId);
		const sent = await postAnswers(conversation);
		let queued = sent.queued;
		const live = async () => (await harness.snapshot(LiveDoc, conversationId, context))?.run;
		for (const [requestId, obligation] of Object.entries(await obligations())) {
			if (
				obligation.owes !== "call" ||
				obligation.fulfilled ||
				obligation.conversationId !== conversationId
			)
				continue;
			const record = await submissionOf(conversationId, requestId);
			queued ||= record?.status === "queued";
			if (!record || !sent.settled) continue;
			if (record.status === "unanswered") {
				if (!(await deliveriesPending(conversationId)).length)
					await finishCall(conversationId, requestId, {
						status: "failed",
						error: `Pi run unanswered: ${record.reason}`,
						executionId: ctx.executionId,
					});
				continue;
			}
			if (
				record.status !== "done" ||
				(await live()) ||
				(await deliveriesPending(conversationId)).length
			)
				continue;
			// A call returns the lead's answer; its background children's reports arrive later as input.
			const answer =
				record.type === "input" ? await answerOf(conversation, record.answer) : undefined;
			await finishCall(conversationId, requestId, { status: "completed", output: answer ?? "" });
		}
		// Pi places queued input only at a boundary of a run or with the next submission. After a failed run nothing
		// is running, so an invisible write places what is still queued.
		if (queued && !(await live()))
			await conversation.submit({ type: "write", entry: { kind: "fabrial.wake" } }, context);
		if (!Object.values(await obligations()).some((each) => outstanding(each, conversationId)))
			await closeView(conversationId);
	}

	return { stop, obligations, submissionOf, replyThread, evaluate, deliveriesPending, track };
}

const UNREACHABLE =
	"I couldn't reach the model provider while handling your request. Please try again in a few minutes.";
const FAILED =
	"I hit an error while handling your request. Please try again, rephrase, or reach out if it keeps failing.";

/** Id of one failed sent request, shown in its error reply and logged with the provider's error text. */
export const newErrorId = () => randomBytes(6).toString("hex");

/**
 * The error reply for sent input Pi settled `unanswered`, or undefined for none. A model error whose text
 * is transient exhausted the durable retry budget; any other model error, or no model, is not retried.
 * Aborted, reset, and stale input was ended on purpose and gets no reply. The provider's text stays in logs.
 */
export function sentErrorReply(
	reason: string,
	detail: string | undefined,
	errorId: string,
): string | undefined {
	if (reason !== "model_error" && reason !== "no_model") return undefined;
	const transient =
		reason === "model_error" &&
		isRetryableAssistantError({ stopReason: "error", errorMessage: detail } as AssistantMessage);
	return `${transient ? UNREACHABLE : FAILED}\n\n_Error id: \`${errorId}\`_`;
}
