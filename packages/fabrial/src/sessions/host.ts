// The Session host: one keyed `fabrial.session` execution per Pi Session, hosting it while it has work.
import {
	type Conversation,
	type ConversationId,
	defineDoc,
	Harness,
	type HarnessSettings,
} from "@earendil-works/pi-durable";
import type { Models } from "@earendil-works/pi-ai";
import type { Sql } from "postgres";
import type { TaskContext } from "pgconductor-js";
import {
	awaitWithContext,
	BACKGROUND_CONTEXT,
	withAbortSignal,
	withContextValue,
} from "@earendil-works/chord/context";
import type { FabrialHost } from "../app.ts";
import { definedAgents } from "../agents/definition.ts";
import { agentRegistry } from "../agents/registry.ts";
import { type BridgeFrame, BridgeKey } from "../agents/tool-context.ts";
import { cloneJson, type Json, type JsonObject } from "../schema.ts";
import { SESSION_WORKFLOW, sessionDelivery, type SessionInput, signalSession } from "./delivery.ts";
import { type InvocationMetadata, metadataOf } from "../workflows/metadata.ts";
import {
	CHILD_SIGNAL,
	childOutcome,
	ChildResults,
	type ChildSignal,
	effectWorkflow,
	reportWorkflow,
} from "./workflows.ts";
import { isCancelled, nativeMethods, settlement } from "../execution.ts";
import { LeaseLost, PostgresStorage } from "./storage.ts";
import { type Progress, progressOf } from "../chat/status.ts";
import {
	Binding,
	type CallOutcome,
	Invocation,
	Obligations,
	outstanding,
	sessionObligations,
} from "./obligations.ts";
import { backgroundPending } from "../agents/subagents.ts";
import type { WorkflowTask } from "../conductor.ts";
import { reclaimWorkflow } from "./reclaim.ts";

export const Conversations = defineDoc({
	kind: "fabrial.conversations",
	scope: "session",
	version: 1,
	initial: (): { ids: Record<string, number> } => ({ ids: {} }),
});
/** How often the Session's execution suspended on its tools and inputs: names each wait. */
const Suspensions = defineDoc({
	kind: "fabrial.suspensions",
	scope: "session",
	version: 1,
	initial: (): { count: number } => ({ count: 0 }),
});

export interface PiOptions {
	models: Models;
	sql: Sql;
	settings?: Omit<HarnessSettings, "extensions">;
	/**
	 * Days a finished `ctx.agent` call's Pi Session is kept after its Session execution settled (completed, failed or
	 * cancelled), before `fabrial.sessions.reclaim` deletes it. Default 7.
	 */
	retainCallSessionsDays?: number;
	/** Cron schedule (UTC) of `fabrial.sessions.reclaim`, or `false` to not schedule it. Default `30 3 * * *`. */
	reclaimSchedule?: string | false;
	/** Call Sessions `fabrial.sessions.reclaim` examines per step. Default 100. */
	reclaimBatchSize?: number;
}

/** Pi's agents, their state and evaluation, connected to the host. Core calls it directly. */
export type Agents = ReturnType<typeof connectAgents>;

/** Connect Pi to the host. An app without agents touches no Pi storage and registers no Pi workflow. */
export function connectAgents(host: FabrialHost, options: PiOptions) {
	const scope = host.definitionScope ?? host;
	/** Whether the instance defines agents: decided when the app starts, after which none can be defined. */
	const used = () => !!definedAgents.get(scope)?.size;
	const { registry, agents, configs, noReply, internal, installAll } = agentRegistry(host);
	const active = new Map<BridgeFrame, Promise<void>>();
	let started = false;
	/**
	 * The Session execution: hosts the Pi Session while it has work. Once every live task waits on a workflow, or only
	 * the posts of its replies are pending, it closes Pi (freeing the lease) and suspends until input arrives or
	 * one of those workflows or posts settles; the next attempt reopens Pi, and the waiting tools run again with their
	 * workflows' outcomes. Only idempotent tools run again: while a non-idempotent tool runs, Pi stays open. Signals arriving while it opens or closes wait for the next opening (and win the wait); a
	 * signal delivered after the handler returned is sent again, which re-queues the execution.
	 *
	 * A `ctx.agent` call's Session execution is a child of its caller and returns the call's outcome. Cancelled while
	 * running, it aborts the call's Pi run before closing Pi; cancelled while suspended, nothing runs: its workflow tools
	 * are cancelled with it, and the call's Pi Session is left as it is (never reopened; reclaim deletes it later).
	 */
	async function hostSession(input: Json, ctx: TaskContext): Promise<Json> {
		if (!started) throw new Error("Start the Pi integration before running agents");
		const { session: key, call } = input as { session: string; call?: SessionInput };
		let phase: "opening" | "open" | "closing" | "returned" = "opening";
		let hosted: SessionHost | undefined;
		const late: { payload: SessionInput; id: string; done: (error?: unknown) => void }[] = [];
		// A `ctx.agent` call arrives as the input of its own Session's execution, a child of the caller.
		if (call?.kind === "call") late.push({ payload: call, id: call.requestId, done: () => {} });
		// Registered before any wait: a signal still being handled when the wait registers wins it.
		ctx.onSignal("input", async (payload, { id }) => {
			const input = payload as unknown as SessionInput;
			if (phase === "returned") {
				await signalSession(host, key, `${id}:requeued`, input, metadataOf(ctx));
				return;
			}
			if (phase === "open" && hosted) return hosted.handle(input);
			await new Promise<void>((resolve, reject) =>
				late.push({ payload: input, id, done: (error) => (error ? reject(error) : resolve()) }),
			);
		});
		// Committed outcomes reporters sent with their `child` signal, by reporter id: the waiting tool resumes in
		// process, also when the Session is busy with other work. Kept for this attempt, across reopenings.
		const reported = new Map<string, PromiseWithResolvers<JsonObject>>();
		const reportOf = (executionId: string) => {
			let entry = reported.get(executionId);
			if (!entry) reported.set(executionId, (entry = Promise.withResolvers<JsonObject>()));
			return entry;
		};
		// Returns at once: a signal arriving while the Session closes does not win its wait, but the reporter's execution
		// branch does once it settled. While suspended, the signal wins the wait.
		ctx.onSignal(CHILD_SIGNAL, (payload) => {
			const { executionId, result } = payload as unknown as ChildSignal;
			reportOf(executionId).resolve(result);
		});
		try {
			for (;;) {
				phase = "opening";
				const session = await openSession(key, ctx, reportOf);
				hosted = session;
				phase = "open";
				for (const each of late.splice(0))
					session.handle(each.payload).then(() => each.done(), each.done);
				let idle: Idle;
				let missed: boolean;
				try {
					idle = await session.idle;
				} finally {
					phase = "closing";
					hosted = undefined;
					missed = await session.close();
				}
				// Input handled after Pi went idle has not run yet.
				if (missed) continue;
				if (idle.kind === "parked") {
					const branches: Record<string, { signal: string } | { execution: string }> = {
						input: { signal: "input" },
						child: { signal: CHILD_SIGNAL },
					};
					for (const [taskId, executionId] of idle.children)
						branches[`task:${taskId}`] = { execution: executionId };
					for (const executionId of idle.deliveries)
						branches[`delivery:${executionId}`] = { execution: executionId };
					await nativeMethods(ctx).waitForAny(`idle:${idle.n}`, branches as never);
					continue;
				}
				if (late.length) continue;
				if (call?.kind !== "call") return null;
				const outcome = session.outcomes.get(call.requestId);
				if (!outcome) throw new Error(`Agent call ${call.requestId} ended without an outcome`);
				return outcome as unknown as Json;
			}
		} catch (error) {
			for (const each of late.splice(0)) each.done(error);
			throw error;
		} finally {
			phase = "returned";
		}
	}

	async function openSession(
		key: string,
		ctx: TaskContext,
		reportOf: (executionId: string) => PromiseWithResolvers<JsonObject>,
	): Promise<SessionHost> {
		// A crashed owner's lease lasts until it expires: SessionBusy
		// fails the attempt, and Conductor retries it with backoff (the Session workflow's attempts cover a lease).
		const storage = await PostgresStorage.open(options.sql, key, {
			executionId: ctx.executionId,
		});
		const signal = AbortSignal.any([ctx.signal, storage.lost.signal]);
		/** The agents' conversations this opening configured, by agent name. */
		const conversations = new Map<string, Hosted>();
		/** Progress of the conversations with outstanding obligations, by conversation id. */
		const views = new Map<number, Progress>();
		const outcomes = new Map<string, CallOutcome>();
		const frame: BridgeFrame = {
			host,
			ctx,
			models: options.models,
			harness: undefined as unknown as Harness,
			session: key,
			children: new Map(),
			unsafe: new Set(),
			waiting: new Set(),
			waitChild: (taskId, executionId, context) =>
				awaitWithContext(
					(async () => {
						const info = await host.conductor.getExecution(executionId);
						if (!info) throw new Error(`Execution ${executionId} not found`);
						const settled = settlement(info);
						// The reporter signals once the outcome is committed, and settles right after.
						const result = settled
							? childOutcome(cloneJson(settled) as unknown as JsonObject)
							: await reportOf(executionId).promise;
						return harness.commit(
							async (tx) => ((await tx.doc(ChildResults)).results[executionId] ??= result),
							context,
						);
					})(),
					context,
				).finally(() => frame.children.delete(taskId)),
		};
		const context = withContextValue(BridgeKey, frame, withAbortSignal(signal, BACKGROUND_CONTEXT));
		let queue: Promise<unknown> = Promise.resolve();
		const serial = <T>(fn: () => Promise<T>): Promise<T> => {
			const next = queue.then(fn);
			queue = next.catch(() => {});
			return next;
		};
		let inflight = 0;
		let scheduled = false;
		let closed = false;
		let settled = false;
		let missed = false;
		let resolveIdle!: (idle: Idle) => void;
		let rejectIdle!: (error: unknown) => void;
		const idle = new Promise<Idle>((resolve, reject) => {
			resolveIdle = (value) => {
				settled = true;
				resolve(value);
			};
			rejectIdle = reject;
		});
		idle.catch(() => {});
		const schedule = () => {
			if (scheduled || closed) return;
			scheduled = true;
			setImmediate(() => {
				scheduled = false;
				if (!closed) serial(tick).catch(rejectIdle);
			});
		};
		const onLost = () =>
			rejectIdle(
				ctx.signal.aborted ? ctx.signal.reason : new LeaseLost("Pi Session ownership lost"),
			);
		signal.addEventListener("abort", onLost, { once: true });
		let joined!: () => void;
		active.set(
			frame,
			new Promise<void>((resolve) => {
				joined = resolve;
			}),
		);
		try {
			frame.harness = await Harness.open(
				storage,
				{
					models: options.models,
					registry,
					settings: { ...options.settings, extensions: [internal] },
					onReport: (error) => host.logger.warn("Pi report", { error }),
				},
				context,
			);
		} catch (error) {
			signal.removeEventListener("abort", onLost);
			await storage.close(BACKGROUND_CONTEXT);
			active.delete(frame);
			joined();
			throw error;
		}
		const harness = frame.harness;
		const { stop, obligations, submissionOf, replyThread, evaluate, deliveriesPending, track } =
			sessionObligations({
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
			});

		async function conversationOf(agentName: string) {
			const known = conversations.get(agentName);
			if (known) return known;
			if (!agents.has(agentName))
				throw new Error(
					`Agent ${agentName} is not installed; defineAgent must be evaluated before app.start()`,
				);
			const ids = await harness.snapshot(Conversations, context);
			const id = ids?.ids[agentName] as ConversationId | undefined;
			let conversation = id ? await harness.conversation(id, context) : undefined;
			if (!conversation) {
				conversation = await harness.createConversation(
					{
						ownership: { kind: "ownerless" },
						init: async (tx, created) => {
							(await tx.doc(Conversations)).ids[agentName] = created;
						},
					},
					context,
				);
			}
			await conversation.configure(configs.get(agentName)!, context);
			const entry: Hosted = { conversation };
			conversations.set(agentName, entry);
			await offer(entry, await threaded(conversation.id));
			return entry;
		}

		/** An agent's conversation by id: obligations other than a child's task are owed on the agents' conversations. */
		async function hostedOf(conversationId: number) {
			const ids = (await harness.snapshot(Conversations, context))?.ids ?? {};
			const agentName = Object.keys(ids).find((name) => ids[name] === conversationId);
			if (!agentName) throw new Error(`Conversation ${conversationId} is not an agent's`);
			return conversationOf(agentName);
		}

		// Tools are not filtered by who is talking: a conversation can have many participants, and a tool that needs
		// permission asks its own approvers. `no_reply` is offered only while the conversation answers input with a reply
		// thread.
		async function offer(hosted: Hosted, withNoReply: boolean) {
			if (hosted.replying === withNoReply) return;
			await hosted.conversation.configure(
				{ tools: withNoReply ? null : { remove: [noReply] } },
				context,
			);
			hosted.replying = withNoReply;
		}

		/** Whether an outstanding obligation of the conversation answers into a reply thread. */
		async function threaded(conversationId: number) {
			for (const [requestId, obligation] of Object.entries(await obligations()))
				if (
					obligation.conversationId === conversationId &&
					!obligation.fulfilled &&
					(await replyThread(requestId))
				)
					return true;
			return false;
		}

		/** The progress of a conversation with outstanding obligations; a call's shows on the calling workflow's thread. */
		async function viewOf(conversation: Conversation, call?: InvocationMetadata) {
			let view = views.get(conversation.id);
			if (!view) {
				const io =
					call?.replyTo?.kind === "thread" && host.chat()
						? await host.chat()!.thread(call.replyTo)
						: undefined;
				view = await progressOf(host, conversation, io, harness, context);
				views.set(conversation.id, view);
			}
			return view;
		}

		async function closeView(conversationId: number) {
			const view = views.get(conversationId);
			if (!view) return;
			views.delete(conversationId);
			await view.stop();
		}

		// The invocation and obligation are committed before the submission, so a crash in between leaves them for the
		// redelivered signal, which submits it then. Pi owns ordering, steering, follow-ups and settlement from there.
		async function admit(input: Extract<SessionInput, { kind: "input" | "call" }>) {
			const { requestId, metadata } = input;
			const sent = input.kind === "input";
			const hosted = await conversationOf(input.agent);
			const { conversation } = hosted;
			const fulfilled = await harness.commit(async (tx) => {
				await tx.doc(Invocation, requestId, cloneJson(metadata));
				(await tx.doc(Binding, conversation.id)).requestId ??= requestId;
				const requests = (await tx.doc(Obligations)).requests;
				requests[requestId] ??= { conversationId: conversation.id, owes: sent ? "reply" : "call" };
				return !!requests[requestId].fulfilled;
			}, context);
			// Settled or stopped, or submitted already: Pi dedupes by request id too.
			if (fulfilled || (await submissionOf(conversation.id, requestId))) return;
			if (sent) {
				if (await replyThread(requestId)) await offer(hosted, true);
				await (await viewOf(conversation)).follow(requestId);
			} else await viewOf(conversation, metadata);
			const delivery = sent ? input.delivery : "steer";
			host.logger.debug("Submitting input", { requestId });
			await conversation.submit(
				delivery === "write"
					? {
							type: "write",
							requestId,
							entry: {
								kind: "fabrial.notification",
								model: [{ role: "user", content: input.content, timestamp: Date.now() }],
							},
						}
					: { type: "input", content: input.content, requestId, whenBusy: delivery },
				context,
			);
			host.logger.debug("Submitted input", { requestId });
		}

		const apply = (input: SessionInput) =>
			input.kind === "stop" ? stop(input.agent, input.reason) : admit(input);

		/**
		 * No work, or every running task waits on a workflow (or on a subagent that does) or on a reply's post. A running
		 * non-idempotent tool keeps Pi open even while it waits: its workflow's outcome reaches it in process.
		 */
		async function idleState(): Promise<Idle["kind"] | undefined> {
			if (inflight) return undefined;
			const inspection = await harness.inspect(context);
			const live = inspection.tasks.filter((task) => !task.record.background);
			if (!live.length && !inspection.submissions.length) {
				if (await backgroundPending(harness, context)) return undefined;
				return (await deliveriesPending()).length ? "parked" : "done";
			}
			const runners = live.filter((task) => task.state.kind === "running");
			if (runners.some((task) => frame.unsafe.has(task.record.id))) return undefined;
			const workflow = (id: number) => frame.children.has(id);
			const parked = (id: number) => workflow(id) || frame.waiting.has(id);
			return runners.some((task) => workflow(task.record.id)) &&
				live.every(
					(task) =>
						task.state.kind === "waiting" ||
						task.state.kind === "completing" ||
						(task.state.kind === "running" && parked(task.record.id)),
				)
				? "parked"
				: undefined;
		}

		async function tick() {
			const pending = new Set<number>();
			for (const obligation of Object.values(await obligations()))
				if (outstanding(obligation)) pending.add(obligation.conversationId);
			for (const conversationId of pending) await evaluate(conversationId as ConversationId);
			if (scheduled || settled) return;
			const state = await idleState();
			if (state === undefined) return;
			const deliveries = state === "parked" ? await deliveriesPending() : [];
			// A post that settled meanwhile (its commit schedules a pass) re-evaluates its conversation first: its signal is
			// spent, so a wait would not see it.
			if (scheduled) return;
			if (state === "done") return resolveIdle({ kind: "done" });
			// Every wait has its own name, so a resumed execution never reuses an earlier wait's winner.
			const n = await harness.commit(async (tx) => (await tx.doc(Suspensions)).count++, context);
			resolveIdle({
				kind: "parked",
				n,
				children: new Map(
					[...frame.children].map(([taskId, child]) => [taskId, child.executionId]),
				),
				deliveries,
			});
		}

		// Commit listeners only schedule a pass; they must not call Session APIs.
		const unsubscribe = harness.subscribeCommits((publication) => {
			if (publication.changes.length) schedule();
		});
		try {
			for (const [requestId, obligation] of Object.entries(await obligations())) {
				if (obligation.outcome) outcomes.set(requestId, obligation.outcome);
				for (const [key, delivery] of Object.entries(obligation.deliveries ?? {}))
					if (delivery.status === "pending")
						track(delivery.executionId, requestId, key, obligation.conversationId);
				if (!outstanding(obligation)) continue;
				// Outstanding work shows its progress again; an answered request whose post is still pending does not.
				const { conversation } = await hostedOf(obligation.conversationId);
				if (obligation.owes === "call")
					await viewOf(
						conversation,
						(await harness.snapshot(Invocation, requestId, context)) as InvocationMetadata,
					);
				else if (!obligation.origin) {
					const record = await submissionOf(conversation.id, requestId);
					if (record?.status === "queued" || record?.status === "placed")
						await (await viewOf(conversation)).follow(requestId);
				}
			}
			harness.resume();
		} catch (error) {
			rejectIdle(error);
		}
		schedule();
		return {
			idle,
			outcomes,
			async handle(input) {
				if (settled) missed = true;
				inflight++;
				try {
					await serial(() => apply(input));
				} finally {
					inflight--;
					schedule();
				}
			},
			async close() {
				closed = true;
				unsubscribe();
				signal.removeEventListener("abort", onLost);
				try {
					// A cancelled call (its caller was cancelled or failed) aborts its Pi work before Pi closes, so no further
					// model calls happen. The Session is the call's own, so the abort reaches its background children too.
					// Best effort: a call cancelled while suspended runs nothing, and its Session is never reopened (its
					// workflow tools are cancelled with the Session's execution).
					if (isCancelled(ctx.signal))
						for (const obligation of Object.values(
							(await harness.snapshot(Obligations, BACKGROUND_CONTEXT))?.requests ?? {},
						))
							if (obligation.owes === "call" && !obligation.fulfilled)
								await harness
									.conversation(obligation.conversationId as ConversationId, BACKGROUND_CONTEXT)
									.then((conversation) =>
										conversation?.abort(BACKGROUND_CONTEXT, { background: true }),
									)
									.catch((error: unknown) =>
										host.logger.warn("Could not abort a cancelled agent call", { error }),
									);
					await queue;
					for (const view of views.values()) await view.stop();
				} finally {
					try {
						await harness.close(BACKGROUND_CONTEXT);
					} finally {
						await storage.close(BACKGROUND_CONTEXT);
						// Include status cleanup in the shutdown join: Chat must stay connected until it is done.
						active.delete(frame);
						joined();
					}
				}
				return missed;
			},
		};
	}

	const session: WorkflowTask = {
		name: SESSION_WORKFLOW,
		triggers: [],
		// Retries back off 15s, 30s, 1m, …: the first attempts outlast a crashed owner's 30s lease.
		retries: { maxAttempts: 10 },
		// No retentionDays, so kept forever: callers wait on a call's result and replay from it, and reclaim reads when
		// it settled. A finite retention must outlast both, and retainCallSessionsDays.
		handler: (input, ctx) => hostSession(input, ctx),
	};
	const reclaim = reclaimWorkflow(host, options);
	const delivery = sessionDelivery(host, agents);
	const effect = effectWorkflow(host, agents, delivery);
	return {
		...delivery,
		/** The instance's Pi AI models: `ctx.evaluate` classifies with them too. */
		models: options.models,
		/** Pi's workflows, if the instance defines agents (core registers them when the app starts). */
		get workflows(): WorkflowTask[] {
			return used() ? [session, effect, reportWorkflow, reclaim] : [];
		},
		async start() {
			if (!used()) return;
			await PostgresStorage.migrate(options.sql);
			installAll(scope);
			started = true;
		},
		async stop() {
			started = false;
			// Orchestrator stop aborts the drivers, but native workers race abort against
			// handlers rather than joining their finally blocks. Join the entire
			// cleanup, not just Harness.close, before core shuts down Chat.
			await Promise.all(active.values());
		},
	};
}

/**
 * Pi has no work left (`done`), or every live task waits on a workflow (`parked`, by task id) or Pi waits only on
 * the posts of its replies (`deliveries`, by execution id).
 */
type Idle =
	| { kind: "done" }
	| { kind: "parked"; n: number; children: Map<number, string>; deliveries: string[] };

/** An agent's conversation in an opened Session: the tools it offers, and whether `no_reply` is among them. */
export interface Hosted {
	conversation: Conversation;
	replying?: boolean;
}

interface SessionHost {
	handle(input: SessionInput): Promise<void>;
	/** Resolves when Pi is idle; rejects when the lease is lost. */
	idle: Promise<Idle>;
	/** Closes Pi; true when input was handled after `idle` resolved, so Pi has work again. */
	close(): Promise<boolean>;
	/** Outcomes of the calls this Session finished, by request id. */
	outcomes: Map<string, CallOutcome>;
}
