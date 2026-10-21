import type { Context } from "@earendil-works/chord";
import {
	awaitWithContext,
	BACKGROUND_CONTEXT,
	withAbortSignal,
	withContextValue,
} from "@earendil-works/chord/context";
import type { Models, TSchema } from "@earendil-works/pi-ai";
import {
	createRegistry,
	defineExtension,
	defineTool as piTool,
	GenerationTask,
	Harness,
	hook,
	section as piSection,
	ToolTask,
	watchEvents,
	type AgentChange,
	type Conversation,
	type ConversationId,
	type Extension,
	type HarnessSettings,
	type ToolRegistration,
	LiveDoc,
	type EntryId,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type AgentPort,
	type AgentRef,
	type DurableExecution,
	CHILD_SIGNAL,
	type ChildSignal,
	ExecutionError,
	isCancelled,
	type WaitBranch,
	type FabrialHost,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type RuntimeWorkflow,
	type Schema,
	type StatePort,
	type ThreadIO,
} from "../modules.ts";
import {
	definedAgents,
	isWorkflow,
	ownToolNames,
	parseModel,
	resolveSubagents,
	toolName,
	type Agent,
	type AgentObservers,
	type ThinkingLevel,
	type Toolkit,
} from "../agent.ts";
import type { AnyWorkflow } from "../workflow.ts";
import { instructionsSection, piToolOf, toJsonSchema } from "./tools.ts";
import {
	abortBackground,
	backgroundPending,
	reportedAnswer,
	stopTool,
	subagentsSection,
	subagentTasks,
	subagentTool,
	type SubagentDeps,
} from "./subagents.ts";
import type { Sql } from "postgres";
import {
	agentOf,
	BridgeKey,
	cancelChildren,
	cloneJson,
	currentRequest,
	fields,
	registerChild,
	noReplyTool,
	startChild,
	type BridgeFrame,
} from "./context.ts";
import {
	follow,
	prepareSend,
	threadSession,
	conversationReference,
	type ConversationKey,
	type Delivery,
	type SendOptions,
	type SendRequest,
} from "../send.ts";
import { followResource, unfollowResource, type StoredFollowOptions } from "../follows.ts";
import type { ResourceRefJson } from "../plugin.ts";
import {
	applyState,
	Binding,
	ChildResults,
	Conversations,
	Invocation,
	Obligations,
	stateDraft,
	stateRevision,
	Suspensions,
	WorkflowReceipts,
	EFFECT_WORKFLOW,
	type CallOutcome,
	type Obligation,
} from "./documents.ts";
import { newErrorId, sentErrorReply, WAITING_STATUS } from "./errors.ts";
import { createEvaluator } from "./evaluate.ts";
import { LeaseLost, PostgresStorage } from "./storage.ts";
import { definedStates, type StateDefinition } from "../state.ts";
import { duration } from "../internal.ts";
import type { AgentIntegration, Logger } from "../app.ts";

const SESSION_WORKFLOW = "fabrial.session";

/** Whether something is still owed for an obligation, on `conversationId` if given; a child's task owes nothing. */
const outstanding = (obligation: Obligation, conversationId?: number) =>
	!obligation.fulfilled &&
	obligation.owes !== "task" &&
	(conversationId === undefined || obligation.conversationId === conversationId);

/** The signals a Session execution receives, all named `input`; ids dedupe them. */
type SessionInput =
	| {
			kind: "input";
			agent: string;
			requestId: string;
			content: string;
			delivery: Delivery;
			metadata: InvocationMetadata;
	  }
	| {
			kind: "call";
			agent: string;
			requestId: string;
			content: string;
			metadata: InvocationMetadata;
	  }
	| { kind: "stop"; agent: string; reason: string }
	| StateRequest;

/**
 * A workflow's `ctx.state` operation, handled by the Session's host: a read, or a replacement of the value read at
 * `revision`. `operation` is stable across resends and dedupes it; the response event carries it back.
 */
type StateRequest = {
	kind: "state";
	operation: string;
	state: string;
	write?: { value: JsonObject; revision: number };
};
/** The response to a `StateRequest`: the value at its revision, a conflict (the revision moved on), or an error. */
type StateResponse =
	| { value: JsonObject; revision: number }
	| { conflict: true; revision: number }
	| { error: string };
const STATE_EVENT = "fabrial.state.response";
/** A workflow resends an unanswered state request (same operation) this often before failing. */
const STATE_REQUESTS = 5;

/** The Pi Session of one `ctx.agent` call, private to it: keyed by the calling execution and the call's id. */
export const callSession = (executionId: string, id: string) =>
	`${CALL_SESSION}${executionId}:${id}`;
const CALL_SESSION = "run:";

/** The Session of a workflow's `ctx.state`: its thread's, else its own. */
export function sessionKey(metadata: InvocationMetadata): string {
	const surface = metadata.replyTo;
	if (surface?.kind === "thread") return threadSession(surface.provider, surface.threadId);
	if (surface?.kind === "channel")
		throw new Error(
			"Post to the channel with ctx.thread.post before using Pi; a channel is not a Session key",
		);
	return metadata.requestId;
}

const THINKING: Record<ThinkingLevel, "off" | "low" | "medium" | "high"> = {
	off: "off",
	low: "low",
	medium: "medium",
	high: "high",
};

/** Fabrial observers as Pi hooks on the built-in generation and tool tasks. */
function observerHooks(observers: AgentObservers[], host: FabrialHost) {
	const notify = async (run: (observer: AgentObservers) => void | Promise<void> | undefined) => {
		for (const observer of observers)
			try {
				await run(observer);
			} catch (error) {
				host.logger.warn("Agent observer failed", { error });
			}
	};
	return [
		hook(GenerationTask, {
			async afterResponse(message, api, context) {
				const agent = await agentOf(api, api.conversationId, context);
				await notify((observer) =>
					observer.onModelResponse?.({
						agent,
						model: { provider: message.provider, modelId: message.model },
						text: message.content
							.flatMap((block) => (block.type === "text" ? [block.text] : []))
							.join("\n"),
						toolCalls: message.content.flatMap((block) =>
							block.type === "toolCall"
								? [{ id: block.id, name: block.name, arguments: block.arguments as JsonObject }]
								: [],
						),
						stopReason: message.stopReason,
						...(message.errorMessage ? { error: message.errorMessage } : {}),
						...(message.usage
							? {
									usage: {
										input: message.usage.input,
										output: message.usage.output,
										totalTokens: message.usage.totalTokens,
									},
								}
							: {}),
					}),
				);
			},
		}),
		hook(ToolTask, {
			async afterTool(call, result, api, context) {
				const agent = await agentOf(api, api.conversationId, context);
				await notify((observer) =>
					observer.onToolCall?.({
						agent,
						callId: call.id,
						name: call.name,
						arguments: call.arguments as JsonObject,
						result: (result.content ?? [])
							.flatMap((block) => (block.type === "text" ? [block.text] : []))
							.join("\n"),
						isError: !!result.isError,
					}),
				);
				return undefined;
			},
		}),
	];
}

export interface PiOptions {
	models: Models;
	sql: Sql;
	settings?: Omit<HarnessSettings, "extensions">;
	/** How long a workflow's `ctx.state` waits for its Session to answer before it resends the request. */
	stateTimeoutMs?: number;
	/** How often a workflow's state update may conflict (the state changed between its read and write) before it fails. Default 20. */
	stateConflicts?: number;
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

const RECLAIM_WORKFLOW = "fabrial.sessions.reclaim";

export function pi(options: PiOptions): AgentIntegration {
	return { kind: "fabrial.agents", connect: (host) => connect(options, host) };
}

function connect(options: PiOptions, host: FabrialHost): ReturnType<AgentIntegration["connect"]> {
	const registry = createRegistry();
	const evaluator = createEvaluator(options.models);
	const agents = new Map<string, Agent>();
	const configs = new Map<string, AgentChange>();
	const active = new Map<BridgeFrame, Promise<void>>();
	let started = false;
	/** Workflow state resolves by name against this instance's definitions, on both sides of the Session's signal. */
	const stateOf = (name: string): StateDefinition => {
		const definition = definedStates.get(host.definitionScope ?? host)?.get(name);
		if (!definition)
			throw new Error(
				`State ${name} is not defined on this Fabrial instance; define it with defineState`,
			);
		if (definition.scope !== "session")
			throw new Error(
				`Agent state ${name} requires a Pi conversation; use it inside an agent tool`,
			);
		return definition;
	};
	const observers = host.plugins.flatMap((plugin) =>
		plugin.hooks?.agent ? [plugin.hooks.agent] : [],
	);
	const noReply = noReplyTool();
	const subagentDeps: SubagentDeps = {
		childAgent,
		logger: host.logger,
		busy: new Map(),
	};
	const internal = defineExtension({
		name: "fabrial.internal",
		tools: [noReply],
		// Background subagent tasks resume after a restart wherever a Session is opened.
		tasks: subagentTasks(subagentDeps),
		hooks: observers.length ? observerHooks(observers, host) : [],
	});
	registry.install(internal);
	const pluginToolkits = new Map<string, Extension>();
	const appToolkits = new Map<string, { toolkit: Toolkit; extension: Extension }>();
	const workflowTools = new WeakMap<ToolRegistration, AnyWorkflow>();
	/** Each agent's extensions without, and its subagent tools as, an extension. */
	const extensionsOf = new Map<string, { base: Extension[]; subagents?: Extension }>();
	/** Workflow tools drop out until their workflow is registered. */
	const registered = (tool: ToolRegistration) => {
		const supplied = workflowTools.get(tool);
		return !supplied || host.workflows.some((candidate) => candidate.name === supplied.name);
	};

	/** A child's agent: its own definition, subagent tools only with levels left, and never `no_reply`. */
	function childAgent(agent: Agent, depth: number): AgentChange {
		const { model, thinking } = agent.definition;
		const parts = extensionsOf.get(agent.name)!;
		const extensions = [...parts.base, ...(depth > 0 && parts.subagents ? [parts.subagents] : [])];
		const tools = new Map<string, ToolRegistration>();
		for (const extension of extensions)
			for (const tool of extension.tools ?? []) tools.set(tool.name, tool);
		return {
			// Unset fields keep the copy of the lead's agent the child starts with.
			...(model ? { model: parseModel(model) } : {}),
			...(thinking ? { thinkingLevel: THINKING[thinking] } : {}),
			instructions: null,
			extensions,
			tools: [...tools.values()].filter((tool) => tool !== noReply && registered(tool)),
		};
	}

	function tools(owner: string, list: Toolkit["tools"] = []): ToolRegistration[] {
		return list.map((tool) => (isWorkflow(tool) ? workflowTool(tool, owner) : piToolOf(tool)));
	}

	function toolkit(entry: string | Toolkit, agent: string): Extension {
		if (typeof entry === "string") {
			const resolved = pluginToolkits.get(entry);
			if (!resolved)
				throw new Error(`Agent ${agent} selects plugin ${entry}, which provides no toolkit`);
			return resolved;
		}
		const existing = appToolkits.get(entry.name);
		if (existing && existing.toolkit !== entry)
			throw new Error(`Duplicate Fabrial toolkit name: ${entry.name}`);
		if (existing) return existing.extension;
		const section = instructionsSection(`toolkit-${entry.name}`, entry.instructions);
		const extension = defineExtension({
			name: `fabrial.toolkit.${entry.name}`,
			tools: tools(`toolkit ${entry.name}`, entry.tools),
			sections: section ? [section] : [],
		});
		appToolkits.set(entry.name, { toolkit: entry, extension });
		registry.install(extension);
		return extension;
	}

	function install(agent: Agent): void {
		const previous = agents.get(agent.name);
		if (previous && previous !== agent)
			throw new Error(`Duplicate Fabrial agent name: ${agent.name}`);
		agents.set(agent.name, agent);
		const { name, model, thinking, instructions, state = [] } = agent.definition;
		const selected = (agent.definition.toolkits ?? []).map((entry) => toolkit(entry, name));
		const subagents = resolveSubagents(agent);
		const taken = new Set([
			...ownToolNames(agent.definition),
			...selected.flatMap((extension) => (extension.tools ?? []).map((tool) => tool.name)),
		]);
		for (const subagent of subagents) {
			if (taken.has(subagent.tool))
				throw new Error(
					`Agent ${name}: subagent ${subagent.agent.name} is called ${subagent.tool}, which a toolkit tool already uses`,
				);
			if (subagent.agent !== agent) install(subagent.agent);
		}
		const section = instructionsSection("agent-instructions", instructions);
		const own = defineExtension({
			name: `fabrial.agent.${name}`,
			tools: tools(`agent ${name}`, agent.definition.tools),
			sections: [
				...(section ? [section] : []),
				...state
					.filter((definition) => definition.render)
					.map((definition) =>
						piSection(
							`state-${definition.name.replaceAll(/[^a-z0-9_-]/g, "-")}`,
							async (input, context) => {
								const ctx = await fields(context, input.conversationId);
								return definition.render!(await ctx.state(definition).get());
							},
						),
					),
			],
		});
		registry.install(own);
		const delegates = subagents.length
			? defineExtension({
					name: `fabrial.subagents.${name}`,
					tools: [
						...subagents.map((subagent) => subagentTool(agent, subagent, subagentDeps)),
						stopTool(),
					],
					sections: [subagentsSection],
				})
			: undefined;
		if (delegates) registry.install(delegates);
		extensionsOf.set(name, { base: [internal, ...selected, own], subagents: delegates });
		configs.set(name, {
			model: model ? parseModel(model) : null,
			thinkingLevel: thinking ? THINKING[thinking] : null,
			instructions: null,
			extensions: [internal, ...selected, own, ...(delegates ? [delegates] : [])],
			tools: null,
		});
	}

	function workflowTool(workflow: AnyWorkflow, owner: string): ToolRegistration {
		const { input, description } = workflow.definition;
		const native = piTool({
			name: toolName(workflow.name),
			description: description ?? workflow.name,
			// Core validates the input again when the workflow starts.
			parameters: (input
				? toJsonSchema(input, `Workflow ${workflow.name} (a tool of ${owner})`)
				: { type: "object", properties: {}, additionalProperties: true }) as unknown as TSchema,
			replay: "safe",
			execute: async (input, api, context) => {
				const frame = context.value(BridgeKey);
				if (!frame) throw new Error("Workflow tools require a Fabrial bridge");
				const ctx = await fields(context, api.conversationId, api);
				const child = await ctx.start("workflow", workflow, input as never);
				const result = (await frame.harness.snapshot(ChildResults, context))?.results[child];
				if (result)
					return {
						content: [
							{
								type: "text",
								text: JSON.stringify(result.status === "completed" ? result.output : result),
							},
						],
						isError: result.status !== "completed",
					};
				await registerChild(
					frame,
					await currentRequest(frame, api.conversationId, context),
					api.taskId,
					{ executionId: child, detached: false },
					context,
				);
				const settled = await frame.waitChild(api.taskId, child, context);
				return {
					content: [
						{
							type: "text",
							text: JSON.stringify(settled.status === "completed" ? settled.output : settled),
						},
					],
					isError: settled.status !== "completed",
				};
			},
		});
		workflowTools.set(native, workflow);
		return native;
	}

	async function thread(execution: DurableExecution): Promise<ThreadIO | undefined> {
		return execution.metadata.replyTo && host.chat()
			? host.chat()!.resolve(execution.metadata.replyTo)
			: undefined;
	}

	/** Metadata of a Session execution: it serves many requests, so it carries none of theirs. */
	const sessionMetadata = (key: string): Partial<InvocationMetadata> => ({
		replyTo: null,
		ownerWorkflow: null,
		triggerEvent: null,
		piSession: key,
	});

	/** Every input reaches the Session's execution as a signal, starting one when the Session is closed. */
	const signalSession = (
		key: string,
		id: string,
		payload: SessionInput,
		metadata: InvocationMetadata,
	) =>
		host.runtime.signalWithStart({
			workflow: SESSION_WORKFLOW,
			key,
			signal: { name: "input", id, payload: payload as unknown as Json },
			input: { session: key },
			metadata: { ...metadata, ...sessionMetadata(key) } as InvocationMetadata,
		});

	/**
	 * The Session execution: hosts the Pi Session while it has work. Once every live task waits on a workflow, or only
	 * the posts of its replies are pending, it closes Pi (freeing the lease) and suspends until input arrives or
	 * one of those workflows or posts settles; the next attempt reopens Pi, and the waiting tools run again with their
	 * workflows' outcomes. Signals arriving while it opens or closes wait for the next opening (and win the wait); a
	 * signal delivered after the handler returned is sent again, which re-queues the execution.
	 *
	 * A `ctx.agent` call's Session execution is a child of its caller and returns the call's outcome. Cancelled while
	 * running, it aborts the call's Pi run before closing Pi; cancelled while suspended, nothing runs: its workflow tools
	 * are cancelled with it, and the call's Pi Session is left as it is (never reopened; reclaim deletes it later).
	 */
	async function hostSession(input: Json, execution: DurableExecution): Promise<Json> {
		if (!started) throw new Error("Start the Pi integration before running agents");
		const { session: key, call } = input as { session: string; call?: SessionInput };
		let phase: "opening" | "open" | "closing" | "returned" = "opening";
		let hosted: SessionHost | undefined;
		const late: { payload: SessionInput; id: string; done: (error?: unknown) => void }[] = [];
		// A `ctx.agent` call arrives as the input of its own Session's execution, a child of the caller.
		if (call?.kind === "call") late.push({ payload: call, id: call.requestId, done: () => {} });
		// Registered before any wait: a signal still being handled when the wait registers wins it.
		execution.onSignal("input", async (payload, { id }) => {
			const input = payload as unknown as SessionInput;
			if (phase === "returned") {
				await signalSession(key, `${id}:requeued`, input, execution.metadata);
				return;
			}
			if (phase === "open" && hosted) return hosted.handle(input);
			await new Promise<void>((resolve, reject) =>
				late.push({ payload: input, id, done: (error) => (error ? reject(error) : resolve()) }),
			);
		});
		// Outcomes children sent with their `child` signal, by execution id: the waiting tool resumes in process, also
		// when the Session is busy with other work. Kept for this attempt, across reopenings.
		const reported = new Map<string, PromiseWithResolvers<JsonObject>>();
		const reportOf = (executionId: string) => {
			let entry = reported.get(executionId);
			if (!entry) reported.set(executionId, (entry = Promise.withResolvers<JsonObject>()));
			return entry;
		};
		// Returns at once: a signal arriving while the Session closes does not win its wait, but the child's execution
		// branch does once its outcome is committed. While suspended, the signal wins the wait.
		execution.onSignal(CHILD_SIGNAL, (payload) => {
			const { executionId, result } = payload as unknown as ChildSignal;
			reportOf(executionId).resolve(result);
		});
		try {
			for (;;) {
				phase = "opening";
				const session = await openSession(key, execution, reportOf);
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
					const branches: Record<string, WaitBranch> = {
						input: { signal: "input" },
						child: { signal: CHILD_SIGNAL },
					};
					for (const [taskId, executionId] of idle.children)
						branches[`task:${taskId}`] = { execution: executionId };
					for (const executionId of idle.deliveries)
						branches[`delivery:${executionId}`] = { execution: executionId };
					await execution.waitForAny(`idle:${idle.n}`, branches);
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

	/**
	 * Pi has no work left (`done`), or every live task waits on a workflow (`parked`, by task id) or Pi waits only on
	 * the posts of its replies (`deliveries`, by execution id).
	 */
	type Idle =
		| { kind: "done" }
		| { kind: "parked"; n: number; children: Map<number, string>; deliveries: string[] };

	/** An agent's conversation in an opened Session: the tools it offers, and whether `no_reply` is among them. */
	interface Hosted {
		conversation: Conversation;
		offered: readonly ToolRegistration[];
		replying?: boolean;
	}
	type View = Awaited<ReturnType<typeof progressOf>>;

	interface SessionHost {
		handle(input: SessionInput): Promise<void>;
		/** Resolves when Pi is idle; rejects when the lease is lost. */
		idle: Promise<Idle>;
		/** Closes Pi; true when input was handled after `idle` resolved, so Pi has work again. */
		close(): Promise<boolean>;
		/** Outcomes of the calls this Session finished, by request id. */
		outcomes: Map<string, CallOutcome>;
	}

	async function openSession(
		key: string,
		execution: DurableExecution,
		reportOf: (executionId: string) => PromiseWithResolvers<JsonObject>,
	): Promise<SessionHost> {
		// A crashed owner's lease lasts until it expires: SessionBusy
		// fails the attempt, and Conductor retries it with backoff (the Session workflow's attempts cover a lease).
		const storage = await PostgresStorage.open(options.sql, key, {
			executionId: execution.executionId,
		});
		const signal = AbortSignal.any([execution.signal, storage.lost.signal]);
		/** The agents' conversations this opening configured, by agent name. */
		const conversations = new Map<string, Hosted>();
		/** Progress of the conversations with outstanding obligations, by conversation id; the newest shows child status. */
		const views = new Map<number, View>();
		let latest: View | undefined;
		const outcomes = new Map<string, CallOutcome>();
		const frame: BridgeFrame = {
			host,
			execution,
			evaluator,
			harness: undefined as unknown as Harness,
			session: key,
			children: new Map(),
			waiting: new Set(),
			waitChild: (taskId, executionId, context) =>
				awaitWithContext(
					(async () => {
						const settled = await host.runtime.getExecution(executionId);
						if (!settled) throw new Error(`Execution ${executionId} not found`);
						// The child's signal can arrive before its outcome is committed: it carries the outcome.
						const result = settled.result
							? (cloneJson(settled.result) as unknown as JsonObject)
							: await reportOf(executionId).promise;
						return harness.commit(
							async (tx) => ((await tx.doc(ChildResults)).results[executionId] ??= result),
							context,
						);
					})(),
					context,
				).finally(() => frame.children.delete(taskId)),
			status: async (text) => latest?.status(text),
			clearStatus: async (label) => latest?.clearStatus(label),
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
				execution.signal.aborted
					? execution.signal.reason
					: new LeaseLost("Pi Session ownership lost"),
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
					{ ownership: { kind: "ownerless" } },
					context,
				);
				const created = conversation.id;
				await harness.commit(async (tx) => {
					(await tx.doc(Conversations)).ids[agentName] = created;
				}, context);
			}
			await conversation.configure(configs.get(agentName)!, context);
			const entry: Hosted = { conversation, offered: (await conversation.agent(context)).tools };
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
		// permission asks its own approvers. Only unregistered workflows drop out, and `no_reply` is offered only while
		// the conversation answers input with a reply thread.
		async function offer(hosted: Hosted, withNoReply: boolean) {
			if (hosted.replying === withNoReply) return;
			await hosted.conversation.configure(
				{
					tools: hosted.offered.filter((tool) =>
						tool === noReply ? withNoReply : registered(tool),
					),
				},
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
					call?.replyTo?.kind === "thread"
						? await thread({ metadata: call } as DurableExecution)
						: undefined;
				view = await progressOf(conversation, io, harness, context);
				views.set(conversation.id, view);
			}
			latest = view;
			return view;
		}

		async function closeView(conversationId: number) {
			const view = views.get(conversationId);
			if (!view) return;
			views.delete(conversationId);
			if (latest === view) latest = [...views.values()].at(-1);
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

		/**
		 * A stop in a followed thread aborts the agent's conversation through Pi: its queued inputs are withdrawn and its
		 * live task tree is aborted. The workflows its outstanding requests' tools started are cancelled, and background
		 * children acting for its requests are aborted past the background boundary. Nothing more is owed for its
		 * requests, nor for reports on them. Calls have their own Session and belong to their workflow.
		 */
		async function stop(agentName: string, reason: string) {
			const lead = (await harness.snapshot(Conversations, context))?.ids[agentName];
			const conversation =
				lead === undefined
					? undefined
					: await harness.conversation(lead as ConversationId, context);
			if (!conversation) return;
			const owed = await obligations();
			const requests = new Set(
				Object.keys(owed).filter(
					(id) => owed[id]!.conversationId === lead && owed[id]!.owes === "reply",
				),
			);
			const outstanding = [...requests].filter((id) => !owed[id]!.fulfilled);
			await cancelChildren(
				harness,
				(executionId, why) => host.runtime.cancel(executionId, why),
				new Set(outstanding.flatMap((id) => [id, owed[id]!.origin ?? id])),
				reason,
				context,
			);
			await abortBackground(harness, requests, context);
			await conversation.abort(context);
			await harness.commit(async (tx) => {
				for (const [requestId, obligation] of Object.entries((await tx.doc(Obligations)).requests))
					if (requests.has(requestId) || requests.has(obligation.origin ?? ""))
						obligation.fulfilled = true;
			}, context);
			await closeView(conversation.id);
		}

		/**
		 * A workflow's state operation, between Pi's passes: commits its response with its receipt (a write only if the
		 * state is still at the revision it read), then emits it. A resent operation gets its receipt again.
		 */
		async function stateOperation(input: StateRequest) {
			let response: StateResponse;
			try {
				const definition = stateOf(input.state);
				const replacement = input.write
					? await parseSchema(definition.schema, input.write.value, `state ${definition.name}`)
					: undefined;
				response = await harness.commit(async (tx) => {
					const receipt = await tx.doc(WorkflowReceipts, input.operation, "");
					if (receipt.value !== null) return cloneJson(receipt.value) as StateResponse;
					const revision = await stateRevision(tx, definition);
					const result: StateResponse = !input.write
						? { value: cloneJson(await stateDraft(tx, definition)), revision }
						: input.write.revision !== revision
							? { conflict: true, revision }
							: {
									value: await applyState(tx, definition, undefined, [() => replacement!]),
									revision: revision + 1,
								};
					receipt.value = cloneJson(result) as JsonObject;
					return result;
				}, context);
			} catch (error) {
				if (signal.aborted) throw error;
				response = { error: error instanceof Error ? error.message : String(error) };
			}
			await execution.emit(STATE_EVENT, { operation: input.operation, ...response } as JsonObject, {
				metadata: sessionMetadata(key),
			});
		}

		const apply = (input: SessionInput) =>
			input.kind === "stop"
				? stop(input.agent, input.reason)
				: input.kind === "state"
					? stateOperation(input)
					: admit(input);

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
					obligation?.owes === "reply" && invocation?.send && invocation.replyTo
						? invocation
						: null,
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
				false,
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
				const result = post.stale
					? (((await reportedOutcome(executionId)) as ChildSignal["result"] | undefined) ??
						(await host.runtime.getExecution(executionId))?.result)
					: undefined;
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
		 * Fulfil the conversation's outstanding reply obligations (sent input and background reports on it) that Pi
		 * settled: each answer posts once, to the thread of the newest input it answers (steering inputs share their run's
		 * answer). Input that failed gets the error reply there instead. Returns whether every submitted one settled, and
		 * whether one is still queued. One not submitted yet (a report its reporter still submits) waits.
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
			await io?.setStatus(null).catch(() => {});
		}

		/**
		 * Reconcile one conversation's outstanding obligations with Pi's submissions: post what Pi answered, and settle a
		 * call once its answer is final. A call that failed settles at once; one that answered settles once its background
		 * children reported and the lead answered the last report, and every post of the conversation settled.
		 */
		async function evaluate(conversationId: ConversationId) {
			const { conversation } = await hostedOf(conversationId);
			// Read before the reports: a reporter records its report as it ends, so one that ended by now is among them.
			const background = await backgroundPending(harness, context);
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
							executionId: execution.executionId,
						});
					continue;
				}
				if (
					record.status !== "done" ||
					background ||
					(await live()) ||
					(await deliveriesPending(conversationId)).length
				)
					continue;
				// A call returns its own answer, or the lead's answer to its children's last report.
				const answer =
					(await reportedAnswer(harness, submissionOf, requestId, conversationId, context)) ??
					(record.type === "input" ? await answerOf(conversation, record.answer) : undefined);
				await finishCall(conversationId, requestId, { status: "completed", output: answer ?? "" });
			}
			// Pi places queued input only at a boundary of a run or with the next submission. After a failed run nothing
			// is running, so an invisible write places what is still queued.
			if (queued && !(await live()))
				await conversation.submit({ type: "write", entry: { kind: "fabrial.wake" } }, context);
			if (!Object.values(await obligations()).some((each) => outstanding(each, conversationId)))
				await closeView(conversationId);
		}

		/** No work, or every running task waits on a workflow (or on a subagent that does) or on a reply's post. */
		async function idleState(): Promise<Idle["kind"] | undefined> {
			if (inflight) return undefined;
			const inspection = await harness.inspect(context);
			const live = inspection.tasks.filter((task) => !task.record.background);
			if (!live.length && !inspection.submissions.length) {
				if (await backgroundPending(harness, context)) return undefined;
				return (await deliveriesPending()).length ? "parked" : "done";
			}
			const runners = live.filter((task) => task.state.kind === "running");
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
				// A state operation gives Pi no work.
				if (settled && input.kind !== "state") missed = true;
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
					if (isCancelled(execution.signal))
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

	/**
	 * Progress of a conversation: on the thread a call owns (`io`), or on the reply thread of the latest sent request it
	 * follows. Shows "Thinking…", tool activity, a subagent's activity, and "Waiting for the model provider…" while a
	 * generation backs off.
	 */
	async function progressOf(
		conversation: Conversation,
		io: ThreadIO | undefined,
		harness: Harness,
		context: Context,
	) {
		let progress = io;
		let progressRequest: string | undefined;
		let lastStatus = 0;
		let waiting = false;
		let childStatus: string | undefined;
		const shown = new Set<ThreadIO>();
		const showStatus = async (text: string) => {
			if (!progress) return;
			shown.add(progress);
			lastStatus = Date.now();
			await progress.setStatus(text).catch(() => {});
		};
		const thinking = () => showStatus(waiting ? WAITING_STATUS : "Thinking…");
		const events = await watchEvents(harness, conversation.id, context);
		waiting = !!events.snapshot.generation?.retry;
		events.start(async (batch) => {
			const retry = batch.findLast(
				(event) => event.type === "auto_retry_start" || event.type === "auto_retry_end",
			);
			if (retry) {
				waiting = retry.type === "auto_retry_start";
				await thinking();
				return;
			}
			if (waiting || !progress || Date.now() - lastStatus < 750) return;
			// A subagent call's details update; the child shows its own activity.
			if (batch.every((event) => event.type === "tool_execution_update")) return;
			const tool = batch.find((event) => event.type === "tool_execution_start");
			const text =
				tool?.type === "tool_execution_start"
					? ((await conversation.agent(context)).tools.find((each) => each.name === tool.toolName)
							?.description ?? "Using a tool…")
					: "Thinking…";
			await showStatus(text);
		});
		await thinking();
		return {
			io,
			async follow(requestId: string) {
				const replyTo = (await harness.snapshot(Invocation, requestId, context))?.replyTo;
				if (replyTo?.kind !== "thread" || !host.chat()) return;
				progress = await host.chat()!.resolve(replyTo);
				progressRequest = requestId;
				await thinking();
			},
			async unfollow(requestId: string) {
				if (progressRequest !== requestId) return;
				await progress?.setStatus(null).catch(() => {});
				progress = undefined;
			},
			async status(text: string) {
				if (waiting || text === childStatus) return;
				childStatus = text;
				await showStatus(text);
			},
			// A child that answered gives the status back to the lead: its own activity, or none when idle.
			async clearStatus(label: string) {
				if (!childStatus?.startsWith(`${label}: `)) return;
				childStatus = undefined;
				if (waiting) return;
				if ((await harness.snapshot(LiveDoc, conversation.id, context))?.run) await thinking();
				else await progress?.setStatus(null).catch(() => {});
			},
			async stop() {
				await events.stop();
				for (const surface of shown)
					if (surface !== io) await surface.setStatus(null).catch(() => {});
				shown.clear();
			},
		};
	}

	const session: RuntimeWorkflow = {
		name: SESSION_WORKFLOW,
		triggers: [],
		// Retries back off 15s, 30s, 1m, …: the first attempts outlast a crashed owner's 30s lease.
		retries: { maxAttempts: 10 },
		// No retentionDays, so kept forever: callers wait on a call's result and replay from it, and reclaim reads when
		// it settled. A finite retention must outlast both, and retainCallSessionsDays.
		handler: hostSession,
	};
	const retainDays = options.retainCallSessionsDays ?? 7;
	if (!(retainDays >= 0)) throw new RangeError("retainCallSessionsDays must not be negative");
	const batchSize = options.reclaimBatchSize ?? 100;
	if (!(batchSize >= 1)) throw new RangeError("reclaimBatchSize must be at least 1");
	/**
	 * Deletes the Pi storage of `ctx.agent` calls (`run:` Sessions, never a thread's) whose Session execution settled at
	 * least `retainCallSessionsDays` ago: nothing reopens them once the caller stored the outcome, and a call cancelled
	 * while suspended runs no cleanup of its own. Settlement is read with `getExecution` from the execution that last
	 * hosted the Session; a Session leased, never hosted, or whose execution is unknown is kept. One step per batch, in
	 * key order.
	 */
	const reclaim: RuntimeWorkflow = {
		name: RECLAIM_WORKFLOW,
		triggers: [],
		cron:
			options.reclaimSchedule === false
				? []
				: [{ schedule: options.reclaimSchedule ?? "30 3 * * *", name: RECLAIM_WORKFLOW }],
		concurrency: 1,
		// Nothing waits on it.
		retentionDays: 30,
		handler: async (_input, execution) => {
			const cutoff = await execution.step(
				"cutoff",
				async () => (host.runtime.now?.() ?? Date.now()) - retainDays * 86_400_000,
			);
			let after = CALL_SESSION;
			let reclaimed = 0;
			let batches = 0;
			for (;;) {
				const batch = (await execution.step(`batch:${batches++}`, async () => {
					const candidates = await PostgresStorage.reclaimable(
						options.sql,
						CALL_SESSION,
						after,
						batchSize,
					);
					let count = 0;
					for (const { key, executionId } of candidates) {
						const settled = await host.runtime.getExecution(executionId);
						if (settled?.workflow !== SESSION_WORKFLOW || !settled.result) continue;
						if (settled.settledAt === undefined || settled.settledAt > cutoff) continue;
						if (await PostgresStorage.reclaim(options.sql, key, executionId)) count++;
					}
					return {
						reclaimed: count,
						next: candidates.length < batchSize ? null : candidates.at(-1)!.key,
					};
				})) as { reclaimed: number; next: string | null };
				reclaimed += batch.reclaimed;
				if (batch.next === null) break;
				after = batch.next;
			}
			if (reclaimed) host.logger.info("Reclaimed finished agent call Sessions", { reclaimed });
			return { reclaimed, batches };
		},
	};
	// Effects run in a stable child execution, never in the Session's replaceable execution.
	const effect: RuntimeWorkflow = {
		name: EFFECT_WORKFLOW,
		triggers: [],
		handler: async (input, execution) => {
			const { method, args } = input as { method: string; args: Json[] };
			if (method === "follow" || method === "unfollow") {
				const [resource, options] = args as [ResourceRefJson, StoredFollowOptions];
				const surface = {
					current: execution.metadata.replyTo,
					channelId: async () => {
						const io = await thread(execution);
						if (!io) throw new Error(`Managed Pi ${method} has no chat surface`);
						return io.channelId;
					},
				};
				const apply = method === "follow" ? followResource : unfollowResource;
				return execution.step(`fabrial:pi:${method}`, () =>
					apply(host, execution.metadata, surface, resource as never, options),
				);
			}
			if (method === "send") {
				const [agent, options] = args as [string, SendOptions];
				const ref = agents.get(agent) ?? { kind: "fabrial.agent" as const, name: agent };
				const request = await execution.step("fabrial:pi:send", async () => {
					const prepared = await prepareSend(
						host,
						execution.metadata,
						{
							current: execution.metadata.replyTo,
							channelId: async () => {
								const io = await thread(execution);
								if (!io) throw new Error("Managed Pi send has no chat surface");
								return io.channelId;
							},
						},
						`${execution.executionId}:send`,
						ref,
						options,
					);
					await follow(host, prepared.follow, { session: prepared.session, agent });
					return prepared;
				});
				return port.send(execution, "send", ref, request);
			}
			return execution.step("fabrial:pi:effect", async () => {
				const io = await thread(execution);
				if (!io) throw new Error("Managed Pi thread operation has no thread");
				if (method === "post") {
					const posted = await io.post(args[0] as Parameters<ThreadIO["post"]>[0]);
					// A reply to a channel home created the conversation's thread: follow it.
					const conversation = execution.metadata.follow as ConversationKey | undefined;
					if (conversation && execution.metadata.replyTo?.kind === "channel")
						await follow(
							host,
							{ kind: "thread", provider: posted.provider, threadId: posted.threadId },
							conversation,
						);
					return cloneJson(posted) as unknown as Json;
				}
				if (method === "update") {
					await io.update(
						args[0] as unknown as Parameters<ThreadIO["update"]>[0],
						args[1] as Parameters<ThreadIO["update"]>[1],
					);
					return null;
				}
				if (method === "history")
					return cloneJson(await io.history(args[0] as Parameters<ThreadIO["history"]>[0]));
				throw new Error(`Unknown Pi effect ${method}`);
			});
		},
	};
	const sessionInput = (agent: AgentRef, request: SendRequest): SessionInput => ({
		kind: "input",
		agent: agent.name,
		requestId: request.requestId,
		content: request.content,
		delivery: request.delivery,
		metadata: request.metadata,
	});
	const port: AgentPort = {
		workflows: () => [session, effect],
		async stop(conversation, id, reason) {
			await signalSession(
				conversation.session,
				`fabrial.stop:${id}:${conversation.agent}`,
				{ kind: "stop", agent: conversation.agent, reason },
				{ requestId: id, origin: null, replyTo: null, requestedBy: null },
			);
		},
		async deliver(agent, request) {
			if (!agents.has(agent.name))
				throw new Error(
					`Agent ${agent.name} is not installed; defineAgent must be evaluated before app.start()`,
				);
			await signalSession(
				request.session,
				request.requestId,
				sessionInput(agent, request),
				request.metadata,
			);
			return {
				requestId: request.requestId,
				conversation: conversationReference(agent.name, request.session),
			};
		},
		async send(execution, id, agent, request) {
			const input = sessionInput(agent, request);
			await execution.signalWithStart(`fabrial:pi:deliver:${id}`, {
				workflow: SESSION_WORKFLOW,
				key: request.session,
				signal: { name: "input", id: request.requestId, payload: input as unknown as Json },
				input: { session: request.session },
				metadata: sessionMetadata(request.session),
			});
			return {
				requestId: request.requestId,
				conversation: conversationReference(agent.name, request.session),
			};
		},
		async run(execution, id, agent: AgentRef, request) {
			// A call has its own Session, keyed by the workflow run: it never shares a transcript with a thread's
			// conversation and never follows a thread. Its Session execution is a child of the caller, so cancelling or
			// failing the caller cancels it (and the workflows its tools started), unless detached.
			const metadata = execution.metadata;
			const key = callSession(execution.executionId, id);
			const requestId = `${execution.executionId}:${id}`;
			const content = await execution.step(`fabrial:pi:origin:${id}`, async () => {
				const sections = metadata.origin
					? await Promise.all(
							host.plugins.map(async (plugin) =>
								plugin.context?.(metadata.origin!, host.clients()),
							),
						)
					: [];
				return [
					sections.filter(Boolean).join("\n\n"),
					typeof request.input === "string" ? request.input : JSON.stringify(request.input ?? null),
					request.output
						? "Return your final answer as a single JSON value, without Markdown fences or surrounding prose."
						: "",
				]
					.filter(Boolean)
					.join("\n\n");
			});
			const call: SessionInput = {
				kind: "call",
				agent: agent.name,
				requestId,
				content,
				metadata: cloneJson({ ...metadata, piSession: key }),
			};
			const outcome = (await execution.invoke(
				`fabrial:pi:call:${id}`,
				SESSION_WORKFLOW,
				{ session: key, call } as unknown as Json,
				{
					metadata: sessionMetadata(key),
					detached: !!request.detached,
					...(request.timeout !== undefined ? { timeoutMs: duration(request.timeout) } : {}),
				},
			)) as unknown as CallOutcome;
			if (outcome.status !== "completed")
				throw new ExecutionError(outcome.executionId, { status: "failed", error: outcome.error });
			const answer = outcome.output ?? null;
			if (!request.output) return answer;
			return execution.step(`fabrial:pi:output:${id}`, async () =>
				parseSchema(
					request.output as Schema<Json>,
					JSON.parse(typeof answer === "string" ? answer : JSON.stringify(answer)),
					`agent ${agent.name} output`,
				),
			);
		},
	};
	/**
	 * Sends a state request to the Session's host through its input signal, subscribed to the response first; the
	 * host's signal handling wakes a suspended Session and starts a closed one. Unanswered, the same operation is sent
	 * again, and the host answers it from its receipt.
	 */
	async function stateRequest(
		execution: DurableExecution,
		id: string,
		key: string,
		request: Omit<StateRequest, "kind" | "operation">,
	): Promise<Exclude<StateResponse, { error: string }>> {
		const operation = `${execution.executionId}:${id}`;
		const payload: StateRequest = { kind: "state", operation, ...request };
		for (let n = 0; n < STATE_REQUESTS; n++) {
			const at = n ? `${id}:${n}` : id;
			const response = await execution.subscribe(`${at}:response`, {
				event: STATE_EVENT,
				filter: { operation: [operation] },
			});
			await execution.signalWithStart(`${at}:request`, {
				workflow: SESSION_WORKFLOW,
				key,
				signal: {
					name: "input",
					id: `fabrial.state:${operation}${n ? `:${n}` : ""}`,
					payload: payload as unknown as Json,
				},
				input: { session: key },
				metadata: sessionMetadata(key),
			});
			const won = await execution.waitForAny(
				`${at}:wait`,
				{ response },
				{ timeoutMs: options.stateTimeoutMs ?? 60_000 },
			);
			if (won.kind !== "event") continue;
			const answer = won.event.payload as unknown as StateResponse;
			if ("error" in answer) throw new Error(answer.error);
			return answer;
		}
		throw new Error(
			`State ${request.state}: Session ${key} did not answer operation ${id} after ${STATE_REQUESTS} requests`,
		);
	}
	const state: StatePort = {
		get: async (execution, id, definition) => {
			const key = sessionKey(execution.metadata);
			stateOf(definition.name);
			const answer = await stateRequest(execution, id, key, { state: definition.name });
			if ("conflict" in answer) throw new Error(`State ${definition.name}: a read cannot conflict`);
			return answer.value as ReturnType<typeof definition.initial>;
		},
		/**
		 * Optimistic: reads the value and its revision, applies `change` here (closures stay in the workflow), and
		 * replaces the value if nothing changed it since; otherwise it reads again, up to `stateConflicts` times.
		 */
		update: async (execution, id, definition, change) => {
			const key = sessionKey(execution.metadata);
			stateOf(definition.name);
			const conflicts = options.stateConflicts ?? 20;
			for (let n = 0; n < conflicts; n++) {
				const at = n ? `${id}:retry:${n}` : id;
				const read = await stateRequest(execution, `${at}:read`, key, { state: definition.name });
				if (!("value" in read)) throw new Error(`State ${definition.name}: a read cannot conflict`);
				const value = await execution.step(`${at}:apply`, async () => {
					const draft = cloneJson(read.value) as ReturnType<typeof definition.initial>;
					return parseSchema(
						definition.schema,
						change(draft) ?? draft,
						`state ${definition.name}`,
					) as unknown as Json;
				});
				const written = await stateRequest(execution, `${at}:write`, key, {
					state: definition.name,
					write: { value: value as JsonObject, revision: read.revision },
				});
				if ("value" in written) return written.value as ReturnType<typeof definition.initial>;
			}
			throw new Error(
				`State ${definition.name}: update ${id} conflicted ${conflicts} times; the state kept changing between its read and its write`,
			);
		},
	};
	return {
		agents: port,
		state,
		evaluator,
		workflows: [session, effect, reclaim],
		events: [{ name: STATE_EVENT, filterable: ["operation"] }],
		async start() {
			await PostgresStorage.migrate(options.sql);
			for (const plugin of host.plugins)
				if (plugin.toolkit) {
					const built = plugin.toolkit(host.clients());
					const section = instructionsSection(`plugin-${plugin.id}`, built.instructions);
					const extension = defineExtension({
						name: `fabrial.plugin.${plugin.id}`,
						tools: tools(`plugin ${plugin.id}`, built.tools),
						sections: section ? [section] : [],
					});
					pluginToolkits.set(plugin.id, extension);
					registry.install(extension);
				}
			for (const agent of definedAgents.get(host.definitionScope ?? host) ?? []) install(agent);
			await abortOrphans(
				options.sql,
				registry
					.snapshot()
					.tasks()
					.map((task) => task.definition.name),
				host.logger,
			);
			started = true;
		},
		async stop() {
			started = false;
			// Runtime stop aborts the drivers, but native workers race abort against
			// handlers rather than joining their finally blocks. Join the entire
			// cleanup, not just Harness.close, before core shuts down Chat.
			await Promise.all(active.values());
		},
	};
}

/**
 * Aborts live Pi tasks whose kind is no longer installed (e.g. a removed toolkit), using Pi's abort cascade: a startup
 * policy of the deployment. The commit log only finds candidate Sessions (the latest record of such a task is not
 * terminal), without taking leases; each is then opened and Pi's own inspection decides what is live. A Session held
 * by another owner is skipped with a warning.
 */
export async function abortOrphans(
	sql: Sql,
	installedKinds: readonly string[],
	logger: Logger,
): Promise<number> {
	const candidates = await sql<{ session_key: string }[]>`
		SELECT DISTINCT session_key FROM (
			SELECT DISTINCT ON (c.session_key, w.value->'value'->>'id') c.session_key, w.value->'value' AS task
			FROM fabrial_pi.commits c, jsonb_array_elements(c.writes::jsonb) WITH ORDINALITY AS w(value, n)
			WHERE w.value->>'type' = 'task'
			ORDER BY c.session_key, w.value->'value'->>'id', c.seq DESC, w.n DESC
		) latest
		WHERE task->'state'->>'status' <> 'terminal' AND NOT (task->>'kind' = ANY(${[...installedKinds]}))
		ORDER BY session_key`;
	let aborted = 0;
	for (const { session_key: key } of candidates) {
		const counts = new Map<string, number>();
		try {
			const storage = await PostgresStorage.open(sql, key);
			const { createModels } = await import("@earendil-works/pi-ai/models");
			const harness = await Harness.open(
				storage,
				{ models: createModels(), registry: createRegistry() },
				BACKGROUND_CONTEXT,
			);
			try {
				const orphans = (await harness.inspect(BACKGROUND_CONTEXT)).tasks
					.map((task) => task.record)
					.filter((task) => !installedKinds.includes(task.kind));
				for (const task of orphans) {
					await harness.abortTask(task.id, BACKGROUND_CONTEXT);
					await harness.waitForTask(task.id, BACKGROUND_CONTEXT);
					counts.set(task.kind, (counts.get(task.kind) ?? 0) + 1);
					aborted++;
				}
			} finally {
				await harness.close(BACKGROUND_CONTEXT);
			}
		} catch (error) {
			logger.warn("Could not abort orphaned agent work", { session: key, error });
		}
		for (const [kind, count] of counts)
			logger.warn("Aborted orphaned agent work", { session: key, kind, count });
	}
	return aborted;
}
