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
	createSession,
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
	type Submission,
	type SubmissionId,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type AgentPort,
	type AgentRef,
	AGENT_SETTLED_EVENT,
	type DurableExecution,
	type ExecutionResult,
	type FabrialHost,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type RuntimeWorkflow,
	type Schema,
	type StatePort,
	type ThreadIO,
} from "../index.ts";
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
	BridgeKey,
	cloneJson,
	currentRequest,
	fields,
	registerChild,
	noReplyTool,
	startChild,
	type BridgeFrame,
} from "./context.ts";
import { prepareSend, threadSession, type Delivery, type SendOptions } from "../send.ts";
import {
	prepareSubscribe,
	prepareUnsubscribe,
	type StoredSubscribeOptions,
} from "../subscriptions.ts";
import type { ResourceRefJson } from "../plugin.ts";
import {
	applyState,
	Binding,
	ChildResults,
	Conversations,
	Invocation,
	Runs,
	SubagentWork,
	WorkflowReceipts,
	ChildIntents,
	EFFECT_WORKFLOW,
} from "./documents.ts";
import { newErrorId, sentErrorReply, WAITING_STATUS } from "./errors.ts";
import { createEvaluator } from "./evaluate.ts";
import { LeaseLost, PostgresStorage, SessionBusy } from "./storage.ts";
import type { AgentIntegration, Logger } from "../app.ts";

const SESSION_WORKFLOW = "fabrial.session";

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
			caller: string;
			detached: boolean;
	  }
	| { kind: "replies"; interactionId: string }
	| { kind: "child"; executionId: string; result: ExecutionResult }
	| { kind: "cancel"; caller: string; reason: string };

/** A run of one conversation in the hosted Session: the requests it answers, until Pi has finished them. */
interface HostedRun {
	id: string;
	agent: string;
	conversation: Conversation;
	call?: { requestId: string; metadata: InvocationMetadata };
	submissions: Map<string, Submission>;
	/** Sent requests, whose answers post to their reply threads; the rest are the call's own. */
	sent: Set<string>;
	unanswered: Set<string>;
	/** Answers posted (or skipped) already, by answer entry id. */
	posted: Set<EntryId>;
	/** Requests with their reply thread's invocation, or null for none. */
	threads: Map<string, InvocationMetadata | null>;
	started: boolean;
	/** Whether `no_reply` is offered. */
	replying: boolean;
	/** The thread a call owns. */
	io?: ThreadIO;
	ui?: {
		follow(requestId: string): Promise<void>;
		unfollow(requestId: string): Promise<void>;
		status(text: string): Promise<void>;
		clearStatus(label: string): Promise<void>;
		stop(): Promise<void>;
	};
}
export function sessionKey(metadata: InvocationMetadata): string {
	const surface = metadata.replyTo;
	if (surface?.kind === "thread") return threadSession(surface.provider, surface.threadId);
	if (surface?.kind === "channel")
		throw new Error(
			"Post to the channel with ctx.thread.post before using Pi; a channel is not a Session key",
		);
	return metadata.interactionId;
}

/**
 * Takes the Session lease, retrying while another owner holds it. Two uses remain, both unavoidable: the Session's
 * execution opening after a crashed owner (its lease lasts until it expires) or while `ctx.state` holds the lease
 * briefly; and `ctx.state` from a workflow, which waits until the hosting execution releases the Session (it does
 * once every live task waits on a workflow).
 */
async function acquire(sql: Sql, key: string, signal: AbortSignal): Promise<PostgresStorage> {
	for (;;) {
		signal.throwIfAborted();
		try {
			return await PostgresStorage.open(sql, key);
		} catch (error) {
			if (!(error instanceof SessionBusy)) throw error;
			await new Promise<void>((resolve) => setTimeout(resolve, 100));
		}
	}
}

const THINKING: Record<ThinkingLevel, "off" | "low" | "medium" | "high"> = {
	off: "off",
	low: "low",
	medium: "medium",
	high: "high",
};

/** Fabrial observers as Pi hooks on the built-in generation and tool tasks. */
function observerHooks(
	observers: AgentObservers[],
	agentName: (conversationId: number) => string,
	host: FabrialHost,
) {
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
			async afterResponse(message, api) {
				await notify((observer) =>
					observer.onModelResponse?.({
						agent: agentName(api.conversationId),
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
			async afterTool(call, result, api) {
				await notify((observer) =>
					observer.onToolCall?.({
						agent: agentName(api.conversationId),
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
}

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
	const observers = host.plugins.flatMap((plugin) =>
		plugin.hooks?.agent ? [plugin.hooks.agent] : [],
	);
	const noReply = noReplyTool();
	const subagentDeps: SubagentDeps = {
		childAgent,
		named: (conversationId, agent) => conversationAgents.set(conversationId, agent),
		logger: host.logger,
		busy: new Map(),
	};
	const internal = defineExtension({
		name: "fabrial.internal",
		tools: [noReply],
		// Background subagent tasks resume after a restart wherever a Session is opened.
		tasks: subagentTasks(subagentDeps),
		hooks: observers.length ? observerHooks(observers, agentNameOf, host) : [],
	});
	registry.install(internal);
	const pluginToolkits = new Map<string, Extension>();
	const appToolkits = new Map<string, { toolkit: Toolkit; extension: Extension }>();
	const workflowTools = new WeakMap<ToolRegistration, AnyWorkflow>();
	const conversationAgents = new Map<number, string>();
	function agentNameOf(conversationId: number): string {
		return conversationAgents.get(conversationId) ?? "unknown";
	}
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
					(await frame.runOf(api.conversationId, context)).id,
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
		ownsThread: false,
		ownerWorkflow: null,
		triggerEvent: null,
		piSession: key,
		piChild: null,
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
	 * The Session execution: hosts the Pi Session while it has work. Signals arriving while it opens or closes wait
	 * for the next opening; a signal delivered after the handler returned is sent again, which re-queues the execution.
	 */
	async function hostSession(input: Json, execution: DurableExecution): Promise<Json> {
		if (!started) throw new Error("Start the Pi integration before running agents");
		const key = (input as { session: string }).session;
		let phase: "opening" | "open" | "closing" | "returned" = "opening";
		let hosted: SessionHost | undefined;
		const late: { payload: SessionInput; id: string; done: (error?: unknown) => void }[] = [];
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
		try {
			for (;;) {
				phase = "opening";
				const session = await openSession(key, execution);
				hosted = session;
				phase = "open";
				for (const each of late.splice(0))
					session.handle(each.payload).then(() => each.done(), each.done);
				try {
					await session.idle;
				} finally {
					phase = "closing";
					hosted = undefined;
					await session.close();
				}
				if (!late.length) return null;
			}
		} catch (error) {
			for (const each of late.splice(0)) each.done(error);
			throw error;
		} finally {
			phase = "returned";
		}
	}

	interface SessionHost {
		handle(input: SessionInput): Promise<void>;
		/** Resolves when Pi has no work left but waits on workflows; rejects when the lease is lost. */
		idle: Promise<void>;
		close(): Promise<void>;
	}

	async function openSession(key: string, execution: DurableExecution): Promise<SessionHost> {
		// A crashed owner's lease lasts until it expires, and `ctx.state` takes the lease briefly.
		const storage = await acquire(options.sql, key, execution.signal);
		const signal = AbortSignal.any([execution.signal, storage.lost.signal]);
		const waiters = new Map<string, ((result: JsonObject) => void)[]>();
		const runs = new Map<string, HostedRun>();
		const conversations = new Map<
			string,
			{ conversation: Conversation; offered: readonly ToolRegistration[] }
		>();
		const latest = () => [...runs.values()].findLast((run) => run.started);
		const frame: BridgeFrame = {
			host,
			execution,
			evaluator,
			harness: undefined as unknown as Harness,
			session: key,
			children: new Map(),
			waiting: new Set(),
			runOf: (conversationId, context) => runOf(frame, conversationId, context),
			waitChild: (taskId, executionId, context) =>
				awaitWithContext(
					new Promise<JsonObject>((resolve) => {
						waiters.set(executionId, [...(waiters.get(executionId) ?? []), resolve]);
						// A result recorded before this waiter registered.
						void frame.harness.snapshot(ChildResults, context).then(
							(doc) => {
								const result = doc?.results[executionId];
								if (result) resolve(result);
							},
							() => {},
						);
					}),
					context,
				).finally(() => frame.children.delete(taskId)),
			status: async (text) => latest()?.ui?.status(text),
			clearStatus: async (label) => latest()?.ui?.clearStatus(label),
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
		let resolveIdle!: () => void;
		let rejectIdle!: (error: unknown) => void;
		const idle = new Promise<void>((resolve, reject) => {
			resolveIdle = resolve;
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
			conversationAgents.set(conversation.id, agentName);
			await conversation.configure(configs.get(agentName)!, context);
			const entry = { conversation, offered: (await conversation.agent(context)).tools };
			conversations.set(agentName, entry);
			return entry;
		}

		async function attach(id: string, agentName: string, call?: HostedRun["call"]) {
			const { conversation } = await conversationOf(agentName);
			const run: HostedRun = {
				id,
				agent: agentName,
				conversation,
				call,
				submissions: new Map(),
				sent: new Set(),
				unanswered: new Set(),
				posted: new Set(),
				threads: new Map(),
				started: false,
				replying: false,
			};
			runs.set(id, run);
			return run;
		}

		async function createRun(
			id: string,
			agentName: string,
			call?: { requestId: string; caller: string; detached: boolean; metadata: InvocationMetadata },
		) {
			const run = await attach(id, agentName, call);
			await harness.commit(async (tx) => {
				(await tx.doc(Runs)).runs[id] ??= {
					conversationId: run.conversation.id,
					agent: agentName,
					children: {},
					...(call
						? { call: { requestId: call.requestId, caller: call.caller, detached: call.detached } }
						: {}),
				};
			}, context);
			return run;
		}

		// Persist admission intents before submit; re-opening submits the same durable request set.
		async function admit(
			run: HostedRun,
			requestId: string,
			content: string,
			metadata: InvocationMetadata,
			delivery: Delivery,
			sent: boolean,
		) {
			await harness.commit(async (tx) => {
				await tx.doc(Invocation, requestId, cloneJson(metadata));
				(await tx.doc(Binding, run.conversation.id)).requestId ??= requestId;
				const record = (await tx.doc(Runs)).runs[run.id]!;
				record.requests ??= {};
				record.requests[requestId] ??= {
					content,
					submissionId: null,
					delivery,
					...(sent ? { sent } : {}),
				};
			}, context);
		}

		const admitted = async (requestId: string) =>
			Object.values((await harness.snapshot(Runs, context))?.runs ?? {}).some(
				(run) => run.requests?.[requestId],
			);
		const current = (agentName: string) =>
			[...runs.values()].find((run) => run.agent === agentName);

		async function apply(input: SessionInput) {
			switch (input.kind) {
				case "input": {
					// Pi dedupes by request id too; this keeps a redelivery from opening an empty run.
					if (await admitted(input.requestId)) return;
					const run =
						current(input.agent) ?? (await createRun(`send:${input.requestId}`, input.agent));
					await admit(run, input.requestId, input.content, input.metadata, input.delivery, true);
					if (run.started) await submitPending(run);
					return;
				}
				case "call": {
					if (await admitted(input.requestId)) return;
					const run = await createRun(input.requestId, input.agent, input);
					await admit(run, input.requestId, input.content, input.metadata, "steer", false);
					return;
				}
				case "replies": {
					for (const run of runs.values())
						if (run.started && run.io && run.call?.metadata.interactionId === input.interactionId)
							await drainReplies(run);
					return;
				}
				case "child": {
					const result = cloneJson(input.result) as unknown as JsonObject;
					await harness.commit(async (tx) => {
						(await tx.doc(ChildResults)).results[input.executionId] ??= result;
					}, context);
					for (const resolve of waiters.get(input.executionId) ?? []) resolve(result);
					waiters.delete(input.executionId);
					return;
				}
				case "cancel": {
					// Durable records, not hosted runs: a run whose agent is gone is cancelled too.
					const records = (await harness.snapshot(Runs, context))?.runs ?? {};
					for (const [id, record] of Object.entries(records)) {
						if (record.done || record.call?.caller !== input.caller || record.call.detached)
							continue;
						await cancelRun(harness, id, input.reason);
						const run = runs.get(id);
						if (run) await finish(run, undefined);
						else
							await harness.commit(async (tx) => {
								(await tx.doc(Runs)).runs[id]!.done = true;
							}, context);
					}
				}
			}
		}

		// Tools are not filtered by who is talking: a conversation can have many participants, and a tool that needs
		// permission asks its own approvers. Only unregistered workflows drop out, and `no_reply` is offered only to
		// runs that answer input with a reply thread.
		async function offer(run: HostedRun, withNoReply: boolean) {
			const { offered } = await conversationOf(run.agent);
			await run.conversation.configure(
				{ tools: offered.filter((tool) => (tool === noReply ? withNoReply : registered(tool))) },
				context,
			);
			run.replying = withNoReply;
		}

		/** The invocation of sent input or a report on it, when its answer posts to a reply thread. */
		async function replyThread(run: HostedRun, requestId: string) {
			if (!run.threads.has(requestId)) {
				const invocation = (await harness.snapshot(Invocation, requestId, context)) as
					| InvocationMetadata
					| undefined;
				run.threads.set(requestId, invocation?.send && invocation.replyTo ? invocation : null);
			}
			return run.threads.get(requestId) ?? undefined;
		}

		async function activate(run: HostedRun) {
			run.started = true;
			const record = (await harness.snapshot(Runs, context))!.runs[run.id]!;
			let threaded = false;
			for (const [requestId, request] of Object.entries(record.requests ?? {}))
				if (request.sent && (await replyThread(run, requestId))) threaded = true;
			await offer(run, threaded);
			if (run.call) {
				const requestId = run.call.requestId;
				await harness.commit(async (tx) => {
					(await tx.doc(Binding, run.conversation.id)).requestId = requestId;
				}, context);
				if (run.call.metadata.ownsThread) {
					run.io = await thread({ metadata: run.call.metadata } as DurableExecution);
					if (run.io) await setActive(run.io, run.call.metadata.interactionId, true);
				}
			}
			if (!run.call || run.io) run.ui = await progressOf(run, harness, context);
		}

		async function submitPending(run: HostedRun) {
			const record = (await harness.snapshot(Runs, context))!.runs[run.id]!;
			for (const [requestId, pending] of Object.entries(record.requests ?? {})) {
				if (run.submissions.has(requestId)) continue;
				if (pending.sent) {
					run.sent.add(requestId);
					if (!run.replying && (await replyThread(run, requestId))) await offer(run, true);
					await run.ui?.follow(requestId);
				}
				// Pi dedupes by request id, so a request re-admitted after a crash is submitted once.
				const submission =
					pending.submissionId !== null
						? await harness.submission(pending.submissionId as SubmissionId, context)
						: pending.delivery === "write"
							? await run.conversation.submit(
									{
										type: "write",
										requestId,
										entry: {
											kind: "fabrial.notification",
											model: [{ role: "user", content: pending.content, timestamp: Date.now() }],
										},
									},
									context,
								)
							: await run.conversation.submit(
									{
										type: "input",
										content: pending.content,
										requestId,
										whenBusy: pending.delivery ?? "steer",
									},
									context,
								);
				if (!submission) throw new Error(`Missing Pi submission ${requestId}`);
				run.submissions.set(requestId, submission);
				if (pending.submissionId === null)
					await harness.commit(async (tx) => {
						(await tx.doc(Runs)).runs[run.id]!.requests![requestId]!.submissionId = submission.id;
					}, context);
			}
		}

		/**
		 * Replies in a thread a call owns steer its run, admitted before they leave the thread's buffer. Returns how
		 * many it admitted.
		 */
		async function drainReplies(run: HostedRun): Promise<number> {
			const metadata = run.call!.metadata;
			const routing = await run.io!.getState();
			if (routing?.interactionId !== metadata.interactionId || !routing.bufferedReplies.length)
				return 0;
			const delivered = new Set<string>();
			for (const message of routing.bufferedReplies) {
				const requestId = JSON.stringify([
					"fabrial.pi.reply",
					routing.interactionId,
					message.provider,
					message.messageId,
				]);
				const actor = await host.resolvePrincipal(message.author.identity);
				await admit(
					run,
					requestId,
					`${message.author.name ?? message.author.identity.subjectId}: ${message.text}`,
					{
						...metadata,
						requestedBy: actor as InvocationMetadata["requestedBy"],
						origin: {
							provider: message.provider,
							threadId: message.threadId,
							messageId: message.messageId,
						},
						triggerEvent: null,
						ownerWorkflow: null,
					},
					"steer",
					false,
				);
				delivered.add(message.messageId);
			}
			await submitPending(run);
			// Only acknowledge durably admitted submissions, preserving concurrent ingress.
			await run.io!.updateState((latest) =>
				latest?.interactionId === routing.interactionId
					? {
							...latest,
							bufferedReplies: latest.bufferedReplies.filter(
								(message) => !delivered.has(message.messageId),
							),
							consumedReplyIds: [...new Set([...(latest.consumedReplyIds ?? []), ...delivered])],
						}
					: latest,
			);
			return delivered.size;
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

		// Posted through a detached effect keyed by the answer, so a redelivered or restarted Session posts once.
		async function postAnswer(run: HostedRun, answer: EntryId, requestId: string) {
			run.posted.add(answer);
			const text = await answerOf(run.conversation, answer);
			if (!text?.trim()) return;
			await startChild(
				frame,
				run.id,
				`fabrial:pi:answer:${run.conversation.id}:${answer}`,
				EFFECT_WORKFLOW,
				{ method: "post", args: [text] },
				(await replyThread(run, requestId))!,
				true,
				context,
			);
			host.logger.debug("Posting final reply", { requestId, answer });
			await run.ui?.unfollow(requestId);
		}

		// Input Pi gave up on: one error reply for a model error or no model, in the newest input's reply thread.
		async function settleUnanswered(
			run: HostedRun,
			failures: { requestId: string; id: number; reason: string; detail: Json }[],
		) {
			for (const { requestId } of failures) await run.ui?.unfollow(requestId);
			const newest = failures.reduce((a, b) => (b.id > a.id ? b : a));
			const { requestId, reason, detail } = newest;
			const error =
				typeof detail === "string" ? detail : detail === null ? undefined : JSON.stringify(detail);
			if (!sentErrorReply(reason, error, "")) {
				host.logger.info("Sent input unanswered", { requestId, reason });
				return;
			}
			const fresh = newErrorId();
			const errorId = await harness.commit(async (tx) => {
				const record = (await tx.doc(Runs)).runs[run.id]!;
				record.errors ??= {};
				return (record.errors[requestId] ??= fresh);
			}, context);
			// A stable intent and global dedupe key: a redelivered or restarted Session posts once.
			await startChild(
				frame,
				run.id,
				`fabrial:pi:error:${requestId}`,
				EFFECT_WORKFLOW,
				{ method: "post", args: [sentErrorReply(reason, error, errorId)!] },
				(await replyThread(run, requestId))!,
				true,
				context,
			);
			if (errorId === fresh)
				host.logger.error("Sent input failed", { requestId, reason, errorId, error });
		}

		/** Reports of background children to this run, submitted to the lead by their reporters. */
		async function reports(run: HostedRun): Promise<[string, Submission][]> {
			const work = await harness.snapshot(SubagentWork, context);
			const found: [string, Submission][] = [];
			for (const [id, entry] of Object.entries(work?.reporters ?? {})) {
				if (entry.run !== run.id || entry.lead !== run.conversation.id || entry.report === null)
					continue;
				const submission = await harness.submission(entry.report as SubmissionId, context);
				if (submission) found.push([`fabrial.subagent-report:${id}`, submission]);
			}
			return found;
		}

		/**
		 * Post the answers to input with a reply thread: each answer once, to the thread of the newest input it answers
		 * (steering inputs share their run's answer). Input that failed gets the error reply there instead. Returns whether
		 * every sent request and report settled, from the same reads, so a run never finishes before its answers posted:
		 * a report placed after its Session reopened has no live Pi run yet.
		 */
		async function postAnswers(run: HostedRun): Promise<{ settled: boolean; queued: boolean }> {
			let settled = true;
			let queued = false;
			const answered = new Map<EntryId, { requestId: string; id: number }>();
			const failures: { requestId: string; id: number; reason: string; detail: Json }[] = [];
			const candidates = [
				...[...run.submissions].filter(([requestId]) => run.sent.has(requestId)),
				...(await reports(run)),
			];
			for (const [requestId, submission] of candidates) {
				if (run.unanswered.has(requestId)) continue;
				const status = await submission.status(context);
				if (status.status !== "done" && status.status !== "unanswered") {
					settled = false;
					queued ||= status.status === "queued";
				}
				if (status.status === "unanswered") {
					run.unanswered.add(requestId);
					if (await replyThread(run, requestId))
						failures.push({
							requestId,
							id: submission.id,
							reason: status.reason,
							detail: (status.detail ?? null) as Json,
						});
					else host.logger.info("Sent input unanswered", { requestId, reason: status.reason });
					continue;
				}
				if (status.status !== "done" || status.type !== "input" || run.posted.has(status.answer))
					continue;
				if (!(await replyThread(run, requestId))) continue;
				const newest = answered.get(status.answer);
				if (!newest || submission.id > newest.id)
					answered.set(status.answer, { requestId, id: submission.id });
			}
			for (const [answer, { requestId }] of answered) await postAnswer(run, answer, requestId);
			if (failures.length) await settleUnanswered(run, failures);
			return { settled, queued };
		}

		/** Settle a run: a call's answer becomes the event its caller waits for. */
		async function finish(run: HostedRun, result: JsonObject | undefined) {
			// Emitted before the run is marked done, so a crash in between emits again (deduped by request id).
			if (run.call && result)
				await execution.emit(
					AGENT_SETTLED_EVENT,
					{ requestId: run.call.requestId, ...result },
					{ id: run.call.requestId },
				);
			await harness.commit(async (tx) => {
				(await tx.doc(Runs)).runs[run.id]!.done = true;
			}, context);
			runs.delete(run.id);
			await run.ui?.stop();
			if (run.io && run.call) {
				await setActive(run.io, run.call.metadata.interactionId, false);
				await run.io.setStatus(null).catch(() => {});
			}
		}

		async function evaluate(run: HostedRun) {
			let complete = true;
			let queued = false;
			let failed: string | undefined;
			let answer: Json | undefined;
			const sent = await postAnswers(run);
			complete = sent.settled;
			queued = sent.queued;
			for (const [requestId, submission] of run.submissions) {
				if (run.sent.has(requestId)) continue;
				const status = await submission.status(context);
				// A failed run leaves the input queued behind it in Pi's inbox: keep running that.
				if (status.status === "unanswered") {
					failed ??= status.reason;
					continue;
				}
				if (status.status !== "done") {
					complete = false;
					queued ||= status.status === "queued";
					continue;
				}
				// A call returns its own answer.
				if (status.type === "input") {
					const entry = await run.conversation.commit((tx) => tx.entry(status.answer), context);
					answer =
						entry?.model
							?.flatMap((message) =>
								message.role === "assistant"
									? message.content
											.filter((block) => block.type === "text")
											.map((block) => block.text)
									: [],
							)
							.join("\n") ?? "";
				}
			}
			const live = async () => (await harness.snapshot(LiveDoc, run.conversation.id, context))?.run;
			// Background children keep the run: their reports arrive as follow-ups the lead answers, and a call settles
			// only once the lead has answered the last of them.
			if (complete && !failed && ((await backgroundPending(harness, context)) || (await live())))
				complete = false;
			// A reply buffered while the run was finishing, whose signal found it busy, steers it still.
			if (complete && !failed && run.io && (await drainReplies(run))) complete = false;
			if (complete) {
				if (failed) return finish(run, { status: "failed", error: `Pi run unanswered: ${failed}` });
				answer = (await reportedAnswer(harness, run.id, run.conversation.id, context)) ?? answer;
				return finish(run, { status: "completed", output: run.call ? (answer ?? "") : null });
			}
			// Pi places queued input only at a boundary of a run or with the next submission. After a failed run nothing
			// is running, so an invisible write places what is still queued.
			if (queued && !(await live()))
				await run.conversation.submit({ type: "write", entry: { kind: "fabrial.wake" } }, context);
		}

		/**
		 * No work, or every running task waits on a workflow other than a short effect (or on a subagent that does): the
		 * Session can close.
		 */
		async function closeable() {
			if (inflight || [...runs.values()].some((run) => !run.started)) return false;
			const inspection = await harness.inspect(context);
			const live = inspection.tasks.filter((task) => !task.record.background);
			if (!live.length && !inspection.submissions.length)
				return !(await backgroundPending(harness, context));
			const runners = live.filter((task) => task.state.kind === "running");
			// A short managed effect (a thread post) is waited out; only workflows release the Session.
			const workflow = (id: number) => frame.children.has(id) && !frame.children.get(id)!.effect;
			const parked = (id: number) => workflow(id) || frame.waiting.has(id);
			return (
				runners.some((task) => workflow(task.record.id)) &&
				live.every(
					(task) =>
						task.state.kind === "waiting" ||
						task.state.kind === "completing" ||
						(task.state.kind === "running" && parked(task.record.id)),
				)
			);
		}

		async function tick() {
			const before = runs.size;
			// A Map iterates safely while runs finish.
			for (const run of runs.values()) {
				// One run per conversation at a time, in arrival order.
				if (!runs.has(run.id) || current(run.agent) !== run) continue;
				const fresh = !run.started;
				if (fresh) await activate(run);
				await submitPending(run);
				// Replies buffered before the run started steer it after its own input.
				if (fresh && run.io) await drainReplies(run);
				await evaluate(run);
			}
			if (runs.size !== before) return schedule();
			if (!scheduled && (await closeable())) resolveIdle();
		}

		storage.onCommit = schedule;
		try {
			for (const [id, record] of Object.entries(
				(await harness.snapshot(Runs, context))?.runs ?? {},
			)) {
				if (record.done || !record.agent) continue;
				const invocation = record.call
					? ((await harness.snapshot(Invocation, record.call.requestId, context)) as
							| InvocationMetadata
							| undefined)
					: undefined;
				await attach(
					id,
					record.agent,
					record.call && invocation
						? { requestId: record.call.requestId, metadata: invocation }
						: undefined,
				);
			}
			harness.resume();
		} catch (error) {
			rejectIdle(error);
		}
		schedule();
		return {
			idle,
			async handle(input) {
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
				signal.removeEventListener("abort", onLost);
				try {
					await queue;
					for (const run of runs.values()) await run.ui?.stop();
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
			},
		};
	}

	/** The run a conversation's tool acts for: the run of its current request, else the conversation's latest. */
	async function runOf(
		frame: BridgeFrame,
		conversationId: ConversationId,
		context: Context,
	): Promise<{ id: string; agent: string }> {
		const requestId = await currentRequest(frame, conversationId, context);
		const records = Object.entries((await frame.harness.snapshot(Runs, context))?.runs ?? {});
		const found =
			records.find(([, run]) => run.requests?.[requestId]) ??
			records.findLast(([, run]) => run.conversationId === conversationId) ??
			records.findLast(([, run]) => !run.done);
		if (!found) throw new Error("Missing Pi bridge run binding");
		return { id: found[0], agent: found[1].agent ?? agentNameOf(conversationId) };
	}

	/**
	 * Progress of a run: on the thread a call owns, or on the reply thread of the latest sent request. Shows "Thinking…",
	 * tool activity, a subagent's activity, and "Waiting for the model provider…" while a generation backs off.
	 */
	async function progressOf(run: HostedRun, harness: Harness, context: Context) {
		let progress = run.io;
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
		const events = await watchEvents(harness, run.conversation.id, context);
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
					? ((await run.conversation.agent(context)).tools.find(
							(each) => each.name === tool.toolName,
						)?.description ?? "Using a tool…")
					: "Thinking…";
			await showStatus(text);
		});
		await thinking();
		return {
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
				if ((await harness.snapshot(LiveDoc, run.conversation.id, context))?.run) await thinking();
				else await progress?.setStatus(null).catch(() => {});
			},
			async stop() {
				await events.stop();
				for (const surface of shown)
					if (surface !== run.io) await surface.setStatus(null).catch(() => {});
				shown.clear();
			},
		};
	}

	async function cancelRun(harness: Harness, runId: string, reason: string) {
		const context = BACKGROUND_CONTEXT;
		const run = (await harness.snapshot(Runs, context))?.runs[runId];
		if (!run) return;
		// Even a start whose receipt was lost has a durable cancellation relationship.
		for (const key of run.intents ?? []) {
			const intent = await harness.snapshot(ChildIntents, key, context);
			if (!intent || intent.detached) continue;
			const child =
				intent.executionId ??
				(await host.runtime.invoke(intent.workflow, intent.input, {
					metadata: intent.metadata,
					dedupeKey: intent.dedupeKey,
				}));
			await host.runtime.cancel(child, reason);
		}
		for (const child of Object.values(run.children))
			if (!child.detached) await host.runtime.cancel(child.executionId, reason);
		// Cancelling the interaction reaches past the background boundary to the children this run started.
		await abortBackground(harness, runId, context);
		const inspection = await harness.inspect(context);
		const requests = new Set(Object.keys(run.requests ?? {}));
		const ids = new Set<number>();
		for (const submission of inspection.submissions) {
			if (!submission.requestId || !requests.has(submission.requestId)) continue;
			ids.add(submission.id);
			await (await harness.submission(submission.id, context))?.abort(context);
		}
		// Abort only the task tree that owns these inputs, never a continuation's newer run.
		const live = await harness.snapshot(LiveDoc, run.conversationId as ConversationId, context);
		if (live?.run?.inputs.some((id) => ids.has(id)))
			await harness.abortTask(live.run.taskId, context);
	}

	const session: RuntimeWorkflow = {
		name: SESSION_WORKFLOW,
		triggers: [],
		handler: hostSession,
	};
	// Effects run in a stable child execution, never in the Session's replaceable execution.
	const effect: RuntimeWorkflow = {
		name: EFFECT_WORKFLOW,
		triggers: [],
		handler: async (input, execution) => {
			const { method, args } = input as { method: string; args: Json[] };
			if (method === "subscribe" || method === "unsubscribe") {
				const store = host.subscriptions;
				if (!store) throw new Error("Subscriptions need a subscription store");
				const [resource, options] = args as [ResourceRefJson, StoredSubscribeOptions];
				const surface = {
					current: execution.metadata.replyTo,
					channelId: async () => {
						const io = await thread(execution);
						if (!io) throw new Error("Managed Pi subscribe has no chat surface");
						return io.channelId;
					},
				};
				return execution.step(`fabrial:pi:${method}`, async (): Promise<JsonObject> => {
					if (method === "unsubscribe") {
						const target = await prepareUnsubscribe(
							host,
							execution.metadata,
							surface,
							resource,
							options,
						);
						return { removed: await store.remove(target), ...target };
					}
					const saved = await store.upsert(
						await prepareSubscribe(
							host,
							execution.metadata,
							surface,
							`${execution.executionId}:subscribe`,
							resource,
							options,
						),
					);
					return {
						subscriptionId: saved.id,
						resource: saved.resource,
						session: saved.session,
						agent: saved.agent,
					};
				});
			}
			if (method === "send") {
				const [agent, options] = args as [string, SendOptions];
				const request = await execution.step("fabrial:pi:send", () =>
					prepareSend(
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
						options,
					),
				);
				return port.send(execution, "send", { kind: "fabrial.agent", name: agent }, request);
			}
			return execution.step("fabrial:pi:effect", async () => {
				const io = await thread(execution);
				if (!io) throw new Error("Managed Pi thread operation has no thread");
				if (method === "post")
					return cloneJson(
						await io.post(args[0] as Parameters<ThreadIO["post"]>[0]),
					) as unknown as Json;
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
	const port: AgentPort = {
		workflows: () => [session, effect],
		async send(execution, id, agent, request) {
			const input: SessionInput = {
				kind: "input",
				agent: agent.name,
				requestId: request.requestId,
				content: request.content,
				delivery: request.delivery,
				metadata: request.metadata,
			};
			await execution.signalWithStart(`fabrial:pi:deliver:${id}`, {
				workflow: SESSION_WORKFLOW,
				key: request.session,
				signal: { name: "input", id: request.requestId, payload: input as unknown as Json },
				input: { session: request.session },
				metadata: sessionMetadata(request.session),
			});
			return { requestId: request.requestId, session: request.session };
		},
		async run(execution, id, agent: AgentRef, request) {
			// A channel locator is not a durable Session identity. Core rebinds metadata from
			// the first workflow post receipt; never promote a provisional ThreadIO.ref.
			const metadata = execution.metadata;
			const key = sessionKey(metadata);
			const requestId = `${execution.executionId}:${id}`;
			// Taken before the input is sent, so the answer is never emitted before the wait.
			const cursor = await execution.cursor(`fabrial:pi:cursor:${id}`);
			const content = await execution.step(`fabrial:pi:origin:${id}`, async () => {
				// A cancelled caller finds the Session it may have reached (see `settled`).
				await options.sql`INSERT INTO fabrial_pi.sessions (key) VALUES (${key}) ON CONFLICT DO NOTHING`;
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
			const input: SessionInput = {
				kind: "call",
				agent: agent.name,
				requestId,
				content,
				metadata: cloneJson({ ...metadata, piSession: key }),
				caller: execution.executionId,
				detached: !!request.detached,
			};
			await execution.signalWithStart(`fabrial:pi:call:${id}`, {
				workflow: SESSION_WORKFLOW,
				key,
				signal: { name: "input", id: requestId, payload: input as unknown as Json },
				input: { session: key },
				metadata: sessionMetadata(key),
			});
			const settled = await execution.waitForEvent(`fabrial:pi:settled:${id}`, {
				event: AGENT_SETTLED_EVENT,
				filter: { requestId: [requestId] },
				after: cursor,
			});
			if (settled?.payload.status !== "completed") {
				const error = settled?.payload.error ?? "Agent run did not settle";
				throw new Error(typeof error === "string" ? error : JSON.stringify(error));
			}
			const answer = settled.payload.output ?? null;
			if (!request.output) return answer;
			return execution.step(`fabrial:pi:output:${id}`, async () =>
				parseSchema(
					request.output as Schema<Json>,
					JSON.parse(typeof answer === "string" ? answer : JSON.stringify(answer)),
					`agent ${agent.name} output`,
				),
			);
		},
		async settled(executionId, metadata, result) {
			if (metadata.piSession && metadata.piChild === null) return;
			// A workflow a tool started reports to its Session, which resumes the tool waiting on it.
			if (typeof metadata.piChild === "string")
				await signalSession(
					metadata.piChild,
					`fabrial.child:${executionId}`,
					{ kind: "child", executionId, result },
					metadata,
				);
			if (result.status !== "cancelled") return;
			// A cancelled caller cancels its calls (unless detached) in the Session they reached.
			let key: string;
			try {
				key = sessionKey(metadata);
			} catch {
				return;
			}
			const [known] = await options.sql`SELECT 1 FROM fabrial_pi.sessions WHERE key = ${key}`;
			if (known)
				await signalSession(
					key,
					`fabrial.cancel:${executionId}`,
					{ kind: "cancel", caller: executionId, reason: result.reason },
					metadata,
				);
		},
		async reply(thread, interactionId, dedupeId) {
			const key = threadSession(thread.provider, thread.threadId);
			await signalSession(
				key,
				`fabrial.reply:${dedupeId}`,
				{ kind: "replies", interactionId },
				{
					interactionId,
					origin: null,
					replyTo: null,
					requestedBy: null,
					ownsThread: false,
				},
			);
		},
	};
	const state: StatePort = {
		get: (execution, id, definition) => state.update(execution, id, definition, () => {}),
		update: (execution, id, definition, change) =>
			execution.step(id, async () => {
				const storage = await acquire(
					options.sql,
					sessionKey(execution.metadata),
					execution.signal,
				);
				const context = withAbortSignal(execution.signal, BACKGROUND_CONTEXT);
				const session = createSession(storage);
				try {
					return await session.commit(async (tx) => {
						const receipt = await tx.doc(
							WorkflowReceipts,
							JSON.stringify([execution.executionId, id]),
							"",
						);
						if (receipt.value !== null)
							return cloneJson(receipt.value) as ReturnType<typeof definition.initial>;
						const value = await applyState(
							tx,
							definition,
							execution.metadata.interactionId,
							undefined,
							[change],
						);
						receipt.value = cloneJson(value);
						return value;
					}, context);
				} finally {
					await session.close(BACKGROUND_CONTEXT);
				}
			}),
	};
	return {
		agents: port,
		state,
		evaluator,
		workflows: [session, effect],
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

async function setActive(io: ThreadIO, interactionId: string, agentActive: boolean) {
	await io.updateState((routing) =>
		routing?.interactionId === interactionId ? { ...routing, agentActive } : routing,
	);
}

/**
 * Aborts live Pi tasks whose kind is no longer installed (e.g. a removed toolkit), using Pi's abort
 * cascade. Finds them from the latest task records without taking leases, then acquires only the
 * affected Sessions; a Session held by another owner is skipped with a warning.
 */
export async function abortOrphans(
	sql: Sql,
	installedKinds: readonly string[],
	logger: Logger,
): Promise<number> {
	const rows =
		await sql`SELECT session_key, writes FROM fabrial_pi.commits ORDER BY session_key, seq`;
	type Task = Extract<import("@earendil-works/pi-durable").StorageWrite, { type: "task" }>["value"];
	const tasks = new Map<string, Map<Task["id"], Task>>();
	for (const row of rows)
		for (const write of JSON.parse(
			String(row.writes),
		) as import("@earendil-works/pi-durable").StorageWrite[]) {
			if (write.type !== "task") continue;
			const key = String(row.session_key);
			const session = tasks.get(key) ?? new Map();
			session.set(write.value.id, write.value);
			tasks.set(key, session);
		}
	let aborted = 0;
	for (const [key, session] of tasks) {
		const orphans = new Map(
			[...session].filter(
				([, task]) => task.state.status !== "terminal" && !installedKinds.includes(task.kind),
			),
		);
		if (!orphans.size) continue;
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
				for (const [id, task] of orphans) {
					await harness.abortTask(id, BACKGROUND_CONTEXT);
					await harness.waitForTask(id, BACKGROUND_CONTEXT);
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
