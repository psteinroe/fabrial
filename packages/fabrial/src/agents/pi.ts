import {
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
	type ConversationId,
	type Extension,
	type HarnessSettings,
	type ToolRegistration,
	LiveDoc,
	type Submission,
	type SubmissionId,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type AgentPort,
	type AgentRef,
	type DurableExecution,
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
	parseModel,
	type Agent,
	type AgentObservers,
	type ThinkingLevel,
	type Toolkit,
} from "../agent.ts";
import type { AnyWorkflow } from "../workflow.ts";
import { instructionsSection, piToolOf, toJsonSchema } from "./tools.ts";
import type { Sql } from "postgres";
import {
	BridgeKey,
	cloneJson,
	fields,
	registerChild,
	replyTool,
	type BridgeFrame,
} from "./context.ts";
import {
	prepareSend,
	shortRequestId,
	threadSession,
	type Delivery,
	type SendOptions,
} from "../send.ts";
import {
	prepareSubscribe,
	prepareUnsubscribe,
	type StoredSubscribeOptions,
} from "../subscriptions.ts";
import type { ResourceRefJson } from "../plugin.ts";
import { Inbox } from "./inbox.ts";
import {
	applyState,
	Binding,
	ChildResults,
	Conversations,
	Invocation,
	RequestAlias,
	Runs,
	WorkflowReceipts,
	ChildIntents,
	EFFECT_WORKFLOW,
} from "./documents.ts";
import { createEvaluator } from "./evaluate.ts";
import { LeaseLost, PostgresStorage, SessionBusy } from "./storage.ts";
import type { AgentIntegration, Logger } from "../app.ts";

const RUN_WORKFLOW = "fabrial.pi.run";
export function sessionKey(metadata: InvocationMetadata): string {
	const surface = metadata.replyTo;
	if (surface?.kind === "thread") return threadSession(surface.provider, surface.threadId);
	if (surface?.kind === "channel")
		throw new Error(
			"Post to the channel with ctx.thread.post before using Pi; a channel is not a Session key",
		);
	return metadata.interactionId;
}

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
	const reply = replyTool();
	const inbox = new Inbox(options.sql);
	const internal = defineExtension({
		name: "fabrial.internal",
		tools: [reply],
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
		configs.set(name, {
			model: model ? parseModel(model) : null,
			thinkingLevel: thinking ? THINKING[thinking] : null,
			instructions: null,
			extensions: [internal, ...selected, own],
			tools: null,
		});
	}

	function workflowTool(workflow: AnyWorkflow, owner: string): ToolRegistration {
		const { input, description } = workflow.definition;
		const native = piTool({
			name: workflow.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"),
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
				await registerChild(frame, api.taskId, { executionId: child, detached: false }, context);
				return new Promise((_, reject) => {
					const signal = context.abortSignal;
					if (signal?.aborted) reject(signal.reason);
					else signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
				});
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

	async function drive(input: Json, execution: DurableExecution): Promise<Json> {
		if (!started) throw new Error("Start the Pi integration before running agents");
		// A call drives its own request; a send drains the conversation's inbox. Both drain sent input.
		const request = input as {
			agent: string;
			session: string;
			mode?: "call" | "send";
			requestId?: string;
			input?: Json;
			structured?: boolean;
		};
		const sending = request.mode === "send";
		const agent = agents.get(request.agent);
		if (!agent)
			throw new Error(
				`Agent ${request.agent} is not installed; defineAgent must be evaluated before app.start()`,
			);
		const io = await thread(execution);
		let cycle = 0;
		for (;;) {
			const storage = await acquire(options.sql, request.session, execution.signal);
			const signal = AbortSignal.any([execution.signal, storage.lost.signal]);
			const frame: BridgeFrame = {
				host,
				execution,
				evaluator,
				harness: undefined as unknown as Harness,
				session: request.session,
				agent: request.agent,
				thread: io,
				children: new Map(),
			};
			const context = withContextValue(
				BridgeKey,
				frame,
				withAbortSignal(signal, BACKGROUND_CONTEXT),
			);
			let conversationId: ConversationId | undefined;
			let events: Awaited<ReturnType<typeof watchEvents>> | undefined;
			let watcher: ReturnType<Inbox["watch"]> | undefined;
			let outcome: Json | undefined;
			let children: { executionId: string; detached: boolean }[] = [];
			const shown = new Set<ThreadIO>();
			let finish!: () => void;
			active.set(
				frame,
				new Promise<void>((resolve) => {
					finish = resolve;
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
				const ids = await frame.harness.snapshot(Conversations, context);
				conversationId = ids?.ids[request.agent] as ConversationId | undefined;
				let conversation = conversationId
					? await frame.harness.conversation(conversationId, context)
					: undefined;
				if (!conversation) {
					conversation = await frame.harness.createConversation(
						{ ownership: { kind: "ownerless" } },
						context,
					);
					conversationId = conversation.id;
					await frame.harness.commit(async (tx) => {
						(await tx.doc(Conversations)).ids[request.agent] = conversationId!;
					}, context);
				}
				conversationAgents.set(conversationId!, request.agent);
				await conversation.configure(configs.get(request.agent)!, context);
				const offered = (await conversation.agent(context)).tools;
				// Tools are not filtered by who is talking: a conversation can have many participants,
				// and a tool that needs permission asks its own approvers. Only unregistered workflows drop
				// out, and `reply` is offered only to runs that answer sent input.
				let replying = false;
				const offer = async (withReply: boolean) => {
					replying = withReply;
					const allowed = offered.map((tool) => {
						if (tool === reply) return withReply;
						const supplied = workflowTools.get(tool);
						return (
							!supplied || host.workflows.some((candidate) => candidate.name === supplied.name)
						);
					});
					await conversation!.configure(
						{ tools: offered.filter((_tool, index) => allowed[index]) },
						context,
					);
				};
				await offer(sending);
				const originContext = await execution.step("fabrial:pi:origin", async () => {
					if (sending || !execution.metadata.origin) return "";
					const sections = await Promise.all(
						host.plugins.map(async (plugin) =>
							plugin.context?.(execution.metadata.origin!, host.clients()),
						),
					);
					return sections.filter(Boolean).join("\n\n");
				});
				await frame.harness.commit(async (tx) => {
					if (request.requestId) {
						await tx.doc(Invocation, request.requestId, cloneJson(execution.metadata));
						(await tx.doc(Binding, conversationId!)).requestId = request.requestId;
					}
					const runs = await tx.doc(Runs);
					runs.runs[execution.executionId] ??= { conversationId: conversationId!, children: {} };
				}, context);
				if (execution.metadata.ownsThread && io)
					await setActive(io, execution.metadata.interactionId, true);
				let lastStatus = 0;
				// Progress shows on the owned thread, or on the reply thread of the latest sent request.
				let progress = execution.metadata.ownsThread ? io : undefined;
				const showStatus = async (text: string) => {
					if (!progress) return;
					shown.add(progress);
					lastStatus = Date.now();
					await progress.setStatus(text).catch(() => {});
				};
				if (sending || (execution.metadata.ownsThread && io)) {
					events = await watchEvents(frame.harness, conversationId!, context);
					events.start(async (batch) => {
						if (!progress || Date.now() - lastStatus < 750) return;
						const tool = batch.find((event) => event.type === "tool_execution_start");
						const text =
							tool?.type === "tool_execution_start"
								? ((await conversation!.agent(context)).tools.find(
										(each) => each.name === tool.toolName,
									)?.description ?? "Using a tool…")
								: "Thinking…";
						await showStatus(text);
					});
					await showStatus("Thinking…");
				}
				const content = [
					originContext,
					typeof request.input === "string" ? request.input : JSON.stringify(request.input ?? null),
					request.structured
						? "Return your final answer as a single JSON value, without Markdown fences or surrounding prose."
						: "",
				]
					.filter(Boolean)
					.join("\n\n");
				// Persist admission intents before submit; re-opening drains the same durable request set.
				const admit = async (
					requestId: string,
					content: string,
					metadata: InvocationMetadata,
					delivery: Delivery = "steer",
					sent = false,
				) => {
					await frame.harness.commit(async (tx) => {
						await tx.doc(Invocation, requestId, cloneJson(metadata));
						await tx.doc(RequestAlias, shortRequestId(requestId), requestId);
						(await tx.doc(Binding, conversationId!)).requestId ??= requestId;
						const run = (await tx.doc(Runs)).runs[execution.executionId]!;
						run.requests ??= {};
						run.requests[requestId] ??= {
							content,
							submissionId: null,
							delivery,
							...(sent ? { sent } : {}),
						};
					}, context);
				};
				if (request.requestId) await admit(request.requestId, content, execution.metadata);
				const submissions = new Map<string, Submission>();
				// Requests from the inbox, answered through `reply`; the rest are this call's own.
				const sent = new Set<string>();
				const unanswered = new Set<string>();
				const submitPending = async () => {
					const run = (await frame.harness.snapshot(Runs, context))!.runs[execution.executionId]!;
					for (const [requestId, pending] of Object.entries(run.requests ?? {})) {
						if (submissions.has(requestId)) continue;
						if (pending.sent) {
							sent.add(requestId);
							const replyTo = (await frame.harness.snapshot(Invocation, requestId, context))
								?.replyTo;
							if (replyTo?.kind === "thread" && host.chat()) {
								progress = await host.chat()!.resolve(replyTo);
								await showStatus("Thinking…");
							}
						}
						// Pi dedupes by request id, so a request re-admitted after a crash is submitted once.
						const submission =
							pending.submissionId !== null
								? await frame.harness.submission(pending.submissionId as SubmissionId, context)
								: pending.delivery === "write"
									? await conversation!.submit(
											{
												type: "write",
												requestId,
												entry: {
													kind: "fabrial.notification",
													model: [
														{ role: "user", content: pending.content, timestamp: Date.now() },
													],
												},
											},
											context,
										)
									: await conversation!.submit(
											{
												type: "input",
												content: pending.content,
												requestId,
												whenBusy: pending.delivery ?? "steer",
											},
											context,
										);
						if (!submission) throw new Error(`Missing Pi submission ${requestId}`);
						submissions.set(requestId, submission);
						await frame.harness.commit(async (tx) => {
							(await tx.doc(Runs)).runs[execution.executionId]!.requests![requestId]!.submissionId =
								submission.id;
						}, context);
					}
				};
				// Sent input for this conversation, admitted durably before its inbox row is pruned.
				const drain = async (force = false) => {
					if (!watcher?.due() && !force) return false;
					const rows = await inbox.pending(request.session, request.agent);
					if (!rows.length) return false;
					if (!replying) await offer(true);
					for (const row of rows)
						await admit(row.request_id, row.content, row.metadata, row.delivery, true);
					await submitPending();
					await inbox.remove(rows.map((row) => row.request_id));
					return true;
				};
				watcher = inbox.watch(request.session);
				await submitPending();
				await drain();
				frame.harness.resume();
				while (outcome === undefined) {
					execution.signal.throwIfAborted();
					if (storage.lost.signal.aborted) throw new LeaseLost("Pi Session ownership lost");
					await drain();
					if (io && execution.metadata.ownsThread) {
						const routing = await io.getState();
						if (
							routing?.interactionId === execution.metadata.interactionId &&
							routing.bufferedReplies.length
						) {
							const delivered = new Set<string>();
							for (const reply of routing.bufferedReplies) {
								const requestId = JSON.stringify([
									"fabrial.pi.reply",
									routing.interactionId,
									reply.provider,
									reply.messageId,
								]);
								const actor = await host.resolvePrincipal(reply.author.identity);
								await admit(
									requestId,
									`${reply.author.name ?? reply.author.identity.subjectId}: ${reply.text}`,
									{
										...execution.metadata,
										requestedBy: actor as InvocationMetadata["requestedBy"],
										origin: {
											provider: reply.provider,
											threadId: reply.threadId,
											messageId: reply.messageId,
										},
										triggerEvent: null,
										ownerWorkflow: null,
									},
								);
								await submitPending();
								delivered.add(reply.messageId);
							}
							// Only acknowledge durably admitted submissions, preserving concurrent ingress.
							await io.updateState((latest) =>
								latest?.interactionId === routing.interactionId
									? {
											...latest,
											bufferedReplies: latest.bufferedReplies.filter(
												(reply) => !delivered.has(reply.messageId),
											),
											consumedReplyIds: [
												...new Set([...(latest.consumedReplyIds ?? []), ...delivered]),
											],
										}
									: latest,
							);
						}
					}
					let complete = true;
					let queued = false;
					let failed: string | undefined;
					let answer: Json | undefined;
					for (const [requestId, submission] of submissions) {
						const status = await submission.status(context);
						// A failed run leaves the input queued behind it in Pi's inbox: keep driving that.
						if (status.status === "unanswered") {
							if (!sent.has(requestId)) failed ??= status.reason;
							else if (!unanswered.has(requestId)) {
								unanswered.add(requestId);
								host.logger.warn("Sent input unanswered", { requestId, reason: status.reason });
							}
							continue;
						}
						if (status.status !== "done") {
							complete = false;
							queued ||= status.status === "queued";
							continue;
						}
						// Sent input is answered through `reply`; a call returns its own answer.
						if (status.type === "input" && !sent.has(requestId)) {
							const entry = await conversation.commit((tx) => tx.entry(status.answer), context);
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
					if (complete) {
						// Input sent while this run ended is ours to answer; its own driver may already be done.
						if (await drain(true)) continue;
						if (failed) throw new Error(`Pi run unanswered: ${failed}`);
						// A sent run answers through `reply`; nothing is returned to post.
						outcome = sending ? null : (answer ?? "");
						break;
					}
					// Pi places queued input only at a boundary of a run or with the next submission. After a
					// failed run nothing is running, so an invisible write places what is still queued.
					if (queued && !(await frame.harness.snapshot(LiveDoc, conversationId!, context))?.run)
						await conversation.submit({ type: "write", entry: { kind: "fabrial.wake" } }, context);
					const inspection = await frame.harness.inspect(context);
					const live = inspection.tasks.filter((task) => !task.record.background);
					const runners = live.filter((task) => task.state.kind === "running");
					if (
						runners.length &&
						runners.every((task) => frame.children.has(task.record.id)) &&
						live.every(
							(task) =>
								task.state.kind === "waiting" ||
								task.state.kind === "completing" ||
								(task.state.kind === "running" && frame.children.has(task.record.id)),
						)
					) {
						children = runners.map((task) => frame.children.get(task.record.id)!);
						break;
					}
					await new Promise<void>((resolve) => setTimeout(resolve, 25));
				}
			} finally {
				try {
					try {
						if (execution.signal.aborted && frame.harness)
							await cancelRun(
								frame.harness,
								execution.executionId,
								execution.signal.reason?.message ?? "Agent cancelled",
							);
					} finally {
						// A failed cleanup is retried by onSettled; it must not retain the Session lease.
						watcher?.close();
						try {
							await events?.stop();
						} finally {
							try {
								await frame.harness?.close(BACKGROUND_CONTEXT);
							} finally {
								await storage.close(BACKGROUND_CONTEXT);
								if (execution.metadata.ownsThread && io) {
									await setActive(io, execution.metadata.interactionId, false);
									await io.setStatus(null).catch(() => {});
								}
								for (const surface of shown)
									if (surface !== io || !execution.metadata.ownsThread)
										await surface.setStatus(null).catch(() => {});
							}
						}
					}
				} finally {
					// Include routing-state/status cleanup in the shutdown join: Chat must
					// stay connected until these operations release their durable locks.
					active.delete(frame);
					finish();
				}
			}
			if (outcome !== undefined) return outcome;
			const branches = Object.fromEntries(
				children.map((child) => [
					child.executionId,
					{ kind: "execution" as const, executionId: child.executionId },
				]),
			);
			const waitId = `fabrial:pi:children${cycle ? `#${cycle}` : ""}`;
			cycle++;
			const settled = await execution.waitForAny(waitId, branches);
			if (settled.kind !== "execution")
				throw new Error("Pi bridge expected an execution settlement");
			const storageForResult = await acquire(options.sql, request.session, execution.signal);
			const session = createSession(storageForResult);
			try {
				await session.commit(async (tx) => {
					(await tx.doc(ChildResults)).results[settled.key] = cloneJson(
						settled.result,
					) as JsonObject;
				}, BACKGROUND_CONTEXT);
			} finally {
				await session.close(BACKGROUND_CONTEXT);
			}
		}
	}

	async function cancelRun(harness: Harness, executionId: string, reason: string) {
		const context = BACKGROUND_CONTEXT;
		const run = (await harness.snapshot(Runs, context))?.runs[executionId];
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

	const workflow: RuntimeWorkflow = {
		name: RUN_WORKFLOW,
		triggers: [],
		mutex: (input) => (input as JsonObject).session as string,
		handler: drive,
		onSettled: async (executionId, metadata, result) => {
			if (result.status !== "cancelled") return;
			const storage = await acquire(
				options.sql,
				typeof metadata.piSession === "string" ? metadata.piSession : sessionKey(metadata),
				new AbortController().signal,
			);
			const harness = await Harness.open(
				storage,
				{
					models: options.models,
					registry,
					settings: { ...options.settings, extensions: [internal] },
				},
				BACKGROUND_CONTEXT,
			);
			try {
				await cancelRun(harness, executionId, result.reason);
			} finally {
				await harness.close(BACKGROUND_CONTEXT);
			}
		},
	};
	// Effects run in a stable child execution, never in the Session's replaceable driver.
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
		workflows: () => [workflow, effect],
		async send(execution, id, agent, request) {
			await execution.step(`fabrial:pi:enqueue:${id}`, async () => {
				await inbox.enqueue({
					request_id: request.requestId,
					session_key: request.session,
					agent: agent.name,
					content: request.content,
					delivery: request.delivery,
					metadata: request.metadata,
				});
			});
			// Every send ensures a driver; one queued behind a busy Session finds its input already taken.
			await execution.start(
				`fabrial:pi:deliver:${id}`,
				RUN_WORKFLOW,
				{ agent: agent.name, session: request.session, mode: "send" },
				{
					metadata: {
						...execution.metadata,
						ownsThread: false,
						ownerWorkflow: null,
						triggerEvent: null,
						piSession: request.session,
					},
					dedupeKey: JSON.stringify(["fabrial.pi.send", request.requestId]),
					mutex: request.session,
				},
			);
			return { requestId: request.requestId, session: request.session };
		},
		async run(execution, id, agent: AgentRef, request) {
			// A channel locator is not a durable Session identity. Core rebinds metadata from
			// the first workflow post receipt; never promote a provisional ThreadIO.ref.
			const metadata = execution.metadata;
			const key = sessionKey(metadata);
			const answer = await execution.invoke(
				id,
				RUN_WORKFLOW,
				{
					agent: agent.name,
					mode: "call",
					requestId: `${execution.executionId}:${id}`,
					input: request.input,
					structured: !!request.output,
					session: key,
				},
				{ mutex: key, metadata: { ...metadata, piSession: key }, detached: request.detached },
			);
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
		workflows: [workflow, effect],
		async start() {
			await PostgresStorage.migrate(options.sql);
			await inbox.start();
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
			await inbox.stop();
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
