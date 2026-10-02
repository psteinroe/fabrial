import {
	BACKGROUND_CONTEXT,
	withAbortSignal,
	withContextValue,
} from "@earendil-works/chord/context";
import type { Models } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import {
	createRegistry,
	createSession,
	defineExtension,
	defineTool as piTool,
	Harness,
	section as piSection,
	watchEvents,
	type AgentChange,
	type ConversationId,
	type Extension,
	type HarnessSettings,
	type HookRegistration,
	type ToolRegistration,
} from "@earendil-works/pi-durable";
import {
	parseSchema,
	type AgentIntegration,
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
	type WorkflowTool,
} from "fabrial";
import type { Sql } from "postgres";
import { definedAgents, type DefinedAgent } from "./agent.ts";
import { BridgeKey, cloneJson, fields, registerChild, type BridgeFrame } from "./context.ts";
import { applyState, Binding, ChildResults, Conversations, Invocation, Runs } from "./documents.ts";
import { createEvaluator } from "./evaluate.ts";
import { LeaseLost, PostgresStorage, SessionBusy } from "./storage.ts";

const RUN_WORKFLOW = "fabrial.pi.run";
export function sessionKey(metadata: InvocationMetadata): string {
	const surface = metadata.replyTo;
	if (surface?.kind === "thread") return `${surface.provider}:${surface.threadId}`;
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
	const agents = new Map<string, DefinedAgent>();
	const configs = new Map<string, AgentChange>();
	const active = new Set<BridgeFrame>();
	let started = false;
	const internal = defineExtension({
		name: "fabrial.internal",
		hooks: host.plugins.flatMap((plugin) => plugin.hooks?.agent ?? []) as HookRegistration[],
	});
	registry.install(internal);
	const pluginExtensions = new Map<string, Extension>();
	const workflowTools = new WeakMap<ToolRegistration, WorkflowTool>();

	function install(agent: DefinedAgent): void {
		const previous = agents.get(agent.name);
		if (previous && previous !== agent)
			throw new Error(`Duplicate Fabrial agent name: ${agent.name}`);
		agents.set(agent.name, agent);
		const {
			name,
			extensions = [],
			tools,
			state = [],
			configure: _configure,
			...rest
		} = agent.definition;
		const selected = extensions.map((extension) => {
			if (typeof extension !== "string") return extension;
			const resolved = pluginExtensions.get(extension);
			if (!resolved)
				throw new Error(
					`Agent ${name} selects plugin ${extension}, which provides no Pi extension`,
				);
			return resolved;
		});
		for (const extension of selected) registry.install(extension);
		const nativeTools = tools?.map((tool) =>
			"kind" in tool && tool.kind === "fabrial.workflow-tool"
				? workflowTool(tool)
				: (tool as ToolRegistration),
		);
		const own = defineExtension({
			name: `fabrial.agent.${name}`,
			tools: nativeTools,
			sections: state
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
		});
		registry.install(own);
		configs.set(name, {
			...rest,
			extensions: [internal, ...selected, own],
			...(nativeTools ? { tools: nativeTools } : {}),
		});
	}

	function workflowTool(tool: WorkflowTool): ToolRegistration {
		// Standard Schema cannot generally expose JSON Schema. Validation still runs in Fabrial core.
		const native = piTool({
			name: tool.name,
			description: tool.description,
			parameters: (() => {
				const schema = tool.workflow.definition.input;
				if (schema && "toJSONSchema" in schema && typeof schema.toJSONSchema === "function")
					return schema.toJSONSchema() as import("typebox").TSchema;
				return Type.Object({}, { additionalProperties: true });
			})(),
			replay: "safe",
			execute: async (input, api, context) => {
				const frame = context.value(BridgeKey);
				if (!frame) throw new Error("Workflow tools require a Fabrial bridge");
				const ctx = await fields(context, api.conversationId, api);
				const child = await ctx.start("workflow", tool.workflow, input);
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
					api.taskId,
					{ executionId: child, detached: tool.detached },
					context,
				);
				return new Promise((_, reject) => {
					const signal = context.abortSignal;
					if (signal?.aborted) reject(signal.reason);
					else signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
				});
			},
		});
		workflowTools.set(native, tool);
		return native;
	}

	async function thread(execution: DurableExecution): Promise<ThreadIO | undefined> {
		return execution.metadata.replyTo && host.chat()
			? host.chat()!.resolve(execution.metadata.replyTo)
			: undefined;
	}

	async function drive(input: Json, execution: DurableExecution): Promise<Json> {
		if (!started) throw new Error("Start the Pi integration before running agents");
		const request = input as {
			agent: string;
			requestId: string;
			input: Json;
			structured: boolean;
			session: string;
		};
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
			let outcome: Json | undefined;
			let children: { executionId: string; detached: boolean }[] = [];
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
				active.add(frame);
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
				await conversation.configure(configs.get(request.agent)!, context);
				await agent.definition.configure?.(conversation);
				const offered = (await conversation.agent(context)).tools;
				const allowed = await Promise.all(
					offered.map(async (tool) => {
						const workflow = workflowTools.get(tool)?.workflow;
						const groups = workflow?.definition.access?.invoke;
						if (!groups) return true;
						if (!execution.metadata.requestedBy) return false;
						return (
							await Promise.all(
								(Array.isArray(groups) ? groups : [groups]).map((group) =>
									host.directory.isMember(execution.metadata.requestedBy!, group),
								),
							)
						).some(Boolean);
					}),
				);
				if (allowed.some((value) => !value))
					await conversation.configure(
						{ tools: offered.filter((_tool, index) => allowed[index]) },
						context,
					);
				const originContext = await execution.step("pi:origin", async () => {
					if (!execution.metadata.origin) return "";
					const sections = await Promise.all(
						host.plugins.map(async (plugin) =>
							plugin.context?.(execution.metadata.origin!, host.clients()),
						),
					);
					return sections.filter(Boolean).join("\n\n");
				});
				await frame.harness.commit(async (tx) => {
					await tx.doc(Invocation, request.requestId, cloneJson(execution.metadata));
					(await tx.doc(Binding, conversationId!)).requestId = request.requestId;
					const runs = await tx.doc(Runs);
					runs.runs[execution.executionId] ??= { conversationId: conversationId!, children: {} };
				}, context);
				if (execution.metadata.ownsThread && io)
					await setActive(io, execution.metadata.interactionId, true);
				let lastStatus = 0;
				if (execution.metadata.ownsThread && io) {
					events = await watchEvents(frame.harness, conversationId!, context);
					events.start(async (batch) => {
						if (Date.now() - lastStatus < 750) return;
						const tool = batch.find((event) => event.type === "tool_execution_start");
						const text =
							tool?.type === "tool_execution_start"
								? ((await conversation!.agent(context)).tools.find(
										(each) => each.name === tool.toolName,
									)?.description ?? "Using a tool…")
								: "Thinking…";
						lastStatus = Date.now();
						await io.setStatus(text).catch(() => {});
					});
					await io.setStatus("Thinking…").catch(() => {});
				}
				const content = [
					originContext,
					typeof request.input === "string" ? request.input : JSON.stringify(request.input),
					request.structured
						? "Return your final answer as a single JSON value, without Markdown fences or surrounding prose."
						: "",
				]
					.filter(Boolean)
					.join("\n\n");
				const submission = await conversation.submit(
					{ type: "input", content, requestId: request.requestId },
					context,
				);
				while (outcome === undefined) {
					if (execution.signal.aborted) {
						await conversation.abort(BACKGROUND_CONTEXT);
						execution.signal.throwIfAborted();
					}
					if (storage.lost.signal.aborted) throw new LeaseLost("Pi Session ownership lost");
					if (io && execution.metadata.ownsThread) {
						const routing = await io.getState();
						if (
							routing?.interactionId === execution.metadata.interactionId &&
							routing.bufferedReplies.length
						) {
							for (const reply of routing.bufferedReplies)
								await conversation.submit(
									{
										type: "input",
										content: `${reply.author.name ?? reply.author.identity.subjectId}: ${reply.text}`,
										whenBusy: "steer",
										requestId: `reply:${reply.provider}:${reply.messageId}`,
									},
									context,
								);
							const delivered = new Set(routing.bufferedReplies.map((reply) => reply.messageId));
							const latest = await io.getState();
							if (latest?.interactionId === routing.interactionId)
								await io.setState({
									...latest,
									bufferedReplies: latest.bufferedReplies.filter(
										(reply) => !delivered.has(reply.messageId),
									),
									consumedReplyIds: [
										...new Set([...(latest.consumedReplyIds ?? []), ...delivered]),
									],
								});
						}
					}
					const status = await submission.status(context);
					if (status.status === "unanswered")
						throw new Error(`Pi run unanswered: ${status.reason}`);
					if (status.status === "done" && status.type === "input") {
						const entry = await conversation.commit((tx) => tx.entry(status.answer), context);
						outcome =
							entry?.model
								?.flatMap((message) =>
									message.role === "assistant"
										? message.content
												.filter((block) => block.type === "text")
												.map((block) => block.text)
										: [],
								)
								.join("\n") ?? "";
						break;
					}
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
				if (execution.signal.aborted && conversationId && frame.harness) {
					const conversation = await frame.harness.conversation(conversationId, BACKGROUND_CONTEXT);
					await conversation?.abort(BACKGROUND_CONTEXT);
				}
				if (execution.signal.aborted)
					for (const child of frame.children.values())
						if (!child.detached) await execution.cancel(child.executionId, "Agent cancelled");
				await events?.stop();
				await frame.harness?.close(BACKGROUND_CONTEXT);
				await storage.close(BACKGROUND_CONTEXT);
				active.delete(frame);
				if (execution.metadata.ownsThread && io) {
					await setActive(io, execution.metadata.interactionId, false);
					await io.setStatus(null).catch(() => {});
				}
			}
			if (outcome !== undefined) return outcome;
			const branches = Object.fromEntries(
				children.map((child) => [
					child.executionId,
					{ kind: "execution" as const, executionId: child.executionId },
				]),
			);
			const settled = await execution.waitForAny(`pi:children:${cycle++}`, branches);
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

	const workflow: RuntimeWorkflow = {
		name: RUN_WORKFLOW,
		triggers: [],
		mutex: (input) => (input as JsonObject).session as string,
		handler: drive,
		onSettled: async (executionId, metadata, result) => {
			if (result.status !== "cancelled") return;
			const storage = await acquire(
				options.sql,
				sessionKey(metadata),
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
				const run = (await harness.snapshot(Runs, BACKGROUND_CONTEXT))?.runs[executionId];
				if (!run) return;
				for (const child of Object.values(run.children))
					if (!child.detached) await host.runtime.cancel(child.executionId, result.reason);
				const conversation = await harness.conversation(
					run.conversationId as ConversationId,
					BACKGROUND_CONTEXT,
				);
				await conversation?.abort(BACKGROUND_CONTEXT);
			} finally {
				await harness.close(BACKGROUND_CONTEXT);
			}
		},
	};
	const port: AgentPort = {
		workflows: () => [workflow],
		async run(execution, id, agent: AgentRef, request) {
			if (request.configure)
				throw new Error(
					"Call-time configure cannot cross a durable execution boundary; move it to defineAgent({ configure })",
				);
			// A channel locator is not a durable Session identity. Core rebinds metadata from
			// the first workflow post receipt; never promote a provisional ThreadIO.ref.
			const metadata = execution.metadata;
			const key = sessionKey(metadata);
			const answer = await execution.invoke(
				id,
				RUN_WORKFLOW,
				{
					agent: agent.name,
					requestId: `${execution.executionId}:${id}`,
					input: request.input,
					structured: !!request.output,
					session: key,
				},
				{ mutex: key, metadata, detached: request.detached },
			);
			if (!request.output) return answer;
			return execution.step(`${id}:output`, async () =>
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
					return await session.commit(
						(tx) =>
							applyState(tx, definition, execution.metadata.interactionId, undefined, [change]),
						context,
					);
				} finally {
					await session.close(BACKGROUND_CONTEXT);
				}
			}),
	};
	return {
		agents: port,
		state,
		evaluator,
		workflows: [workflow],
		async start() {
			await PostgresStorage.migrate(options.sql);
			for (const plugin of host.plugins)
				if (plugin.extension) {
					const extension = plugin.extension(host.clients()) as Extension;
					pluginExtensions.set(plugin.id, extension);
					registry.install(extension);
				}
			for (const agent of definedAgents) install(agent);
			await checkOrphans(
				options.sql,
				registry
					.snapshot()
					.tasks()
					.map((task) => task.definition.name),
			);
			started = true;
		},
		async stop() {
			started = false;
			await Promise.all([...active].map((frame) => frame.harness.close(BACKGROUND_CONTEXT)));
		},
	};
}

async function setActive(io: ThreadIO, interactionId: string, agentActive: boolean) {
	const routing = await io.getState();
	if (routing?.interactionId === interactionId) await io.setState({ ...routing, agentActive });
}

export async function checkOrphans(sql: Sql, installedKinds: readonly string[]): Promise<void> {
	const rows =
		await sql`SELECT session_key, writes FROM fabrial_pi.commits ORDER BY session_key, seq`;
	const tasks = new Map<string, { kind: string; state: { status: string } }>();
	for (const row of rows)
		for (const write of JSON.parse(
			String(row.writes),
		) as import("@earendil-works/pi-durable").StorageWrite[]) {
			if (write.type === "task") tasks.set(`${row.session_key}:${write.value.id}`, write.value);
		}
	const missing = [
		...new Set(
			[...tasks.values()]
				.filter((task) => task.state.status !== "terminal" && !installedKinds.includes(task.kind))
				.map((task) => task.kind),
		),
	];
	if (missing.length)
		throw new Error(
			`Orphaned Pi task kinds: ${missing.join(", ")}. Keep their extension installed until they finish, or call abortOrphans(sql, ${JSON.stringify(missing)}) (fabrial pi abort-orphans --kind …).`,
		);
}

export async function abortOrphans(sql: Sql, kinds: readonly string[]): Promise<number> {
	let aborted = 0;
	const sessions = await sql`SELECT key FROM fabrial_pi.sessions ORDER BY key`;
	for (const row of sessions) {
		const storage = await PostgresStorage.open(sql, String(row.key));
		const { createModels } = await import("@earendil-works/pi-ai/models");
		const harness = await Harness.open(
			storage,
			{ models: createModels(), registry: createRegistry() },
			BACKGROUND_CONTEXT,
		);
		try {
			const tasks = await harness.inspect(BACKGROUND_CONTEXT);
			for (const task of tasks.tasks)
				if (kinds.includes(task.record.kind)) {
					await harness.abortTask(task.record.id, BACKGROUND_CONTEXT);
					await harness.waitForTask(task.record.id, BACKGROUND_CONTEXT);
					aborted++;
				}
		} finally {
			await harness.close(BACKGROUND_CONTEXT);
		}
	}
	return aborted;
}
