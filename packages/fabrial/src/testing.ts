import type { AssistantMessage, MutableModels } from "@earendil-works/pi-ai";
import { createModels as createPiModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { parseModel } from "./agent.ts";
import { INTERNAL } from "./instance.ts";
import type { AgentIntegration, ChatIntegration, FabrialHost } from "./app.ts";
import type { Evaluator } from "./evaluate.ts";
import type { ExternalIdentity } from "./identity.ts";
import { identityKey } from "./directory.ts";
import { THREAD_TTL } from "./internal.ts";
import type { Json, JsonObject } from "./json.ts";
import type {
	AgentPort,
	ChatPort,
	DurableExecution,
	DurableRuntime,
	ExecutionResult,
	InvocationMetadata,
	RuntimeEvent,
	RuntimeWorkflow,
	StatePort,
	ThreadIO,
	ThreadRoutingState,
} from "./runtime.ts";
import { conversationReference } from "./send.ts";
import { parseSchema } from "./schema.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface, ThreadRef } from "./thread.ts";

/** Conductor's test clock; pass it as `conductor(..., { clock })` over a single-connection pool. */
export { createTestClock, type TestClock } from "pgconductor-js/testing";
import { createTestClock, type TestClock } from "pgconductor-js/testing";
import { Conductor, TaskSchemas } from "pgconductor-js";
import postgres from "postgres";
import { conductor } from "./runtime/conductor/index.ts";

const clone = <T>(value: T): T => structuredClone(value);

/**
 * A runtime port for unit tests that never run a workflow: it records registrations and emitted events
 * and keeps the time. Starting work (`invoke`, `signal`, …) throws.
 */
export class StubRuntime implements DurableRuntime {
	readonly workflows = new Map<string, RuntimeWorkflow>();
	readonly events: RuntimeEvent[] = [];
	readonly emitted: { name: string; payload: JsonObject; metadata: InvocationMetadata }[] = [];
	constructor(public time = Date.now()) {}
	now(): number {
		return this.time;
	}
	register(definition: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void {
		for (const workflow of definition.workflows) this.workflows.set(workflow.name, workflow);
		this.events.push(...definition.events);
	}
	async start(): Promise<void> {}
	async stop(): Promise<void> {}
	async emit(
		name: string,
		payload: JsonObject,
		options: Parameters<DurableRuntime["emit"]>[2],
	): Promise<void> {
		this.emitted.push(clone({ name, payload, metadata: options.metadata }));
	}
	invoke(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	cancel(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	signalWithStart(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	signal(): never {
		throw new Error("StubRuntime runs no workflows");
	}
}

function messageText(content: OutboundMessage): string {
	return typeof content === "string"
		? content
		: "markdown" in content
			? content.markdown
			: [content.card.title, content.card.text].filter(Boolean).join("\n");
}

export interface FakePost {
	ref: MessageRef;
	content: OutboundMessage;
	updates: OutboundMessage[];
}
class FakeThread implements ThreadIO {
	readonly posts: FakePost[] = [];
	readonly messages: ChatMessage[] = [];
	readonly statuses: (string | null)[] = [];
	/** Whether the thread is subscribed (Chat SDK delivers its messages as subscribed messages). */
	subscribed = false;
	private state: ThreadRoutingState | null = null;
	private expires = 0;
	constructor(
		readonly ref: ThreadRef,
		readonly channelId: string,
		readonly isDM: boolean,
		private readonly owner: FakeChat,
	) {}
	async post(content: OutboundMessage): Promise<MessageRef> {
		const ref = {
			provider: this.ref.provider,
			threadId: this.ref.threadId,
			messageId: `message-${this.owner.nextMessageId++}`,
		};
		this.posts.push({ ref, content: clone(content), updates: [] });
		this.messages.push({
			provider: ref.provider,
			threadId: ref.threadId,
			channelId: this.channelId,
			messageId: ref.messageId,
			text: messageText(content),
			author: {
				identity: { provider: ref.provider, installationId: "testing", subjectId: "bot" },
				name: "Fabrial",
				isBot: true,
			},
			isMention: false,
			isDM: this.isDM,
			sentAt: new Date(this.owner.now()).toISOString(),
		});
		return ref;
	}
	async update(ref: MessageRef, content: OutboundMessage): Promise<void> {
		const post = this.posts.find((p) => p.ref.messageId === ref.messageId);
		if (!post) throw new Error(`Unknown message ${ref.messageId}`);
		post.content = clone(content);
		post.updates.push(clone(content));
		const message = this.messages.find((message) => message.messageId === ref.messageId);
		if (message) message.text = messageText(content);
	}
	async setStatus(text: string | null): Promise<void> {
		this.statuses.push(text);
	}
	async history(options: { limit?: number; sinceLastBotReply?: boolean }): Promise<ChatMessage[]> {
		const lastBot = options.sinceLastBotReply
			? this.messages.findLastIndex((m) => m.author.isBot)
			: -1;
		return clone(this.messages.slice(lastBot + 1).slice(-(options.limit ?? 50)));
	}
	async subscribe(): Promise<void> {
		this.subscribed = true;
	}
	async getState(): Promise<ThreadRoutingState | null> {
		return this.owner.now() >= this.expires ? null : clone(this.state);
	}
	async updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null> {
		// No await between read and write: atomic within this in-memory process.
		this.state = clone(fn(this.owner.now() >= this.expires ? null : clone(this.state)));
		this.expires = this.owner.now() + THREAD_TTL;
		// Like Chat SDK state writes, a stored state subscribes the thread.
		if (this.state) this.subscribed = true;
		return clone(this.state);
	}
}

class FakeChannelThread implements ThreadIO {
	private actual?: FakeThread;
	constructor(
		private readonly surface: Extract<Surface, { kind: "channel" }>,
		private readonly owner: FakeChat,
	) {}
	get ref(): ThreadRef {
		return (
			this.actual?.ref ?? {
				kind: "thread",
				provider: this.surface.provider,
				threadId: `provisional:${this.surface.channelId}`,
			}
		);
	}
	get channelId(): string {
		return this.surface.channelId;
	}
	get isDM(): boolean {
		return false;
	}
	async post(content: OutboundMessage): Promise<MessageRef> {
		this.actual ??= await this.owner.thread({
			kind: "thread",
			provider: this.surface.provider,
			threadId: `channel:${this.surface.channelId}:root-${this.owner.nextThreadId++}`,
		});
		return this.actual.post(content);
	}
	async update(ref: MessageRef, content: OutboundMessage): Promise<void> {
		await (
			await this.owner.thread({ kind: "thread", provider: ref.provider, threadId: ref.threadId })
		).update(ref, content);
	}
	async setStatus(text: string | null): Promise<void> {
		await this.actual?.setStatus(text);
	}
	async history(options: { limit?: number; sinceLastBotReply?: boolean }): Promise<ChatMessage[]> {
		return this.actual ? this.actual.history(options) : [];
	}
	async subscribe(): Promise<void> {
		if (!this.actual) throw new Error("Cannot subscribe a provisional channel thread");
		await this.actual.subscribe();
	}
	async getState(): Promise<ThreadRoutingState | null> {
		return this.actual ? this.actual.getState() : null;
	}
	async updateState(
		fn: (state: ThreadRoutingState | null) => ThreadRoutingState | null,
	): Promise<ThreadRoutingState | null> {
		if (!this.actual)
			throw new Error("Cannot persist routing state for a provisional channel thread");
		return this.actual.updateState(fn);
	}
}

/** Fake chat integration with inspectable posts/DMs/ephemeral notices and authenticated ingress helpers. */
export class FakeChat implements ChatPort, ChatIntegration {
	readonly kind = "fabrial.chat" as const;
	readonly threads = new Map<string, FakeThread>();
	readonly ephemeral: { thread: ThreadRef; identity: ExternalIdentity; text: string }[] = [];
	readonly routes: Record<string, (request: Request) => Promise<Response>> = {};
	nextMessageId = 1;
	nextThreadId = 1;
	private host?: FabrialHost;
	constructor(readonly now: () => number = Date.now) {}
	connect(host: FabrialHost): ReturnType<ChatIntegration["connect"]> {
		this.host = host;
		return { port: this, routes: this.routes };
	}
	async thread(ref: ThreadRef): Promise<FakeThread> {
		const key = `${ref.provider}:${ref.threadId}`;
		let thread = this.threads.get(key);
		if (!thread) {
			thread = new FakeThread(
				clone(ref),
				ref.threadId.split(":")[1] ?? ref.threadId,
				ref.threadId.startsWith("dm:"),
				this,
			);
			this.threads.set(key, thread);
		}
		return thread;
	}
	async resolve(surface: Surface): Promise<ThreadIO> {
		return surface.kind === "thread" ? this.thread(surface) : new FakeChannelThread(surface, this);
	}
	async openDM(identity: ExternalIdentity): Promise<FakeThread> {
		return this.thread({
			kind: "thread",
			provider: identity.provider,
			threadId: `dm:${identityKey(identity)}`,
		});
	}
	async postEphemeral(thread: ThreadRef, identity: ExternalIdentity, text: string): Promise<void> {
		this.ephemeral.push(clone({ thread, identity, text }));
	}
	async receive(
		message: ChatMessage,
		options: { events?: readonly string[]; event?: string; dedupeId?: string } = {},
	): Promise<"accepted" | "ignored"> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		const thread = await this.thread({
			kind: "thread",
			provider: message.provider,
			threadId: message.threadId,
		});
		if (!thread.messages.some((m) => m.messageId === message.messageId))
			thread.messages.push(clone(message));
		return this.host.receiveMessage(message, {
			events: options.events ?? [options.event ?? `${message.provider}.message`],
			dedupeId: options.dedupeId ?? message.messageId,
		});
	}
	async cancel(
		thread: ThreadRef,
		actor: ExternalIdentity,
		dedupeId: string = crypto.randomUUID(),
	): Promise<void> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		await this.host.receiveCancellation({ thread, actor, dedupeId });
	}
	async click(
		ref: MessageRef,
		actionId: string,
		actor: ExternalIdentity,
		options: { value?: string; dedupeId?: string } = {},
	): Promise<void> {
		if (!this.host) throw new Error("FakeChat must be connected to an app");
		await this.host.receiveAction({
			actionId,
			value: options.value,
			actor,
			thread: { kind: "thread", provider: ref.provider, threadId: ref.threadId },
			messageId: ref.messageId,
			dedupeId: options.dedupeId ?? crypto.randomUUID(),
		});
	}
}

/** Injectable agent/evaluator hooks and memoized, schema-checked state for core/adapter unit tests. */
export function fakeAgents(
	options: {
		run?: AgentPort["run"];
		send?: AgentPort["send"];
		evaluate?: Evaluator["evaluate"];
		state?: StatePort;
		workflows?: RuntimeWorkflow[];
	} = {},
): AgentIntegration & {
	values: Map<string, JsonObject>;
	/** Inputs delivered with `ctx.send`, in order (default `send` only). */
	sent: (import("./send.ts").SendRequest & { agent: string })[];
} {
	const values = new Map<string, JsonObject>();
	const sent: (import("./send.ts").SendRequest & { agent: string })[] = [];
	const key = (execution: DurableExecution, state: { name: string; scope: string }) =>
		`${state.scope}:${state.scope === "thread" ? (execution.metadata.replyTo?.kind === "thread" ? execution.metadata.replyTo.threadId : execution.metadata.interactionId) : state.scope === "agent" ? execution.executionId : execution.metadata.interactionId}:${state.name}`;
	const state: StatePort = {
		get: (execution, id, definition) =>
			execution.step(id, async () => {
				const value = await parseSchema(
					definition.schema,
					clone(values.get(key(execution, definition)) ?? definition.initial()),
				);
				values.set(key(execution, definition), clone(value));
				return value;
			}) as never,
		update: (execution, id, definition, fn) =>
			execution.step(id, async () => {
				const draft = clone(values.get(key(execution, definition)) ?? definition.initial());
				const value = await parseSchema(definition.schema, fn(draft as never) ?? draft);
				values.set(key(execution, definition), clone(value));
				return value;
			}) as never,
	};
	return {
		kind: "fabrial.agents",
		values,
		sent,
		connect: () => ({
			agents: {
				workflows: () => options.workflows ?? [],
				run: options.run ?? ((execution, id, _agent, opts) => execution.step(id, () => opts.input)),
				send:
					options.send ??
					((execution, id, agent, request) =>
						execution.step(`fabrial:fake:send:${id}`, () => {
							sent.push({ ...request, agent: agent.name });
							return {
								requestId: request.requestId,
								conversation: conversationReference(agent.name, request.session),
							};
						})),
				async deliver(agent, request) {
					// Like a conversation, dedupe a redelivered request per session.
					if (
						!sent.some(
							(s) =>
								s.requestId === request.requestId &&
								s.agent === agent.name &&
								s.session === request.session,
						)
					)
						sent.push(clone({ ...request, agent: agent.name }));
					return {
						requestId: request.requestId,
						conversation: conversationReference(agent.name, request.session),
					};
				},
			},
			state: options.state ?? state,
			evaluator: {
				evaluate:
					options.evaluate ??
					(async (request) => ({ stopReason: "stop", answers: {}, model: request.model })),
			},
			workflows: options.workflows ?? [],
			async start() {},
			async stop() {},
		}),
	};
}

/** Build an app from an instance with explicit test ports (a runtime, fake or real chat, agents). */
export function testApp<const P extends readonly import("./plugin.ts").AnyPlugin[]>(
	instance: import("./instance.ts").Fabrial<P>,
	config: Omit<import("./instance.ts").AppConfig, "agents"> & {
		runtime: DurableRuntime;
		chat?: ChatIntegration;
		agents?: AgentIntegration;
		/** `f.app({ agents })`: agents whose triggers receive events. */
		routedAgents?: import("./instance.ts").AppConfig["agents"];
	},
): import("./app.ts").App<import("./instance.ts").ClientsOf<P>> {
	const { workflows, routedAgents, logger, ...overrides } = config;
	return internal(instance).app({ workflows, agents: routedAgents, logger }, overrides) as never;
}

function internal(instance: object): import("./instance.ts").InternalInstance {
	const value = (instance as { [INTERNAL]?: import("./instance.ts").InternalInstance })[INTERNAL];
	if (!value) throw new Error("Expected an instance from createFabrial()");
	return value;
}

export interface TestAppConfig extends Omit<import("./instance.ts").AppConfig, "agents"> {
	/**
	 * Postgres connection string for Conductor's tables, e.g. from a testcontainers `PostgreSqlContainer`
	 * started in Vitest's `globalSetup`. Apps share the database; each runs on a queue of its own.
	 */
	database: string;
	/** Defaults to `fakeAgents()`. */
	agents?: AgentIntegration;
	/** `f.app({ agents })`: agents whose triggers receive events. */
	routedAgents?: import("./instance.ts").AppConfig["agents"];
	/** Defaults to a `FakeChat` on the app's clock. */
	chat?: FakeChat;
}

export interface TestApp<C> {
	readonly app: import("./app.ts").App<C>;
	readonly chat: FakeChat;
	readonly runtime: DurableRuntime;
	/**
	 * Conductor's test clock for the app's workers and database: `set` moves time to a date, from which
	 * it keeps running, and `advance` jumps it forward. Advance only after the wait it should end is
	 * registered.
	 */
	readonly clock: TestClock;
	/** The execution's outcome once it settles; `undefined` while it runs or waits. */
	result: (executionId: string) => Promise<ExecutionResult | undefined>;
	/** Wait until the execution settles (default timeout 30s). */
	settled: (executionId: string, options?: { timeoutMs?: number }) => Promise<ExecutionResult>;
	/**
	 * Wait until every event emitted so far was dispatched and every execution of this app settled,
	 * suspended waits included: for asserting that something did not happen.
	 */
	idle: (options?: { timeoutMs?: number }) => Promise<void>;
	/** Stop the app and close its connection. */
	stop: () => Promise<void>;
}

const silent = { debug() {}, info() {}, warn() {}, error() {} };

/**
 * Build an app on real Conductor from the same typed catalog as production, with fake chat and agents
 * and fast polling. Start it with `app.start()`; drive it through `chat`, `app.fetch` or
 * `runtime.invoke`, and wait for outcomes with `settled`.
 */
export function createTestApp<const P extends readonly import("./plugin.ts").AnyPlugin[]>(
	instance: import("./instance.ts").Fabrial<P>,
	config: TestAppConfig,
): TestApp<import("./instance.ts").ClientsOf<P>> {
	const { database, chat: fake, ...appConfig } = config;
	// The test clock is a session setting: it holds for queries on this one connection.
	const sql = postgres(database, { max: 1, onnotice: () => {} });
	const queue = `test_${crypto.randomUUID().replaceAll("-", "")}`;
	const clock = createTestClock(sql);
	const runtime = conductor(
		{ sql },
		{
			logger: silent,
			queue,
			clock,
			pollIntervalMs: 100,
			worker: { concurrency: 8, pollIntervalMs: 20, flushIntervalMs: 20, fetchBatchSize: 10 },
		},
	);
	const chat = fake ?? new FakeChat(() => runtime.now!());
	const app = testApp(instance, {
		...appConfig,
		runtime,
		chat,
		agents: config.agents ?? fakeAgents(),
	});
	// Reads any execution by id; runs no tasks.
	const inspector = Conductor.create({
		sql,
		tasks: TaskSchemas.fromSchema([]),
		context: {},
		logger: silent,
		telemetry: false,
	}) as unknown as Conductor;
	const result = async (id: string): Promise<ExecutionResult | undefined> => {
		const info = await inspector.getExecution(id);
		if (!info) throw new Error(`Unknown execution: ${id}`);
		if (info.status === "completed")
			return {
				status: "completed",
				output: (info.result as { output: Json } | null)?.output ?? null,
			};
		if (info.status === "failed")
			return { status: "failed", error: info.error ?? "Execution failed" };
		if (info.status === "cancelled")
			return { status: "cancelled", reason: info.error ?? "Cancelled" };
		return undefined;
	};
	const until = async (description: string, probe: () => Promise<boolean>, timeoutMs = 30_000) => {
		const deadline = Date.now() + timeoutMs;
		while (!(await probe())) {
			if (Date.now() > deadline) throw new Error(`Timed out waiting for ${description}`);
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
	};
	// Conductor's tables: the public API lists no executions.
	const unsettled = async (queue: string) =>
		(
			await sql`select 1 from pgconductor._private_executions
				where queue = ${queue} and completed_at is null and failed_at is null
					and task_key != 'pgconductor.maintenance' limit 1`
		).length > 0;
	return {
		app,
		chat,
		runtime,
		clock,
		result,
		async idle(options = {}) {
			await until(
				"events to dispatch",
				async () => !(await unsettled("pgconductor.internal")),
				options.timeoutMs,
			);
			await until(
				"the app's executions to settle",
				async () => !(await unsettled(queue)),
				options.timeoutMs,
			);
		},
		async settled(id, options = {}) {
			const deadline = Date.now() + (options.timeoutMs ?? 30_000);
			for (;;) {
				const settled = await result(id);
				if (settled) return settled;
				if (Date.now() > deadline) throw new Error(`Timed out waiting for ${id} to settle`);
				await new Promise((resolve) => setTimeout(resolve, 20));
			}
		},
		async stop() {
			try {
				await app.stop();
			} finally {
				await sql.end();
			}
		},
	};
}

/** A scripted model reply, built with `reply.text()` or `reply.toolCall()`. */
export interface ScriptedReply {
	readonly kind: "fabrial.scripted-reply";
	readonly message: AssistantMessage;
}
export const reply = {
	text: (text: string): ScriptedReply => ({
		kind: "fabrial.scripted-reply",
		message: fauxAssistantMessage(text),
	}),
	toolCall: (name: string, input: JsonObject): ScriptedReply => ({
		kind: "fabrial.scripted-reply",
		message: fauxAssistantMessage(fauxToolCall(name, input), { stopReason: "toolUse" }),
	}),
};
/** A transcript message as a scripted step sees it. */
export interface ScriptedMessage {
	readonly role: string;
	readonly toolName?: string;
	readonly content: unknown;
}
export type ScriptedStep =
	| ScriptedReply
	| ((context: { messages: readonly ScriptedMessage[] }) => ScriptedReply);
export interface ScriptedModel {
	/** Replace the remaining replies; each model request consumes the next step. */
	respond(steps: readonly ScriptedStep[]): void;
	/** Model requests so far. */
	readonly calls: number;
}
export type ScriptedAnswer =
	| { probability: number }
	| { choice: string; probabilities: Record<string, number> }
	| { score: number; confidence: number };
export interface ScriptedModels {
	/** A chat model, `"provider/model"`; one per provider. */
	model(name: string): ScriptedModel;
	/** A classifier for `ctx.evaluate`; answers keyed by question. */
	classifier(name: string, answers: (questions: string[]) => Record<string, ScriptedAnswer>): void;
}

const scripted = new WeakMap<ScriptedModels, MutableModels>();

/** Scripted models for `createIntegrationApp`, without credentials or network. */
export function scriptedModels(): ScriptedModels {
	const models = createPiModels();
	const handles = new Map<string, { modelId: string; model: ScriptedModel }>();
	const result: ScriptedModels = {
		model(name) {
			const { provider, modelId } = parseModel(name);
			const existing = handles.get(provider);
			if (existing) {
				if (existing.modelId !== modelId)
					throw new Error(`Scripted provider ${provider} already has model ${existing.modelId}`);
				return existing.model;
			}
			const faux = fauxProvider({ provider, models: [{ id: modelId }] });
			models.setProvider(faux.provider);
			const model: ScriptedModel = {
				respond: (steps) =>
					faux.setResponses(
						steps.map((step) =>
							typeof step === "function"
								? (context) => step({ messages: context.messages }).message
								: step.message,
						),
					),
				get calls() {
					return faux.state.callCount;
				},
			};
			handles.set(provider, { modelId, model });
			return model;
		},
		classifier(name, answers) {
			const { provider, modelId } = parseModel(name);
			const faux = fauxProvider({ provider });
			models.setProvider({
				...faux.provider,
				getAllModels: () => [{ ...faux.getModel(), id: modelId, type: "classifier" as const }],
				classify: async (_model, context) =>
					({
						api: faux.api,
						provider,
						model: modelId,
						answers: Object.fromEntries(
							Object.entries(answers(Object.keys(context.questions))).map(([key, answer]) => [
								key,
								"probability" in answer
									? { type: "bool" as const, probability: answer.probability }
									: "choice" in answer
										? { type: "choice" as const, ...answer }
										: { type: "score" as const, ...answer },
							]),
						),
						stopReason: "stop" as const,
						timestamp: Date.now(),
					}) as never,
			});
		},
	};
	scripted.set(result, models);
	return result;
}

/**
 * Build the real app (Postgres, workflows, agents, chat state on the instance's database) with scripted
 * models instead of credentials, model retries off, and fast polling.
 */
export function createIntegrationApp<const P extends readonly import("./plugin.ts").AnyPlugin[]>(
	instance: import("./instance.ts").Fabrial<P>,
	config: import("./instance.ts").AppConfig & {
		models: ScriptedModels;
		/** Durable model retries: off by default; `true` for production's policy, or a policy override (e.g. tiny delays). */
		modelRetries?: boolean | Partial<import("@earendil-works/pi-durable").ConversationRetryPolicy>;
		/** Worker and wait polling interval. Default 20ms. */
		pollIntervalMs?: number;
		/** Concurrent workflow executions per worker. Default 8. */
		concurrency?: number;
	},
): import("./app.ts").App<import("./instance.ts").ClientsOf<P>> {
	const { workflows, agents: routed, logger, models, modelRetries = false } = config;
	const poll = config.pollIntervalMs ?? 20;
	return internal(instance).app(
		{ workflows, agents: routed, logger },
		{
			models: scripted.get(models),
			agentSettings: {
				retry:
					typeof modelRetries === "boolean"
						? { enabled: modelRetries }
						: { enabled: true, ...modelRetries },
			},
			conductor: {
				worker: {
					concurrency: config.concurrency ?? 8,
					pollIntervalMs: poll,
					flushIntervalMs: poll,
					fetchBatchSize: 10,
				},
				pollIntervalMs: poll * 2,
			},
		},
	) as never;
}
