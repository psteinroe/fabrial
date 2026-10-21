// Fixtures over Fabrial's internal ports, for Fabrial's own tests and its plugins' tests. Not public:
// the dev-only `fabrial/internal/testing` entry, omitted from the published exports.
import { createTestClock, type TestClock } from "pgconductor-js/testing";
import { Conductor, TaskSchemas } from "pgconductor-js";
import postgres from "postgres";
import type { AgentIntegration, App, HostedApp } from "./app.ts";
import { routingState, type ChatOptions } from "./chat/index.ts";
import type { Evaluator } from "./evaluate.ts";
import { FakeChat } from "./fake-chat.ts";
import {
	INTERNAL,
	type AppConfig,
	type ClientsOf,
	type Fabrial,
	type InternalInstance,
} from "./instance.ts";
import type { Json, JsonObject } from "./json.ts";
import type { AnyPlugin } from "./plugin.ts";
import { conductor } from "./runtime/conductor/index.ts";
import type {
	AgentPort,
	DurableExecution,
	DurableRuntime,
	ExecutionResult,
	InvocationMetadata,
	RuntimeEvent,
	RuntimeWorkflow,
	StatePort,
} from "./runtime.ts";
import { parseSchema } from "./schema.ts";
import { conversationReference, type SendRequest } from "./send.ts";
import type { Surface, ThreadRef } from "./thread.ts";
import type { AnyWorkflow, WorkflowArgs } from "./workflow.ts";

export { FakeChat } from "./fake-chat.ts";
/** Conductor's test clock; pass it as `conductor(..., { clock })` over a single-connection pool. */
export { createTestClock, type TestClock } from "pgconductor-js/testing";

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
	async getExecution(): Promise<null> {
		return null;
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
	sent: (SendRequest & { agent: string })[];
} {
	const values = new Map<string, JsonObject>();
	const sent: (SendRequest & { agent: string })[] = [];
	const key = (execution: DurableExecution, state: { name: string; scope: string }) =>
		`${state.scope}:${state.scope === "session" ? (execution.metadata.replyTo?.kind === "thread" ? execution.metadata.replyTo.threadId : execution.metadata.requestId) : state.scope === "agent" ? execution.executionId : execution.metadata.requestId}:${state.name}`;
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
export function testApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: Omit<AppConfig, "agents"> & {
		runtime: DurableRuntime;
		chat?: ChatOptions;
		agents?: AgentIntegration;
		/** `f.app({ agents })`: agents whose triggers receive events. */
		routedAgents?: AppConfig["agents"];
	},
): HostedApp<ClientsOf<P>> {
	const { workflows, routedAgents, logger, ...overrides } = config;
	return internal(instance).app({ workflows, agents: routedAgents, logger }, overrides) as never;
}

export function internal(instance: object): InternalInstance {
	const value = (instance as { [INTERNAL]?: InternalInstance })[INTERNAL];
	if (!value) throw new Error("Expected an instance from createFabrial()");
	return value;
}

export interface TestAppConfig extends Omit<AppConfig, "agents"> {
	/**
	 * Postgres connection string for Conductor's tables, e.g. from a testcontainers `PostgreSqlContainer`
	 * started in Vitest's `globalSetup`. Apps share the database; each runs on a queue of its own.
	 */
	database: string;
	/** `f.app({ agents })`: agents whose triggers receive events. */
	routedAgents?: AppConfig["agents"];
	/** Defaults to a `FakeChat` on the app's clock. */
	chat?: FakeChat;
}

/** The app `createTestApp` and `createIntegrationApp` build: the production `App`, plus test inspection. */
export interface TestingApp<C = Record<string, unknown>> extends App<C> {
	/** Start an execution of the workflow, as a script would: no surface, origin, or requester. Returns its id. */
	readonly invoke: <W extends AnyWorkflow>(
		workflow: W,
		...args: WorkflowArgs<W>
	) => Promise<string>;
	/** Who follows the thread; empty lists when nobody does. */
	routing(thread: ThreadRef): Promise<ThreadFollowers>;
}

/** The conversations following a thread, as `TestingApp.routing` reads them. */
export interface ThreadFollowers {
	/** Conversations the thread's messages reach. */
	readonly follows: readonly { readonly session: string; readonly agent: string }[];
	/** Conversations following the thread's resource (`ctx.follow`), notified of its events. */
	readonly resourceFollows: readonly {
		readonly session: string;
		readonly agent: string;
		/** Resource key, e.g. `github:acme/app#42`. */
		readonly resource: string;
		/** Where the notifications' final replies go. */
		readonly replyTo: Surface | null;
	}[];
}

/** Add test inspection to a hosted app. */
export function testingApp<C>(app: HostedApp<C>): TestingApp<C> {
	return Object.assign(app, {
		invoke: (workflow: AnyWorkflow, input: Json = null) =>
			app.host.runtime.invoke(workflow.name, input, {
				metadata: {
					requestId: crypto.randomUUID(),
					origin: null,
					replyTo: null,
					requestedBy: null,
				},
			}),
		async routing(thread: ThreadRef): Promise<ThreadFollowers> {
			const chat = app.host.chat();
			if (!chat) throw new Error("The app has no chat integration");
			const state = await routingState(await chat.thread(thread));
			return {
				follows: (state?.follows ?? []).map(({ session, agent }) => ({ session, agent })),
				resourceFollows: (state?.resourceFollows ?? []).map(
					({ session, agent, resource, replyTo }) => ({ session, agent, resource, replyTo }),
				),
			};
		},
	}) as never;
}

export interface TestApp<C> {
	readonly app: TestingApp<C>;
	readonly chat: FakeChat;
	/** `app.invoke`. */
	invoke: TestingApp<C>["invoke"];
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
	 * Wait until every event emitted on the database so far was dispatched and every execution of this
	 * app settled, suspended waits included: for asserting that something did not happen.
	 */
	idle: (options?: { timeoutMs?: number }) => Promise<void>;
	/** Stop the app and close its connection. */
	stop: () => Promise<void>;
}

const silent = { debug() {}, info() {}, warn() {}, error() {} };

/**
 * `fabrial/testing`'s `createTestApp`, with injectable agents and the app's runtime, for driving it below
 * the public API.
 */
export function createTestApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: TestAppConfig & {
		/** Defaults to `fakeAgents()`. */
		agents?: AgentIntegration;
	},
): TestApp<ClientsOf<P>> & { readonly runtime: DurableRuntime } {
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
			worker: { concurrency: 8, pollIntervalMs: 20, flushIntervalMs: 20, fetchBatchSize: 10 },
		},
	);
	const chat = fake ?? new FakeChat();
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
	// Conductor's tables: the public API lists no executions.
	const pending = (queue: string, limit: number) =>
		sql<
			{
				id: string;
				task_key: string;
				attempts: number;
				run_at: Date;
				locked: boolean;
				waiting: string | null;
				last_error: string | null;
			}[]
		>`
			select id, task_key, attempts, run_at, locked_at is not null as locked,
				coalesce(waiting_step_key, waiting_on_execution_id::text) as waiting, last_error
			from pgconductor._private_executions
			where queue = ${queue} and completed_at is null and failed_at is null
				and task_key != 'pgconductor.maintenance'
				-- a cron's next occurrence waits for its schedule, not on the app
				and not (cron_expression is not null and attempts = 0 and locked_at is null)
			limit ${limit}`;
	const drained = async (description: string, queue: string, timeoutMs = 30_000) => {
		const deadline = Date.now() + timeoutMs;
		while ((await pending(queue, 1)).length > 0) {
			if (Date.now() > deadline) {
				// Say what is stuck: a retry backing off, a wait nobody ends, or work no worker claims.
				const rows = await pending(queue, 10);
				throw new Error(`Timed out waiting for ${description}; unsettled ${JSON.stringify(rows)}`);
			}
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
	};
	const testing = testingApp(app);
	return {
		app: testing,
		chat,
		invoke: testing.invoke,
		runtime,
		clock,
		result,
		async idle(options = {}) {
			await drained("events to dispatch", "pgconductor.internal", options.timeoutMs);
			await drained("the app's executions to settle", queue, options.timeoutMs);
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
