// Fixtures over Fabrial's internal ports, for Fabrial's own tests and its plugins' tests. Not public:
// the dev-only `fabrial/internal/testing` entry, omitted from the published exports.
import { createTestClock, type TestClock } from "pgconductor-js/testing";
import { Conductor, TaskSchemas } from "pgconductor-js";
import { createModels as createPiModels } from "@earendil-works/pi-ai/models";
import type { MutableModels } from "@earendil-works/pi-ai";
import postgres, { type Sql } from "postgres";
import type { PiOptions } from "./agents/pi.ts";
import type { App, HostedApp } from "./app.ts";
import { routingState, type ChatOptions } from "./chat/index.ts";
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
	DurableRuntime,
	ExecutionResult,
	InvocationMetadata,
	RuntimeEvent,
	RuntimeWorkflow,
} from "./runtime.ts";
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

/** The Pi models behind each `scriptedModels()`, for tests building apps below the public API. */
export const scriptedPi = new WeakMap<object, MutableModels>();

/** Stands in for Pi's database in apps that define no agents or state, which never touch it. */
const noDatabase = new Proxy(function () {} as unknown as Sql, {
	apply() {
		throw new Error("This test app has no database for Pi; pass agents: { models, sql }");
	},
	get() {
		throw new Error("This test app has no database for Pi; pass agents: { models, sql }");
	},
});

/** One input signalled to a conversation's Session: a delivery (`kind: "input"`), a stop, or a state request. */
export type SessionSignal = JsonObject & {
	session: string;
	id: string;
	kind: string;
	agent?: string;
};

/** The input signals of the Sessions on `queue`, in the order they were sent (Conductor's tables). */
export async function sessionSignals(sql: Sql, queue: string): Promise<SessionSignal[]> {
	const rows = await sql<{ session: string; id: string; payload: JsonObject }[]>`
		select coalesce(e.payload->'input'->>'session', e.payload->>'session') as session, s.id, s.payload
		from pgconductor._private_execution_signals s
		join pgconductor._private_executions e on e.id = s.execution_id and e.queue = s.queue
		where s.queue = ${queue} and e.task_key = 'fabrial.session' and s.name = 'input'
		order by s.seq`;
	return rows.map((row) => ({ ...row.payload, session: row.session, id: row.id }) as SessionSignal);
}

/** Build an app from an instance with explicit test ports (a runtime, fake or real chat, agents). */
export function testApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: Omit<AppConfig, "agents"> & {
		runtime: DurableRuntime;
		chat?: ChatOptions;
		/** Pi. Defaults to no models and no database: enough for apps that define no agents or state. */
		agents?: PiOptions;
		/** `f.app({ agents })`: agents whose triggers receive events. */
		routedAgents?: AppConfig["agents"];
	},
): HostedApp<ClientsOf<P>> {
	const { workflows, routedAgents, logger, agents, ...overrides } = config;
	return internal(instance).app(
		{ workflows, agents: routedAgents, logger },
		{ ...overrides, agents: agents ?? { models: createPiModels(), sql: noDatabase } },
	) as never;
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
	/** Inputs signalled to the app's Pi Sessions so far, in order. */
	sessionSignals: () => Promise<SessionSignal[]>;
	/** Stop the app and close its connection. */
	stop: () => Promise<void>;
}

const silent = { debug() {}, info() {}, warn() {}, error() {} };

/**
 * `fabrial/testing`'s `createTestApp`, with the app's runtime, for driving it below the public API. Pi
 * runs on `models` (none by default) over a pool of its own, opened only if the app defines agents or state.
 */
export function createTestApp<const P extends readonly AnyPlugin[]>(
	instance: Fabrial<P>,
	config: TestAppConfig & { models?: MutableModels },
): TestApp<ClientsOf<P>> & { readonly runtime: DurableRuntime } {
	const { database, chat: fake, models, ...appConfig } = config;
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
	let pi: Sql | undefined;
	const app = testApp(instance, {
		...appConfig,
		runtime,
		chat,
		// Off the clock's connection: Pi's leases run on database time.
		agents: {
			models: models ?? createPiModels(),
			sql: (pi = postgres(database, { max: 4, onnotice: () => {} })),
			settings: { retry: { enabled: false } },
		},
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
		sessionSignals: () => sessionSignals(sql, queue),
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
				await pi?.end();
				await sql.end();
			}
		},
	};
}
