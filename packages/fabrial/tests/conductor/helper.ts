import postgres from "postgres";
import { Conductor, TaskSchemas, type ExecutionInfo } from "pgconductor-js";
import { afterAll, afterEach, beforeAll, beforeEach, inject } from "vitest";
import type {
	DurableRuntime,
	ExecutionResult,
	Json,
	RuntimeEvent,
	RuntimeWorkflow,
} from "../internals.ts";
import { conductor, type ConductorConfig } from "../../src/runtime/conductor/index.ts";
import { createTestClock, type TestClock } from "../../src/testing-internal.ts";
// Declares the `fabrialDatabaseUrl` the global setup provides, also for other packages' tests.
import type {} from "./global-setup.ts";

/**
 * Real Conductor for Fabrial's tests: one Postgres per Vitest run (`global-setup.ts`), one database per
 * test file, a fresh queue per runtime, and Conductor's test clock. Tests observe executions through
 * handles; nothing here schedules work. Advance the clock only after the wait it should end is
 * registered: `await handle.suspended("pause"); await db.clock.advance(10)`.
 */
export interface ConductorFixture {
	/** The test file's database. */
	readonly url: string;
	/**
	 * Single-connection pool (`max: 1`) shared with every runtime, so all of them see the clock. Replaced
	 * after a test whose runtimes don't stop in time, so the next test doesn't queue behind them.
	 */
	readonly sql: postgres.Sql;
	/** Database time for `sql`; reset to real time after each test. */
	readonly clock: TestClock;
	/** Register and start a runtime on a fresh queue. Runtimes stop after each test. */
	start(workflows: RuntimeWorkflow[], options?: StartOptions): Promise<TestRuntime>;
	/** A runtime on a fresh queue for an app to register and start; it stops after each test. */
	runtime(options?: Omit<StartOptions, "events">): DurableRuntime & { readonly queue: string };
	/** A handle on an execution of any runtime on this database. */
	execution(id: string): ExecutionHandle;
}

export interface StartOptions {
	events?: RuntimeEvent[];
	/** Defaults to a queue no other runtime in this file uses. */
	queue?: string;
	config?: Omit<ConductorConfig, "queue" | "clock">;
}

export interface TestRuntime extends DurableRuntime {
	readonly queue: string;
	/** Stop this runtime and start a new one with the same definitions and queue, as after a deploy. */
	restart(): Promise<TestRuntime>;
}

export interface ExecutionHandle {
	readonly id: string;
	/** Conductor's public view; suspended work reports `pending`. */
	info(): Promise<ExecutionInfo | null>;
	/** Wait until a worker holds the execution and its handler runs. */
	running(): Promise<ExecutionHandle>;
	/**
	 * Wait until the execution released its worker at a wait: a sleep, a child, an event or `step`.
	 * Readiness for advancing the clock or emitting the awaited event.
	 */
	suspended(step?: string): Promise<ExecutionHandle>;
	/** Wait until the execution settles. */
	settled(): Promise<ExecutionResult>;
	/** Wait until the execution settles; its output, or throw when it failed or was cancelled. */
	output(): Promise<Json>;
}

/** Below the integration `testTimeout`, so a stuck wait reports what it waited for. */
const WAIT_MS = 40_000;
const POLL_MS = 20;
/** How long the runtimes of a finished test may take to stop before the fixture abandons them. */
const STOP_MS = 20_000;
const silent = { debug() {}, info() {}, warn() {}, error() {} };
const testWorker = { pollIntervalMs: POLL_MS, flushIntervalMs: POLL_MS, fetchBatchSize: 10 };

/**
 * The running test's signal: Vitest aborts it when the test times out. A timed-out test body keeps
 * running, and its waits would otherwise keep polling the file's single connection while the next
 * test's hooks queue behind them.
 */
let current: AbortSignal | undefined;

/** Poll `probe` until it returns a value other than `undefined`; gives up once the test ended. */
export async function until<T>(
	description: string,
	probe: () => Promise<T | undefined>,
	timeoutMs = WAIT_MS,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	const signal = current;
	while (true) {
		if (signal?.aborted) throw new Error(`Stopped waiting for ${description}: the test ended`);
		const value = await probe();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`Timed out waiting for ${description}`);
		await new Promise((resolve) => setTimeout(resolve, POLL_MS));
	}
}

function settlement(info: ExecutionInfo): ExecutionResult | undefined {
	if (info.status === "completed")
		return {
			status: "completed",
			output: (info.result as Json | undefined) ?? null,
		};
	if (info.status === "failed")
		return { status: "failed", error: info.error ?? "Execution failed" };
	if (info.status === "cancelled")
		return { status: "cancelled", reason: info.error ?? "Cancelled by user" };
	return undefined;
}

/**
 * Create a database for this test file, for apps that bring their own connection (`createTestApp`); call
 * once at the top of the file. Ingress events, their dedupe and routing are database-wide: apps running at
 * once on one database route each other's events.
 */
export function useDatabase(): { readonly url: string } {
	const database = `fabrial_${crypto.randomUUID().replaceAll("-", "")}`;
	const server = () => postgres(inject("fabrialDatabaseUrl"), { max: 1, onnotice: () => {} });
	let url: string | undefined;
	beforeAll(async () => {
		const admin = server();
		try {
			await admin.unsafe(`create database ${database}`);
		} finally {
			await admin.end();
		}
		const location = new URL(inject("fabrialDatabaseUrl"));
		location.pathname = `/${database}`;
		url = location.toString();
	});
	afterAll(async () => {
		const admin = server();
		try {
			await admin.unsafe(`drop database if exists ${database} with (force)`);
		} finally {
			await admin.end();
		}
	});
	return {
		get url() {
			if (url === undefined) throw new Error("useDatabase() is only usable inside tests");
			return url;
		},
	};
}

/** Create this test file's database and Conductor test clock; call once at the top of the file. */
export function useConductor(): ConductorFixture {
	let url: string | undefined;
	let sql: postgres.Sql | undefined;
	let clock: TestClock | undefined;
	let inspector: Conductor | undefined;
	let queues = 0;
	const database = `fabrial_${crypto.randomUUID().replaceAll("-", "")}`;
	const server = () => postgres(inject("fabrialDatabaseUrl"), { max: 1, onnotice: () => {} });
	const runtimes: DurableRuntime[] = [];
	const ready = <T>(value: T | undefined): T => {
		if (value === undefined) throw new Error("useConductor() is only usable inside tests");
		return value;
	};

	beforeAll(async () => {
		const admin = server();
		try {
			await admin.unsafe(`create database ${database}`);
		} finally {
			await admin.end();
		}
		const location = new URL(inject("fabrialDatabaseUrl"));
		location.pathname = `/${database}`;
		url = location.toString();
		connect();
	});
	beforeEach(({ signal }) => {
		current = signal;
	});
	afterEach(async ({ signal }) => {
		const stopping = runtimes.splice(0).reverse();
		// A timed-out test body keeps running, and so do its runtimes' workers: cut them off the
		// connection first, or their queries queue the next tests behind them.
		if (signal.aborted) await reconnect();
		const stopped = (async () => {
			for (const runtime of stopping) await runtime.stop();
			return true;
		})();
		let timer: ReturnType<typeof setTimeout> | undefined;
		const late = new Promise<false>((resolve) => (timer = setTimeout(resolve, STOP_MS, false)));
		// Off the old connection, their stops may fail; this test failed already.
		const done = await Promise.race([signal.aborted ? stopped.catch(() => true) : stopped, late]);
		clearTimeout(timer);
		if (!done) {
			// Still stopping: abandon them on the old connection, where their work fails.
			stopped.catch(() => {});
			await reconnect();
		}
		// Emits share Conductor's internal dispatch queue, which keeps receipts to dedupe ingress: forget
		// this test's events, or the next test's `message("initial")` is a duplicate.
		// A test that never started Conductor (e.g. run alone) has no schema to clean.
		const [schema] = sql
			? await sql`select to_regclass('pgconductor._private_executions') is not null as installed`
			: [];
		if (sql && schema?.installed)
			await sql`delete from pgconductor._private_executions where queue = 'pgconductor.internal'`;
		await clock?.reset();
	});
	afterAll(async () => {
		await sql?.end();
		const admin = server();
		try {
			await admin.unsafe(`drop database if exists ${database} with (force)`);
		} finally {
			await admin.end();
		}
	});

	function connect() {
		// Fake time is a session setting: the clock only holds for queries on this one connection.
		sql = postgres(ready(url), { max: 1, onnotice: () => {} });
		clock = createTestClock(sql);
		// Inspection only: getExecution reads any task's execution by id.
		inspector = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([]),
			context: {},
			logger: silent,
			telemetry: false,
		}) as unknown as Conductor;
	}
	/** Replace the file's connection; queries still running or queued on the old one fail. */
	async function reconnect() {
		const old = ready(sql);
		connect();
		await old.end({ timeout: 0 });
	}

	function runtime(options: Omit<StartOptions, "events"> = {}) {
		const queue = options.queue ?? `test_${++queues}`;
		const created = conductor(
			{ sql: ready(sql) },
			{
				logger: silent,
				...options.config,
				worker: { concurrency: 8, ...testWorker, ...options.config?.worker },
				queue,
				clock: ready(clock),
			},
		);
		// Apps stop their runtime too; the fixture's stop after the test must not stop it twice.
		const stop = created.stop.bind(created);
		let stopped: Promise<void> | undefined;
		created.stop = () => (stopped ??= stop());
		runtimes.push(created);
		return Object.assign(created, { queue });
	}

	async function start(
		workflows: RuntimeWorkflow[],
		options: StartOptions = {},
	): Promise<TestRuntime> {
		const created = runtime(options);
		const { queue } = created;
		created.register({ workflows, events: options.events ?? [] });
		await created.start();
		return Object.assign(created, {
			restart: async () => {
				await created.stop();
				runtimes.splice(runtimes.indexOf(created), 1);
				return start(workflows, { ...options, queue });
			},
		});
	}

	/** Engine state the public API does not expose: whether a released execution waits, and where. */
	async function waitsAt(id: string, step: string | undefined): Promise<boolean> {
		const [row] = await ready(sql)<{ waiting: boolean }[]>`
			select e.locked_by is null and e.completed_at is null and e.failed_at is null and (
				(${step ?? null}::text is null and (e.waiting_step_key is not null or e.waiting_on_execution_id is not null))
				or e.waiting_step_key = ${step ?? null}
				-- a sleep saves its step on release and reschedules the execution
				or (e.run_at > pgconductor._private_current_time() and exists (
					select 1 from pgconductor._private_steps p
					where p.execution_id = e.id and (${step ?? null}::text is null or p.key = ${step ?? null})
				))
				or exists (
					select 1 from pgconductor._private_custom_event_subscriptions s
					where s.execution_id = e.id and (${step ?? null}::text is null or s.step_key = ${step ?? null})
				)
			) as waiting
			from pgconductor._private_executions e where e.id = ${id}::uuid
		`;
		return row?.waiting ?? false;
	}

	function execution(id: string): ExecutionHandle {
		const info = () => ready(inspector).getExecution(id);
		const handle: ExecutionHandle = {
			id,
			info,
			running: async () => {
				await until(`${id} to run`, async () =>
					(await info())?.status === "running" ? true : undefined,
				);
				return handle;
			},
			suspended: async (step) => {
				await until(`${id} to wait${step ? ` at ${step}` : ""}`, async () =>
					(await waitsAt(id, step)) ? true : undefined,
				);
				return handle;
			},
			settled: async () => {
				let last: ExecutionInfo | undefined;
				try {
					return await until(`${id} to settle`, async () => {
						last = (await info()) ?? undefined;
						if (!last) throw new Error(`Unknown execution: ${id}`);
						return settlement(last);
					});
				} catch (error) {
					// Say where it is stuck: a pending retry, a wait nobody ends, or a worker that never ran it.
					if (!last) throw error;
					const { status, attempts, error: lastError } = last;
					throw new Error(
						`${(error as Error).message}; last seen ${JSON.stringify({ status, attempts, error: lastError, waiting: await waitsAt(id, undefined) })}`,
					);
				}
			},
			output: async () => {
				const result = await handle.settled();
				if (result.status === "completed") return result.output;
				throw new Error(
					`Execution ${id} ${result.status}: ${result.status === "failed" ? result.error : result.reason}`,
				);
			},
		};
		return handle;
	}

	return {
		get url() {
			return ready(url);
		},
		get sql() {
			return ready(sql);
		},
		get clock() {
			return ready(clock);
		},
		start,
		runtime,
		execution,
	};
}
