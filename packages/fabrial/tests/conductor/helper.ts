import postgres from "postgres";
import { Conductor, TaskSchemas, type ExecutionInfo } from "pgconductor-js";
import { afterAll, afterEach, beforeAll, inject } from "vitest";
import type {
	DurableRuntime,
	ExecutionResult,
	Json,
	RuntimeEvent,
	RuntimeWorkflow,
} from "../../src/index.ts";
import { conductor, type ConductorConfig } from "../../src/runtime/conductor/index.ts";
import { createTestClock, type TestClock } from "../../src/testing.ts";

/**
 * Real Conductor for Fabrial's tests: one Postgres per Vitest run (`global-setup.ts`), one database per
 * test file, a fresh queue per runtime, and Conductor's test clock. Tests observe executions through
 * handles; nothing here schedules work. Advance the clock only after the wait it should end is
 * registered: `await handle.suspended("pause"); await db.clock.advance(10)`.
 */
export interface ConductorFixture {
	/** The test file's database. */
	readonly url: string;
	/** Single-connection pool (`max: 1`) shared with every runtime, so all of them see the clock. */
	readonly sql: postgres.Sql;
	/** Database time for `sql`; reset to real time after each test. */
	readonly clock: TestClock;
	/** Register and start a runtime on a fresh queue. Runtimes stop after each test. */
	start(workflows: RuntimeWorkflow[], options?: StartOptions): Promise<TestRuntime>;
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

const WAIT_MS = 20_000;
const POLL_MS = 20;
const silent = { debug() {}, info() {}, warn() {}, error() {} };
const testWorker = { pollIntervalMs: POLL_MS, flushIntervalMs: POLL_MS, fetchBatchSize: 10 };

/** Poll `probe` until it returns a value other than `undefined`. */
export async function until<T>(
	description: string,
	probe: () => Promise<T | undefined>,
	timeoutMs = WAIT_MS,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (true) {
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
			output: (info.result as { output: Json } | null)?.output ?? null,
		};
	if (info.status === "failed")
		return { status: "failed", error: info.error ?? "Execution failed" };
	if (info.status === "cancelled")
		return { status: "cancelled", reason: info.error ?? "Cancelled by user" };
	return undefined;
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
		// Fake time is a session setting: the clock only holds for queries on this one connection.
		sql = postgres(url, { max: 1, onnotice: () => {} });
		clock = createTestClock(sql);
		// Inspection only: getExecution reads any task's execution by id.
		inspector = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([]),
			context: {},
			logger: silent,
			telemetry: false,
		}) as unknown as Conductor;
	});
	afterEach(async () => {
		for (const runtime of runtimes.splice(0).reverse()) await runtime.stop();
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

	async function start(
		workflows: RuntimeWorkflow[],
		options: StartOptions = {},
	): Promise<TestRuntime> {
		const queue = options.queue ?? `test_${++queues}`;
		const runtime = conductor(
			{ sql: ready(sql) },
			{
				logger: silent,
				pollIntervalMs: POLL_MS * 5,
				...options.config,
				worker: { concurrency: 8, ...testWorker, ...options.config?.worker },
				queue,
				clock: ready(clock),
			},
		);
		runtime.register({ workflows, events: options.events ?? [] });
		runtimes.push(runtime);
		await runtime.start();
		return Object.assign(runtime, {
			queue,
			restart: async () => {
				await runtime.stop();
				runtimes.splice(runtimes.indexOf(runtime), 1);
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
					where s.execution_id = e.id and (${step ?? null}::text is null or s.step_key = ${step ?? null}
						-- PHASE2: Fabrial's custom race keys its subscriptions per branch until task 9.
						or starts_with(s.step_key, ${`__fabrial:any:${step}:`}))
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
			settled: () =>
				until(`${id} to settle`, async () => {
					const current = await info();
					if (!current) throw new Error(`Unknown execution: ${id}`);
					return settlement(current);
				}),
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
		execution,
	};
}
