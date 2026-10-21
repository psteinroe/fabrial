import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll } from "vitest";
import type { HostedApp } from "../../../src/app.ts";
import type { RuntimeWorkflow } from "../../internals.ts";
import { PostgresStorage } from "../../../src/agents/index.ts";
import { FakeChat, testApp } from "../../../src/testing-internal.ts";
import { until, useConductor } from "../../conductor/helper.ts";

type Config = Parameters<typeof testApp>[1];

/**
 * Routed agents on real Conductor, in real time: one database per test file holds Conductor and Pi's
 * storage. Tests wait for what an app did (posts, model calls, submitted requests) and, before asserting
 * that something did not happen, for `idle`: every event routed and every execution of the app settled.
 */
export function useAgents() {
	const db = useConductor();
	let pool: Sql | undefined;
	const apps: HostedApp<never>[] = [];
	beforeAll(async () => {
		pool = postgres(db.url, { max: 10, onnotice: () => {} });
		await PostgresStorage.migrate(pool);
	});
	afterEach(async () => {
		for (const app of apps.splice(0).reverse()) await app.stop();
	});
	afterAll(async () => {
		await pool?.end();
	});
	const sql = () => {
		if (!pool) throw new Error("useAgents() is only usable inside tests");
		return pool;
	};

	/**
	 * Start an app on a fresh queue, or on `queue` to restart a stopped app. `retries` overrides the attempts of internal workflows such as
	 * `fabrial.session`, registered before the runtime starts.
	 */
	async function launch(
		instance: Parameters<typeof testApp>[0],
		config: Omit<Config, "runtime" | "chat"> & { chat?: FakeChat; queue?: string },
		retries: Record<string, RuntimeWorkflow["retries"]> = {},
	) {
		const { queue, ...rest } = config;
		const runtime = db.runtime(queue ? { queue } : {});
		const register = runtime.register.bind(runtime);
		runtime.register = (definitions) =>
			register({
				...definitions,
				workflows: definitions.workflows.map((workflow) =>
					retries[workflow.name] ? { ...workflow, retries: retries[workflow.name] } : workflow,
				),
			});
		const chat = rest.chat ?? new FakeChat();
		const app = testApp(instance, { ...rest, runtime, chat });
		await app.start();
		apps.push(app as HostedApp<never>);
		return { runtime, chat, app };
	}

	/** Tasks of the executions on `queue` (or of all apps) that have not settled. */
	async function unsettled(queue: string | undefined, tasks: string) {
		const rows = await sql()<{ task: string }[]>`
			select task_key as task from pgconductor._private_executions
			where (${queue ?? null}::text is null or queue = ${queue ?? null})
				and task_key like ${tasks} and completed_at is null and failed_at is null
				-- each queue's maintenance task runs for as long as its workers do
				and task_key != 'pgconductor.maintenance'
				-- a cron's next occurrence waits for its schedule, not on the app
				and not (cron_expression is not null and attempts = 0 and locked_at is null)
		`;
		return rows.map((row) => row.task);
	}

	/**
	 * Every event emitted so far was dispatched, then every execution of the app on `queue` (or of all
	 * apps) settled: routes, workflows, Sessions and their children, including pending retries and
	 * suspended waits. Dispatch inserts the routes it starts, and a route or workflow starts what it hands
	 * work to before it settles, so the order is causal. Wait for the effect a test expects first: work
	 * started later, such as a background report, is not waited for.
	 */
	async function idle(queue?: string, timeoutMs?: number) {
		for (const [description, tasks, on] of [
			["events to dispatch", "pgconductor.event-dispatch", "pgconductor.internal"],
			["the app's executions to settle", "%", queue],
		] as const) {
			let last: string[] = [];
			try {
				await until(
					description,
					async () => ((last = await unsettled(on, tasks)).length === 0 ? true : undefined),
					timeoutMs,
				);
			} catch (error) {
				throw new Error(`${(error as Error).message}; unsettled: ${last.join(", ")}`);
			}
		}
	}

	/** Executions of `workflow` on the app on `queue` that have not settled, oldest first. */
	async function open(queue: string, workflow: string) {
		const rows = await sql()<{ id: string }[]>`
			select id from pgconductor._private_executions
			where queue = ${queue} and task_key = ${workflow} and completed_at is null and failed_at is null
			order by created_at
		`;
		return rows.map((row) => row.id);
	}

	/** Executions of `workflow` on the app on `queue`, settled or not, oldest first. */
	async function all(queue: string, workflow: string) {
		const rows = await sql()<{ id: string }[]>`
			select id from pgconductor._private_executions
			where queue = ${queue} and task_key = ${workflow}
			order by created_at
		`;
		return rows.map((row) => row.id);
	}

	/**
	 * How the router routed the ingress `id` on `queue` (a message's dedupe id defaults to its message id),
	 * once it finished: `follow`, `new` or `ignored`.
	 */
	async function routed(queue: string, id: string) {
		return until(`ingress ${id} to be routed`, async () => {
			const [row] = await sql()<{ route: string | null }[]>`
				select result->'output'->>'route' as route from pgconductor._private_executions
				where queue = ${queue} and task_key = 'fabrial.route' and payload->'payload'->>'id' = ${id}
					and completed_at is not null
			`;
			return row?.route ?? undefined;
		});
	}

	return { db, sql, launch, idle, open, all, routed };
}
