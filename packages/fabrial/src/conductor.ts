import type postgres from "postgres";
import {
	type AnyTask,
	Conductor,
	type Logger,
	type Middleware,
	Orchestrator,
	type TaskContext,
	type TaskDefinition,
	TaskSchemas,
	type WorkerConfig,
} from "pgconductor-js";
import type { EventFilter } from "./routing/events.ts";
import type { Surface } from "./chat/messages.ts";
import type { Json, JsonObject } from "./schema.ts";

export interface ConductorConfig {
	/** Conductor's tables live in this database; the pool stays the caller's. */
	sql: postgres.Sql;
	/** The queue of the app's tasks. Defaults to `default`. */
	queue?: string;
	logger?: Logger;
	worker?: Partial<WorkerConfig>;
	/**
	 * Time source for workers (cron, sleeps, time windows) and Fabrial's clock. Tests pass
	 * `createTestClock(sql)` from `fabrial/internal/testing`, with `sql` a single-connection pool (`max: 1`).
	 */
	clock?: { now(): Date };
}

/**
 * A workflow core registers as a Conductor task. App workflows have no event triggers: the routing workflow starts
 * them. Internal workflows (the router) trigger on events directly.
 */
export interface WorkflowTask {
	name: string;
	/** Event triggers: each matching event starts one execution, with the event's payload and metadata. */
	triggers: { event: string; filter?: EventFilter }[];
	/** Cron triggers; an execution's input is `{ name }`, and the handler gets the trigger as `tick`. */
	cron?: CronTrigger[];
	concurrency?: number;
	retries?: { maxAttempts?: number };
	/**
	 * Days a settled execution stays readable (`getExecution`, result waits); kept forever by default.
	 * Never 0 for executions others wait on: a wait registered after removal fails.
	 */
	retentionDays?: number;
	/**
	 * Wraps every attempt, including each resume (Conductor middleware). `next` settles when the attempt
	 * ends: it resolves when the handler returns, suspends or is cancelled, and rejects when it throws.
	 * The object passed to `next` is merged onto Conductor's context (`ctx`), which reaches `handler`.
	 */
	attempt?: (ctx: TaskContext, next: (additions: object) => Promise<void>) => Promise<void>;
	/**
	 * Its return value is the execution's native result (`getExecution`, result waits). `tick` is the cron
	 * trigger that started a static cron tick (which has no metadata).
	 */
	handler: (input: Json, ctx: TaskContext, tick?: CronTrigger) => Promise<Json | void>;
}

export interface CronTrigger {
	schedule: string;
	name: string;
	replyTo?: Surface;
}

/** An event the app's tasks emit, trigger on or wait for. */
export interface TaskEvent {
	name: string;
	/** Fields filterable by triggers and waits. */
	filterable: readonly string[];
}

/**
 * The app's Conductor client and the Orchestrator running its tasks on `queue`. `middleware` runs after each
 * workflow's attempt hook (plugin workflow middleware).
 */
export function createConductor(
	config: ConductorConfig,
	definition: {
		workflows: readonly WorkflowTask[];
		events: readonly TaskEvent[];
		middleware?: Middleware<TaskContext>[];
	},
): { conductor: Conductor; orchestrator: Orchestrator; queue: string } {
	const queue = config.queue ?? "default";
	const definitions = new Map<string, WorkflowTask>();
	for (const workflow of definition.workflows) {
		if (definitions.has(workflow.name) || workflow.name.startsWith("pgconductor."))
			throw new Error(`Duplicate or reserved workflow: ${workflow.name}`);
		definitions.set(workflow.name, workflow);
	}
	// Per-attempt hooks run as native middleware: its `next` settles when the attempt suspends.
	const attempt: Middleware<TaskContext> = async ({ execution, ctx }, next) => {
		const hook =
			execution.queue === queue ? definitions.get(execution.task_key)?.attempt : undefined;
		if (!hook) return next({});
		// A hook that doesn't call `next` still runs the attempt; the result itself may be null.
		let ran = false;
		let result: Awaited<ReturnType<typeof next>> | undefined;
		await hook(ctx, async (additions) => {
			ran = true;
			result = await next(additions);
		});
		return ran ? result! : next({});
	};
	const client = Conductor.create({
		sql: config.sql,
		// Type-only: Fabrial validates workflow input itself, so Conductor has no schema to check.
		tasks: TaskSchemas.fromUnion<TaskDefinition<string, JsonObject, JsonObject, string>>(),
		events: definition.events.map((event) => ({
			name: event.name,
			payload: undefined as unknown as JsonObject,
			filterable: [...event.filterable],
		})),
		context: {},
		middleware: [attempt, ...(definition.middleware ?? [])] as never,
		logger: config.logger,
		clock: config.clock,
		telemetry: false,
	});
	// Dynamic catalogs cannot express Conductor's literal-name conditional types. This boundary
	// erases only compile-time validation; createTask still validates triggers at runtime.
	const createTask = client.createTask.bind(client) as unknown as (
		definition: {
			name: string;
			queue: string;
			maxAttempts?: number;
			concurrency?: number;
			removeOnComplete?: { days: number };
			removeOnFail?: { days: number };
		},
		triggers: object[],
		handler: (
			event: { name: string; payload?: JsonObject },
			ctx: TaskContext,
		) => Promise<JsonObject>,
	) => AnyTask;
	const tasks = definition.workflows.map((workflow) =>
		createTask(
			{
				name: workflow.name,
				queue,
				maxAttempts: workflow.retries?.maxAttempts,
				concurrency: workflow.concurrency,
				...(workflow.retentionDays !== undefined
					? {
							removeOnComplete: { days: workflow.retentionDays },
							removeOnFail: { days: workflow.retentionDays },
						}
					: {}),
			},
			[
				{ invocable: true },
				// Native triggers on the workflow's own task: events start it with their payload and metadata.
				...workflow.triggers.map((trigger) => ({ event: trigger.event, filter: trigger.filter })),
				...(workflow.cron ?? []).map((cron) => ({ cron: cron.schedule, name: cron.name })),
			],
			// Input and output are what Conductor sees: the invoke or event payload, the native result.
			// A static cron tick has no payload: its event is the trigger's name.
			async (event, ctx) =>
				((await workflow.handler(
					event.payload ?? { name: event.name },
					ctx,
					event.payload === undefined
						? workflow.cron?.find((cron) => cron.name === event.name)
						: undefined,
				)) ?? null) as JsonObject,
		),
	);
	const orchestrator = Orchestrator.create({
		conductor: client,
		workers: [client.createWorker({ queue, tasks, config: config.worker })],
		defaultWorker: config.worker,
	});
	return { conductor: client as unknown as Conductor, orchestrator, queue };
}
