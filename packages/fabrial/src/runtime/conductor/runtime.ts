import postgres from "postgres";
import {
	EXECUTION_SETTLED_EVENT,
	type DurableExecution,
	type DurableRuntime,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type RuntimeEvent,
	type RuntimeSignal,
	type RuntimeWorkflow,
	type SignalWithStartOptions,
	type WaitBranch,
	type WaitForAnyResult,
	type ExecutionResult,
	type EventCursor,
} from "../../index.ts";
import { matchesFilter } from "../../internal.ts";
import {
	Conductor,
	Orchestrator,
	TaskSchemas,
	type TaskContext,
	type AnyTask,
	type Logger,
	type EventDefinition,
	type TaskDefinition,
	type Middleware,
	type WorkerConfig,
} from "pgconductor-js";

const DISPATCH_QUEUE = "pgconductor.internal";
const DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_STREAM_LOCK = [724831, 1] as const;
const emptyMetadata = (interactionId: string): InvocationMetadata => ({
	interactionId,
	origin: null,
	replyTo: null,
	requestedBy: null,
});

/** Set by the attempt middleware on the native context; the handler reuses its execution. */
const ATTEMPT = Symbol("fabrial.attempt");
type Attempt = { execution: ConductorExecution; value: unknown };
type NativeExecution = {
	id: string;
	locked_by: string;
	claim_token: string;
	task_key: string;
	queue: string;
};
// SHIM(conductor#3): ctx.executionId is public, but the claim (locked_by, claim_token) that fences
// the custom race SQL is not. Keep the private access in one place; core's per-attempt wrapper still
// runs on every replay. Never catch Conductor's hang-up control flow.
// PHASE2: removed in task 12
function nativeExecution(ctx: TaskContext): NativeExecution {
	return (ctx as unknown as { opts: { execution: NativeExecution } }).opts.execution;
}
function release(ctx: TaskContext, ms: number): Promise<never> {
	(
		ctx as unknown as {
			opts: { abortController: { abort(reason: unknown): void } };
		}
	).opts.abortController.abort({
		__pgconductorTaskAborted: true,
		reason: "released",
		reschedule_in_ms: Math.ceil(ms),
	});
	return new Promise(() => {});
}
async function rawOutput(sql: postgres.Sql, ctx: TaskContext, output: Json): Promise<void> {
	// SHIM(conductor#7): independent native executions don't retain their output. Overwrite, not
	// memoize: an attempt re-queued for undelivered signals discards its result and returns again.
	await sql`insert into pgconductor._private_steps (execution_id, queue, key, result)
		values (${ctx.executionId}::uuid, ${ctx.queue}, '__fabrial:output', ${sql.json({ result: output })})
		on conflict (execution_id, key) do update set result = excluded.result`;
}
function metadataOf(ctx: TaskContext): InvocationMetadata {
	return (ctx.metadata as InvocationMetadata | undefined) ?? emptyMetadata(ctx.executionId);
}
function mergeMetadata(
	metadata: InvocationMetadata,
	override?: Partial<InvocationMetadata>,
): InvocationMetadata {
	const result = { ...metadata };
	for (const [key, value] of Object.entries(override ?? {}))
		if (value !== undefined) result[key] = value;
	return result;
}
function resultOf(
	row: Pick<
		TerminalRow,
		"payload" | "cancelled" | "last_error" | "failed_at" | "completed_at" | "result"
	>,
): ExecutionResult | undefined {
	if (!row.failed_at && !row.completed_at) return undefined;
	// SHIM(conductor#5): cancellation is stored as failed_at + cancelled, not a terminal status.
	if (row.cancelled) return { status: "cancelled", reason: row.last_error ?? "Cancelled by user" };
	if (row.failed_at) return { status: "failed", error: row.last_error ?? "Execution failed" };
	if (row.completed_at) return { status: "completed", output: row.result?.result ?? null };
	return undefined;
}
type TerminalRow = {
	id: string;
	task_key: string;
	payload: JsonObject;
	metadata: InvocationMetadata | null;
	cancelled: boolean;
	last_error: string | null;
	failed_at: Date | null;
	completed_at: Date | null;
	result: { result: Json } | null;
};

// SHIM(conductor#8): the internal dispatcher defaults to immediate deletion. Override its native
// retention config before worker registration, so even events queued before restart keep receipts.
function retainDispatchReceipts(orchestrator: Orchestrator): void {
	const { workers } = orchestrator as unknown as {
		workers: { queueName: string; tasks: Map<string, AnyTask> }[];
	};
	const task = workers
		.find((worker) => worker.queueName === DISPATCH_QUEUE)
		?.tasks.get(DISPATCH_TASK);
	if (!task) throw new Error("Missing Conductor event dispatcher");
	Object.assign(task, { removeOnComplete: false, removeOnFail: false });
}

function operationId(id: string): string {
	if (id.startsWith("__fabrial:")) throw new Error("Reserved Fabrial operation id");
	return id;
}
function cursorPosition(cursor: EventCursor): string {
	if (!/^pgconductor:[0-9]+$/.test(cursor)) throw new Error("Invalid event cursor");
	return cursor.slice("pgconductor:".length);
}
function duration(ms: number): number {
	if (!Number.isFinite(ms) || ms < 0) throw new Error("Invalid duration");
	return ms;
}

/** The live Conductor `TaskContext` stays internal to the runtime; apps never see it. */
export type ConductorExecution = DurableExecution & { readonly raw: TaskContext };
export type ConductorOptions = { sql: postgres.Sql } | { connectionString: string };
export interface ConductorConfig {
	queue?: string;
	logger?: Logger;
	worker?: Partial<WorkerConfig>;
	/** Polling fallback for execution branches, timers, and terminal notifications. */
	pollIntervalMs?: number;
	/**
	 * Time source for workers (cron, sleeps, time windows) and `now()`. Tests pass
	 * `createTestClock(sql)` from `fabrial/testing`, with `sql` a single-connection pool (`max: 1`).
	 */
	clock?: { now(): Date };
}

export function conductor(options: ConductorOptions, config: ConductorConfig = {}): DurableRuntime {
	return new ConductorRuntime(options, config);
}

class ConductorRuntime implements DurableRuntime {
	private readonly sql: postgres.Sql;
	private readonly ownsSql: boolean;
	private readonly queue: string;
	private readonly pollMs: number;
	private definitions = new Map<string, RuntimeWorkflow>();
	private eventDefinitions: EventDefinition<string, JsonObject, string>[] = [];
	private client?: Conductor;
	private orchestrator?: Orchestrator;
	private monitor?: ReturnType<typeof setTimeout>;
	private monitoring?: Promise<void>;
	private settlementCursor?: string;
	private running = false;
	private registered = false;

	constructor(
		options: ConductorOptions,
		private readonly config: ConductorConfig,
	) {
		this.ownsSql = "connectionString" in options;
		this.sql = "sql" in options ? options.sql : postgres(options.connectionString);
		this.queue = config.queue ?? "default";
		this.pollMs = config.pollIntervalMs ?? 250;
		if (!Number.isFinite(this.pollMs) || this.pollMs <= 0)
			throw new Error("Invalid pollIntervalMs");
	}

	now(): number {
		return this.config.clock?.now().getTime() ?? Date.now();
	}

	register({ workflows, events }: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void {
		if (this.registered) throw new Error("Conductor runtime is already registered");
		for (const workflow of workflows) {
			if (this.definitions.has(workflow.name) || workflow.name.startsWith("pgconductor.")) {
				throw new Error(`Duplicate or reserved workflow: ${workflow.name}`);
			}
			this.definitions.set(workflow.name, workflow);
		}
		const catalog = new Map(events.map((event) => [event.name, event]));
		catalog.set(EXECUTION_SETTLED_EVENT, {
			name: EXECUTION_SETTLED_EVENT,
			filterable: ["executionId"],
		});
		this.eventDefinitions = [...catalog.values()].map((event) => ({
			name: event.name,
			payload: undefined as unknown as JsonObject,
			filterable: [...event.filterable],
		}));
		// Per-attempt hooks run as native middleware: its `next` settles when the attempt suspends.
		const attempt: Middleware<TaskContext> = async ({ execution, ctx }, next) => {
			const definition =
				execution.queue === this.queue ? this.definitions.get(execution.task_key) : undefined;
			if (!definition?.attempt) return next({});
			const durable = this.execution(definition.name, ctx, metadataOf(ctx));
			let result: Awaited<ReturnType<typeof next>> | undefined;
			await definition.attempt(durable, async (value) => {
				result = await next({ [ATTEMPT]: { execution: durable, value } satisfies Attempt });
			});
			return result ?? next({});
		};
		const client = Conductor.create({
			sql: this.sql,
			// Type-only: Fabrial validates workflow input itself, so Conductor has no schema to check.
			tasks: TaskSchemas.fromUnion<TaskDefinition<string, JsonObject, JsonObject, string>>(),
			events: this.eventDefinitions,
			context: {},
			middleware: [attempt] as never,
			logger: this.config.logger,
			clock: this.config.clock,
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
				groupConcurrency?: number;
			},
			triggers: object[],
			handler: (
				event: { name: string; payload?: JsonObject },
				ctx: TaskContext,
			) => Promise<JsonObject>,
		) => AnyTask;
		const tasks: AnyTask[] = [];
		for (const definition of workflows) {
			// Native triggers on the workflow's own task: events start it with their payload and metadata.
			const triggers: object[] = [
				{ invocable: true },
				...definition.triggers.map((trigger) => ({ event: trigger.event, filter: trigger.filter })),
				...(definition.cron ?? []).map((cron) => ({ cron: cron.schedule, name: cron.name })),
			];
			tasks.push(
				createTask(
					{
						name: definition.name,
						queue: this.queue,
						maxAttempts: definition.retries?.maxAttempts,
						concurrency: definition.concurrency,
					},
					triggers,
					async (event, ctx) => {
						const attempt = (ctx as unknown as { [ATTEMPT]?: Attempt })[ATTEMPT];
						const execution =
							attempt?.execution ?? this.execution(definition.name, ctx, metadataOf(ctx));
						const input: Json =
							event.name === "pgconductor.invoke"
								? event.payload!.input!
								: (event.payload ?? { name: event.name });
						const output = await definition.handler(input, execution, attempt?.value);
						await rawOutput(this.sql, ctx, output ?? null);
						return { output: output ?? null };
					},
				),
			);
		}
		this.client = client as unknown as Conductor;
		this.orchestrator = Orchestrator.create({
			conductor: client,
			workers: [client.createWorker({ queue: this.queue, tasks, config: this.config.worker })],
			defaultWorker: this.config.worker,
		});
		retainDispatchReceipts(this.orchestrator);
		this.registered = true;
	}

	private assertReady(): void {
		if (!this.running) throw new Error("Conductor runtime is not started");
	}
	private definition(name: string): RuntimeWorkflow {
		const definition = this.definitions.get(name);
		if (!definition) throw new Error(`Unknown workflow: ${name}`);
		return definition;
	}
	private event(name: string): EventDefinition<string, JsonObject, string> {
		const event = this.eventDefinitions.find((event) => event.name === name);
		if (!event) throw new Error(`Unknown event: ${name}`);
		return event;
	}

	// SHIM(conductor#7): Conductor.invoke's dedupe UPDATES/reinvokes existing executions. An
	// INSERT ... DO NOTHING receipt instead keeps the original execution, including after completion.
	// PHASE2: only client invocations (task 10 deletes cancelRun's recovery) and event receipts with
	// cursors (tasks 9 and 12) still insert directly.
	private async insert(
		task: string,
		queue: string,
		payload: JsonObject,
		metadata: InvocationMetadata,
		dedupe?: string,
		isEvent = false,
	): Promise<string> {
		return this.sql.begin(async (sql) => {
			if (isEvent) payload = { ...payload, __fabrialCursor: await this.position(sql) };
			const rows = await sql<{ id: string }[]>`
				insert into pgconductor._private_executions (task_key, queue, payload, metadata, dedupe_key)
				values (${task}, ${queue}, ${sql.json(payload)}, ${sql.json(metadata)}, ${dedupe ?? null})
				on conflict (task_key, dedupe_key, queue) where singleton_on is null do nothing returning id
			`;
			if (rows[0]) return rows[0].id;
			const [existing] = await sql<{ id: string }[]>`
				select id from pgconductor._private_executions
				where task_key = ${task} and queue = ${queue} and dedupe_key = ${dedupe!} and singleton_on is null
			`;
			if (!existing) throw new Error("Execution receipt was removed concurrently");
			return existing.id;
		});
	}
	// SHIM(conductor#1): nextval alone is not enough: an earlier emitter could commit
	// after a cursor and disappear behind it. Hold the same database-wide lock through
	// receipt commit and cursor capture. Sequence gaps (rollback/dedupe) are harmless.
	private async position(sql: postgres.TransactionSql): Promise<EventCursor> {
		await sql`select pg_advisory_xact_lock(${EVENT_STREAM_LOCK[0]}, ${EVENT_STREAM_LOCK[1]})`;
		const [existing] = await sql<
			{ exists: boolean }[]
		>`select to_regclass('pgconductor.__fabrial_event_position') is not null as exists`;
		if (!existing!.exists) await sql`create sequence pgconductor.__fabrial_event_position`;
		const [row] = await sql<{ position: string }[]>`
			select nextval('pgconductor.__fabrial_event_position')::text as position
		`;
		return `pgconductor:${row!.position}`;
	}
	private captureCursor(): Promise<EventCursor> {
		return this.sql.begin((sql) => this.position(sql));
	}
	async invoke(
		workflow: string,
		input: Json,
		options: { metadata: InvocationMetadata; dedupeKey?: string },
	): Promise<string> {
		this.assertReady();
		this.definition(workflow);
		return this.insert(workflow, this.queue, { input }, options.metadata, options.dedupeKey);
	}
	async emit(
		event: string,
		payload: JsonObject,
		options: { id?: string; metadata: InvocationMetadata },
	): Promise<void> {
		this.assertReady();
		this.event(event);
		await this.insert(
			DISPATCH_TASK,
			DISPATCH_QUEUE,
			{ eventKey: event, payload },
			options.metadata,
			options.id === undefined ? undefined : JSON.stringify([event, options.id]),
			true,
		);
	}

	async cancel(executionId: string, reason = "Cancelled by user"): Promise<boolean> {
		this.assertReady();
		// Attached children (`cancelWithParent`) are cancelled with it, transitively.
		return this.client!.cancel(executionId, { reason });
	}

	async signal(executionId: string, signal: RuntimeSignal): Promise<boolean> {
		this.assertReady();
		return this.client!.signal(executionId, signal);
	}
	async signalWithStart(
		options: SignalWithStartOptions & { metadata: InvocationMetadata },
	): Promise<string> {
		this.assertReady();
		this.definition(options.workflow);
		return this.client!.signalWithStart({
			task: options.workflow,
			queue: this.queue,
			key: options.key,
			signal: options.signal,
			input: { input: options.input ?? null },
			metadata: options.metadata,
		} as unknown as Parameters<Conductor["signalWithStart"]>[0]);
	}

	async start(): Promise<void> {
		if (!this.orchestrator) throw new Error("Register workflows before starting Conductor");
		if (this.running) return;
		// Resumed handlers may emit while worker startup is still completing.
		this.running = true;
		try {
			await this.orchestrator.start();
		} catch (error) {
			this.running = false;
			throw error;
		}
		this.scheduleMonitor();
	}
	async stop(): Promise<void> {
		this.running = false;
		clearTimeout(this.monitor);
		await this.monitoring;
		if (this.orchestrator) {
			await this.orchestrator.stop();
			await this.orchestrator.stopped;
		}
		if (this.ownsSql) await this.sql.end();
	}
	private scheduleMonitor(): void {
		this.monitor = setTimeout(() => {
			this.monitoring = this.reconcile()
				.catch((error: unknown) =>
					this.client!.logger.error("Conductor reconciliation failed", error),
				)
				.finally(() => {
					if (this.running) this.scheduleMonitor();
				});
		}, this.pollMs);
	}
	private async reconcile(): Promise<void> {
		// SHIM(conductor#5): settle failures/cancellations the core wrapper cannot observe, even after
		// restart. Prefer the core settlement hook; without it, success notifications belong to core.
		// Receipts live only in Conductor's tables. Settlement hooks must be idempotent.
		const rows = await this.sql<TerminalRow[]>`
			select e.*, s.result from pgconductor._private_executions e
			left join pgconductor._private_steps s on s.execution_id = e.id and s.key = '__fabrial:output'
			where e.queue = ${this.queue} and e.task_key = any(${[...this.definitions.keys()]})
			and (e.failed_at is not null or e.completed_at is not null) and not exists (
				select 1 from pgconductor._private_steps s where s.execution_id = e.id and s.key = '__fabrial:settled'
			) and (${this.settlementCursor ?? null}::uuid is null or e.id > ${this.settlementCursor ?? null}::uuid)
			order by e.id limit 100
		`;
		// SHIM(conductor#5): a poison hook must not starve other settlements. Scan cyclically;
		// only successful delivery gets a durable receipt, so failures also retry after restart.
		if (!rows.length) this.settlementCursor = undefined;
		for (const row of rows) {
			if (!this.running) return;
			this.settlementCursor = row.id;
			try {
				const result = resultOf(row)!;
				const metadata = row.metadata ?? emptyMetadata(row.id);
				const hook = this.definition(row.task_key).onSettled;
				if (hook) await hook(row.id, metadata, result);
				else if (result.status !== "completed")
					await this.emit(
						EXECUTION_SETTLED_EVENT,
						{ executionId: row.id, result },
						{ id: row.id, metadata },
					);
				await this.sql`insert into pgconductor._private_steps (execution_id, queue, key, result)
					values (${row.id}::uuid, ${this.queue}, '__fabrial:settled', '{}'::jsonb) on conflict do nothing`;
			} catch (error) {
				this.client!.logger.error("Conductor settlement delivery failed", {
					executionId: row.id,
					error,
				});
			}
		}
	}

	private execution(
		workflow: string,
		raw: TaskContext,
		metadata: InvocationMetadata,
	): ConductorExecution {
		const executionId = raw.executionId;
		const start: DurableExecution["start"] = (id, name, input, options) => {
			this.definition(name);
			return raw.invoke(
				operationId(id),
				{ name, queue: this.queue },
				{ input },
				{
					metadata: mergeMetadata(metadata, options?.metadata),
					...(options?.dedupeKey !== undefined ? { dedupe_key: options.dedupeKey } : {}),
				},
			);
		};
		return {
			executionId,
			workflow,
			metadata,
			signal: raw.signal,
			raw,
			step: (id, fn) => raw.step(operationId(id), fn),
			sleep: async (id, ms) => {
				operationId(id);
				duration(ms);
				await raw.checkpoint();
				await raw.sleep(id, ms);
			},
			cursor: (id) => raw.step(operationId(id), () => this.captureCursor()),
			waitForEvent: async (id, options) => {
				operationId(id);
				const branches: Record<string, WaitBranch> = {
					event: {
						kind: "event",
						event: options.event,
						filter: options.filter,
						after: options.after,
					},
				};
				if (options.deadline !== undefined)
					branches.timeout = { kind: "timer", at: options.deadline };
				const winner = await this.waitForAny(raw, id, branches);
				if (winner.kind === "timer") return null;
				if (winner.kind !== "event") throw new Error("Invalid event result");
				return { ...winner.event, cursor: winner.cursor };
			},
			waitForAny: (id, branches) => this.waitForAny(raw, operationId(id), branches),
			start,
			invoke: async (id, name, input, options) => {
				operationId(id);
				if (options?.timeoutMs !== undefined) duration(options.timeoutMs);
				this.definition(name);
				// Not invokeAndWait: a failed child must reach the caller's catch as an ExecutionError.
				const child = await raw.invoke(
					`__fabrial:invoke:${id}`,
					{ name, queue: this.queue },
					{ input },
					{
						metadata: mergeMetadata(metadata, options?.metadata),
						cancelWithParent: !options?.detached,
					},
				);
				const outcome = await raw.waitForResult<{ output: Json }>(
					id,
					child,
					options?.timeoutMs !== undefined ? { timeout: options.timeoutMs } : undefined,
				);
				if (outcome.status === "timeout") {
					// A native timeout leaves the child running; an attached call owns its child.
					if (!options?.detached) await this.cancel(child, "Parent invocation timed out");
					throw new Error(`Child execution timed out after ${options?.timeoutMs}ms`);
				}
				if (outcome.status === "completed") return outcome.result.output;
				throw new ExecutionError(child, outcome);
			},
			onSignal: (name, handler) =>
				raw.onSignal(name, (payload, info) => handler(payload as Json, { id: info.id })),
			signalWithStart: (id, options) => {
				this.definition(options.workflow);
				return raw.signalWithStart(operationId(id), {
					task: options.workflow,
					queue: this.queue,
					key: options.key,
					signal: options.signal,
					input: { input: options.input ?? null },
					metadata: mergeMetadata(metadata, options.metadata),
				} as unknown as Parameters<TaskContext["signalWithStart"]>[1]);
			},
			emit: (event, payload, options) =>
				this.emit(event, payload, {
					id: options?.id,
					metadata: mergeMetadata(metadata, options?.metadata),
				}),
			cancel: (id, reason) => this.cancel(id, reason),
		};
	}

	private async waitForAny(
		raw: TaskContext,
		id: string,
		branches: Record<string, WaitBranch>,
	): Promise<WaitForAnyResult> {
		const entries = Object.entries(branches);
		if (!entries.length) throw new Error("waitForAny requires at least one branch");
		for (const [, branch] of entries)
			if (branch.kind === "timer" && (!Number.isFinite(branch.at) || branch.at < 0))
				throw new Error("Invalid timer deadline");
		await raw.checkpoint();
		const execution = nativeExecution(raw);
		const key = `__fabrial:any:${id}`;
		const cached = await this.client!.db.loadStep(
			{ executionId: execution.id, queue: this.queue, key },
			{ signal: raw.signal },
		);
		if (cached !== undefined) return (cached as { result: WaitForAnyResult }).result;
		// SHIM(conductor#6): register native subscriptions under branch keys, then poll durable
		// receipts/results/timers. No worker is occupied while waiting. Losing subscriptions are
		// removed in the transaction saving the winner. Ready branches use database timestamps, with
		// stream order breaking event ties, then record order. Retained dispatch receipts bridge claim-time races.
		const boundary = await raw.step(`${key}:cursor`, () => this.captureCursor());
		const positions = entries.flatMap(([, branch]) =>
			branch.kind === "event" ? [cursorPosition(branch.after ?? boundary)] : [],
		);
		const earliest = positions.reduce(
			(a, b) => (BigInt(a) < BigInt(b) ? a : b),
			cursorPosition(boundary),
		);
		// One statement observes every branch and its clock together; serial branch reads could
		// otherwise pick a late event over a timer that expired while another branch was queried.
		// Observe only committed receipts: an in-flight pre-deadline emitter must finish
		// before an expired timer can win. Later emitters timestamp their receipt after this clock.
		const observed = await this.sql.begin(async (sql) => {
			await sql`select pg_advisory_xact_lock(${EVENT_STREAM_LOCK[0]}, ${EVENT_STREAM_LOCK[1]})`;
			return sql<
				(Omit<TerminalRow, "id"> & {
					id: string | null;
					event_at: number;
					settled_at: number | null;
					observed_at: number;
				})[]
			>`
			with clock as materialized (select pgconductor._private_current_time() as observed_at)
			select e.*, s.result,
				extract(epoch from clock.observed_at)::double precision * 1000 as observed_at,
				extract(epoch from e.created_at)::double precision * 1000 as event_at,
				extract(epoch from coalesce(e.failed_at, e.completed_at))::double precision * 1000 as settled_at
			from clock
			left join pgconductor._private_executions e on (
				(e.queue = ${DISPATCH_QUEUE} and e.task_key = ${DISPATCH_TASK}
				and e.payload ->> 'eventKey' = any(${entries.flatMap(([, branch]) => (branch.kind === "event" ? [branch.event] : []))}::text[])
				and (e.payload ->> '__fabrialCursor') like 'pgconductor:%'
				and substring(e.payload ->> '__fabrialCursor' from 13)::bigint > ${earliest}::bigint)
				or e.id = any(${entries.flatMap(([, branch]) => (branch.kind === "execution" ? [branch.executionId] : []))}::uuid[])
			)
			left join pgconductor._private_steps s on s.execution_id = e.id and s.key = '__fabrial:output'
			order by substring(e.payload ->> '__fabrialCursor' from 13)::bigint, e.created_at, e.id
		`;
		});
		const now = observed[0]!.observed_at;
		const candidates: { result: WaitForAnyResult; at: number; position?: string }[] = [];
		let delay = this.pollMs;
		for (const [name, branch] of entries) {
			if (branch.kind === "timer") {
				const at = branch.at;
				const remaining = at - now;
				if (remaining <= 0) candidates.push({ result: { key: name, kind: "timer" }, at });
				if (remaining > 0) delay = Math.min(delay, remaining);
			} else if (branch.kind === "execution") {
				const row = observed.find((row) => row.id === branch.executionId);
				if (!row) throw new Error(`Unknown execution: ${branch.executionId}`);
				const result = resultOf(row);
				if (result)
					candidates.push({
						result: { key: name, kind: "execution", result },
						at: row.settled_at ?? now,
					});
			} else {
				const branchKey = `${key}:event:${name}`;
				const event = observed.find(
					(row) =>
						row.id !== null &&
						row.task_key === DISPATCH_TASK &&
						row.payload.eventKey === branch.event &&
						BigInt(cursorPosition(row.payload.__fabrialCursor as string)) >
							BigInt(cursorPosition(branch.after ?? boundary)) &&
						matchesFilter(row.payload.payload as JsonObject, branch.filter),
				);
				if (event) {
					candidates.push({
						result: {
							key: name,
							kind: "event",
							event: { name: branch.event, payload: event.payload.payload as JsonObject },
							cursor: event.payload.__fabrialCursor as string,
						},
						at: event.event_at,
						position: cursorPosition(event.payload.__fabrialCursor as string),
					});
					continue;
				}
				// Public, claim-fenced registration; only the buffered receipt polling above stays private.
				await (
					raw.subscribe as (
						key: string,
						options: { event: EventDefinition<string, JsonObject, string>; filter?: object },
					) => Promise<unknown>
				)(branchKey, { event: this.event(branch.event), filter: branch.filter });
			}
		}
		const winner = candidates.sort(
			(a, b) =>
				a.at - b.at ||
				(a.position && b.position ? Number(BigInt(a.position) - BigInt(b.position)) : 0),
		)[0]?.result;
		if (!winner) return release(raw, delay);
		const saved = await this.sql.begin(async (sql) => {
			const [claim] = await sql<{ id: string }[]>`
				select id from pgconductor._private_executions where id = ${execution.id}::uuid
				and locked_by = ${execution.locked_by}::uuid and claim_token = ${execution.claim_token}::uuid
				and not cancelled
				and failed_at is null and completed_at is null for update
			`;
			if (!claim) return false;
			await sql`insert into pgconductor._private_steps (execution_id, queue, key, result)
				values (${execution.id}::uuid, ${this.queue}, ${key}, ${sql.json({ result: winner! })}) on conflict do nothing`;
			await sql`delete from pgconductor._private_custom_event_subscriptions where execution_id = ${execution.id}::uuid
				and step_key = any(${entries.filter(([, branch]) => branch.kind === "event").map(([name]) => `${key}:event:${name}`)})`;
			return true;
		});
		if (!saved) return release(raw, 0);
		return winner;
	}
}

/** A child terminal failure, distinguishable from ordinary handler failures. */
export class ExecutionError extends Error {
	constructor(
		readonly executionId: string,
		readonly result: Exclude<ExecutionResult, { status: "completed" }>,
	) {
		super(result.status === "cancelled" ? result.reason : result.error);
		this.name = "ExecutionError";
	}
}
