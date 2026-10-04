import postgres from "postgres";
import { releaseQueuedExecutionsOnStop } from "./shutdown.ts";
import {
	EXECUTION_SETTLED_EVENT,
	type DurableExecution,
	type DurableRuntime,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type RuntimeEvent,
	type RuntimeWorkflow,
	type WaitBranch,
	type WaitForAnyResult,
	type ExecutionResult,
	type EventCursor,
} from "fabrial";
import {
	Conductor,
	Orchestrator,
	TaskSchemas,
	EventSchemas,
	type TaskContext,
	type AnyTask,
	type Logger,
	compileEventFilter,
	type EventDefinition,
	type WorkerConfig,
	type EventFilterTerm,
} from "./pgconductor.ts";

const RESERVED = "__fabrial";
const DISPATCH_QUEUE = "pgconductor.internal";
const DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_STREAM_LOCK = [724831, 1] as const;
const emptyMetadata = (interactionId: string): InvocationMetadata => ({
	interactionId,
	origin: null,
	replyTo: null,
	requestedBy: null,
	ownsThread: false,
});

type Carrier = {
	metadata: InvocationMetadata;
	owner?: string;
	dispatchId?: string;
	dispatchOwner?: { workflow: string; event: string };
	parent?: string;
	cancelReason?: string;
};
type NativeExecution = { id: string; locked_by: string; task_key: string };
// SHIM(conductor#3): today's context hides execution identity. Keep the private access in one place;
// core's per-attempt wrapper still runs on every replay. Never catch Conductor's hang-up control flow.
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
async function rawOutput(ctx: TaskContext, output: Json): Promise<void> {
	// SHIM(conductor#7): independent native executions don't retain their output.
	await ctx.step("__fabrial:output", () => output);
}
function carrier(payload: JsonObject): Carrier {
	return payload[RESERVED] as unknown as Carrier;
}
function clean(payload: JsonObject): JsonObject {
	const { [RESERVED]: ignored, __fabrialOwner: owner, ...result } = payload;
	void ignored;
	void owner;
	return result;
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
	if (row.cancelled)
		return {
			status: "cancelled",
			// Native running settlement overwrites last_error using the claim's stale error.
			reason: carrier(row.payload)?.cancelReason ?? row.last_error ?? "Cancelled by user",
		};
	if (row.failed_at) return { status: "failed", error: row.last_error ?? "Execution failed" };
	if (row.completed_at) return { status: "completed", output: row.result?.result ?? null };
	return undefined;
}
type TerminalRow = {
	id: string;
	task_key: string;
	payload: JsonObject;
	cancelled: boolean;
	last_error: string | null;
	failed_at: Date | null;
	completed_at: Date | null;
	result: { result: Json } | null;
};

// SHIM(conductor#6): buffered fallback for events dispatched during a polling replay's claim.
// Terms are compiled/validated by Conductor; fields are ANDed and alternatives are ORed.
function matchesTerms(payload: JsonObject, terms: EventFilterTerm[]): boolean {
	const fields = new Map<string, boolean>();
	for (const term of terms) {
		const value = payload[term.field_name];
		const scalar = value === null ? "null" : typeof value;
		let matches = false;
		switch (term.operator) {
			case "exists":
				matches = (value !== undefined) === term.boolean_value;
				break;
			case "prefix":
				matches = typeof value === "string" && value.startsWith(term.text_value!);
				break;
			case "numeric_range":
				matches =
					typeof value === "number" &&
					(term.lower_value == null ||
						(term.lower_inclusive ? value >= term.lower_value : value > term.lower_value)) &&
					(term.upper_value == null ||
						(term.upper_inclusive ? value <= term.upper_value : value < term.upper_value));
				break;
			case "exact":
			case "anything_but": {
				const expected =
					term.scalar_type === "null"
						? null
						: term.scalar_type === "string"
							? term.text_value
							: term.scalar_type === "number"
								? term.number_value
								: term.boolean_value;
				const equal = scalar === term.scalar_type && value === expected;
				matches =
					term.operator === "exact"
						? equal
						: value !== undefined &&
							["string", "number", "boolean", "null"].includes(scalar) &&
							!equal;
				break;
			}
		}
		fields.set(term.field_name, (fields.get(term.field_name) ?? false) || matches);
	}
	return [...fields.values()].every(Boolean);
}

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

/** Native handles remain available under core's ctx.raw.execution.raw. */
export type ConductorExecution = DurableExecution & { readonly raw: TaskContext };
export type ConductorOptions = { sql: postgres.Sql } | { connectionString: string };
export interface ConductorConfig {
	queue?: string;
	logger?: Logger;
	worker?: Partial<WorkerConfig>;
	/** Polling fallback for execution branches, timers, and terminal notifications. */
	pollIntervalMs?: number;
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
		return Date.now();
	}

	register({ workflows, events }: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void {
		if (this.registered) throw new Error("Conductor runtime is already registered");
		for (const workflow of workflows) {
			if (
				this.definitions.has(workflow.name) ||
				workflow.name.startsWith("fabrial.route:") ||
				workflow.name.startsWith("pgconductor.")
			) {
				throw new Error(`Duplicate or reserved workflow: ${workflow.name}`);
			}
			this.definitions.set(workflow.name, workflow);
		}
		const catalog = new Map(events.map((event) => [event.name, event]));
		for (const event of events) {
			if (event.filterable.includes(RESERVED) || event.filterable.includes("__fabrialOwner"))
				throw new Error("Reserved Fabrial event field");
		}
		catalog.set(EXECUTION_SETTLED_EVENT, {
			name: EXECUTION_SETTLED_EVENT,
			filterable: ["executionId"],
		});
		this.eventDefinitions = [...catalog.values()].map((event) => ({
			name: event.name,
			payload: undefined as unknown as JsonObject,
			// SHIM(conductor#2): metadata lives in a reserved field; payload filters remain top-level.
			filterable: [...event.filterable, "__fabrialOwner"],
		}));
		const schemas = [...this.definitions.keys()].flatMap((name) => [
			{ name, queue: this.queue, payload: {} as JsonObject, returns: {} as JsonObject },
			{
				name: `fabrial.route:${name}`,
				queue: this.queue,
				payload: {} as JsonObject,
				returns: {} as JsonObject,
			},
		]);
		const client = Conductor.create({
			sql: this.sql,
			tasks: TaskSchemas.fromSchema(schemas),
			events: EventSchemas.fromSchema(this.eventDefinitions),
			context: {},
			logger: this.config.logger,
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
			tasks.push(
				createTask(
					{
						name: definition.name,
						queue: this.queue,
						maxAttempts: definition.retries?.maxAttempts,
						concurrency: definition.concurrency,
						// SHIM(conductor#4): soft, per (queue, task, group), NOT a cross-task strict lock.
						groupConcurrency: 1,
					},
					[{ invocable: true }],
					async (event, ctx) => {
						const payload = event.payload!;
						const execution = this.execution(definition.name, ctx, carrier(payload).metadata);
						const output = await definition.handler(payload.input!, execution);
						await rawOutput(ctx, output ?? null);
						return { output: output ?? null };
					},
				),
			);
			const triggers: object[] = definition.triggers.map((trigger) => ({
				event: trigger.event,
				filter: {
					...trigger.filter,
					...(trigger.role === "owner" ? { __fabrialOwner: [definition.name] } : {}),
				},
			}));
			triggers.push(
				...(definition.cron ?? []).map((cron) => ({ cron: cron.schedule, name: cron.name })),
			);
			if (triggers.length)
				tasks.push(
					createTask(
						{
							name: `fabrial.route:${definition.name}`,
							queue: this.queue,
						},
						triggers,
						async (event, ctx) => {
							const raw = nativeExecution(ctx);
							const payload = event.payload;
							const inherited = payload ? carrier(payload).metadata : emptyMetadata(raw.id);
							const isOwner =
								payload &&
								carrier(payload).owner === definition.name &&
								definition.triggers.some(
									(trigger) =>
										trigger.role === "owner" &&
										trigger.event === event.name &&
										matchesTerms(
											payload,
											compileEventFilter(trigger.event, trigger.filter, this.eventDefinitions)
												.terms,
										),
								);
							// Canonical ownership is carried on every category of the ingress. Suppress
							// this workflow's competing observer submissions BEFORE insert-on-conflict.
							const dispatchOwner = payload && carrier(payload).dispatchOwner;
							if (
								dispatchOwner &&
								dispatchOwner.workflow === definition.name &&
								(event.name !== dispatchOwner.event || !isOwner)
							)
								return {};
							const metadata = {
								...inherited,
								ownsThread: Boolean(isOwner && inherited.ownsThread),
								replyTo: isOwner ? inherited.replyTo : null,
							};
							const input: Json = payload ? clean(payload) : { name: event.name };
							// Multiple matching subscriptions of one workflow collapse to one actual invocation.
							const [source] = await this.sql<{ parent_execution_id: string | null }[]>`
					select parent_execution_id from pgconductor._private_executions where id = ${raw.id}::uuid
				`;
							await this.launch(
								definition.name,
								input,
								metadata,
								// SHIM(conductor#8): one ingress can emit several event categories. The
								// shared dispatch ID dedupes the actual workflow, not the event receipt.
								payload && carrier(payload).dispatchId !== undefined
									? `__fabrial:dispatch:${JSON.stringify([carrier(payload).dispatchId, definition.name])}`
									: `event:${source?.parent_execution_id ?? raw.id}:${definition.name}`,
							);
							return {};
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
		releaseQueuedExecutionsOnStop(this.orchestrator);
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
	private async insert(
		task: string,
		queue: string,
		payload: JsonObject,
		dedupe?: string,
		group?: string,
		isEvent = false,
	): Promise<string> {
		return this.sql.begin(async (sql) => {
			if (isEvent) payload = { ...payload, __fabrialCursor: await this.position(sql) };
			const rows = await sql<{ id: string }[]>`
				insert into pgconductor._private_executions (task_key, queue, payload, dedupe_key, "group")
				values (${task}, ${queue}, ${sql.json(payload)}, ${dedupe ?? null}, ${group ?? null})
				on conflict (task_key, dedupe_key, queue) do nothing returning id
			`;
			if (rows[0]) return rows[0].id;
			const [existing] = await sql<{ id: string }[]>`
				select id from pgconductor._private_executions where task_key = ${task} and queue = ${queue} and dedupe_key = ${dedupe!}
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
	private async launch(
		name: string,
		input: Json,
		metadata: InvocationMetadata,
		dedupe?: string,
		mutex?: string,
		parent?: string,
	): Promise<string> {
		const definition = this.definition(name);
		const group = mutex ?? definition.mutex?.(input, metadata);
		return this.insert(
			name,
			this.queue,
			{ input, [RESERVED]: { metadata, ...(parent ? { parent } : {}) } },
			dedupe,
			group,
		);
	}

	async invoke(
		workflow: string,
		input: Json,
		options: { metadata: InvocationMetadata; dedupeKey?: string },
	): Promise<string> {
		this.assertReady();
		return this.launch(workflow, input, options.metadata, options.dedupeKey);
	}
	async emit(
		event: string,
		payload: JsonObject,
		options: {
			id?: string;
			metadata: InvocationMetadata;
			owner?: string;
			dispatchId?: string;
			dispatchOwner?: { workflow: string; event: string };
		},
	): Promise<void> {
		this.assertReady();
		this.event(event);
		if (RESERVED in payload || "__fabrialOwner" in payload)
			throw new Error("Reserved Fabrial event field");
		await this.insert(
			DISPATCH_TASK,
			DISPATCH_QUEUE,
			{
				eventKey: event,
				payload: {
					...payload,
					[RESERVED]: {
						metadata: options.metadata,
						...(options.owner ? { owner: options.owner } : {}),
						...(options.dispatchId !== undefined ? { dispatchId: options.dispatchId } : {}),
						...(options.dispatchOwner ? { dispatchOwner: options.dispatchOwner } : {}),
					},
					__fabrialOwner: options.owner ?? null,
				},
			},
			options.id === undefined ? undefined : JSON.stringify([event, options.id]),
			undefined,
			true,
		);
	}

	async cancel(executionId: string, reason = "Cancelled by user"): Promise<boolean> {
		this.assertReady();
		// SHIM(conductor#5): structured children are persisted in the metadata envelope. Detached
		// calls/start have no parent link. Mark pending native cancellations too (upstream omits it).
		const cancelled = await this.sql.begin(async (sql) => {
			const [row] = await sql<{ cancelled: boolean }[]>`
				select pgconductor.cancel_execution(${executionId}::uuid, ${reason}) as cancelled
			`;
			if (!row?.cancelled) return false;
			// Preserve the requested reason atomically: native settlement can replace last_error.
			await sql`update pgconductor._private_executions set cancelled = true,
				payload = jsonb_set(payload, array[${RESERVED}, 'cancelReason'], to_jsonb(${reason}::text))
				where id = ${executionId}::uuid`;
			return true;
		});
		if (!cancelled) return false;
		const children = await this.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where payload -> ${RESERVED} ->> 'parent' = ${executionId}
			and completed_at is null and failed_at is null and not cancelled
		`;
		for (const child of children) await this.cancel(child.id, reason);
		return true;
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
		const orphans = await this.sql<{ id: string; last_error: string | null }[]>`
			select child.id, coalesce(parent.payload -> ${RESERVED} ->> 'cancelReason', parent.last_error) as last_error
			from pgconductor._private_executions child
			join pgconductor._private_executions parent on parent.id::text = child.payload -> ${RESERVED} ->> 'parent'
			where child.queue = ${this.queue} and child.completed_at is null and child.failed_at is null and not child.cancelled
			and (parent.cancelled or parent.failed_at is not null)
		`;
		for (const child of orphans)
			if (this.running) await this.cancel(child.id, child.last_error ?? "Parent execution ended");
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
				const metadata = carrier(row.payload).metadata;
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
		const executionId = nativeExecution(raw).id;
		const start: DurableExecution["start"] = (id, name, input, options) =>
			raw.step(operationId(id), () =>
				this.launch(
					name,
					input,
					mergeMetadata(metadata, options?.metadata),
					options?.dedupeKey ?? `__fabrial:start:${JSON.stringify([executionId, id])}`,
					options?.mutex,
				),
			);
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
				// SHIM(conductor#5): independent deduped submission plus a persisted structured link
				// replaces native invoke so detached calls can wait without cancellation propagation.
				const child = await raw.step(`__fabrial:invoke:${id}`, () =>
					this.launch(
						name,
						input,
						mergeMetadata(metadata, options?.metadata),
						`__fabrial:invoke:${JSON.stringify([executionId, id])}`,
						options?.mutex,
						options?.detached ? undefined : executionId,
					),
				);
				const branches: Record<string, WaitBranch> = {
					child: { kind: "execution", executionId: child },
				};
				if (options?.timeoutMs !== undefined)
					branches.timeout = {
						kind: "timer",
						at: await raw.step(`__fabrial:invoke:${id}:deadline`, async () => {
							const [row] = await this.sql<
								{ at: number }[]
							>`select extract(epoch from pgconductor._private_current_time())::double precision * 1000 as at`;
							return row!.at + options.timeoutMs!;
						}),
					};
				const winner = await this.waitForAny(raw, id, branches);
				if (winner.kind === "timer") {
					if (!options?.detached) await this.cancel(child, "Parent invocation timed out");
					throw new Error(`Child execution timed out after ${options?.timeoutMs}ms`);
				}
				if (winner.kind !== "execution") throw new Error("Invalid child result");
				if (winner.result.status === "completed") return winner.result.output;
				throw new ExecutionError(child, winner.result);
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
				const compiled = compileEventFilter(branch.event, branch.filter, this.eventDefinitions);
				const event = observed.find(
					(row) =>
						row.id !== null &&
						row.task_key === DISPATCH_TASK &&
						row.payload.eventKey === branch.event &&
						BigInt(cursorPosition(row.payload.__fabrialCursor as string)) >
							BigInt(cursorPosition(branch.after ?? boundary)) &&
						matchesTerms(row.payload.payload as JsonObject, compiled.terms),
				);
				if (event) {
					candidates.push({
						result: {
							key: name,
							kind: "event",
							event: { name: branch.event, payload: clean(event.payload.payload as JsonObject) },
							cursor: event.payload.__fabrialCursor as string,
						},
						at: event.event_at,
						position: cursorPosition(event.payload.__fabrialCursor as string),
					});
					continue;
				}
				const registered = await this.sql.begin(async (sql) => {
					const [claim] = await sql<{ id: string }[]>`
						select id from pgconductor._private_executions where id = ${execution.id}::uuid
						and locked_by = ${execution.locked_by}::uuid and not cancelled
						and failed_at is null and completed_at is null for update
					`;
					if (!claim) return false;
					const [subscription] = await sql<{ id: string }[]>`
						insert into pgconductor._private_custom_event_subscriptions
						(event_key, task_key, queue, required_field_count, kind, execution_id, step_key)
						values (${branch.event}, ${execution.task_key}, ${this.queue}, ${compiled.required_field_count}, 'execution_wait', ${execution.id}::uuid, ${branchKey})
						on conflict (execution_id, step_key) do nothing returning id
					`;
					if (subscription)
						await sql`
						insert into pgconductor._private_event_filter_terms
						(subscription_id, term_number, event_key, field_name, operator, scalar_type, text_value, number_value, boolean_value, number_range)
						select * from pgconductor._private_expand_event_filter_terms(${subscription.id}::uuid, ${branch.event}, ${sql.json(compiled.terms)})
					`;
					return true;
				});
				if (!registered) return release(raw, 0);
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
				and locked_by = ${execution.locked_by}::uuid and not cancelled
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
