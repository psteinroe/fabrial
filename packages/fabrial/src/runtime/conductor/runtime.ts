import postgres from "postgres";
import {
	type DurableExecution,
	type DurableRuntime,
	type InvocationMetadata,
	type Json,
	type JsonObject,
	type RuntimeEvent,
	type RuntimeSignal,
	type RuntimeWorkflow,
	type SignalWithStartOptions,
	type Subscription,
	type WaitBranch,
	type ExecutionResult,
} from "../../modules.ts";
import { ExecutionError } from "../../runtime.ts";
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
	type EventSubscription,
} from "pgconductor-js";

const emptyMetadata = (interactionId: string): InvocationMetadata => ({
	interactionId,
	origin: null,
	replyTo: null,
	requestedBy: null,
});

/** Set by the attempt middleware on the native context; the handler reuses its execution. */
const ATTEMPT = Symbol("fabrial.attempt");
type Attempt = { execution: DurableExecution; value: unknown };
function metadataOf(ctx: TaskContext): InvocationMetadata {
	return (ctx.metadata as InvocationMetadata | undefined) ?? emptyMetadata(ctx.executionId);
}
function mergeMetadata(
	metadata: InvocationMetadata,
	override?: Partial<InvocationMetadata>,
): InvocationMetadata {
	// `notify` addresses only the execution it was given to.
	const { notify: _notify, ...result } = metadata as InvocationMetadata & { notify?: unknown };
	for (const [key, value] of Object.entries(override ?? {}))
		if (value !== undefined) result[key] = value;
	return result;
}
function operationId(id: string): string {
	if (id.startsWith("__fabrial:")) throw new Error("Reserved Fabrial operation id");
	return id;
}
function duration(ms: number): number {
	if (!Number.isFinite(ms) || ms < 0) throw new Error("Invalid duration");
	return ms;
}

type NativeSubscription = Subscription & { native: EventSubscription<unknown> };

export type ConductorOptions = { sql: postgres.Sql } | { connectionString: string };
export interface ConductorConfig {
	queue?: string;
	logger?: Logger;
	worker?: Partial<WorkerConfig>;
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
	private definitions = new Map<string, RuntimeWorkflow>();
	private eventDefinitions: EventDefinition<string, JsonObject, string>[] = [];
	private client?: Conductor;
	private orchestrator?: Orchestrator;
	private running = false;
	private registered = false;

	constructor(
		options: ConductorOptions,
		private readonly config: ConductorConfig,
	) {
		this.ownsSql = "connectionString" in options;
		this.sql = "sql" in options ? options.sql : postgres(options.connectionString);
		this.queue = config.queue ?? "default";
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
		this.eventDefinitions = events.map((event) => ({
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
				removeOnComplete?: { days: number };
				removeOnFail?: { days: number };
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
						...(definition.retentionDays !== undefined
							? {
									removeOnComplete: { days: definition.retentionDays },
									removeOnFail: { days: definition.retentionDays },
								}
							: {}),
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
						// The native result: getExecution and result waits read `output`.
						return { output: (await definition.handler(input, execution, attempt?.value)) ?? null };
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

	async invoke(
		workflow: string,
		input: Json,
		options: { metadata: InvocationMetadata },
	): Promise<string> {
		this.assertReady();
		this.definition(workflow);
		return this.client!.invoke({ name: workflow, queue: this.queue } as never, { input } as never, {
			metadata: options.metadata,
		});
	}
	async emit(
		event: string,
		payload: JsonObject,
		options: { id?: string; metadata: InvocationMetadata },
	): Promise<void> {
		this.assertReady();
		this.event(event);
		await this.client!.emit(event as never, payload as never, {
			metadata: options.metadata,
			...(options.id !== undefined ? { dedupe_key: options.id } : {}),
		});
	}

	async cancel(executionId: string, reason = "Cancelled by user"): Promise<boolean> {
		this.assertReady();
		// Attached children (`cancelWithParent`) are cancelled with it, transitively.
		return this.client!.cancel(executionId, { reason });
	}

	async getExecution(
		executionId: string,
	): Promise<{ workflow: string; result: ExecutionResult | null; settledAt?: number } | null> {
		this.assertReady();
		// Ids come from clicks and other untrusted input: anything but a UUID is unknown.
		if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(executionId))
			return null;
		const info = await this.client!.getExecution(executionId);
		if (!info || info.queue !== this.queue) return null;
		const result: ExecutionResult | null =
			info.status === "completed"
				? { status: "completed", output: (info.result as { output: Json } | null)?.output ?? null }
				: info.status === "failed"
					? { status: "failed", error: info.error ?? "Execution failed" }
					: info.status === "cancelled"
						? { status: "cancelled", reason: info.error ?? "Cancelled by user" }
						: null;
		const settledAt = result ? (info.completedAt ?? info.failedAt)?.getTime() : undefined;
		return { workflow: info.taskKey, result, ...(settledAt !== undefined ? { settledAt } : {}) };
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
	}
	async stop(): Promise<void> {
		this.running = false;
		if (this.orchestrator) {
			await this.orchestrator.stop();
			await this.orchestrator.stopped;
		}
		if (this.ownsSql) await this.sql.end();
	}
	private execution(
		workflow: string,
		raw: TaskContext,
		metadata: InvocationMetadata,
	): DurableExecution {
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
					...(options?.cancelWithParent ? { cancelWithParent: true } : {}),
				},
			);
		};
		return {
			executionId,
			workflow,
			metadata,
			signal: raw.signal,
			step: (id, fn) => raw.step(operationId(id), fn),
			finalAttempt: async () => {
				const info = await this.client!.getExecution(executionId);
				// Conductor's default is 3 attempts; `attempts` counts this one.
				return !info || info.attempts >= (this.definition(workflow).retries?.maxAttempts ?? 3);
			},
			sleep: async (id, ms) => {
				operationId(id);
				duration(ms);
				await raw.checkpoint();
				await raw.sleep(id, ms);
			},
			subscribe: async (id, options) => {
				const native = await (
					raw.subscribe as (
						key: string,
						options: { event: EventDefinition<string, JsonObject, string>; filter?: object },
					) => Promise<EventSubscription<unknown>>
				)(operationId(id), { event: this.event(options.event), filter: options.filter });
				return { kind: "fabrial.subscription", id, native } satisfies NativeSubscription;
			},
			waitForAny: async (id, branches, options) => {
				operationId(id);
				if (options?.timeoutMs !== undefined) duration(options.timeoutMs);
				if ("timeout" in branches) throw new Error("waitForAny branch name timeout is reserved");
				const native = Object.fromEntries(
					Object.entries(branches).map(([key, branch]: [string, WaitBranch]) => [
						key,
						"kind" in branch ? (branch as NativeSubscription).native : branch,
					]),
				);
				const winner = (await raw.waitForAny(
					id,
					native as never,
					options?.timeoutMs !== undefined ? { timeout: options.timeoutMs } : undefined,
				)) as
					| { key: string; event: { name: string; payload: JsonObject } }
					| { key: string; outcome: ExecutionResult | { status: "completed"; result: unknown } }
					| { key: string; signal: { id: string; name: string } }
					| { key: "timeout" };
				if ("event" in winner)
					return {
						key: winner.key,
						kind: "event",
						event: { name: winner.event.name, payload: winner.event.payload },
					};
				if ("outcome" in winner) {
					const outcome = winner.outcome;
					return {
						key: winner.key,
						kind: "execution",
						result:
							outcome.status === "completed"
								? {
										status: "completed",
										output: (outcome as { result: { output: Json } | null }).result?.output ?? null,
									}
								: (outcome as ExecutionResult),
					};
				}
				if ("signal" in winner) return { key: winner.key, kind: "signal", signal: winner.signal };
				return { key: "timeout", kind: "timeout" };
			},
			waitForResult: async (id, executionId) => {
				const outcome = await raw.waitForResult<{ output: Json }>(operationId(id), executionId);
				if (outcome.status === "timeout") throw new Error("Unexpected result wait timeout");
				return outcome.status === "completed"
					? { status: "completed", output: outcome.result.output }
					: outcome;
			},
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
			emit: async (event, payload, options) => {
				this.event(event);
				await raw.emit(event as never, payload as never, {
					metadata: mergeMetadata(metadata, options?.metadata),
					...(options?.id !== undefined ? { dedupe_key: options.id } : {}),
				});
			},
		};
	}
}
