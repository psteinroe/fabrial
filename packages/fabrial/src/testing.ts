import type { AgentIntegration, ChatIntegration, FabrialHost } from "./app.ts";
import type { Evaluator } from "./evaluate.ts";
import type { ExternalIdentity } from "./identity.ts";
import { identityKey } from "./directory.ts";
import { matchesFilter, THREAD_TTL } from "./internal.ts";
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
	WaitBranch,
	WaitForAnyResult,
} from "./runtime.ts";
import { parseSchema } from "./schema.ts";
import type { ChatMessage, MessageRef, OutboundMessage, Surface, ThreadRef } from "./thread.ts";

const clone = <T>(value: T): T => structuredClone(value);
class Suspension extends Error {}
class Cancellation extends Error {}
interface RecordedEvent {
	name: string;
	payload: JsonObject;
	sequence: number;
	time: number;
}
interface Run {
	id: string;
	workflow: RuntimeWorkflow;
	input: Json;
	metadata: InvocationMetadata;
	controller: AbortController;
	cache: Map<string, unknown>;
	waits: Map<string, { branches: Record<string, WaitBranch>; started: number }>;
	children: Set<string>;
	consumed: Set<number>;
	sequence: number;
	attempts: number;
	status: "pending" | "running" | "suspended" | "settled";
	result?: ExecutionResult;
	settledAt?: number;
	mutex?: string;
}

/** Replaying, in-memory port implementation. Emit/invoke enqueue work; await flush() to run until idle,
 * and advanceBy(ms) to drive timers. No wall-clock timers or database are used. */
export class MemoryRuntime implements DurableRuntime {
	readonly workflows = new Map<string, RuntimeWorkflow>();
	readonly events = new Map<string, RuntimeEvent>();
	readonly emitted: RecordedEvent[] = [];
	private readonly runs = new Map<string, Run>();
	private readonly emitKeys = new Set<string>();
	private readonly invocationKeys = new Map<string, string>();
	private readonly mutexes = new Map<string, string>();
	private readonly queue = new Set<string>();
	private readonly settlements = new Set<string>();
	private clock: number;
	private nextId = 1;
	private flushing?: Promise<void>;
	private active = false;
	constructor(options: { now?: number | Date } = {}) {
		this.clock =
			options.now instanceof Date
				? options.now.getTime()
				: (options.now ?? Date.parse("2026-01-05T00:00:00Z"));
	}
	now(): number {
		return this.clock;
	}
	register(definition: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void {
		for (const workflow of definition.workflows) {
			if (
				workflow.concurrency !== undefined &&
				(!Number.isInteger(workflow.concurrency) || workflow.concurrency < 1)
			)
				throw new Error("Workflow concurrency must be a positive integer");
			if (this.workflows.has(workflow.name))
				throw new Error(`Duplicate workflow: ${workflow.name}`);
			this.workflows.set(workflow.name, workflow);
		}
		for (const event of definition.events) this.events.set(event.name, event);
	}
	async start(): Promise<void> {
		this.active = true;
	}
	async stop(): Promise<void> {
		await this.flushing;
		this.active = false;
	}
	async emit(
		name: string,
		payload: JsonObject,
		options: { id?: string; metadata: InvocationMetadata; owner?: string; dispatchId?: string },
	): Promise<void> {
		const key = options.id === undefined ? undefined : JSON.stringify([name, options.id]);
		if (key && this.emitKeys.has(key)) return;
		if (key) this.emitKeys.add(key);
		this.emitted.push({
			name,
			payload: clone(payload),
			sequence: this.emitted.length,
			time: this.clock,
		});
		for (const run of this.runs.values())
			if (
				run.status === "suspended" &&
				[...run.waits.values()].some((wait) =>
					Object.values(wait.branches).some(
						(branch) =>
							branch.kind === "event" &&
							branch.event === name &&
							matchesFilter(payload, branch.filter),
					),
				)
			)
				this.queue.add(run.id);
		for (const workflow of this.workflows.values()) {
			const matching = workflow.triggers.filter(
				(t) =>
					t.event === name &&
					matchesFilter(payload, t.filter) &&
					(t.role === "observer" || workflow.name === options.owner),
			);
			if (!matching.length) continue;
			const owns = matching.some((t) => t.role === "owner" && workflow.name === options.owner);
			await this.invoke(workflow.name, payload, {
				metadata: {
					...options.metadata,
					ownsThread: owns,
					replyTo: owns ? options.metadata.replyTo : null,
				},
				dedupeKey: options.dispatchId
					? `dispatch:${options.dispatchId}:${workflow.name}`
					: undefined,
			});
		}
	}
	async invoke(
		workflow: string,
		input: Json,
		options: { metadata: InvocationMetadata; dedupeKey?: string },
	): Promise<string> {
		const invocationKey =
			options.dedupeKey === undefined ? undefined : JSON.stringify([workflow, options.dedupeKey]);
		if (invocationKey !== undefined && this.invocationKeys.has(invocationKey))
			return this.invocationKeys.get(invocationKey)!;
		const definition = this.workflows.get(workflow);
		if (!definition) throw new Error(`Unknown workflow: ${workflow}`);
		const id = `execution-${this.nextId++}`;
		const run: Run = {
			id,
			workflow: definition,
			input: clone(input),
			metadata: clone(options.metadata),
			controller: new AbortController(),
			cache: new Map(),
			waits: new Map(),
			children: new Set(),
			consumed: new Set(),
			sequence: this.emitted.length,
			attempts: 0,
			status: "pending",
		};
		run.mutex = definition.mutex?.(input, options.metadata);
		this.runs.set(id, run);
		this.queue.add(id);
		if (invocationKey !== undefined) this.invocationKeys.set(invocationKey, id);
		return id;
	}
	async cancel(id: string, reason = "Cancelled"): Promise<boolean> {
		const run = this.runs.get(id);
		if (!run || run.result) return false;
		run.controller.abort(reason);
		for (const child of run.children) await this.cancel(child, reason);
		if (run.status !== "running") await this.settle(run, { status: "cancelled", reason });
		return true;
	}
	result(id: string): ExecutionResult | undefined {
		return clone(this.runs.get(id)?.result);
	}
	executions(workflow?: string): {
		executionId: string;
		workflow: string;
		status: Run["status"];
		metadata: InvocationMetadata;
		result?: ExecutionResult;
	}[] {
		return [...this.runs.values()]
			.filter((r) => !workflow || r.workflow.name === workflow)
			.map((r) => ({
				executionId: r.id,
				workflow: r.workflow.name,
				status: r.status,
				metadata: clone(r.metadata),
				result: clone(r.result),
			}));
	}
	stepIds(id: string): string[] {
		return [...(this.runs.get(id)?.cache.keys() ?? [])];
	}
	async advanceBy(ms: number): Promise<void> {
		if (ms < 0 || !Number.isFinite(ms))
			throw new Error("Clock must advance by a finite nonnegative duration");
		this.clock += ms;
		for (const run of this.runs.values())
			if (
				run.status === "suspended" &&
				[...run.waits.values()].some((wait) =>
					Object.values(wait.branches).some(
						(branch) => branch.kind === "timer" && wait.started + branch.ms <= this.clock,
					),
				)
			)
				this.queue.add(run.id);
		await this.flush();
	}
	async flush(): Promise<void> {
		if (this.flushing) return this.flushing;
		if (!this.active) return;
		this.flushing = this.drain();
		try {
			await this.flushing;
		} finally {
			this.flushing = undefined;
		}
	}
	private async drain() {
		while (this.queue.size || this.settlements.size) {
			for (const id of this.settlements) {
				const run = this.runs.get(id)!;
				await run.workflow.onSettled?.(id, clone(run.metadata), clone(run.result!));
				this.settlements.delete(id);
			}
			const batch: Run[] = [];
			const concurrency = new Map<string, number>();
			for (const id of this.queue) {
				const run = this.runs.get(id)!;
				if (run.result) {
					this.queue.delete(id);
					continue;
				}
				if (run.mutex && this.mutexes.has(run.mutex)) continue;
				const active = concurrency.get(run.workflow.name) ?? 0;
				if (active >= (run.workflow.concurrency ?? Infinity)) continue;
				concurrency.set(run.workflow.name, active + 1);
				this.queue.delete(id);
				if (run.mutex) this.mutexes.set(run.mutex, id);
				batch.push(run);
			}
			if (!batch.length) break;
			await Promise.all(batch.map((run) => this.attempt(run)));
		}
	}
	private async settle(run: Run, result: ExecutionResult) {
		if (run.result) return;
		run.result = clone(result);
		run.settledAt = this.clock;
		run.status = "settled";
		run.waits.clear();
		this.queue.delete(run.id);
		this.settlements.add(run.id);
		for (const waiting of this.runs.values())
			if (
				waiting.status === "suspended" &&
				[...waiting.waits.values()].some((wait) =>
					Object.values(wait.branches).some(
						(branch) => branch.kind === "execution" && branch.executionId === run.id,
					),
				)
			)
				this.queue.add(waiting.id);
	}
	private async attempt(run: Run) {
		run.status = "running";
		try {
			const output = await run.workflow.handler(clone(run.input), this.execution(run));
			await this.settle(
				run,
				run.controller.signal.aborted
					? { status: "cancelled", reason: String(run.controller.signal.reason) }
					: { status: "completed", output: output ?? null },
			);
		} catch (error) {
			if (error instanceof Suspension) run.status = "suspended";
			else if (error instanceof Cancellation || run.controller.signal.aborted)
				await this.settle(run, {
					status: "cancelled",
					reason: String(run.controller.signal.reason),
				});
			else if (++run.attempts < (run.workflow.retries?.maxAttempts ?? 1)) {
				run.status = "pending";
				this.queue.add(run.id);
			} else
				await this.settle(run, {
					status: "failed",
					error: error instanceof Error ? error.message : String(error),
				});
		} finally {
			if (run.mutex) this.mutexes.delete(run.mutex);
		}
	}
	private execution(run: Run): DurableExecution {
		const boundary = () => {
			if (run.controller.signal.aborted) throw new Cancellation();
		};
		const step: DurableExecution["step"] = async (id, fn) => {
			boundary();
			if (run.cache.has(id)) return clone(run.cache.get(id)) as never;
			const value = await fn();
			run.cache.set(id, clone(value));
			return clone(value);
		};
		const wait = async (
			id: string,
			branches: Record<string, WaitBranch>,
		): Promise<WaitForAnyResult> => {
			boundary();
			if (!Object.keys(branches).length) throw new Error("waitForAny needs at least one branch");
			for (const branch of Object.values(branches)) {
				if (branch.kind === "timer" && (!Number.isFinite(branch.ms) || branch.ms < 0))
					throw new Error("Invalid timer duration");
				if (branch.kind === "execution" && !this.runs.has(branch.executionId))
					throw new Error(`Unknown execution: ${branch.executionId}`);
			}
			if (run.cache.has(id)) return clone(run.cache.get(id)) as WaitForAnyResult;
			let registration = run.waits.get(id);
			if (!registration) {
				registration = { branches: clone(branches), started: this.clock };
				run.waits.set(id, registration);
			}
			const candidates: {
				time: number;
				sequence: number;
				result: WaitForAnyResult;
				event?: number;
			}[] = [];
			for (const [key, branch] of Object.entries(registration.branches)) {
				if (branch.kind === "timer") {
					const due = registration.started + branch.ms;
					if (due <= this.clock)
						candidates.push({ time: due, sequence: Infinity, result: { key, kind: "timer" } });
				} else if (branch.kind === "execution") {
					const child = this.runs.get(branch.executionId);
					if (child?.result)
						candidates.push({
							time: child.settledAt!,
							sequence: -1,
							result: { key, kind: "execution", result: child.result },
						});
				} else {
					const event = this.emitted.find(
						(e) =>
							e.sequence >= run.sequence &&
							!run.consumed.has(e.sequence) &&
							e.name === branch.event &&
							matchesFilter(e.payload, branch.filter),
					);
					if (event)
						candidates.push({
							time: event.time,
							sequence: event.sequence,
							event: event.sequence,
							result: { key, kind: "event", event: { name: event.name, payload: event.payload } },
						});
				}
			}
			candidates.sort((a, b) => a.time - b.time || a.sequence - b.sequence);
			const winner = candidates[0];
			if (!winner) throw new Suspension();
			if (winner.event !== undefined) run.consumed.add(winner.event);
			run.cache.set(id, clone(winner.result));
			run.waits.delete(id);
			return clone(winner.result);
		};
		const start: DurableExecution["start"] = async (id, workflow, input, options = {}) =>
			step(id, async () => {
				const childId = await this.invoke(workflow, input, {
					metadata: { ...run.metadata, ...options.metadata } as InvocationMetadata,
					dedupeKey: options.dedupeKey ?? `${run.id}:${id}`,
				});
				if (options.mutex) this.runs.get(childId)!.mutex = options.mutex;
				return childId;
			});
		return {
			executionId: run.id,
			workflow: run.workflow.name,
			metadata: clone(run.metadata),
			signal: run.controller.signal,
			step,
			sleep: async (id, ms) => {
				await wait(id, { timer: { kind: "timer", ms } });
			},
			waitForAny: wait,
			waitForEvent: async (id, options) => {
				const branches: Record<string, WaitBranch> = {
					event: { kind: "event", event: options.event, filter: options.filter },
				};
				if (options.timeoutMs !== undefined)
					branches.timeout = { kind: "timer", ms: options.timeoutMs };
				const result = await wait(id, branches);
				return result.kind === "event" ? result.event : null;
			},
			start,
			invoke: async (id, workflow, input, options = {}) => {
				const childId = await start(`${id}:start`, workflow, input, options);
				if (!options.detached) run.children.add(childId);
				const branches: Record<string, WaitBranch> = {
					child: { kind: "execution", executionId: childId },
				};
				if (options.timeoutMs !== undefined)
					branches.timeout = { kind: "timer", ms: options.timeoutMs };
				const result = await wait(`${id}:result`, branches);
				if (result.kind !== "execution") throw new Error("Child invocation timed out");
				if (result.result.status !== "completed")
					throw new Error(
						result.result.status === "failed" ? result.result.error : result.result.reason,
					);
				return result.result.output;
			},
			emit: (event, payload, options = {}) =>
				this.emit(event, payload, {
					...options,
					metadata: { ...run.metadata, ...options.metadata } as InvocationMetadata,
				}),
			cancel: (id, reason) => this.cancel(id, reason),
		};
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
	async getState(): Promise<ThreadRoutingState | null> {
		return this.owner.now() >= this.expires ? null : clone(this.state);
	}
	async setState(state: ThreadRoutingState | null): Promise<void> {
		this.state = clone(state);
		this.expires = this.owner.now() + THREAD_TTL;
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
	async getState(): Promise<ThreadRoutingState | null> {
		return this.actual ? this.actual.getState() : null;
	}
	async setState(state: ThreadRoutingState | null): Promise<void> {
		if (!this.actual)
			throw new Error("Cannot persist routing state for a provisional channel thread");
		await this.actual.setState(state);
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
	): Promise<"reply" | "new" | "ignored"> {
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
		evaluate?: Evaluator["evaluate"];
		state?: StatePort;
		workflows?: RuntimeWorkflow[];
	} = {},
): AgentIntegration & { values: Map<string, JsonObject> } {
	const values = new Map<string, JsonObject>();
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
		connect: () => ({
			agents: {
				workflows: () => options.workflows ?? [],
				run: options.run ?? ((execution, id, _agent, opts) => execution.step(id, () => opts.input)),
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

export function createTestRuntime(
	options: ConstructorParameters<typeof MemoryRuntime>[0] = {},
): MemoryRuntime {
	return new MemoryRuntime(options);
}
