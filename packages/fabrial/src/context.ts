import type { AgentIntegration, FabrialHost } from "./app.ts";
import { createApproval } from "./protocol.ts";
import { duration } from "./internal.ts";
import type { Json, JsonObject } from "./json.ts";
import type {
	DurableExecution,
	InvocationMetadata,
	ThreadIO,
	WaitBranch,
	WaitForAnyResult,
} from "./runtime.ts";
import { parseSchema } from "./schema.ts";
import { follow, prepareSend } from "./send.ts";
import { followResource, storedFollowOptions, unfollowResource } from "./follows.ts";
import type { Awaitable, MessageRef, OutboundMessage, Thread, Surface } from "./thread.ts";
import type { AnyWorkflow, WorkflowContext } from "./workflow.ts";

/** An awaitable is a native wait branch, or a timer that starts when its race first runs. */
export interface InternalAwaitable extends Awaitable {
	branch?: WaitBranch;
	timerMs?: number;
	/** Turn the winning branch into the awaitable's value. */
	accept?: (winner: WaitForAnyResult) => Json;
}

export interface ContextOptions {
	host: FabrialHost;
	execution: DurableExecution;
	metadata: InvocationMetadata;
	agents?: ReturnType<AgentIntegration["connect"]>;
	now: () => number;
}

export async function createContext(options: ContextOptions): Promise<WorkflowContext> {
	const { host, execution, metadata, agents, now } = options;
	const ids = new Map<string, number>();
	const used = new Set<string>();
	const op = (id: string): string => {
		if (!id || typeof id !== "string")
			throw new Error("Durable operations require a non-empty explicit id");
		if (id.startsWith("fabrial:") || id.includes("#"))
			throw new Error("Operation ids beginning with fabrial: or containing # are reserved");
		let count = ids.get(id) ?? 0;
		let key = count === 0 ? id : `${id}#${count}`;
		while (used.has(key)) key = `${id}#${++count}`;
		ids.set(id, count + 1);
		used.add(key);
		return key;
	};
	let io: ThreadIO | undefined;
	let realThread = metadata.replyTo?.kind === "thread";
	const getIO = async (): Promise<ThreadIO> => {
		if (io) return io;
		if (!metadata.replyTo || !host.chat()) throw new Error("This execution has no chat surface");
		io =
			metadata.replyTo.kind === "thread"
				? await host.chat()!.thread(metadata.replyTo)
				: await host.chat()!.resolve(metadata.replyTo);
		return io;
	};
	if (metadata.replyTo?.kind === "thread" && host.chat()) await getIO();

	const post = async (id: string, message: OutboundMessage): Promise<MessageRef> => {
		const ref = (await execution.step(
			id,
			async () => (await (await getIO()).post(message)) as unknown as JsonObject,
		)) as unknown as MessageRef;
		io = await host
			.chat()!
			.thread({ kind: "thread", provider: ref.provider, threadId: ref.threadId });
		realThread = true;
		metadata.replyTo = io.ref as InvocationMetadata["replyTo"];
		execution.metadata.replyTo = metadata.replyTo;
		return ref;
	};

	/**
	 * One native wait: branches race, and the earliest timer is its timeout. Timers are fixed when the
	 * race first runs; losing subscriptions close, losing executions keep running.
	 */
	const race = async (
		id: string,
		awaitables: Record<string, Awaitable>,
	): Promise<{ key: string; value: Json }> => {
		const entries = Object.entries(awaitables) as [string, InternalAwaitable][];
		if (!entries.length) throw new Error("race needs at least one awaitable");
		for (const [, awaitable] of entries)
			if (!awaitable.branch && awaitable.timerMs === undefined)
				throw new Error("Invalid Fabrial awaitable");
		const deadlines = (await execution.step(`fabrial:${id}:timers`, () =>
			Object.fromEntries(
				entries.flatMap(([key, awaitable]) =>
					awaitable.timerMs === undefined ? [] : [[key, now() + awaitable.timerMs]],
				),
			),
		)) as Record<string, number>;
		// Ties go to the timer listed first.
		const timer = Object.entries(deadlines).reduce<[string, number] | undefined>(
			(earliest, entry) => (!earliest || entry[1] < earliest[1] ? entry : earliest),
			undefined,
		);
		const branches = Object.fromEntries(
			entries.flatMap(([key, awaitable]) =>
				awaitable.branch ? [[`branch:${key}`, awaitable.branch]] : [],
			),
		);
		const remaining = timer && Math.max(0, timer[1] - now());
		if (!Object.keys(branches).length) {
			await execution.sleep(`fabrial:${id}:wait`, remaining!);
			return { key: timer![0], value: null };
		}
		const winner = await execution.waitForAny(
			`fabrial:${id}:wait`,
			branches,
			remaining === undefined ? undefined : { timeoutMs: remaining },
		);
		if (winner.kind === "timeout") return { key: timer![0], value: null };
		const key = winner.key.slice("branch:".length);
		const awaitable = awaitables[key] as InternalAwaitable;
		const value = awaitable.accept
			? awaitable.accept(winner)
			: winner.kind === "event"
				? winner.event.payload
				: winner.kind === "execution"
					? (winner.result as unknown as Json)
					: (winner.signal as unknown as Json);
		return { key, value };
	};
	const surface = metadata.replyTo as Surface | null;
	const thread: Thread | undefined =
		surface && host.chat()
			? {
					get ref() {
						return (
							io?.ref ??
							(surface.kind === "thread"
								? surface
								: { kind: "thread", provider: surface.provider, threadId: surface.channelId })
						);
					},
					get channelId() {
						return (
							io?.channelId ?? (surface.kind === "channel" ? surface.channelId : surface.threadId)
						);
					},
					get isDM() {
						return io?.isDM ?? false;
					},
					post: (id, message) => post(op(id), message),
					update: async (id, message, content) =>
						execution.step(op(id), async () =>
							(
								await host.chat()!.thread({
									kind: "thread",
									provider: message.provider,
									threadId: message.threadId,
								})
							).update(message, content),
						),
					history: (id, opts = {}) =>
						execution.step(op(id), async () => (await getIO()).history(opts)),
				}
			: undefined;
	const checkInput = async (workflow: AnyWorkflow, input: Json): Promise<Json> => {
		const registered = host.workflows.find((w) => w.name === workflow.name);
		if (!registered) throw new Error(`Unregistered workflow: ${workflow.name}`);
		return registered.definition.input
			? parseSchema(registered.definition.input, input, `${workflow.name} input`)
			: input;
	};
	// An agent Session awaits only the workflows its tools start, not their children.
	const { piChild: _piChild, ...inherited } = metadata;
	const childMetadata = (): Partial<InvocationMetadata> => ({
		...inherited,
		ownerWorkflow: null,
		triggerEvent: null,
		replyTo: (realThread
			? (io?.ref ?? metadata.replyTo)
			: metadata.replyTo) as InvocationMetadata["replyTo"],
	});
	const surfaces = () => ({
		current: (realThread && io ? io.ref : metadata.replyTo) as InvocationMetadata["replyTo"],
		channelId: async () => (await getIO()).channelId,
	});
	const ctx: WorkflowContext = {
		executionId: execution.executionId,
		interactionId: metadata.interactionId,
		thread,
		clients: host.clients(),
		actor: metadata.requestedBy,
		metadata,
		signal: execution.signal,
		step: (id, fn) => execution.step(op(id), fn),
		sleep: (id, value) => execution.sleep(op(id), duration(value)),
		agent: async (id, agent, opts) => {
			if (!agents) throw new Error("Agent integration required");
			return (await agents.agents.run(execution, op(id), agent, opts)) as never;
		},
		send: async (id, agent, opts) => {
			if (!agents) throw new Error("Agent integration required");
			const key = op(id);
			const request = await execution.step(`fabrial:${key}:send`, async () => {
				const prepared = await prepareSend(
					host,
					metadata,
					surfaces(),
					`${execution.executionId}:${key}`,
					agent,
					opts,
				);
				// Later messages in the thread reach the conversation directly.
				await follow(host, prepared.follow, { session: prepared.session, agent: agent.name });
				return prepared;
			});
			return agents.agents.send(execution, key, agent, request);
		},
		follow: (id, resource, opts) =>
			execution.step(op(id), () =>
				followResource(host, metadata, surfaces(), resource, storedFollowOptions(opts)),
			),
		unfollow: (id, resource, opts) =>
			execution.step(op(id), () =>
				unfollowResource(host, metadata, surfaces(), resource, storedFollowOptions(opts)),
			),
		evaluate: async (id, request) => {
			if (!agents) throw new Error("Agent integration required");
			return (await execution.step(
				op(id),
				async () => (await agents.evaluator.evaluate(request, execution.signal)) as unknown as Json,
			)) as never;
		},
		state: (state) => ({
			get: (id) => {
				if (!agents) throw new Error("State integration required");
				return agents.state.get(execution, op(id), state);
			},
			update: (id, fn) => {
				if (!agents) throw new Error("State integration required");
				return agents.state.update(execution, op(id), state, fn);
			},
		}),
		requestApproval: async (id, opts) => createApproval(op(id), opts, { ...options, race, op }),
		waitForApproval: async (id, opts) => {
			const key = op(id);
			const approval = await createApproval(key, opts, { ...options, race, op });
			return (await race(`${key}:decision`, { decision: approval.decision() })).value as never;
		},
		race: async (id, awaitables) => (await race(op(id), awaitables)) as never,
		timer: (value) =>
			({
				kind: "fabrial.awaitable",
				timerMs: duration(value),
			}) as InternalAwaitable as Awaitable<null>,
		invoke: async (id, workflow, input, opts) => {
			const key = op(id);
			const value = await execution.step(`fabrial:${key}:input`, () => checkInput(workflow, input));
			return execution.invoke(key, workflow.name, value, {
				...opts,
				metadata: childMetadata(),
			}) as never;
		},
		start: async (id, workflow, input) => {
			const key = op(id);
			const value = await execution.step(`fabrial:${key}:input`, () => checkInput(workflow, input));
			return execution.start(key, workflow.name, value, { metadata: childMetadata() });
		},
		isMember: (principal, group) => host.directory.isMember(principal, group),
	};
	return ctx;
}
