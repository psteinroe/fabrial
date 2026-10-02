import type { AgentIntegration, FabrialHost } from "./app.ts";
import { CLEANUP_WORKFLOW, PRESENTATION_EVENT } from "./cleanup.ts";
import { createApproval } from "./protocol.ts";
import { boundedTimeout, duration } from "./internal.ts";
import type { Json, JsonObject } from "./json.ts";
import type { DurableExecution, InvocationMetadata, ThreadIO, WaitBranch } from "./runtime.ts";
import { parseSchema } from "./schema.ts";
import type {
	Awaitable,
	ChatMessage,
	MessageRef,
	OutboundMessage,
	Thread,
	Surface,
} from "./thread.ts";
import type { AnyWorkflow, WorkflowContext } from "./workflow.ts";

export interface InternalAwaitable extends Awaitable {
	branch: WaitBranch;
	deadline?: number;
	timerMs?: number;
	prepare?: (id: string) => Promise<{ ready: boolean; value: Json }>;
	accept?: (id: string, value: Json) => Promise<{ accepted: boolean; value: Json }>;
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
	const reserve = async (thread: ThreadIO) => {
		if (!metadata.ownsThread || metadata.initialMessageId || metadata.handoffFrom) return;
		await execution.step("fabrial:thread:reserve", async () => {
			const reservedAt = now();
			await thread.updateState((state) =>
				state?.interactionId != null
					? state
					: {
							interactionId: metadata.interactionId,
							handlerExecutionId: null,
							reservedAt,
							...(state?.ingestedDedupeIds ? { ingestedDedupeIds: state.ingestedDedupeIds } : {}),
							agentActive: false,
							statusMessageId: null,
							bufferedReplies: [],
							...(metadata.requestedBy
								? {
										requesterId: metadata.requestedBy.id,
										participantIds: [metadata.requestedBy.id],
									}
								: {}),
						},
			);
		});
	};
	const bind = async (thread: ThreadIO) => {
		if (!metadata.ownsThread) return;
		await thread.updateState((state) =>
			state?.interactionId === metadata.interactionId &&
			(!state.handlerExecutionId ||
				state.handlerExecutionId === execution.executionId ||
				state.handlerExecutionId === metadata.handoffFrom)
				? { ...state, handlerExecutionId: execution.executionId }
				: state,
		);
	};
	const getIO = async (): Promise<ThreadIO> => {
		if (io) return io;
		if (!metadata.replyTo || !host.chat()) throw new Error("This execution has no chat surface");
		io =
			metadata.replyTo.kind === "thread"
				? await host.chat()!.thread(metadata.replyTo)
				: await host.chat()!.resolve(metadata.replyTo);
		if (realThread) {
			await reserve(io);
			await bind(io);
		}
		return io;
	};
	if (metadata.replyTo?.kind === "thread" && host.chat()) await getIO();

	const post = async (
		id: string,
		message: OutboundMessage,
		presentationId?: string,
	): Promise<MessageRef> => {
		const threadPresentation =
			metadata.ownsThread && metadata.replyTo?.kind === "channel"
				? `${execution.executionId}:thread`
				: undefined;
		const presentationCursor = threadPresentation
			? await execution.cursor("fabrial:thread:cursor")
			: undefined;
		if (threadPresentation)
			await execution.start(
				"fabrial:thread:cleanup",
				CLEANUP_WORKFLOW,
				{
					executionId: execution.executionId,
					presentationId: threadPresentation,
					after: presentationCursor!,
				},
				{
					metadata: {
						...metadata,
						replyTo: null,
						ownsThread: false,
						ownerWorkflow: null,
						triggerEvent: null,
					},
				},
			);
		const ref = (await execution.step(id, async () => {
			const receipt = await (await getIO()).post(message);
			for (const presentation of [threadPresentation, presentationId])
				if (presentation)
					await execution.emit(
						PRESENTATION_EVENT,
						{
							presentationId: presentation,
							ref: receipt as unknown as JsonObject,
						},
						{ id: `${presentation}:${id}` },
					);
			return receipt as unknown as JsonObject;
		})) as unknown as MessageRef;
		io = await host
			.chat()!
			.thread({ kind: "thread", provider: ref.provider, threadId: ref.threadId });
		realThread = true;
		metadata.replyTo = io.ref as InvocationMetadata["replyTo"];
		execution.metadata.replyTo = metadata.replyTo;
		if (!execution.signal.aborted) {
			await reserve(io);
			await bind(io);
		}
		return ref;
	};

	const reply = (from?: import("./identity.ts").Principal): InternalAwaitable => {
		const eligible = async (
			message: ChatMessage,
			state: Awaited<ReturnType<ThreadIO["getState"]>>,
		) =>
			!state?.consumedReplyIds?.includes(message.messageId) &&
			(!from || (await host.resolvePrincipal(message.author.identity)).id === from.id);
		const acknowledge = async (id: string, message: ChatMessage) => {
			const state = await (
				await getIO()
			).updateState((state) => {
				if (state?.interactionId !== metadata.interactionId) return state;
				const owner = state.consumedReplyOperations?.[message.messageId];
				if (
					(owner && owner !== id) ||
					(!owner && state.consumedReplyIds?.includes(message.messageId))
				)
					return state;
				return {
					...state,
					bufferedReplies: state.bufferedReplies.filter((m) => m.messageId !== message.messageId),
					consumedReplyIds: [...new Set([...(state.consumedReplyIds ?? []), message.messageId])],
					consumedReplyOperations: { ...state.consumedReplyOperations, [message.messageId]: id },
				};
			});
			return (
				state?.interactionId === metadata.interactionId &&
				state.consumedReplyOperations?.[message.messageId] === id
			);
		};
		return {
			kind: "fabrial.awaitable",
			branch: {
				kind: "event",
				event: "fabrial.reply",
				filter: { interactionId: [metadata.interactionId], ...(from ? { from: [from.id] } : {}) },
			},
			prepare: async (id) => {
				// Persist selection BEFORE removing the message. A crash during acknowledgement can replay it.
				const selected = await execution.step(id, async () => {
					const state = await (await getIO()).getState();
					if (state?.interactionId === metadata.interactionId)
						for (const message of state.bufferedReplies)
							if (await eligible(message, state)) return { ready: true, value: message };
					return { ready: false, value: null };
				});
				if (selected.ready && !(await acknowledge(id, selected.value as ChatMessage)))
					return { ready: false, value: null };
				return selected;
			},
			accept: async (id, value) => {
				if (value === null) return { accepted: true, value: null };
				const selected = await execution.step(id, async () => {
					const message = (value as JsonObject).message as ChatMessage;
					return (await eligible(message, await (await getIO()).getState())) ? message : null;
				});
				return selected && (await acknowledge(id, selected))
					? { accepted: true, value: selected }
					: { accepted: false, value: null };
			},
		};
	};
	const race = async (
		id: string,
		awaitables: Record<string, Awaitable>,
	): Promise<{ key: string; value: Json }> => {
		const entries = Object.entries(awaitables) as [string, InternalAwaitable][];
		if (!entries.length) throw new Error("race needs at least one awaitable");
		const cursor = await execution.cursor(`fabrial:${id}:cursor`);
		const eventBranches: Record<string, WaitBranch> = {};
		for (const [key, awaitable] of entries) {
			if (!awaitable.branch) throw new Error("Invalid Fabrial awaitable");
			eventBranches[key] =
				awaitable.branch.kind === "event"
					? { ...awaitable.branch, after: awaitable.branch.after ?? cursor }
					: awaitable.branch;
			if (awaitable.timerMs !== undefined)
				eventBranches[key] = {
					kind: "timer",
					at: await execution.step(`fabrial:${id}:timer:${key}`, () => now() + awaitable.timerMs!),
				};
			const prepared = await awaitable.prepare?.(`fabrial:${id}:prepare:${key}`);
			if (prepared?.ready) return { key, value: prepared.value };
		}
		for (let attempt = 0; ; attempt++) {
			const branches: Record<string, WaitBranch> = {};
			for (const [key, awaitable] of entries) {
				branches[key] = eventBranches[key]!;
				if (awaitable.deadline !== undefined)
					branches[`fabrial:timeout:${key}`] = {
						kind: "timer",
						at: awaitable.deadline,
					};
			}
			const winner = await execution.waitForAny(`fabrial:${id}:wait:${attempt}`, branches);
			if (winner.kind === "event")
				for (const [key, branch] of Object.entries(eventBranches))
					if (branch.kind === "event") eventBranches[key] = { ...branch, after: winner.cursor };
			const timeout = winner.key.startsWith("fabrial:timeout:");
			const key = timeout ? winner.key.slice("fabrial:timeout:".length) : winner.key;
			const awaitable = entries.find(([k]) => k === key)![1];
			const value =
				winner.kind === "event"
					? winner.event.payload
					: winner.kind === "execution"
						? (winner.result as unknown as Json)
						: null;
			const accepted = (await awaitable.accept?.(`fabrial:${id}:accept:${attempt}`, value)) ?? {
				accepted: true,
				value,
			};
			if (accepted.accepted) return { key, value: accepted.value };
		}
	};
	const waitReply = async (
		id: string,
		opts: { from?: import("./identity.ts").Principal; timeout?: string | number } = {},
	) => {
		const awaitable = reply(opts.from);
		if (opts.timeout !== undefined)
			awaitable.deadline = await execution.step(
				`fabrial:${id}:deadline`,
				() => now() + boundedTimeout(opts.timeout!),
			);
		return (await race(id, { reply: awaitable })).value as ChatMessage | null;
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
					waitForReply: (id, opts) => waitReply(op(id), opts),
					ask: async (id, message, opts) => {
						const key = op(id);
						await post(`fabrial:${key}:post`, message);
						return waitReply(`${key}:reply`, opts);
					},
					nextReply: (opts) => reply(opts?.from) as Awaitable<ChatMessage>,
					history: (id, opts = {}) =>
						execution.step(op(id), async () => (await getIO()).history(opts)),
				}
			: undefined;
	const checkInput = async (workflow: AnyWorkflow, input: Json): Promise<Json> => {
		const registered = host.workflows.find((w) => w.name === workflow.name);
		if (!registered) throw new Error(`Unregistered workflow: ${workflow.name}`);
		const groups = registered.definition.access?.invoke;
		if (
			groups &&
			(!metadata.requestedBy ||
				!(
					await Promise.all(
						(Array.isArray(groups) ? groups : [groups]).map((g) =>
							host.directory.isMember(metadata.requestedBy!, g),
						),
					)
				).some(Boolean))
		)
			throw new Error(`Not authorized to invoke ${workflow.name}`);
		return registered.definition.input
			? parseSchema(registered.definition.input, input, `${workflow.name} input`)
			: input;
	};
	const childMetadata = (ownsThread: boolean): Partial<InvocationMetadata> => ({
		...metadata,
		ownsThread,
		ownerWorkflow: null,
		triggerEvent: null,
		replyTo: (realThread
			? (io?.ref ?? metadata.replyTo)
			: metadata.replyTo) as InvocationMetadata["replyTo"],
	});
	const ctx: WorkflowContext = {
		executionId: execution.executionId,
		interactionId: metadata.interactionId,
		thread,
		clients: host.clients(),
		actor: metadata.requestedBy,
		metadata,
		signal: execution.signal,
		raw: { execution, chat: host.chat(), agents: agents?.agents },
		step: (id, fn) => execution.step(op(id), fn),
		sleep: (id, value) => execution.sleep(op(id), duration(value)),
		agent: async (id, agent, opts) => {
			if (!agents) throw new Error("Agent integration required");
			return (await agents.agents.run(execution, op(id), agent, opts)) as never;
		},
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
		requestApproval: async (id, opts) =>
			createApproval(op(id), opts, { ...options, post, race, op }),
		waitForApproval: async (id, opts) => {
			const key = op(id);
			const approval = await createApproval(key, opts, { ...options, post, race, op });
			return (await race(`${key}:decision`, { decision: approval.decision() })).value as never;
		},
		race: async (id, awaitables) => (await race(op(id), awaitables)) as never,
		timer: (value) =>
			({
				kind: "fabrial.awaitable",
				branch: { kind: "timer", at: 0 },
				timerMs: duration(value),
			}) as InternalAwaitable as Awaitable<null>,
		invoke: async (id, workflow, input, opts) => {
			const key = op(id);
			const value = await execution.step(`fabrial:${key}:input`, () => checkInput(workflow, input));
			return execution.invoke(key, workflow.name, value, {
				...opts,
				metadata: childMetadata(false),
			}) as never;
		},
		start: async (id, workflow, input) => {
			const key = op(id);
			const value = await execution.step(`fabrial:${key}:input`, () => checkInput(workflow, input));
			return execution.start(key, workflow.name, value, { metadata: childMetadata(false) });
		},
		handoff: async (id, workflow, input) => {
			const key = op(id);
			const value = await execution.step(`fabrial:${key}:input`, () => checkInput(workflow, input));
			const target = await execution.start(`fabrial:${key}:start`, workflow.name, value, {
				metadata: { ...childMetadata(true), handoffFrom: execution.executionId },
			});
			if (thread && realThread)
				await execution.step(`fabrial:${key}:transfer`, async () => {
					const dest = await getIO();
					await dest.updateState((state) =>
						state?.interactionId === metadata.interactionId &&
						state.handlerExecutionId === execution.executionId
							? { ...state, handlerExecutionId: target }
							: state,
					);
				});
			return { handedOffTo: workflow.name, executionId: target };
		},
		isMember: (principal, group) => host.directory.isMember(principal, group),
	};
	return ctx;
}
