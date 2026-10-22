// Approval requests: the API types, the immutable request record, and `ctx.requestApproval`.
import { duration, type nativeMethods } from "../execution.ts";
import type { Json } from "../schema.ts";
import {
	definedGroup,
	type GroupDefinition,
	type Principal,
	type UserDefinition,
} from "../identity.ts";
import type { Surface } from "../chat/messages.ts";
import type { ContextOptions } from "../workflows/context.ts";
import type { FabrialHost } from "../app.ts";
import type { Origin } from "../routing/events.ts";
import { RESERVED_LABELS } from "./cards.ts";
import {
	APPROVAL_CANCEL_SIGNAL,
	APPROVAL_DECISION_WORKFLOW,
	type ApprovalCard,
	type DecisionInput,
	type DecisionResult,
} from "./decision.ts";
import type { WorkflowTask } from "../conductor.ts";

const THREAD_TTL = 30 * 24 * 60 * 60 * 1000;

function boundedTimeout(value: string | number = "24h"): number {
	const ms = duration(value);
	if (ms >= THREAD_TTL) throw new Error("Timeout must be below 30 days");
	return ms;
}

/**
 * App content added to an approval card in its own labelled section, after the proposal; it never
 * replaces it. Field labels Fabrial sets ("Requested by", "Source", "Expires", …) are rejected.
 */
export interface ApprovalCardContent {
	text?: string;
	fields?: { label: string; value: string }[];
}

export interface ApprovalOptions {
	title: string;
	/**
	 * Shown verbatim on every card; also what the approval binds to (a changed proposal needs a new
	 * approval).
	 */
	details: Json;
	approvers: GroupDefinition | UserDefinition | readonly (GroupDefinition | UserDefinition)[];
	/** Duration string ("24h") or ms. Must stay below 30 days (Chat SDK thread-state TTL). */
	timeout?: string | number;
	/**
	 * Extra card content, shown in a section labelled as the app's, after the proposal, the verified
	 * requester, the source, the reply thread, and the deadline, which every card shows.
	 */
	card?: ApprovalCardContent;
	/**
	 * Where the approver card is posted (e.g. a channel) instead of each approver's DM. It grants no
	 * authority: only assigned approvers may decide, whoever sees the card.
	 */
	destination?: Surface;
	/** Show [Cancel request] on the requester's status message. Default true. */
	requesterControls?: boolean;
}

export type DecisionStatus = "approved" | "rejected" | "expired" | "cancelled";

export interface ApprovalDecision {
	status: DecisionStatus;
	approved: boolean;
	decidedBy: Principal | null;
	/** The rejection reason an approver gave, or the reason passed to `cancel`. */
	comment?: string;
	decidedAt: string;
}

export interface ApprovalHandle {
	readonly approvalId: string;
	/** Assigned approvers, resolved when the request was made. */
	readonly approvers: Principal[];
	/**
	 * The decision as a `ctx.waitForAny` branch (its decision execution). When it wins, `wait` returns the
	 * decision at once; losing leaves the approval pending.
	 */
	readonly branch: { execution: string };
	/** Durably wait for the decision. */
	wait(id: string): Promise<ApprovalDecision>;
	/** Durable: cancel the pending request; the card shows "Cancelled"/"Superseded". */
	cancel(id: string, reason: "cancelled" | "superseded" | (string & {})): Promise<void>;
}

interface ApprovalContext extends ContextOptions {
	op(id: string): string;
	native: ReturnType<typeof nativeMethods>;
}

function canonical(value: Json): string {
	if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
	if (value !== null && typeof value === "object")
		return `{${Object.keys(value)
			.sort()
			.map((key) => `${JSON.stringify(key)}:${canonical(value[key]!)}`)
			.join(",")}}`;
	return JSON.stringify(value);
}

/** A link to a thread, else a description of it. */
async function describe(host: FabrialHost, surface: Surface): Promise<string> {
	if (surface.kind === "channel") return `${surface.provider} channel ${surface.channelId}`;
	const link = await host
		.chat()
		?.permalink?.(surface)
		.catch(() => null);
	return link ?? `${surface.provider} thread ${surface.threadId}`;
}

/**
 * Where the request came from: the authenticated ingress origin (e.g. the GitHub review that steered an
 * agent), linked when it names a thread, with its reference; and the reply thread when it differs.
 */
async function provenance(
	host: FabrialHost,
	origin: Origin | null,
	replyTo: Surface | null,
): Promise<{ source: string | null; thread: string | null }> {
	const thread = replyTo ? await describe(host, replyTo) : null;
	if (!origin) return { source: null, thread };
	const { provider, installationId: _installation, threadId, ...rest } = origin;
	const reference = [
		provider,
		...Object.entries(rest).map(([key, value]) => `${key} ${canonical(value)}`),
	]
		.join(" ")
		.replace(/"/g, "");
	if (typeof threadId !== "string") return { source: reference, thread };
	const source = `${await describe(host, { kind: "thread", provider, threadId })} (${reference})`;
	const same =
		replyTo?.kind === "thread" && replyTo.provider === provider && replyTo.threadId === threadId;
	return { source, thread: same ? null : thread };
}

export async function createApproval(
	id: string,
	options: ApprovalOptions,
	ctx: ApprovalContext,
): Promise<ApprovalHandle & { waitAt(stepKey: string): Promise<ApprovalDecision> }> {
	const { host, raw, native, metadata, now } = ctx;
	if (!host.chat()) throw new Error("Approvals require a chat integration");
	const originatingProvider = metadata.origin?.provider ?? metadata.replyTo?.provider;
	const targets = Array.isArray(options.approvers) ? options.approvers : [options.approvers];
	for (const field of options.card?.fields ?? [])
		if (RESERVED_LABELS.some((label) => label.toLowerCase() === field.label.trim().toLowerCase()))
			throw new Error(`Approval ${id}: card field label "${field.label}" is reserved`);
	const request = (await native.step(`fabrial:${id}:request`, async () => {
		const resolved: Principal[] = [];
		const users: Principal[] = [];
		for (const target of targets) {
			if (target.kind === "fabrial.group") {
				// Clicks are checked against the group's members then, by its id: it must name this group.
				if (definedGroup(target.id) !== target)
					throw new Error(
						`Approval group ${target.id} must be defined once with defineGroup to be assigned`,
					);
				resolved.push(...(await host.directory.members(target)));
			} else {
				const [user] = await host.directory.members({
					kind: "fabrial.group",
					id: `approval:${target.id}`,
					members: [target],
				});
				users.push(user!);
				resolved.push(user!);
			}
		}
		const approvers = [...new Map(resolved.map((p) => [p.id, p])).values()];
		if (!approvers.length) throw new Error("Approval requires at least one approver");
		return {
			callerId: raw.executionId,
			operationId: id,
			assigned: {
				users,
				groups: targets.flatMap((t) => (t.kind === "fabrial.group" ? [t.id] : [])),
			},
			approvers,
			requester: metadata.requestedBy,
			...(await provenance(host, metadata.origin, (metadata.replyTo as Surface | null) ?? null)),
			destination: options.destination ?? null,
			deadline: now() + boundedTimeout(options.timeout),
			title: options.title,
			details: options.details,
			requesterControls: options.requesterControls ?? true,
			content: options.card ?? null,
		} satisfies ApprovalRequest as unknown as Json;
	})) as unknown as ApprovalRequest;
	if (request.title !== options.title || canonical(request.details) !== canonical(options.details))
		throw new Error(
			`Approval ${id} proposal changed: title/details must match the persisted request`,
		);
	// The request's own execution: its result is what the routing workflow authorizes clicks against.
	const internal = { replyTo: null, ownerWorkflow: null, triggerEvent: null };
	const approvalId = await native.invoke(
		`fabrial:${id}:record`,
		{ name: APPROVAL_REQUEST_WORKFLOW, queue: raw.queue },
		request as never,
		{ metadata: { ...metadata, ...internal } } as never,
	);
	const approvers = request.approvers;
	const decide = [
		{ id: `fabrial.approval.approve:${approvalId}`, label: "Approve", style: "primary" as const },
		{ id: `fabrial.approval.reject:${approvalId}`, label: "Reject", style: "danger" as const },
	];
	// One card in the destination, else one per approver's DM: either way, only the assigned may decide.
	const cards: ApprovalCard[] = request.destination
		? [
				{
					destination: { kind: "surface", surface: request.destination },
					approver: true,
					status: "Approval requested",
					actions: decide,
				},
			]
		: approvers.map((approver) => ({
				destination: { kind: "dm", approver, provider: originatingProvider ?? null },
				approver: true,
				status: "Approval requested",
				actions: decide,
			}));
	if (metadata.replyTo)
		cards.push({
			destination: { kind: "surface", surface: metadata.replyTo as Surface },
			approver: false,
			status: "Waiting for approval",
			actions:
				request.requesterControls === false
					? []
					: [{ id: `fabrial.approval.cancel:${approvalId}`, label: "Cancel request" }],
		});
	// Subscribes to decisions before its cleanups post the cards; independent of this execution.
	const decisionId = await native.invoke(
		`fabrial:${id}:decision`,
		{ name: APPROVAL_DECISION_WORKFLOW, queue: raw.queue },
		{
			approvalId,
			callerId: raw.executionId,
			deadline: request.deadline,
			proposal: {
				title: request.title,
				details: request.details,
				requester: request.requester,
				source: request.source,
				thread: request.thread,
				deadline: request.deadline,
				content: request.content,
			},
			cards,
		} satisfies DecisionInput as never,
		{ metadata: { ...metadata, ...internal } } as never,
	);

	// A wait that loses `waitForAny` leaves the decision running: the approval can be awaited again.
	const branch = { execution: decisionId };
	const waitAt = async (stepKey: string): Promise<ApprovalDecision> => {
		const winner = await ctx.native.waitForAny(stepKey, { decision: branch });
		if (!("outcome" in winner)) throw new Error("Invalid approval decision");
		const outcome = winner.outcome;
		if (outcome.status === "completed") {
			const output = outcome.result as unknown as DecisionResult;
			if ("error" in output) throw new Error(`Approval failed: ${output.error}`);
			return output.decision;
		}
		if (outcome.status === "failed") throw new Error(`Approval failed: ${outcome.error}`);
		return {
			status: "cancelled",
			approved: false,
			decidedBy: null,
			decidedAt: new Date(now()).toISOString(),
		};
	};
	return {
		approvalId,
		approvers,
		branch,
		wait: (waitId) => waitAt(ctx.op(waitId)),
		waitAt,
		// Decides the approval unless a decision came first. A signal, not an event: it is kept for the
		// decision execution even when that has not subscribed yet.
		cancel: async (cancelId, reason) => {
			const op = ctx.op(cancelId);
			await native.step(op, async () => {
				await native.sendSignal(decisionId, {
					name: APPROVAL_CANCEL_SIGNAL,
					id: op,
					payload: { reason },
				});
			});
		},
	};
}

/** One execution per approval request; its result is the request the routing workflow authorizes clicks against. */
export const APPROVAL_REQUEST_WORKFLOW = "fabrial.approval.request";

/**
 * An approval request as stored in its execution's result. Assignment is by identity and group id, never
 * by resolver: a click is checked against the assigned users and the groups' current members.
 */
export interface ApprovalRequest {
	/** The execution that requested the approval, and the operation id it used. */
	callerId: string;
	operationId: string;
	assigned: { users: Principal[]; groups: string[] };
	/** Who received a card: the assigned users and the groups' members when it was requested. */
	approvers: Principal[];
	/** The verified requester: may cancel the request from its status card, unless `requesterControls` is false. */
	requester: Principal | null;
	/** The authenticated input the request came from: a link and its reference. */
	source: string | null;
	/** The thread replies go to, when it is not the source. */
	thread: string | null;
	/** Where the approver card goes instead of the approvers' DMs; grants no authority. */
	destination: Surface | null;
	/** Epoch ms. */
	deadline: number;
	title: string;
	details: Json;
	requesterControls: boolean;
	content: ApprovalCardContent | null;
}

/**
 * Longer than the longest approval (30 days), so a request stays readable while clicks can still
 * arrive. Only the routing workflow and the decision execution wait on these; callers wait on the
 * decision, which is kept forever.
 */
export const REQUEST_RETENTION_DAYS = 60;

export function approvalRequestWorkflow(): WorkflowTask {
	return {
		name: APPROVAL_REQUEST_WORKFLOW,
		triggers: [],
		retentionDays: REQUEST_RETENTION_DAYS,
		handler: async (input) => input,
	};
}
