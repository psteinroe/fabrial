import type { ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { ContextOptions, nativeMethods } from "./context.ts";
import {
	APPROVAL_CANCEL_SIGNAL,
	APPROVAL_DECISION_WORKFLOW,
	APPROVAL_REQUEST_WORKFLOW,
	type ApprovalCard,
	type ApprovalRequest,
	type DecisionInput,
	type DecisionResult,
} from "./approval-workflows.ts";
import { definedGroup, type Principal } from "./identity.ts";
import { boundedTimeout } from "./internal.ts";
import type { Json } from "./json.ts";
import type { Origin } from "./events.ts";
import { RESERVED_LABELS } from "./approval-card.ts";
import type { FabrialHost } from "./app.ts";
import type { Surface } from "./thread.ts";

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
): Promise<ApprovalHandle> {
	const { host, execution, metadata, now } = ctx;
	if (!host.chat()) throw new Error("Approvals require a chat integration");
	const originatingProvider = metadata.origin?.provider ?? metadata.replyTo?.provider;
	const targets = Array.isArray(options.approvers) ? options.approvers : [options.approvers];
	for (const field of options.card?.fields ?? [])
		if (RESERVED_LABELS.some((label) => label.toLowerCase() === field.label.trim().toLowerCase()))
			throw new Error(`Approval ${id}: card field label "${field.label}" is reserved`);
	const request = (await execution.step(`fabrial:${id}:request`, async () => {
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
			callerId: execution.executionId,
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
	const approvalId = await execution.start(
		`fabrial:${id}:record`,
		APPROVAL_REQUEST_WORKFLOW,
		request as unknown as Json,
		{ metadata: { ...metadata, ...internal } },
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
	const decisionId = await execution.start(
		`fabrial:${id}:decision`,
		APPROVAL_DECISION_WORKFLOW,
		{
			approvalId,
			callerId: execution.executionId,
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
		} satisfies DecisionInput as unknown as Json,
		{ metadata: { ...metadata, ...internal } },
	);

	// A wait that loses `waitForAny` leaves the decision running: the approval can be awaited again.
	const branch = { execution: decisionId };
	return {
		approvalId,
		approvers,
		branch,
		wait: async (waitId) => {
			const winner = await ctx.native.waitForAny(ctx.op(waitId), { decision: branch });
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
		},
		// Decides the approval unless a decision came first. A signal, not an event: it is kept for the
		// decision execution even when that has not subscribed yet.
		cancel: async (cancelId, reason) => {
			const op = ctx.op(cancelId);
			await execution.step(op, async () => {
				await host.runtime.signal(decisionId, {
					name: APPROVAL_CANCEL_SIGNAL,
					id: op,
					payload: { reason },
				});
			});
		},
	};
}
