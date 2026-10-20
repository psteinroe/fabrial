import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { ContextOptions, InternalAwaitable } from "./context.ts";
import {
	APPROVAL_DECIDED_EVENT,
	APPROVAL_REQUEST_WORKFLOW,
	type ApprovalRequest,
} from "./approval-workflows.ts";
import { definedGroup, type Principal } from "./identity.ts";
import { APPROVAL_CLOSED_EVENT, CLEANUP_WORKFLOW, PRESENTATION_EVENT } from "./cleanup.ts";
import { boundedTimeout } from "./internal.ts";
import type { Json, JsonObject } from "./json.ts";
import type { Awaitable, MessageRef, OutboundMessage } from "./thread.ts";

interface ApprovalContext extends ContextOptions {
	post(id: string, message: OutboundMessage, presentationId?: string): Promise<MessageRef>;
	op(id: string): string;
	race(id: string, awaitables: Record<string, Awaitable>): Promise<{ key: string; value: Json }>;
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

export async function createApproval(
	id: string,
	options: ApprovalOptions,
	ctx: ApprovalContext,
): Promise<ApprovalHandle> {
	const { host, execution, metadata, now } = ctx;
	const chat = host.chat();
	if (!chat) throw new Error("Approvals require a chat integration");
	const originatingProvider = metadata.origin?.provider ?? metadata.replyTo?.provider;
	const targets = Array.isArray(options.approvers) ? options.approvers : [options.approvers];
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
			deadline: now() + boundedTimeout(options.timeout),
			title: options.title,
			details: options.details,
			requesterControls: options.requesterControls ?? true,
			card: options.card ? (options.card as unknown as Json) : null,
		} satisfies ApprovalRequest as unknown as Json;
	})) as unknown as ApprovalRequest;
	if (request.title !== options.title || canonical(request.details) !== canonical(options.details))
		throw new Error(
			`Approval ${id} proposal changed: title/details must match the persisted request`,
		);
	// The request's own execution: its result is what the routing workflow authorizes clicks against.
	const approvalId = await execution.start(
		`fabrial:${id}:record`,
		APPROVAL_REQUEST_WORKFLOW,
		request as unknown as Json,
		{ metadata: { ...metadata, replyTo: null, ownerWorkflow: null, triggerEvent: null } },
	);
	const after = await execution.cursor(`fabrial:${id}:cursor`);
	const approvers = request.approvers;
	await execution.start(
		`fabrial:${id}:cleanup`,
		CLEANUP_WORKFLOW,
		{
			executionId: execution.executionId,
			approvalId,
			presentationId: approvalId,
			after,
			title: request.title,
		},
		{
			metadata: {
				...metadata,
				replyTo: null,
				ownerWorkflow: null,
				triggerEvent: null,
			},
		},
	);
	const status = metadata.replyTo
		? ctx.post(
				`fabrial:${id}:status`,
				{
					card: {
						title: request.title,
						text: "Waiting for approval",
						actions:
							request.requesterControls === false
								? []
								: [{ id: `fabrial.approval.cancel:${approvalId}`, label: "Cancel request" }],
					},
				},
				approvalId,
			)
		: undefined;
	const deliveries = await Promise.allSettled([
		...approvers.map(async (approver, index) => {
			return (await execution.step(`fabrial:${id}:card:${index}`, async () => {
				const identities = approver.identities
					.map((identity, index) => ({ identity, index }))
					.sort(
						(a, b) =>
							Number(b.identity.provider === originatingProvider) -
								Number(a.identity.provider === originatingProvider) || a.index - b.index,
					);
				let dm: Awaited<ReturnType<typeof chat.openDM>> | undefined;
				for (const { identity } of identities) {
					try {
						dm = await chat.openDM(identity);
						break;
					} catch {
						/* Try the next provider identity. */
					}
				}
				if (!dm)
					throw new Error(`Approver ${approver.id} has no identity whose provider can open DMs`);
				const receipt = await dm.post({
					card: {
						...((request.card as unknown as ApprovalOptions["card"]) ?? {
							title: request.title,
							text:
								typeof request.details === "string"
									? request.details
									: JSON.stringify(request.details),
						}),
						actions: [
							{ id: `fabrial.approval.approve:${approvalId}`, label: "Approve", style: "primary" },
							{ id: `fabrial.approval.reject:${approvalId}`, label: "Reject", style: "danger" },
						],
					},
				});
				await execution.emit(
					PRESENTATION_EVENT,
					{ presentationId: approvalId, ref: receipt as unknown as JsonObject },
					{ id: `${approvalId}:card:${index}` },
				);
				return receipt as unknown as JsonObject;
			})) as unknown as MessageRef;
		}),
		...(status ? [status] : []),
	]);
	const failed = deliveries.find((delivery) => delivery.status === "rejected");
	if (failed?.status === "rejected") throw failed.reason;

	const label = (decision: ApprovalDecision, reason?: string) =>
		decision.status === "approved"
			? `Approved by ${decision.decidedBy?.name ?? decision.decidedBy?.id}`
			: decision.status === "rejected"
				? `Rejected by ${decision.decidedBy?.name ?? decision.decidedBy?.id}`
				: decision.status === "expired"
					? "Expired"
					: reason === "superseded"
						? "Superseded"
						: "Cancelled";
	let resolution: ApprovalDecision | undefined;
	/** Record the decision first; the cleanup watcher then relabels each card on its own. */
	const finish = async (decision: ApprovalDecision, reason?: string): Promise<ApprovalDecision> => {
		const result = await execution.step(`fabrial:${id}:resolved`, async () => {
			await execution.emit(
				APPROVAL_CLOSED_EVENT,
				{ approvalId, label: label(decision, reason) },
				{ id: approvalId },
			);
			return decision as unknown as JsonObject;
		});
		resolution = result as unknown as ApprovalDecision;
		return resolution;
	};

	const awaitable: InternalAwaitable = {
		kind: "fabrial.awaitable",
		branch: {
			kind: "event",
			event: APPROVAL_DECIDED_EVENT,
			filter: { approvalId: [approvalId] },
			after,
		},
		deadline: request.deadline,
		prepare: async () => ({ ready: !!resolution, value: (resolution as unknown as Json) ?? null }),
		// Only authorized decisions reach the approval: the routing workflow checks every click.
		accept: async (_waitId, value) => {
			if (value === null)
				return (await finish({
					status: "expired",
					approved: false,
					decidedBy: null,
					decidedAt: new Date(now()).toISOString(),
				})) as unknown as Json;
			const event = value as JsonObject;
			const decision: ApprovalDecision = {
				status: event.status as ApprovalDecision["status"],
				approved: event.status === "approved",
				decidedBy: event.actor as unknown as Principal | null,
				decidedAt: new Date(now()).toISOString(),
				...(typeof event.comment === "string" ? { comment: event.comment } : {}),
			};
			return (await finish(decision, event.reason as string | undefined)) as unknown as Json;
		},
	};
	return {
		approvalId,
		approvers,
		decision: () => awaitable as Awaitable<ApprovalDecision & Record<string, Json>>,
		wait: async (waitId) =>
			(await ctx.race(ctx.op(waitId), { decision: awaitable }))
				.value as unknown as ApprovalDecision,
		cancel: async (cancelId, reason) => {
			const key = ctx.op(cancelId);
			if (resolution) return;
			await execution.step(key, async () => {
				await execution.emit(
					APPROVAL_DECIDED_EVENT,
					{ approvalId, status: "cancelled", actor: null, internal: execution.executionId, reason },
					{ id: `${approvalId}:cancel` },
				);
			});
			await finish(
				{
					status: "cancelled",
					approved: false,
					decidedBy: null,
					decidedAt: new Date(now()).toISOString(),
					comment: reason,
				},
				reason,
			);
		},
	};
}
