import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { ContextOptions, InternalAwaitable } from "./context.ts";
import {
	APPROVAL_DECIDED_EVENT,
	APPROVAL_DECISION_WORKFLOW,
	APPROVAL_REQUEST_WORKFLOW,
	type ApprovalRequest,
	type DecisionInput,
	type DecisionResult,
} from "./approval-workflows.ts";
import { definedGroup, type Principal } from "./identity.ts";
import type { CardDestination } from "./cleanup.ts";
import { boundedTimeout } from "./internal.ts";
import type { Json } from "./json.ts";
import type { Awaitable, Surface } from "./thread.ts";

interface ApprovalContext extends ContextOptions {
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
	if (!host.chat()) throw new Error("Approvals require a chat integration");
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
	const internal = { replyTo: null, ownerWorkflow: null, triggerEvent: null };
	const approvalId = await execution.start(
		`fabrial:${id}:record`,
		APPROVAL_REQUEST_WORKFLOW,
		request as unknown as Json,
		{ metadata: { ...metadata, ...internal } },
	);
	const approvers = request.approvers;
	const cards: DecisionInput["cards"] = approvers.map((approver) => ({
		destination: {
			kind: "dm",
			approver,
			provider: originatingProvider ?? null,
		} satisfies CardDestination,
		card: {
			...((request.card as unknown as ApprovalOptions["card"]) ?? {
				title: request.title,
				text:
					typeof request.details === "string" ? request.details : JSON.stringify(request.details),
			}),
			actions: [
				{ id: `fabrial.approval.approve:${approvalId}`, label: "Approve", style: "primary" },
				{ id: `fabrial.approval.reject:${approvalId}`, label: "Reject", style: "danger" },
			],
		} as unknown as Json,
	}));
	if (metadata.replyTo)
		cards.push({
			destination: { kind: "surface", surface: metadata.replyTo as Surface },
			card: {
				title: request.title,
				text: "Waiting for approval",
				actions:
					request.requesterControls === false
						? []
						: [{ id: `fabrial.approval.cancel:${approvalId}`, label: "Cancel request" }],
			},
		});
	// Subscribes to decisions before its cleanups post the cards; independent of this execution.
	const decisionId = await execution.start(
		`fabrial:${id}:decision`,
		APPROVAL_DECISION_WORKFLOW,
		{
			approvalId,
			callerId: execution.executionId,
			deadline: request.deadline,
			title: request.title,
			cards,
		} satisfies DecisionInput as unknown as Json,
		{ metadata: { ...metadata, ...internal } },
	);

	const awaitable: InternalAwaitable = {
		kind: "fabrial.awaitable",
		// Losing a race leaves the decision running: the approval can be awaited again.
		branch: { execution: decisionId },
		accept: (winner) => {
			if (winner.kind !== "execution") throw new Error("Invalid approval decision");
			const result = winner.result;
			if (result.status === "completed") {
				const output = result.output as unknown as DecisionResult;
				if ("error" in output) throw new Error(`Approval failed: ${output.error}`);
				return output.decision as unknown as Json;
			}
			if (result.status === "failed") throw new Error(`Approval failed: ${result.error}`);
			return {
				status: "cancelled",
				approved: false,
				decidedBy: null,
				decidedAt: new Date(now()).toISOString(),
			} satisfies ApprovalDecision as unknown as Json;
		},
	};
	return {
		approvalId,
		approvers,
		decision: () => awaitable as Awaitable<ApprovalDecision & Record<string, Json>>,
		wait: async (waitId) =>
			(await ctx.race(ctx.op(waitId), { decision: awaitable }))
				.value as unknown as ApprovalDecision,
		// Decides the approval unless a decision came first; the decision execution ignores later ones.
		cancel: async (cancelId, reason) => {
			await execution.step(ctx.op(cancelId), async () => {
				await execution.emit(
					APPROVAL_DECIDED_EVENT,
					{ approvalId, status: "cancelled", actor: null, comment: reason, reason },
					{ id: `${approvalId}:cancel` },
				);
			});
		},
	};
}
