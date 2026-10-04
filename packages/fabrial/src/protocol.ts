import type { ApprovalDecision, ApprovalHandle, ApprovalOptions } from "./approval.ts";
import type { ContextOptions, InternalAwaitable } from "./context.ts";
import type { ExternalIdentity, Principal } from "./identity.ts";
import { APPROVAL_CLOSED_EVENT, CLEANUP_WORKFLOW, PRESENTATION_EVENT } from "./cleanup.ts";
import { samePrincipal } from "./directory.ts";
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
	const approvalId = `${execution.executionId}:${id}`;
	const originatingProvider = metadata.origin?.provider ?? metadata.replyTo?.provider;
	const targets = Array.isArray(options.approvers) ? options.approvers : [options.approvers];
	const request = await execution.step(`fabrial:${id}:request`, async () => {
		const resolved: Principal[] = [];
		for (const target of targets) {
			if (target.kind === "fabrial.group") resolved.push(...(await host.directory.members(target)));
			else
				resolved.push(
					...(await host.directory.members({
						kind: "fabrial.group",
						id: `approval:${target.id}`,
						members: [target],
					})),
				);
		}
		const approvers = [...new Map(resolved.map((p) => [p.id, p])).values()];
		if (!approvers.length) throw new Error("Approval requires at least one approver");
		return {
			approvers: approvers as unknown as Json,
			deadline: now() + boundedTimeout(options.timeout),
			title: options.title,
			details: options.details,
			requesterControls: options.requesterControls ?? true,
			card: options.card ? (options.card as unknown as Json) : null,
		};
	});
	if (request.title !== options.title || canonical(request.details) !== canonical(options.details))
		throw new Error(
			`Approval ${id} proposal changed: title/details must match the persisted request`,
		);
	const after = await execution.cursor(`fabrial:${id}:cursor`);
	const approvers = request.approvers as unknown as Principal[];
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
				ownsThread: false,
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
	const cards: MessageRef[] = [];
	for (const delivery of deliveries) {
		if (delivery.status === "fulfilled") cards.push(delivery.value);
	}
	const failed = deliveries.find((delivery) => delivery.status === "rejected");
	if (failed?.status === "rejected") throw failed.reason;

	const render = async (decision: ApprovalDecision, reason?: string) => {
		const label =
			decision.status === "approved"
				? `Approved by ${decision.decidedBy?.name ?? decision.decidedBy?.id}`
				: decision.status === "rejected"
					? `Rejected by ${decision.decidedBy?.name ?? decision.decidedBy?.id}`
					: decision.status === "expired"
						? "Expired"
						: reason === "superseded"
							? "Superseded"
							: "Cancelled";
		for (const card of cards)
			await (
				await chat.thread({ kind: "thread", provider: card.provider, threadId: card.threadId })
			).update(card, { card: { title: request.title, text: label, actions: [] } });
	};
	let resolution: ApprovalDecision | undefined;
	const finish = async (decision: ApprovalDecision, reason?: string): Promise<ApprovalDecision> => {
		const result = await execution.step(`fabrial:${id}:resolved`, async () => {
			await render(decision, reason);
			await execution.emit(APPROVAL_CLOSED_EVENT, { approvalId }, { id: approvalId });
			return decision as unknown as JsonObject;
		});
		resolution = result as unknown as ApprovalDecision;
		return resolution;
	};

	const awaitable: InternalAwaitable = {
		kind: "fabrial.awaitable",
		branch: {
			kind: "event",
			event: "fabrial.approval.decided",
			filter: { approvalId: [approvalId] },
			after,
		},
		deadline: request.deadline,
		prepare: async () => ({ ready: !!resolution, value: (resolution as unknown as Json) ?? null }),
		accept: async (waitId, value) => {
			if (value === null)
				return {
					accepted: true,
					value: (await finish({
						status: "expired",
						approved: false,
						decidedBy: null,
						decidedAt: new Date(now()).toISOString(),
					})) as unknown as Json,
				};
			const event = value as JsonObject;
			const actor = event.actor as unknown as Principal | null;
			const cancelled = event.status === "cancelled";
			const eligible = await execution.step(`${waitId}:eligible`, async () => {
				if (event.internal === execution.executionId && cancelled) return true;
				if (!actor) return false;
				if (cancelled)
					return (
						request.requesterControls !== false &&
						!!metadata.requestedBy &&
						samePrincipal(actor, metadata.requestedBy)
					);
				if (!approvers.some((p) => samePrincipal(p, actor))) return false;
				for (const target of targets) {
					if (target.kind === "fabrial.user" && target.id === actor.id) return true;
					if (target.kind === "fabrial.group" && (await host.directory.isMember(actor, target)))
						return true;
				}
				return false;
			});
			if (!eligible) {
				await execution.step(`${waitId}:notice`, async () => {
					if (event.identity && event.thread)
						await chat.postEphemeral(
							event.thread as unknown as import("./thread.ts").ThreadRef,
							event.identity as unknown as ExternalIdentity,
							"You are not authorized to decide this approval.",
						);
				});
				return { accepted: false, value: null };
			}
			const decision: ApprovalDecision = {
				status: event.status as ApprovalDecision["status"],
				approved: event.status === "approved",
				decidedBy: actor,
				decidedAt: new Date(now()).toISOString(),
				...(typeof event.comment === "string" ? { comment: event.comment } : {}),
			};
			return {
				accepted: true,
				value: (await finish(decision, event.reason as string | undefined)) as unknown as Json,
			};
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
					"fabrial.approval.decided",
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
