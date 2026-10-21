import { createHmac, timingSafeEqual } from "node:crypto";
import {
	LinearAdapter,
	type LinearAdapterAPIKeyConfig,
	type LinearAdapterOAuthConfig,
} from "@chat-adapter/linear";
import { chatCapability, chatEvents, chatIdentity, chatTrigger } from "fabrial/plugin";
import { defineGroup, type EmitOptions, type ExternalIdentity } from "fabrial";
import { defineEvent, definePlugin, trigger } from "fabrial/plugin";
import { z } from "zod";

export type LinearOptions = (
	| Omit<LinearAdapterAPIKeyConfig, "mode">
	| Omit<LinearAdapterOAuthConfig, "mode">
) & { organizationId: string };
export type LinearClient = LinearAdapter["linearClient"];
const issueSchema = z.object({
	issueId: z.string(),
	identifier: z.string(),
	title: z.string(),
	description: z.string().nullable(),
	url: z.string(),
	teamKey: z.string(),
	stateType: z.string(),
	threadId: z.string(),
	updatedFrom: z.record(z.string(), z.json()),
});
export type LinearIssue = z.infer<typeof issueSchema>;
const issueEvent = defineEvent({ payload: issueSchema, filterable: ["teamKey", "stateType"] });
const events = { ...chatEvents(), issueCreated: issueEvent, issueUpdated: issueEvent };
const webhookSchema = z.object({
	type: z.string(),
	action: z.string(),
	organizationId: z.string(),
	webhookTimestamp: z.number().optional(),
	actor: z.object({ id: z.string() }).optional(),
	data: z.object({
		id: z.string(),
		identifier: z.string(),
		title: z.string(),
		description: z.string().nullable().optional(),
		url: z.string().optional(),
		team: z.object({ key: z.string() }).optional(),
		teamId: z.string().optional(),
		state: z.object({ type: z.string() }).optional(),
		stateId: z.string().optional(),
		creator: z.object({ id: z.string() }).optional(),
		creatorId: z.string().optional(),
	}),
	url: z.string().optional(),
	updatedFrom: z.record(z.string(), z.json()).optional(),
});

/**
 * Verify a webhook (preserving custom-verifier body rewrites), its freshness, and its organization: one
 * check for both ingress paths, so the SDK's comments, reactions, sessions and revocations are scoped too.
 */
async function verifyWebhook(
	request: Request,
	body: string,
	options: LinearOptions,
): Promise<{ body: string; payload: unknown } | { status: number; error: string }> {
	try {
		if (options.webhookVerifier) {
			const result = await options.webhookVerifier(request, body);
			if (!result) return { status: 401, error: "Invalid signature" };
			if (typeof result === "string") body = result;
		} else {
			const signature = request.headers.get("linear-signature") ?? "";
			const expected = createHmac("sha256", options.webhookSecret!).update(body).digest("hex");
			if (
				signature.length !== expected.length ||
				!timingSafeEqual(Buffer.from(signature), Buffer.from(expected))
			)
				return { status: 401, error: "Invalid signature" };
		}
	} catch {
		return { status: 401, error: "Invalid signature" };
	}
	let payload: unknown;
	try {
		payload = JSON.parse(body);
	} catch {
		return { status: 400, error: "Invalid JSON" };
	}
	const envelope = z
		.object({ organizationId: z.string(), webhookTimestamp: z.number().optional() })
		.safeParse(payload);
	if (!envelope.success) return { status: 400, error: "Invalid payload" };
	const { organizationId, webhookTimestamp } = envelope.data;
	if (
		!options.webhookVerifier &&
		(webhookTimestamp === undefined || Math.abs(Date.now() - webhookTimestamp) > 60_000)
	)
		return { status: 401, error: "Expired webhook" };
	if (organizationId !== options.organizationId)
		return { status: 403, error: "Wrong organization" };
	return { body, payload };
}

/** The SDK adapter in comment mode, scoped to the configured organization. Channels are the SDK's: issues. */
class ScopedLinearAdapter extends LinearAdapter {
	constructor(options: LinearOptions) {
		super({
			...options,
			mode: "comments",
			webhookVerifier: async (request, body) => {
				const verified = await verifyWebhook(request, body, options);
				return "body" in verified ? verified.body : false;
			},
		});
	}
}
const organizations = new WeakMap<LinearClient, string>();
const createPlugin = definePlugin<
	[LinearOptions],
	"linear",
	typeof events,
	{ linear: LinearClient }
>((options) => {
	let instance: ScopedLinearAdapter | undefined;
	const adapter = () => (instance ??= new ScopedLinearAdapter(options));
	return {
		id: "linear",
		init() {
			if (!options.organizationId) throw new Error("Linear requires organizationId");
			if (!options.webhookSecret && !options.webhookVerifier)
				throw new Error("Linear requires webhookSecret or webhookVerifier");
		},
		shutdown() {
			instance = undefined;
		},
		events,
		chat: chatCapability({
			adapter,
			installationId: options.organizationId,
			history: { mode: "thread", limit: 50 },
			status: "message",
		}),
		clients: () => {
			const client = adapter().linearClient;
			organizations.set(client, options.organizationId);
			return { linear: client };
		},
		identity: {
			async lookup(subject, clients) {
				if (subject.provider !== "linear" || subject.installationId !== options.organizationId)
					return undefined;
				const user = await clients.linear.user(subject.subjectId);
				return { name: user.displayName || user.name };
			},
		},
		routes: {
			"POST /linear/webhook": async (request, ctx) => {
				const { clients } = ctx;
				const verified = await verifyWebhook(request, await request.text(), options);
				if (!("body" in verified)) return new Response(verified.error, { status: verified.status });
				const raw = verified.payload;
				const envelope = z.object({ type: z.string(), action: z.string() }).safeParse(raw);
				if (!envelope.success) return new Response("Invalid payload", { status: 400 });
				if (envelope.data.type !== "Issue" || !["create", "update"].includes(envelope.data.action))
					return new Response(null, { status: 200 });
				const parsed = webhookSchema.safeParse(raw);
				if (!parsed.success) return new Response("Invalid issue payload", { status: 400 });
				const payload = parsed.data;
				const delivery = request.headers.get("linear-delivery");
				if (!delivery) return new Response("Missing delivery id", { status: 400 });
				const source = payload.data;
				const teamKey =
					source.team?.key ??
					(source.teamId ? (await clients.linear.team(source.teamId)).key : undefined);
				const stateType =
					source.state?.type ??
					(source.stateId ? (await clients.linear.workflowState(source.stateId)).type : undefined);
				const url = source.url ?? payload.url;
				if (!teamKey || !stateType || !url)
					return new Response("Missing issue team, state or URL", { status: 400 });
				const threadId = adapter().encodeThreadId({ issueId: source.id });
				const requesterId =
					payload.action === "create"
						? (source.creator?.id ?? source.creatorId ?? payload.actor?.id)
						: payload.actor?.id;
				if (payload.action === "create" && !requesterId)
					return new Response("Missing issue author", { status: 400 });
				const ingress: EmitOptions = {
					id: JSON.stringify(["linear", options.organizationId, delivery]),
					origin: {
						provider: "linear",
						installationId: options.organizationId,
						issueId: source.id,
						teamKey,
						threadId,
					},
					replyTo: { kind: "thread", provider: "linear", threadId },
					requestedBy: requesterId
						? identity({ organizationId: options.organizationId, userId: requesterId })
						: undefined,
				};
				await ctx.emit(
					payload.action === "create" ? "issueCreated" : "issueUpdated",
					{
						issueId: source.id,
						identifier: source.identifier,
						title: source.title,
						description: source.description ?? null,
						url,
						teamKey,
						stateType,
						threadId,
						updatedFrom: payload.updatedFrom ?? {},
					},
					ingress,
				);
				return new Response(null, { status: 200 });
			},
		},
	};
});
export interface LinearFilter {
	team?: string;
	stateType?: string;
	observe?: boolean;
}
/** `issue`: the issue's UUID, the SDK's channel. Comment payloads carry no team: see the README. */
export function mentioned(filter: { issue?: string; thread?: string; observe?: boolean } = {}) {
	return chatTrigger("linear", "mentioned", {
		channel: filter.issue,
		thread: filter.thread,
		observe: filter.observe,
	});
}
function issueTrigger(event: "issueCreated" | "issueUpdated", filter: LinearFilter) {
	return trigger<LinearIssue>({
		event: `linear.${event}`,
		filter: {
			...(filter.team ? { teamKey: [filter.team] } : {}),
			...(filter.stateType ? { stateType: [filter.stateType] } : {}),
		},
		observe: filter.observe,
		specificity: Number(Boolean(filter.team)) + Number(Boolean(filter.stateType)),
	});
}
export function issueCreated(filter: LinearFilter = {}) {
	return issueTrigger("issueCreated", filter);
}
export function issueUpdated(filter: LinearFilter = {}) {
	return issueTrigger("issueUpdated", filter);
}
export function identity(options: { organizationId: string; userId: string }): ExternalIdentity {
	return chatIdentity("linear", options.organizationId, options.userId);
}
/** Resolve the API's current responsible user, not the entire historical rotation. */
export function triageResponsibility(options: { id: string; team: string }) {
	return defineGroup({
		id: options.id,
		async resolve(ctx) {
			const client = ctx.clients.linear as LinearClient | undefined;
			const organizationId = client && organizations.get(client);
			if (!client || !organizationId)
				throw new Error("Linear triage resolution requires plugin clients.linear");
			const { nodes } = await client.teams({ filter: { key: { eq: options.team } } });
			const team = nodes[0];
			if (!team) throw new Error(`Linear team ${options.team} not found`);
			const responsibility = await team.triageResponsibility;
			const user = await responsibility?.currentUser;
			if (!user)
				throw new Error(`Linear team ${options.team} has no current triage responsibility`);
			return [identity({ organizationId, userId: user.id })];
		},
	});
}
export const linear = Object.assign(createPlugin, {
	mentioned,
	issueCreated,
	issueUpdated,
	identity,
	triageResponsibility,
});
