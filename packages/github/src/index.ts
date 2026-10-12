import { createHmac, timingSafeEqual } from "node:crypto";
import {
	GitHubAdapter,
	type GitHubAdapterPATConfig,
	type GitHubAdapterAppConfig,
} from "@chat-adapter/github";
import { chatCapability, chatEvents, chatIdentity, chatTrigger } from "fabrial";
import {
	defineEvent,
	definePlugin,
	resource,
	trigger,
	type EmitOptions,
	type ExternalIdentity,
	type InputPriority,
	type PluginEvent,
	type ResourceEvent,
	type ResourceKind,
	type ResourceNotification,
	type ToolDefinition,
} from "fabrial";
import { z } from "zod";

export type GitHubOptions = (
	| (Omit<GitHubAdapterPATConfig, "installationId"> & { installationId?: string | number })
	| GitHubAdapterAppConfig
) & {
	/** Repository owner login. PATs default to the authenticated user; set this for org repos. */
	owner?: string;
};
export type GitHubClient = GitHubAdapter["octokit"];

const conversation = z.object({
	repo: z.string(),
	id: z.number(),
	number: z.number(),
	title: z.string(),
	body: z.string().nullable(),
	url: z.string(),
	authorId: z.string(),
	authorLogin: z.string(),
	threadId: z.string(),
});
export type GitHubConversation = z.infer<typeof conversation>;
const opened = defineEvent({ payload: conversation, filterable: ["repo", "number"] });

/** Fields of every pull request event after opening: the PR and who acted. */
const pullRequestEvent = z.object({
	repo: z.string(),
	number: z.number(),
	title: z.string(),
	url: z.string(),
	threadId: z.string(),
	actorId: z.string(),
	actorLogin: z.string(),
	/** GitHub account type: `User`, `Bot`, or `Organization`. */
	actorType: z.string(),
});
const closed = pullRequestEvent.extend({ merged: z.boolean() });
const labeled = pullRequestEvent.extend({ label: z.string(), labeled: z.boolean() });
const synchronized = pullRequestEvent.extend({ headSha: z.string() });
const reviewed = pullRequestEvent.extend({
	reviewId: z.number(),
	/** `approved`, `changes_requested`, or `commented`. */
	state: z.string(),
	body: z.string().nullable(),
	/** The reviewer's `author_association` (`OWNER`, `MEMBER`, `COLLABORATOR`, `CONTRIBUTOR`, …). */
	association: z.string(),
});
const commented = pullRequestEvent.extend({
	commentId: z.number(),
	body: z.string(),
	association: z.string(),
	/** A review comment on a line of the diff, rather than on the conversation. */
	inline: z.boolean(),
	path: z.string().nullable(),
});
const checkCompleted = z.object({
	repo: z.string(),
	/** Pull requests of the checked commit. */
	pullRequests: z.array(z.number()),
	name: z.string(),
	conclusion: z.string().nullable(),
	headSha: z.string(),
	url: z.string().nullable(),
});
export type GitHubPullRequestEvent = z.infer<typeof pullRequestEvent>;
export type GitHubPullRequestClosed = z.infer<typeof closed>;
export type GitHubPullRequestLabeled = z.infer<typeof labeled>;
export type GitHubPullRequestSynchronized = z.infer<typeof synchronized>;
export type GitHubPullRequestReviewed = z.infer<typeof reviewed>;
export type GitHubPullRequestCommented = z.infer<typeof commented>;
export type GitHubCheckCompleted = z.infer<typeof checkCompleted>;
const prFilter = ["repo", "number"] as const;
const events = {
	...chatEvents(),
	pullRequestOpened: opened,
	issueOpened: opened,
	pullRequestClosed: defineEvent({ payload: closed, filterable: [...prFilter, "merged"] }),
	pullRequestReopened: defineEvent({ payload: pullRequestEvent, filterable: prFilter }),
	pullRequestLabeled: defineEvent({ payload: labeled, filterable: [...prFilter, "label"] }),
	pullRequestSynchronized: defineEvent({ payload: synchronized, filterable: prFilter }),
	pullRequestReviewed: defineEvent({ payload: reviewed, filterable: [...prFilter, "state"] }),
	pullRequestCommented: defineEvent({ payload: commented, filterable: prFilter }),
	checkSuiteCompleted: defineEvent({ payload: checkCompleted, filterable: ["repo", "conclusion"] }),
	checkRunCompleted: defineEvent({ payload: checkCompleted, filterable: ["repo", "conclusion"] }),
};
const user = z.object({ id: z.number(), login: z.string(), type: z.string().optional() });
const webhookConversation = z.object({
	id: z.number(),
	number: z.number().int().positive(),
	title: z.string(),
	body: z.string().nullable(),
	html_url: z.string(),
	user,
});
const webhook = z.object({
	action: z.string(),
	repository: z.object({ full_name: z.string().regex(/^[^/:]+\/[^/:]+$/) }),
	installation: z.object({ id: z.number() }).optional(),
	sender: user,
	pull_request: webhookConversation.extend({ merged: z.boolean().nullish() }).optional(),
	issue: webhookConversation.extend({ pull_request: z.unknown().optional() }).optional(),
	label: z.object({ name: z.string() }).optional(),
	after: z.string().optional(),
	review: z
		.object({
			id: z.number(),
			state: z.string(),
			body: z.string().nullable(),
			user,
			author_association: z.string(),
		})
		.optional(),
	comment: z
		.object({
			id: z.number(),
			body: z.string(),
			user,
			author_association: z.string(),
			path: z.string().optional(),
		})
		.optional(),
	check_suite: z
		.object({
			head_sha: z.string(),
			conclusion: z.string().nullable(),
			app: z.object({ name: z.string() }).nullish(),
			pull_requests: z.array(z.object({ number: z.number() })),
		})
		.optional(),
	check_run: z
		.object({
			name: z.string(),
			head_sha: z.string(),
			conclusion: z.string().nullable(),
			html_url: z.string().nullish(),
			pull_requests: z.array(z.object({ number: z.number() })),
		})
		.optional(),
});
type Webhook = z.infer<typeof webhook>;
type EventKey = Exclude<keyof typeof events, "mentioned" | "message" | "dm">;

/** The pull request event a webhook carries, if any, after `opened`. */
function pullRequestEventOf(
	kind: string,
	payload: Webhook,
	threadId: (number: number) => string,
): { event: EventKey; payload: Record<string, unknown>; actor: number } | undefined {
	const repo = payload.repository.full_name;
	const base = (
		source: { number: number; title: string; html_url: string },
		actor: z.infer<typeof user>,
	) => ({
		repo,
		number: source.number,
		title: source.title,
		url: source.html_url,
		threadId: threadId(source.number),
		actorId: String(actor.id),
		actorLogin: actor.login,
		actorType: actor.type ?? "User",
	});
	const pr = payload.pull_request;
	if (kind === "pull_request" && pr) {
		const fields = base(pr, payload.sender);
		const actor = payload.sender.id;
		if (payload.action === "closed")
			return { event: "pullRequestClosed", payload: { ...fields, merged: !!pr.merged }, actor };
		if (payload.action === "reopened")
			return { event: "pullRequestReopened", payload: fields, actor };
		if ((payload.action === "labeled" || payload.action === "unlabeled") && payload.label)
			return {
				event: "pullRequestLabeled",
				payload: { ...fields, label: payload.label.name, labeled: payload.action === "labeled" },
				actor,
			};
		if (payload.action === "synchronize" && payload.after)
			return {
				event: "pullRequestSynchronized",
				payload: { ...fields, headSha: payload.after },
				actor,
			};
		return undefined;
	}
	if (kind === "pull_request_review" && pr && payload.review && payload.action === "submitted") {
		const review = payload.review;
		return {
			event: "pullRequestReviewed",
			payload: {
				...base(pr, review.user),
				reviewId: review.id,
				state: review.state.toLowerCase(),
				body: review.body,
				association: review.author_association,
			},
			actor: review.user.id,
		};
	}
	const comment = payload.comment;
	const commentedOn =
		kind === "pull_request_review_comment"
			? pr
			: payload.issue?.pull_request
				? payload.issue
				: undefined;
	if (
		(kind === "issue_comment" || kind === "pull_request_review_comment") &&
		comment &&
		commentedOn &&
		payload.action === "created"
	)
		return {
			event: "pullRequestCommented",
			payload: {
				...base(commentedOn, comment.user),
				commentId: comment.id,
				body: comment.body,
				association: comment.author_association,
				inline: kind === "pull_request_review_comment",
				path: comment.path ?? null,
			},
			actor: comment.user.id,
		};
	const check =
		kind === "check_suite" && payload.check_suite
			? {
					event: "checkSuiteCompleted" as const,
					name: payload.check_suite.app?.name ?? "checks",
					url: null,
					...payload.check_suite,
				}
			: kind === "check_run" && payload.check_run
				? {
						event: "checkRunCompleted" as const,
						url: payload.check_run.html_url ?? null,
						...payload.check_run,
					}
				: undefined;
	if (check && payload.action === "completed" && check.pull_requests.length)
		return {
			event: check.event,
			payload: {
				repo,
				pullRequests: check.pull_requests.map((p) => p.number),
				name: check.name,
				conclusion: check.conclusion,
				headSha: check.head_sha,
				url: check.url,
			},
			actor: payload.sender.id,
		};
	return undefined;
}

/** A pull request a conversation can follow with `ctx.subscribe`. */
export interface PullRequestRef {
	[key: string]: string | number;
	/** `owner/name`. */
	repo: string;
	number: number;
}

const TRUSTED = ["OWNER", "MEMBER", "COLLABORATOR"];
const PULL_REQUEST_EVENTS: readonly EventKey[] = [
	"pullRequestClosed",
	"pullRequestReopened",
	"pullRequestLabeled",
	"pullRequestSynchronized",
	"pullRequestReviewed",
	"pullRequestCommented",
	"checkSuiteCompleted",
	"checkRunCompleted",
];
const REVIEW_KINDS = ["commit", "review", "comment", "merged", "closed", "reopened"];

/** Fields of the pull request and check events, as resource kinds read them. */
type PullRequestFields = GitHubPullRequestEvent &
	Partial<
		GitHubPullRequestClosed &
			GitHubPullRequestLabeled &
			GitHubPullRequestSynchronized &
			GitHubPullRequestReviewed &
			GitHubPullRequestCommented &
			GitHubCheckCompleted
	>;

function notifyPullRequest({ name, payload }: PluginEvent): ResourceNotification | undefined {
	const p = payload as unknown as PullRequestFields;
	const pr = `${p.repo}#${p.number}`;
	const by = { actor: p.actorLogin, actorId: p.actorId, actorType: p.actorType };
	switch (name as EventKey) {
		case "pullRequestClosed":
			return p.merged
				? { kind: "merged", summary: `${pr} was merged by ${by.actor}.`, attributes: by }
				: { kind: "closed", summary: `${pr} was closed by ${by.actor}.`, attributes: by };
		case "pullRequestReopened":
			return { kind: "reopened", summary: `${pr} was reopened by ${by.actor}.`, attributes: by };
		case "pullRequestLabeled":
			return {
				kind: "label",
				summary: `${by.actor} ${p.labeled ? "added" : "removed"} the label "${p.label}" on ${pr}.`,
				priority: "low",
				attributes: { ...by, label: p.label ?? null, labeled: p.labeled ?? null },
			};
		case "pullRequestSynchronized":
			return {
				kind: "commit",
				summary: `${by.actor} pushed new commits to ${pr} (head ${String(p.headSha).slice(0, 7)}).`,
				attributes: { ...by, sha: p.headSha ?? null },
			};
		case "pullRequestReviewed": {
			const verb =
				p.state === "approved"
					? "approved"
					: p.state === "changes_requested"
						? "requested changes on"
						: "reviewed";
			return {
				kind: "review",
				summary: `${by.actor} ${verb} ${pr}${p.body ? `:\n\n${p.body}` : "."}`,
				attributes: { ...by, state: p.state ?? null, association: p.association ?? null },
			};
		}
		case "pullRequestCommented":
			return {
				kind: "comment",
				summary: `${by.actor} commented on ${pr}${p.path ? ` (${p.path})` : ""}:\n\n${p.body}`,
				attributes: { ...by, association: p.association ?? null, inline: p.inline ?? null },
			};
		case "checkSuiteCompleted":
		case "checkRunCompleted": {
			const passed = ["success", "neutral", "skipped"].includes(p.conclusion ?? "");
			return {
				kind: "check",
				summary: `Check "${p.name}" completed: ${p.conclusion ?? "no conclusion"} (${String(p.headSha).slice(0, 7)})${p.url ? ` ${p.url}` : ""}.`,
				priority: (passed ? "low" : "normal") satisfies InputPriority,
				attributes: { name: p.name ?? null, conclusion: p.conclusion ?? null },
			};
		}
		default:
			return undefined;
	}
}

/** Comments and reviews count only from people with write access, or from this app's own bot. */
function trustedPullRequestEvent(botUserId: string | undefined) {
	return (event: ResourceEvent) => {
		if (event.kind !== "comment" && event.kind !== "review") return true;
		const { actorType, actorId, association } = event.attributes;
		if (actorType === "Bot") return botUserId !== undefined && actorId === botUserId;
		return TRUSTED.includes(String(association));
	};
}

function pullRequestKind(botUserId: string | undefined): ResourceKind<PullRequestRef> {
	return {
		events: PULL_REQUEST_EVENTS,
		key: ({ repo, number }) => `${repo}#${number}`,
		match: ({ payload }) => {
			const p = payload as unknown as PullRequestFields;
			return (p.pullRequests ?? [p.number]).map((number) => `${p.repo}#${number}`);
		},
		notify: notifyPullRequest,
		presets: { review: REVIEW_KINDS, working: [...REVIEW_KINDS, "label", "check"] },
		until: { "merged-or-closed": (event) => event.kind === "merged" || event.kind === "closed" },
		filter: trustedPullRequestEvent(botUserId),
	};
}

const pullRequestInput = z.object({
	repo: z.string().describe("Repository as owner/name"),
	number: z.number().int().positive().describe("Pull request number"),
});
const subscribeTool: ToolDefinition<
	z.infer<typeof pullRequestInput> & { preset?: "review" | "working" }
> = {
	name: "github_subscribe_pull_request",
	description:
		"Follow a GitHub pull request in this conversation until it is merged or closed: new commits, reviews, and comments from collaborators arrive as notifications (preset `working` also adds labels and CI results).",
	input: pullRequestInput.extend({ preset: z.enum(["review", "working"]).optional() }),
	async run({ repo, number, preset }, ctx) {
		const receipt = await ctx.subscribe("subscribe", pullRequest({ repo, number }), {
			preset: preset ?? "review",
			until: "merged-or-closed",
		});
		return `Subscribed to ${receipt.resource}.`;
	},
};
const unsubscribeTool: ToolDefinition<z.infer<typeof pullRequestInput>> = {
	name: "github_unsubscribe_pull_request",
	description: "Stop following a GitHub pull request in this conversation.",
	input: pullRequestInput,
	async run({ repo, number }, ctx) {
		const receipt = await ctx.unsubscribe("unsubscribe", pullRequest({ repo, number }));
		return receipt.removed
			? `Unsubscribed from ${receipt.resource}.`
			: `Not subscribed to ${receipt.resource}.`;
	},
};

async function verify(request: Request, body: string, options: GitHubOptions): Promise<boolean> {
	try {
		if (options.webhookVerifier) return Boolean(await options.webhookVerifier(request, body));
		const signature = request.headers.get("x-hub-signature-256") ?? "";
		const expected = `sha256=${createHmac("sha256", options.webhookSecret!).update(body).digest("hex")}`;
		return (
			signature.length === expected.length &&
			timingSafeEqual(Buffer.from(signature), Buffer.from(expected))
		);
	} catch {
		return false;
	}
}
function isPAT(options: GitHubOptions): options is Extract<GitHubOptions, { token: string }> {
	return typeof options.token === "string";
}

class ScopedGitHubAdapter extends GitHubAdapter {
	private owner: Promise<string> | undefined;
	constructor(private readonly options: GitHubOptions) {
		let accepts: (payload: unknown) => Promise<boolean>;
		super({
			...(isPAT(options) ? { ...options, installationId: undefined } : options),
			webhookVerifier: async (request, body) => {
				if (!(await verify(request, body, options))) return false;
				try {
					return await accepts(JSON.parse(body) as unknown);
				} catch {
					return false;
				}
			},
		});
		accepts = (payload) => this.acceptPayload(payload);
	}

	async acceptPayload(raw: unknown): Promise<boolean> {
		const parsed = z
			.object({
				installation: z.object({ id: z.number() }).optional(),
				repository: z
					.object({
						full_name: z.string().regex(/^[^/:]+\/[^/:]+$/),
						owner: z.object({ login: z.string() }).optional(),
					})
					.optional(),
			})
			.safeParse(raw);
		if (!parsed.success) return false;
		const payload = parsed.data;
		if (!isPAT(this.options) && payload.installation?.id !== this.options.installationId)
			return false;
		if (
			isPAT(this.options) &&
			typeof this.options.installationId === "number" &&
			payload.installation &&
			payload.installation.id !== this.options.installationId
		)
			return false;
		if (isPAT(this.options) || this.options.owner) {
			const owner =
				this.options.owner ??
				(await (this.owner ??= this.octokit.rest.users
					.getAuthenticated()
					.then(({ data }) => data.login)
					.catch((error: unknown) => {
						this.owner = undefined;
						throw error;
					})));
			const repoOwner = payload.repository?.full_name.split("/")[0];
			if (!repoOwner || repoOwner.toLowerCase() !== owner.toLowerCase()) return false;
			if (
				payload.repository?.owner &&
				payload.repository.owner.login.toLowerCase() !== owner.toLowerCase()
			)
				return false;
		}
		return true;
	}
}

const createPlugin = definePlugin<
	[GitHubOptions],
	"github",
	typeof events,
	{ github: GitHubClient }
>((options) => {
	const installationId = String(options.installationId ?? "default");
	let instance: ScopedGitHubAdapter | undefined;
	const adapter = () => (instance ??= new ScopedGitHubAdapter(options));
	return {
		id: "github",
		init() {
			if (!options.webhookSecret && !options.webhookVerifier)
				throw new Error("GitHub requires webhookSecret or webhookVerifier");
		},
		shutdown() {
			instance = undefined;
		},
		events,
		chat: chatCapability({
			adapter,
			installationId,
			history: { mode: "thread", limit: 50 },
			status: "message",
			isNewThread: (thread, message) => thread.id.endsWith(`:rc:${message.id}`),
		}),
		clients: () => ({ github: adapter().octokit }),
		resources: {
			pullRequest: pullRequestKind(
				options.botUserId === undefined ? undefined : String(options.botUserId),
			),
		},
		toolkit: () => ({ tools: [subscribeTool, unsubscribeTool] }),
		identity: {
			async lookup(subject, clients) {
				if (subject.provider !== "github" || subject.installationId !== installationId)
					return undefined;
				try {
					const { data } = /^\d+$/.test(subject.subjectId)
						? await clients.github.request("GET /user/{account_id}", {
								account_id: Number(subject.subjectId),
							})
						: await clients.github.rest.users.getByUsername({ username: subject.subjectId });
					return { name: data.name || data.login };
				} catch (error) {
					if (error instanceof Error && "status" in error && error.status === 404) return undefined;
					throw error;
				}
			},
		},
		routes: {
			"POST /github/webhook": async (request, ctx) => {
				const body = await request.text();
				if (!(await verify(request, body, options)))
					return new Response("Invalid signature", { status: 401 });
				const kind = request.headers.get("x-github-event") ?? "";
				if (!HANDLED.includes(kind)) return new Response(null, { status: 200 });
				let raw: unknown;
				try {
					raw = JSON.parse(body);
				} catch {
					return new Response("Invalid JSON", { status: 400 });
				}
				const parsed = webhook.safeParse(raw);
				if (!parsed.success) return new Response("Invalid payload", { status: 400 });
				const payload = parsed.data;
				if (!(await adapter().acceptPayload(raw)))
					return new Response("Wrong installation", { status: 403 });
				const [owner, repo] = payload.repository.full_name.split("/") as [string, string];
				if (payload.action !== "opened" || (kind !== "pull_request" && kind !== "issues")) {
					const found = pullRequestEventOf(kind, payload, (prNumber) =>
						adapter().encodeThreadId({ owner, repo, prNumber, type: "pr" }),
					);
					if (!found) return new Response(null, { status: 200 });
					const delivery = request.headers.get("x-github-delivery");
					if (!delivery) return new Response("Missing delivery id", { status: 400 });
					const threadId = found.payload.threadId as string | undefined;
					await ctx.emit(found.event, found.payload as never, {
						id: JSON.stringify(["github", installationId, delivery]),
						origin: {
							provider: "github",
							installationId,
							repo: payload.repository.full_name,
							...(threadId ? { number: found.payload.number as number, threadId } : {}),
						},
						replyTo: threadId ? { kind: "thread", provider: "github", threadId } : undefined,
						requestedBy: identity({ installationId, userId: found.actor }),
					});
					return new Response(null, { status: 200 });
				}
				const source = kind === "pull_request" ? payload.pull_request : payload.issue;
				if (!source) return new Response("Missing conversation", { status: 400 });
				if (kind === "issues" && payload.issue?.pull_request)
					return new Response(null, { status: 200 });
				const delivery = request.headers.get("x-github-delivery");
				if (!delivery) return new Response("Missing delivery id", { status: 400 });
				const threadId = adapter().encodeThreadId({
					owner,
					repo,
					prNumber: source.number,
					type: kind === "issues" ? "issue" : "pr",
				});
				const ingress: EmitOptions = {
					id: JSON.stringify(["github", installationId, delivery]),
					origin: {
						provider: "github",
						installationId,
						repo: payload.repository.full_name,
						number: source.number,
						threadId,
					},
					replyTo: { kind: "thread", provider: "github", threadId },
					requestedBy: identity({ installationId, userId: source.user.id }),
				};
				await ctx.emit(
					kind === "issues" ? "issueOpened" : "pullRequestOpened",
					{
						repo: payload.repository.full_name,
						id: source.id,
						number: source.number,
						title: source.title,
						body: source.body,
						url: source.html_url,
						authorId: String(source.user.id),
						authorLogin: source.user.login,
						threadId,
					},
					ingress,
				);
				return new Response(null, { status: 200 });
			},
		},
	};
});

const HANDLED = [
	"pull_request",
	"issues",
	"pull_request_review",
	"issue_comment",
	"pull_request_review_comment",
	"check_suite",
	"check_run",
];

/** A pull request to follow: `ctx.subscribe("watch", github.pullRequest({ repo, number }), …)`. */
export function pullRequest(ref: PullRequestRef) {
	return resource<PullRequestRef>("github", "pullRequest", { repo: ref.repo, number: ref.number });
}

export interface GitHubFilter {
	repo?: string;
	thread?: string;
	observe?: boolean;
}
export function mentioned(filter: GitHubFilter = {}) {
	return chatTrigger("github", "mentioned", {
		channel: filter.repo,
		thread: filter.thread,
		observe: filter.observe,
	});
}
function openedTrigger(
	event: "pullRequestOpened" | "issueOpened",
	filter: Pick<GitHubFilter, "repo" | "observe">,
) {
	return trigger<GitHubConversation>({
		event: `github.${event}`,
		filter: filter.repo ? { repo: [filter.repo] } : {},
		observe: filter.observe,
		specificity: filter.repo ? 1 : 0,
	});
}
export function pullRequestOpened(filter: Pick<GitHubFilter, "repo" | "observe"> = {}) {
	return openedTrigger("pullRequestOpened", filter);
}
export function issueOpened(filter: Pick<GitHubFilter, "repo" | "observe"> = {}) {
	return openedTrigger("issueOpened", filter);
}
export function identity(
	options: { installationId?: string | number } & (
		| { login: string; userId?: never }
		| { userId: string | number; login?: never }
	),
): ExternalIdentity {
	return chatIdentity(
		"github",
		String(options.installationId ?? "default"),
		String(options.userId ?? options.login),
	);
}
export const github = Object.assign(createPlugin, {
	mentioned,
	pullRequestOpened,
	issueOpened,
	identity,
	pullRequest,
});
