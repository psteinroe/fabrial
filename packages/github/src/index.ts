import { createHmac, timingSafeEqual } from "node:crypto";
import {
	GitHubAdapter,
	type GitHubAdapterPATConfig,
	type GitHubAdapterAppConfig,
} from "@chat-adapter/github";
import {
	type EmitOptions,
	type EventFilter,
	type ExternalIdentity,
	type InputPriority,
	type JsonObject,
	type ThreadRef,
	type ToolDefinition,
} from "fabrial";
import {
	chatCapability,
	chatEvents,
	chatIdentity,
	chatTrigger,
	defineEvent,
	definePlugin,
	resource,
	trigger,
	type PluginEvent,
	type ResourceKind,
	type ResourceNotification,
} from "fabrial/plugin";
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

/** A pull request a conversation can follow with `ctx.follow`. */
export interface PullRequestRef {
	[key: string]: string | number;
	/** `owner/name`. */
	repo: string;
	number: number;
}

/** Parse `owner/name#number`, the form notifications render (`pr="acme/app#42"`). */
export function parsePullRequest(pr: string): PullRequestRef {
	const match = /^([^/\s#]+\/[^/\s#]+)#(\d+)$/.exec(pr.trim());
	if (!match) throw new Error(`Expected a pull request as owner/name#number, got "${pr}"`);
	return { repo: match[1]!, number: Number(match[2]) };
}

const TRUSTED = ["OWNER", "MEMBER", "COLLABORATOR"];
/**
 * The author policy for every comment and review that can reach an agent: chat messages, thread history,
 * and the comment/review lifecycle events follows are notified of. People with write access count, and this
 * app's own bot; other bots and outsiders don't.
 */
function trusted(
	user: { id: number; type?: string },
	association: string | undefined,
	botUserId: string | undefined,
): boolean {
	if (botUserId !== undefined && String(user.id) === botUserId) return true;
	return user.type !== "Bot" && TRUSTED.includes(association ?? "");
}
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
/** What a follow receives by default; `events: "all"` adds `label` and `check`. */
const DEFAULT_KINDS = ["commit", "review", "comment", "merged", "closed", "reopened"];

/** Fields of the pull request and check events, as the resource kind reads them. */
type PullRequestFields = GitHubPullRequestEvent &
	Partial<
		GitHubPullRequestClosed &
			GitHubPullRequestLabeled &
			GitHubPullRequestSynchronized &
			GitHubPullRequestReviewed &
			GitHubPullRequestCommented &
			GitHubCheckCompleted
	>;

/**
 * Render a pull request event. Untrusted comments and reviews are never emitted (see `trusted`); this
 * app's own are `low`: seen on the next run, they don't wake the agent.
 */
function notifyPullRequest(
	{ name, payload }: PluginEvent,
	botUserId: string | undefined,
): ResourceNotification | undefined {
	const p = payload as unknown as PullRequestFields;
	const pr = `${p.repo}#${p.number}`;
	const by = { pr, actor: p.actorLogin, actorType: p.actorType };
	const own = botUserId !== undefined && p.actorId === botUserId;
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
				attributes: { ...by, label: p.label ?? null },
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
				priority: own ? "low" : p.state === "changes_requested" ? "urgent" : "normal",
				attributes: { ...by, review: p.reviewId ?? null, state: p.state ?? null },
			};
		}
		case "pullRequestCommented":
			return {
				kind: "comment",
				summary: `${by.actor} commented on ${pr}${p.path ? ` (${p.path})` : ""}:\n\n${p.body}`,
				priority: own ? "low" : "normal",
				attributes: { ...by, comment: p.commentId ?? null, inline: p.inline ?? null },
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

function pullRequestKind(
	botUserId: () => string | undefined,
	threadOf: (repo: string, number: number) => ThreadRef,
): ResourceKind<PullRequestRef> {
	return {
		key: ({ repo, number }) => `${repo}#${number}`,
		thread: ({ repo, number }) => threadOf(repo, number),
		events: PULL_REQUEST_EVENTS,
		threads: ({ payload }) => {
			const p = payload as unknown as PullRequestFields;
			return (p.pullRequests ?? [p.number]).map((number) => threadOf(p.repo, number));
		},
		notify: (event) => notifyPullRequest(event, botUserId()),
		defaults: DEFAULT_KINDS,
	};
}

type Tool<T> = ToolDefinition<T, { github: GitHubClient }>;
const prInput = z.string().describe("Pull request as owner/name#number, e.g. acme/app#42");
const target = (pr: string) => {
	const { repo, number } = parsePullRequest(pr);
	const [owner, name] = repo.split("/") as [string, string];
	return { owner, repo: name, number };
};
const followTool: Tool<{ pr: string; events?: "default" | "all" }> = {
	name: "github_follow_pull_request",
	description:
		"Follow a GitHub pull request in this conversation until you unfollow it: new commits, reviews, comments from collaborators, merge, and close arrive as notifications (`events: all` also adds labels and CI results).",
	// Only a Fabrial durable operation: a call interrupted by a restart resumes it.
	idempotent: true,
	input: z.object({ pr: prInput, events: z.enum(["default", "all"]).optional() }),
	async run({ pr, events }, ctx) {
		const receipt = await ctx.follow("follow", pullRequest(pr), { events });
		return `Following ${receipt.resource}.`;
	},
};
const unfollowTool: Tool<{ pr: string }> = {
	name: "github_unfollow_pull_request",
	description:
		"Stop following a GitHub pull request in this conversation, e.g. after it is merged.",
	// Only a Fabrial durable operation: a call interrupted by a restart resumes it.
	idempotent: true,
	input: z.object({ pr: prInput }),
	async run({ pr }, ctx) {
		const receipt = await ctx.unfollow("unfollow", pullRequest(pr));
		return receipt.removed
			? `Stopped following ${receipt.resource}.`
			: `Not following ${receipt.resource}.`;
	},
};
const commentTool: Tool<{ pr: string; body: string }> = {
	name: "github_comment",
	description: "Comment on a GitHub pull request's conversation.",
	input: z.object({ pr: prInput, body: z.string().min(1).describe("Markdown") }),
	async run({ pr, body }, ctx) {
		const { owner, repo, number } = target(pr);
		const { data } = await ctx.clients.github.rest.issues.createComment({
			owner,
			repo,
			issue_number: number,
			body,
		});
		return `Commented: ${data.html_url}`;
	},
};
const replyToReviewTool: Tool<{ pr: string; review?: number; comment?: number; body: string }> = {
	name: "github_reply_to_review",
	description:
		"Answer a review on a GitHub pull request: pass `comment` to reply in the thread of an inline review comment, or `review` to answer the review as a whole (a pull request comment that links it). Use the ids from the notification.",
	input: z.object({
		pr: prInput,
		review: z.number().int().positive().optional().describe("Review id (`review` attribute)"),
		comment: z
			.number()
			.int()
			.positive()
			.optional()
			.describe("Inline review comment id (`comment` attribute with `inline`)"),
		body: z.string().min(1).describe("Markdown"),
	}),
	async run({ pr, review, comment, body }, ctx) {
		const { owner, repo, number } = target(pr);
		const github = ctx.clients.github;
		if (comment !== undefined) {
			const { data } = await github.rest.pulls.createReplyForReviewComment({
				owner,
				repo,
				pull_number: number,
				comment_id: comment,
				body,
			});
			return `Replied: ${data.html_url}`;
		}
		if (review === undefined) throw new Error("Pass `review` or `comment`");
		// GitHub has no reply to a review itself: answer in the conversation, linking the review.
		const { data: reviewed } = await github.rest.pulls.getReview({
			owner,
			repo,
			pull_number: number,
			review_id: review,
		});
		const { data } = await github.rest.issues.createComment({
			owner,
			repo,
			issue_number: number,
			body: `> In reply to @${reviewed.user?.login ?? "ghost"}'s [review](${reviewed.html_url})\n\n${body}`,
		});
		return `Replied: ${data.html_url}`;
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

/** The SDK adapter, scoped to the configured installation/owner and to trusted comment authors. */
class ScopedGitHubAdapter extends GitHubAdapter {
	private owner: Promise<string> | undefined;
	private detecting: Promise<void> | undefined;
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

	/** This app's bot account: the configured `botUserId`, else the one the SDK detects from its credentials. */
	async ownBotId(): Promise<string | undefined> {
		if (this.botUserId === undefined && this.defaultOctokit)
			await (this.detecting ??= this.detectBotUserId(this.defaultOctokit).finally(() => {
				this.detecting = undefined;
			}));
		return this.botUserId;
	}

	private trusts(comment: { user: { id: number; type?: string } }): boolean {
		const association = (comment as { author_association?: string }).author_association;
		return trusted(comment.user, association, this.botUserId);
	}

	// The SDK has no author filter: drop untrusted comments before they reach Chat, and from history.
	protected override handleIssueComment(...args: Parameters<GitHubAdapter["handleIssueComment"]>) {
		if (this.trusts(args[0].comment)) super.handleIssueComment(...args);
	}
	protected override handleReviewComment(
		...args: Parameters<GitHubAdapter["handleReviewComment"]>
	) {
		if (this.trusts(args[0].comment)) super.handleReviewComment(...args);
	}
	override async fetchMessages(...args: Parameters<GitHubAdapter["fetchMessages"]>) {
		await this.ownBotId();
		const fetched = await super.fetchMessages(...args);
		return { ...fetched, messages: fetched.messages.filter((m) => this.trusts(m.raw.comment)) };
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
				() => adapter().botUserId,
				(repo, number) => {
					const [owner, name] = repo.split("/") as [string, string];
					return {
						kind: "thread",
						provider: "github",
						threadId: adapter().encodeThreadId({ owner, repo: name, prNumber: number, type: "pr" }),
					};
				},
			),
		},
		toolkit: () => ({ tools: [followTool, unfollowTool, commentTool, replyToReviewTool] }),
		identity: {
			async lookup(subject, clients) {
				if (
					subject.provider !== "github" ||
					subject.installationId !== installationId ||
					!/^\d+$/.test(subject.subjectId)
				)
					return undefined;
				try {
					const { data } = await clients.github.request("GET /user/{account_id}", {
						account_id: Number(subject.subjectId),
					});
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
					// Comments and reviews carry an author's words: only trusted ones are emitted. The other
					// lifecycle events, and `opened` below, are public facts with the actor in the payload.
					const association = found.payload.association as string | undefined;
					if (
						association !== undefined &&
						!trusted(
							{ id: found.actor, type: found.payload.actorType as string },
							association,
							await adapter().ownBotId(),
						)
					)
						return new Response(null, { status: 200 });
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

/** A pull request to follow: `ctx.follow("watch", github.pullRequest("acme/app#42"))`. */
export function pullRequest(ref: PullRequestRef | string) {
	const { repo, number } = typeof ref === "string" ? parsePullRequest(ref) : ref;
	return resource<PullRequestRef>("github", "pullRequest", { repo, number });
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
/** Opened by anyone who can open one, outsiders included: check `authorId` before acting for them. */
export function pullRequestOpened(filter: Pick<GitHubFilter, "repo" | "observe"> = {}) {
	return eventTrigger<GitHubConversation>("pullRequestOpened", { ...filter });
}
/** Opened by anyone who can open one, outsiders included: check `authorId` before acting for them. */
export function issueOpened(filter: Pick<GitHubFilter, "repo" | "observe"> = {}) {
	return eventTrigger<GitHubConversation>("issueOpened", { ...filter });
}
type Many<T> = T | readonly T[];
interface PullRequestFilter {
	repo?: Many<string>;
	number?: Many<number>;
	observe?: boolean;
}
/** A trigger on a lifecycle event; each set filter field narrows it and adds specificity. */
function eventTrigger<T extends JsonObject>(
	event: keyof typeof events,
	{
		observe,
		...fields
	}: Record<string, Many<string | number | boolean> | undefined> & {
		observe?: boolean;
	},
) {
	const filter = Object.fromEntries(
		Object.entries(fields).flatMap(([key, value]) =>
			value === undefined ? [] : [[key, Array.isArray(value) ? [...value] : [value]]],
		),
	);
	return trigger<T>({
		event: `github.${event}`,
		// Each event declares these fields filterable; keys are checked by the helpers' signatures.
		filter: filter as EventFilter<T>,
		observe,
		specificity: Object.keys(filter).length,
	});
}
export const pullRequestClosed = (filter: PullRequestFilter & { merged?: boolean } = {}) =>
	eventTrigger<GitHubPullRequestClosed>("pullRequestClosed", { ...filter });
export const pullRequestReopened = (filter: PullRequestFilter = {}) =>
	eventTrigger<GitHubPullRequestEvent>("pullRequestReopened", { ...filter });
export const pullRequestLabeled = (filter: PullRequestFilter & { label?: Many<string> } = {}) =>
	eventTrigger<GitHubPullRequestLabeled>("pullRequestLabeled", { ...filter });
export const pullRequestSynchronized = (filter: PullRequestFilter = {}) =>
	eventTrigger<GitHubPullRequestSynchronized>("pullRequestSynchronized", { ...filter });
/** `state`: `approved`, `changes_requested`, or `commented`. */
export const pullRequestReviewed = (filter: PullRequestFilter & { state?: Many<string> } = {}) =>
	eventTrigger<GitHubPullRequestReviewed>("pullRequestReviewed", { ...filter });
export const pullRequestCommented = (filter: PullRequestFilter = {}) =>
	eventTrigger<GitHubPullRequestCommented>("pullRequestCommented", { ...filter });
interface CheckFilter {
	repo?: Many<string>;
	/** `success`, `failure`, `cancelled`, `timed_out`, … */
	conclusion?: Many<string>;
	observe?: boolean;
}
export const checkSuiteCompleted = (filter: CheckFilter = {}) =>
	eventTrigger<GitHubCheckCompleted>("checkSuiteCompleted", { ...filter });
export const checkRunCompleted = (filter: CheckFilter = {}) =>
	eventTrigger<GitHubCheckCompleted>("checkRunCompleted", { ...filter });
/**
 * A GitHub account by its numeric id, as webhooks and comments identify it (logins can be renamed).
 * Find it with `gh api users/<login> --jq .id`.
 */
export function identity(options: {
	installationId?: string | number;
	userId: string | number;
}): ExternalIdentity {
	const userId = String(options.userId);
	if (!/^\d+$/.test(userId))
		throw new Error(`GitHub identities use numeric account ids, got "${userId}"`);
	return chatIdentity("github", String(options.installationId ?? "default"), userId);
}
export const github = Object.assign(createPlugin, {
	mentioned,
	pullRequestOpened,
	issueOpened,
	pullRequestClosed,
	pullRequestReopened,
	pullRequestLabeled,
	pullRequestSynchronized,
	pullRequestReviewed,
	pullRequestCommented,
	checkSuiteCompleted,
	checkRunCompleted,
	identity,
	pullRequest,
});
