import { createHmac, timingSafeEqual } from "node:crypto";
import {
	GitHubAdapter,
	type GitHubAdapterPATConfig,
	type GitHubAdapterAppConfig,
} from "@chat-adapter/github";
import { chatCapability, chatEvents, chatIdentity, chatTrigger } from "@fabrial/chat";
import {
	defineEvent,
	definePlugin,
	trigger,
	type EmitOptions,
	type ExternalIdentity,
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
const events = { ...chatEvents(), pullRequestOpened: opened, issueOpened: opened };
const user = z.object({ id: z.number(), login: z.string() });
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
	pull_request: webhookConversation.optional(),
	issue: webhookConversation.extend({ pull_request: z.unknown().optional() }).optional(),
});

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
				const kind = request.headers.get("x-github-event");
				if (kind !== "pull_request" && kind !== "issues")
					return new Response(null, { status: 200 });
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
				if (payload.action !== "opened") return new Response(null, { status: 200 });
				const source = kind === "pull_request" ? payload.pull_request : payload.issue;
				if (!source) return new Response("Missing conversation", { status: 400 });
				if (kind === "issues" && payload.issue?.pull_request)
					return new Response(null, { status: 200 });
				const delivery = request.headers.get("x-github-delivery");
				if (!delivery) return new Response("Missing delivery id", { status: 400 });
				const [owner, repo] = payload.repository.full_name.split("/") as [string, string];
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
});
