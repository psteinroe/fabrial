import { createFabrial, trigger, type JsonObject, type ResourceKind } from "fabrial";
import { FakeChat, MemoryRuntime, fakeAgents, testApp } from "fabrial/testing";
import { describe, expect, it, vi } from "vitest";
import { github, parsePullRequest, pullRequest } from "../../src/index.ts";
import { options } from "../helper.ts";
import { bot, deliver, outsider, webhooks } from "../webhooks.ts";

/** The plugin without profile lookups (ingress resolves the actor). */
function plugin() {
	const instance = github(options);
	vi.spyOn(instance.identity!, "lookup").mockResolvedValue(undefined);
	return instance;
}
const kind = github(options).resources!.pullRequest as ResourceKind<any>;
const prThread = { kind: "thread", provider: "github", threadId: "github:acme/app:5" };

/** The event a webhook emits. */
async function emitted(hook: { event: string; payload: unknown }) {
	const runtime = new MemoryRuntime();
	const app = testApp(createFabrial({ plugins: [plugin()] }), {
		runtime,
		chat: new FakeChat(),
		workflows: [],
	});
	await app.start();
	expect((await app.fetch(deliver(hook, "d"))).status).toBe(200);
	await app.stop();
	const event = runtime.emitted.find((e) => e.name.startsWith("github."));
	return event && { name: event.name.slice("github.".length), payload: event.payload };
}
const notified = async (hook: { event: string; payload: unknown }) =>
	kind.notify((await emitted(hook))!);

describe("pull request resources", () => {
	it("maps webhooks to events, the PR's thread, and notification kinds", async () => {
		const cases: [ReturnType<(typeof webhooks)[keyof typeof webhooks]>, string, string][] = [
			[webhooks.review("changes_requested"), "pullRequestReviewed", "review"],
			[webhooks.comment(), "pullRequestCommented", "comment"],
			[webhooks.reviewComment(), "pullRequestCommented", "comment"],
			[webhooks.closed(true), "pullRequestClosed", "merged"],
			[webhooks.closed(false), "pullRequestClosed", "closed"],
			[webhooks.reopened(), "pullRequestReopened", "reopened"],
			[webhooks.labeled(), "pullRequestLabeled", "label"],
			[webhooks.synchronize(), "pullRequestSynchronized", "commit"],
			[webhooks.checkRun(), "checkRunCompleted", "check"],
			[webhooks.checkSuite(), "checkSuiteCompleted", "check"],
		];
		for (const [hook, name, notification] of cases) {
			const event = (await emitted(hook))!;
			expect(event.name).toBe(name);
			expect(kind.events).toContain(name);
			expect(kind.threads(event)).toEqual([prThread]);
			expect(kind.notify(event)).toMatchObject({ kind: notification });
		}
		expect(kind.defaults).toEqual(["commit", "review", "comment", "merged", "closed", "reopened"]);
		expect(kind.thread({ repo: "acme/app", number: 5 })).toEqual(prThread);
		expect(pullRequest("acme/app#5")).toEqual({
			kind: "fabrial.resource",
			provider: "github",
			type: "pullRequest",
			ref: { repo: "acme/app", number: 5 },
		});
		expect(pullRequest({ repo: "acme/app", number: 5 })).toEqual(pullRequest("acme/app#5"));
		expect(() => parsePullRequest("acme/app/5")).toThrow("owner/name#number");
	});

	it("ignores comments on issues and unsupported actions", async () => {
		const issue = webhooks.comment();
		const { pull_request: _, ...plain } = (issue.payload as { issue: JsonObject }).issue;
		expect(
			await emitted({ ...issue, payload: { ...issue.payload, issue: plain } }),
		).toBeUndefined();
		const edited = webhooks.comment();
		expect(
			await emitted({ ...edited, payload: { ...edited.payload, action: "edited" } }),
		).toBeUndefined();
	});

	it("drops comments and reviews from outside collaborators and other bots", async () => {
		for (const association of ["OWNER", "MEMBER", "COLLABORATOR"])
			expect(await notified(webhooks.comment(association))).toBeDefined();
		// Untrusted comments and reviews are never emitted, so neither workflows nor follows see them.
		for (const association of ["CONTRIBUTOR", "NONE", "FIRST_TIME_CONTRIBUTOR"]) {
			expect(await emitted(webhooks.comment(association, outsider))).toBeUndefined();
			expect(await emitted(webhooks.review("approved", association, outsider))).toBeUndefined();
		}
		expect(await notified(webhooks.comment("NONE", bot))).toMatchObject({ priority: "low" });
		expect(await emitted(webhooks.comment("NONE", { ...bot, id: 100 }))).toBeUndefined();
		// Lifecycle events are not filtered by author.
		const merged = webhooks.closed(true);
		expect(
			await notified({ ...merged, payload: { ...merged.payload, sender: outsider } }),
		).toMatchObject({ kind: "merged" });
	});

	it("renders default priorities and the ids the write tools need", async () => {
		expect(await notified(webhooks.review("changes_requested"))).toMatchObject({
			priority: "urgent",
			summary: expect.stringContaining("requested changes on acme/app#5"),
			attributes: { pr: "acme/app#5", review: 11, state: "changes_requested", actor: "alice" },
		});
		expect(await notified(webhooks.review("approved"))).toMatchObject({ priority: "normal" });
		expect(await notified(webhooks.reviewComment())).toMatchObject({
			priority: "normal",
			attributes: { pr: "acme/app#5", comment: 13, inline: true },
		});
		expect(await notified(webhooks.labeled())).toMatchObject({ priority: "low" });
		expect(await notified(webhooks.checkSuite("success"))).toMatchObject({ priority: "low" });
		expect(await notified(webhooks.checkRun("failure"))).toMatchObject({ priority: "normal" });
	});
});

describe("follows in an app", () => {
	async function setup() {
		const f = createFabrial({ plugins: [plugin()] });
		const agent = f.defineAgent({ name: "dev", description: "Answers test questions." });
		const pr = pullRequest("acme/app#5");
		const slackThread = { kind: "thread" as const, provider: "slack", threadId: "slack:C1:1" };
		const watch = f.defineWorkflow({
			name: "watch",
			async run(input: { all?: boolean } | null, ctx) {
				return ctx.follow("watch", pr, {
					to: { agent, session: "pr-5", home: slackThread },
					...(input?.all ? { events: "all", priority: { label: "urgent" } } : {}),
				});
			},
		});
		const unwatch = f.defineWorkflow({
			name: "unwatch",
			async run(_input, ctx) {
				return ctx.unfollow("unwatch", pr, { to: { agent, session: "pr-5" } });
			},
		});
		const orphan = f.defineWorkflow({
			name: "orphan",
			async run(_input, ctx) {
				await ctx.follow("watch", pr);
			},
		});
		// A conversation `ctx.send` reached follows the PR with the receipt's reference as is.
		const sendAndWatch = f.defineWorkflow({
			name: "send-and-watch",
			async run(_input, ctx) {
				const sent = await ctx.send("send", agent, { input: "Watch PR 5", session: "ops" });
				const followed = await ctx.follow("watch", pr, { to: sent.conversation });
				const removed = await ctx.unfollow("unwatch", pr, { to: sent.conversation });
				await ctx.follow("again", pr, { to: sent.conversation });
				return { sent: sent.conversation, followed, removed: removed.removed };
			},
		});
		const observed = vi.fn();
		const owner = f.defineWorkflow({
			name: "review-owner",
			triggers: [trigger({ event: "github.pullRequestReviewed" })],
			async run(input) {
				observed(input);
			},
		});
		const runtime = new MemoryRuntime();
		const agents = fakeAgents();
		const app = testApp(f, {
			runtime,
			chat: new FakeChat(),
			agents,
			workflows: [watch, unwatch, orphan, owner, sendAndWatch],
		});
		await app.start();
		const start = async (workflow: string, input: JsonObject | null = null) => {
			const id = await runtime.invoke(workflow, input, {
				metadata: { interactionId: workflow, origin: null, replyTo: null, requestedBy: null },
			});
			await runtime.flush();
			return runtime.result(id)!;
		};
		const send = async (hook: { event: string; payload: unknown }, delivery: string) => {
			expect((await app.fetch(deliver(hook, delivery))).status).toBe(200);
			await runtime.flush();
		};
		const records = async () =>
			(await (await app.host.chat()!.thread(prThread as never)).getState())?.resourceFollows;
		return { app, agents, start, send, observed, slackThread, records };
	}

	it("delivers notifications to the follower, once per delivery, until it unfollows", async () => {
		const t = await setup();
		expect(await t.start("watch")).toMatchObject({
			status: "completed",
			output: { resource: "github:acme/app#5", session: "app:pr-5", agent: "dev" },
		});
		// The follow is a record in the PR thread's state; following again replaces it.
		await t.start("watch");
		expect(await t.records()).toEqual([
			{ session: "app:pr-5", agent: "dev", resource: "github:acme/app#5", replyTo: t.slackThread },
		]);

		await t.send(webhooks.review("changes_requested"), "r1");
		await t.send(webhooks.review("changes_requested"), "r1");
		await t.send(webhooks.comment("NONE", outsider), "c1");
		expect(t.agents.sent).toHaveLength(1);
		const [review] = t.agents.sent;
		expect(review).toMatchObject({
			agent: "dev",
			session: "app:pr-5",
			delivery: "steer",
			metadata: { replyTo: t.slackThread, send: { kind: "notification", priority: "urgent" } },
		});
		expect(review!.content).toContain(
			'<notification source="github" kind="review" priority="urgent" pr="acme/app#5"',
		);
		expect(review!.content).toContain('review="11"');
		expect(review!.content).toContain("requested changes on acme/app#5");
		// Triggers on the same event still run; the follow takes no ownership.
		expect(t.observed).toHaveBeenCalledOnce();

		await t.send(webhooks.comment(), "c2");
		expect(t.agents.sent.at(-1)).toMatchObject({ delivery: "followUp" });
		// Labels and CI are not default kinds; `events: "all"` adds them, `priority` overrides per kind.
		await t.send(webhooks.labeled(), "l1");
		expect(t.agents.sent).toHaveLength(2);
		await t.start("watch", { all: true });
		expect(await t.records()).toHaveLength(1);
		await t.send(webhooks.labeled(), "l2");
		expect(t.agents.sent.at(-1)).toMatchObject({ delivery: "steer" });
		await t.send(webhooks.checkSuite("success"), "cs1");
		expect(t.agents.sent.at(-1)).toMatchObject({ delivery: "write" });

		// A merge is delivered; the follow lasts until unfollow.
		await t.send(webhooks.closed(true), "m1");
		expect(t.agents.sent.at(-1)!.content).toContain("was merged");
		expect(await t.records()).toHaveLength(1);
		expect(await t.start("unwatch")).toMatchObject({ output: { removed: true } });
		expect(await t.start("unwatch")).toMatchObject({ output: { removed: false } });
		await t.send(webhooks.reopened(), "o1");
		expect(t.agents.sent).toHaveLength(5);
		await t.app.stop();
	});

	it("a send receipt's conversation follows a resource without re-resolving its session", async () => {
		const t = await setup();
		expect(await t.start("send-and-watch")).toMatchObject({
			status: "completed",
			output: {
				sent: { kind: "fabrial.conversation", session: "app:ops", agent: "dev" },
				followed: { resource: "github:acme/app#5", session: "app:ops", agent: "dev" },
				removed: true,
			},
		});
		expect(await t.records()).toEqual([
			{ session: "app:ops", agent: "dev", resource: "github:acme/app#5", replyTo: null },
		]);
		await t.send(webhooks.review("changes_requested"), "r1");
		// The notification reaches the conversation the send started.
		expect(t.agents.sent.map((each) => [each.session, each.agent])).toEqual([
			["app:ops", "dev"],
			["app:ops", "dev"],
		]);
		await t.app.stop();
	});

	it("requires a target outside agent conversations", async () => {
		const t = await setup();
		expect(await t.start("orphan")).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Pass `to: { agent, session }`"),
		});
		await t.app.stop();
	});
});

describe("GitHub write tools", () => {
	function tools() {
		const instance = github(options);
		const client = instance.clients!({}).github;
		const byName = Object.fromEntries(
			instance.toolkit({ github: client }).tools.map((tool) => [tool.name, tool]),
		);
		const ctx = {
			clients: { github: client },
			follow: vi.fn(async () => ({ resource: "github:acme/app#5", session: "s", agent: "dev" })),
			unfollow: vi.fn(async () => ({
				removed: true,
				resource: "github:acme/app#5",
				session: "s",
				agent: "dev",
			})),
		};
		const run = (name: string, input: JsonObject) => byName[name]!.run(input, ctx as never);
		return { client, ctx, run, names: Object.keys(byName) };
	}

	it("lists the follow and write tools", () => {
		expect(tools().names).toEqual([
			"github_follow_pull_request",
			"github_unfollow_pull_request",
			"github_comment",
			"github_reply_to_review",
		]);
	});

	it("follows and unfollows the calling conversation", async () => {
		const t = tools();
		expect(await t.run("github_follow_pull_request", { pr: "acme/app#5", events: "all" })).toBe(
			"Following github:acme/app#5.",
		);
		expect(t.ctx.follow).toHaveBeenCalledWith("follow", pullRequest("acme/app#5"), {
			events: "all",
		});
		expect(await t.run("github_unfollow_pull_request", { pr: "acme/app#5" })).toBe(
			"Stopped following github:acme/app#5.",
		);
		expect(t.ctx.unfollow).toHaveBeenCalledWith("unfollow", pullRequest("acme/app#5"));
	});

	it("comments on a pull request", async () => {
		const t = tools();
		const create = vi.spyOn(t.client.rest.issues, "createComment").mockResolvedValue({
			data: { html_url: "https://github.com/acme/app/pull/5#issuecomment-1" },
		} as never);
		expect(await t.run("github_comment", { pr: "acme/app#5", body: "Done" })).toBe(
			"Commented: https://github.com/acme/app/pull/5#issuecomment-1",
		);
		expect(create).toHaveBeenCalledWith({
			owner: "acme",
			repo: "app",
			issue_number: 5,
			body: "Done",
		});
		await expect(t.run("github_comment", { pr: "5", body: "x" })).rejects.toThrow(
			"owner/name#number",
		);
	});

	it("answers a review in the conversation, linking it, or replies in an inline comment's thread", async () => {
		const t = tools();
		const review = vi.spyOn(t.client.rest.pulls, "getReview").mockResolvedValue({
			data: {
				user: { login: "alice" },
				html_url: "https://github.com/acme/app/pull/5#pullrequestreview-11",
			},
		} as never);
		const comment = vi.spyOn(t.client.rest.issues, "createComment").mockResolvedValue({
			data: { html_url: "https://github.com/acme/app/pull/5#issuecomment-2" },
		} as never);
		const reply = vi.spyOn(t.client.rest.pulls, "createReplyForReviewComment").mockResolvedValue({
			data: { html_url: "https://github.com/acme/app/pull/5#discussion_r3" },
		} as never);
		expect(
			await t.run("github_reply_to_review", { pr: "acme/app#5", review: 11, body: "Added tests" }),
		).toBe("Replied: https://github.com/acme/app/pull/5#issuecomment-2");
		expect(review).toHaveBeenCalledWith({
			owner: "acme",
			repo: "app",
			pull_number: 5,
			review_id: 11,
		});
		expect(comment).toHaveBeenCalledWith({
			owner: "acme",
			repo: "app",
			issue_number: 5,
			body: "> In reply to @alice's [review](https://github.com/acme/app/pull/5#pullrequestreview-11)\n\nAdded tests",
		});
		expect(
			await t.run("github_reply_to_review", { pr: "acme/app#5", comment: 13, body: "Fixed" }),
		).toBe("Replied: https://github.com/acme/app/pull/5#discussion_r3");
		expect(reply).toHaveBeenCalledWith({
			owner: "acme",
			repo: "app",
			pull_number: 5,
			comment_id: 13,
			body: "Fixed",
		});
		await expect(t.run("github_reply_to_review", { pr: "acme/app#5", body: "x" })).rejects.toThrow(
			"Pass `review` or `comment`",
		);
	});
});
