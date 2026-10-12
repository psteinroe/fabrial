import {
	createFabrial,
	matchResources,
	selectDeliveries,
	trigger,
	type JsonObject,
	type SubscriptionRecord,
} from "fabrial";
import { FakeChat, MemoryRuntime, MemorySubscriptions, fakeAgents, testApp } from "fabrial/testing";
import { describe, expect, it, vi } from "vitest";
import { github, pullRequest } from "../../src/index.ts";
import { options } from "../helper.ts";
import { bot, deliver, outsider, webhooks } from "../subscriptions.ts";

/** The plugin without profile lookups (ingress resolves the actor). */
function plugin() {
	const instance = github(options);
	vi.spyOn(instance.identity!, "lookup").mockResolvedValue(undefined);
	return instance;
}

const record = (overrides: Partial<SubscriptionRecord> = {}): SubscriptionRecord => ({
	id: "s1",
	resource: "github:acme/app#5",
	provider: "github",
	type: "pullRequest",
	session: "app:pr",
	agent: "dev",
	replyTo: null,
	preset: "review",
	until: null,
	priority: null,
	policy: null,
	...overrides,
});

/** The event a webhook emits, as the delivery workflow sees it. */
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
	return event && { name: event.name, payload: event.payload };
}

async function plan(
	hook: { event: string; payload: unknown },
	records: SubscriptionRecord[],
	policies = new Map(),
) {
	const event = (await emitted(hook))!;
	const matches = matchResources([github(options)], event.name, event.payload);
	return selectDeliveries(event.name, event.payload, matches, records, policies);
}

describe("pull request resources", () => {
	it("maps webhooks to events, resource keys, and notification kinds", async () => {
		const cases: [ReturnType<(typeof webhooks)[keyof typeof webhooks]>, string, string][] = [
			[webhooks.review("changes_requested"), "github.pullRequestReviewed", "review"],
			[webhooks.comment(), "github.pullRequestCommented", "comment"],
			[webhooks.reviewComment(), "github.pullRequestCommented", "comment"],
			[webhooks.closed(true), "github.pullRequestClosed", "merged"],
			[webhooks.closed(false), "github.pullRequestClosed", "closed"],
			[webhooks.reopened(), "github.pullRequestReopened", "reopened"],
			[webhooks.labeled(), "github.pullRequestLabeled", "label"],
			[webhooks.synchronize(), "github.pullRequestSynchronized", "commit"],
			[webhooks.checkRun(), "github.checkRunCompleted", "check"],
			[webhooks.checkSuite(), "github.checkSuiteCompleted", "check"],
		];
		for (const [hook, name, kind] of cases) {
			const event = (await emitted(hook))!;
			expect(event.name).toBe(name);
			const [match] = matchResources([github(options)], event.name, event.payload);
			expect(match).toMatchObject({ resources: ["github:acme/app#5"], notification: { kind } });
		}
		expect(pullRequest({ repo: "acme/app", number: 5 })).toEqual({
			kind: "fabrial.resource",
			provider: "github",
			type: "pullRequest",
			ref: { repo: "acme/app", number: 5 },
		});
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

	it("selects kinds by preset", async () => {
		expect(await plan(webhooks.labeled(), [record()])).toEqual([]);
		expect(await plan(webhooks.checkRun(), [record()])).toEqual([]);
		expect(await plan(webhooks.synchronize(), [record()])).toHaveLength(1);
		expect(await plan(webhooks.labeled(), [record({ preset: "working" })])).toHaveLength(1);
		expect(await plan(webhooks.checkRun(), [record({ preset: "working" })])).toHaveLength(1);
		expect(await plan(webhooks.comment(), [record({ resource: "github:acme/app#6" })])).toEqual([]);
	});

	it("accepts comments and reviews only from collaborators or the app's own bot", async () => {
		for (const association of ["OWNER", "MEMBER", "COLLABORATOR"])
			expect(await plan(webhooks.comment(association), [record()])).toHaveLength(1);
		for (const association of ["CONTRIBUTOR", "NONE", "FIRST_TIME_CONTRIBUTOR"]) {
			expect(await plan(webhooks.comment(association, outsider), [record()])).toEqual([]);
			expect(await plan(webhooks.review("approved", association, outsider), [record()])).toEqual(
				[],
			);
		}
		expect(await plan(webhooks.comment("NONE", bot), [record()])).toHaveLength(1);
		const other = { ...bot, id: 100 };
		expect(await plan(webhooks.comment("NONE", other), [record()])).toEqual([]);
		// Lifecycle events are not filtered by author.
		const merged = webhooks.closed(true);
		expect(
			await plan({ ...merged, payload: { ...merged.payload, sender: outsider } }, [record()]),
		).toHaveLength(1);
	});

	it("applies app policies after the plugin filter, and priority policy > declared > plugin", async () => {
		const policies = new Map([
			[
				"strict",
				{
					kind: "fabrial.subscription-policy" as const,
					name: "strict",
					filter: (event: { attributes: JsonObject }) => event.attributes.actor !== "alice",
					priority: () => undefined,
				},
			],
			[
				"urgent",
				{
					kind: "fabrial.subscription-policy" as const,
					name: "urgent",
					priority: (event: { kind: string; attributes: JsonObject }) =>
						event.kind === "review" && event.attributes.state === "changes_requested"
							? ("urgent" as const)
							: undefined,
				},
			],
		]);
		expect(await plan(webhooks.comment(), [record({ policy: "strict" })], policies)).toEqual([]);
		const [urgent] = await plan(
			webhooks.review("changes_requested"),
			[record({ policy: "urgent", priority: "low" })],
			policies,
		);
		expect(urgent!.priority).toBe("urgent");
		const [declared] = await plan(
			webhooks.review("approved"),
			[record({ policy: "urgent", priority: { review: "low" } })],
			policies,
		);
		expect(declared!.priority).toBe("low");
		const [plugin] = await plan(webhooks.labeled(), [record({ preset: "working" })]);
		expect(plugin!.priority).toBe("low");
		const [fallback] = await plan(webhooks.comment(), [record()]);
		expect(fallback!.priority).toBe("normal");
		// A removed policy delivers with plugin defaults and warns.
		const warn = vi.fn();
		const event = (await emitted(webhooks.comment()))!;
		const missing = selectDeliveries(
			event.name,
			event.payload,
			matchResources([github(options)], event.name, event.payload),
			[record({ policy: "gone" })],
			new Map(),
			{ warn, debug() {}, info() {}, error() {} },
		);
		expect(missing).toHaveLength(1);
		expect(warn).toHaveBeenCalledOnce();
	});

	it("ends on merge or close, not on reopen", async () => {
		const until = record({ until: "merged-or-closed" });
		expect((await plan(webhooks.closed(true), [until]))[0]!.ends).toBe(true);
		expect((await plan(webhooks.closed(false), [until]))[0]!.ends).toBe(true);
		expect((await plan(webhooks.reopened(), [until]))[0]!.ends).toBe(false);
		expect((await plan(webhooks.closed(true), [record()]))[0]!.ends).toBe(false);
	});
});

describe("dynamic subscriptions in an app", () => {
	async function setup() {
		const f = createFabrial({ plugins: [plugin()] });
		const agent = f.defineAgent({ name: "dev" });
		const policy = f.defineSubscriptionPolicy({
			name: "changes-urgent",
			priority: (event) =>
				event.kind === "review" && event.attributes.state === "changes_requested"
					? "urgent"
					: "normal",
		});
		const pr = pullRequest({ repo: "acme/app", number: 5 });
		const slackThread = { kind: "thread" as const, provider: "slack", threadId: "slack:C1:1" };
		const watch = f.defineWorkflow({
			name: "watch",
			async run(_input, ctx) {
				return ctx.subscribe("watch", pr, {
					to: { agent, session: "pr-5" },
					replyTo: slackThread,
					policy,
					until: "merged-or-closed",
				});
			},
		});
		const unwatch = f.defineWorkflow({
			name: "unwatch",
			async run(_input, ctx) {
				return ctx.unsubscribe("unwatch", pr, { to: { agent, session: "pr-5" } });
			},
		});
		const orphan = f.defineWorkflow({
			name: "orphan",
			async run(_input, ctx) {
				await ctx.subscribe("watch", pr);
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
		const store = new MemorySubscriptions();
		const app = testApp(f, {
			runtime,
			chat: new FakeChat(),
			agents,
			subscriptionStore: store,
			workflows: [watch, unwatch, orphan, owner],
		});
		await app.start();
		const start = async (workflow: string) => {
			const id = await runtime.invoke(workflow, null, {
				metadata: {
					interactionId: workflow,
					origin: null,
					replyTo: null,
					requestedBy: null,
					ownsThread: false,
				},
			});
			await runtime.flush();
			return runtime.result(id)!;
		};
		const send = async (hook: { event: string; payload: unknown }, delivery: string) => {
			expect((await app.fetch(deliver(hook, delivery))).status).toBe(200);
			await runtime.flush();
		};
		return { app, runtime, agents, store, start, send, observed, slackThread };
	}

	it("delivers matching events as notifications, once per delivery, until merged", async () => {
		const t = await setup();
		expect(await t.start("watch")).toMatchObject({
			status: "completed",
			output: { resource: "github:acme/app#5", session: "app:pr-5", agent: "dev" },
		});
		// Re-subscribing the same resource from the same conversation updates the record.
		await t.start("watch");
		expect(t.store.records.size).toBe(1);

		await t.send(webhooks.review("changes_requested"), "r1");
		await t.send(webhooks.review("changes_requested"), "r1");
		await t.send(webhooks.comment("NONE", outsider), "c1");
		expect(t.agents.sent).toHaveLength(1);
		const [review] = t.agents.sent;
		expect(review).toMatchObject({
			agent: "dev",
			session: "app:pr-5",
			delivery: "steer",
			metadata: { replyTo: t.slackThread, ownsThread: false, send: { priority: "urgent" } },
		});
		expect(review!.content).toContain('<notification source="github" kind="review"');
		expect(review!.content).toContain('resource="github:acme/app#5"');
		expect(review!.content).toContain("requested changes on acme/app#5");
		// Triggers on the same event still run; the subscription takes no ownership.
		expect(t.observed).toHaveBeenCalledOnce();

		await t.send(webhooks.comment(), "c2");
		expect(t.agents.sent.at(-1)).toMatchObject({ delivery: "followUp" });
		await t.send(webhooks.closed(true), "m1");
		expect(t.agents.sent.at(-1)!.content).toContain("was merged");
		expect(t.store.records.size).toBe(0);
		await t.send(webhooks.reopened(), "o1");
		expect(t.agents.sent).toHaveLength(3);
		await t.app.stop();
	});

	it("unsubscribes, and requires a target outside agent conversations", async () => {
		const t = await setup();
		await t.start("watch");
		expect(await t.start("unwatch")).toMatchObject({ output: { removed: true } });
		await t.send(webhooks.comment(), "c1");
		expect(t.agents.sent).toHaveLength(0);
		expect(await t.start("orphan")).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Pass `to: { agent, session }`"),
		});
		await t.app.stop();
	});
});
