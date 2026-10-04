import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	defineGroup,
	definePlugin,
	defineState,
	defineUser,
	trigger,
	weeklyRotation,
} from "../../src/index.ts";
import type {
	AnyWorkflow,
	ChatMessage,
	EventFilter,
	ExternalIdentity,
	InvocationMetadata,
	Json,
	JsonObject,
	WorkflowContext,
} from "../../src/index.ts";
import { FakeChat, fakeAgents, MemoryRuntime } from "fabrial/testing";
const { defineWorkflow } = createFabrial({ plugins: [] });

const identity = (subjectId: string): ExternalIdentity => ({
	provider: "chat",
	installationId: "workspace",
	subjectId,
});
const alice = defineUser({ id: "alice", name: "Alice", identities: [identity("A")] });
const bob = defineUser({ id: "bob", name: "Bob", identities: [identity("B")] });
const outsider = defineUser({ id: "outsider", identities: [identity("O")] });
const team = defineGroup({ id: "team", members: [alice, bob] });
const surface = { kind: "thread", provider: "chat", threadId: "channel:general:thread" } as const;
const event = "chat.message";
const message = (id: string, author = alice, text = id): ChatMessage => ({
	provider: "chat",
	threadId: surface.threadId,
	channelId: "general",
	messageId: id,
	text,
	author: {
		identity: author.identities[0]! as ExternalIdentity & JsonObject,
		name: author.name ?? null,
		isBot: false,
	},
	isMention: true,
	isDM: false,
	sentAt: "2026-01-05T00:00:00Z",
});
const plugin = definePlugin({
	id: "chat",
	events: {
		message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
	},
});
function setup(
	workflows: AnyWorkflow[],
	extra: Partial<import("../../src/app.ts").CoreConfig> = {},
) {
	const runtime = new MemoryRuntime();
	const chat = new FakeChat(() => runtime.now());
	const config = {
		runtime,
		chat,
		plugins: [plugin],
		workflows,
		identity: [alice, bob, outsider, team],
		...extra,
	};
	const { plugins, identity: identities, logger, ...appConfig } = config;
	const app = createFabrial({ plugins, identity: identities, logger }).app(appConfig);
	return { runtime, chat, app };
}
const workflow = (
	run: (input: Json, ctx: WorkflowContext) => Promise<Json | void>,
	name = "owner",
) => defineWorkflow({ name, triggers: [trigger({ event })], run });
const metadata: InvocationMetadata = {
	interactionId: "test",
	origin: null,
	replyTo: null,
	requestedBy: null,
	ownsThread: false,
};

async function enter(test: ReturnType<typeof setup>) {
	await test.app.start();
	await test.chat.receive(message("initial"));
	await test.runtime.flush();
}
async function card(test: ReturnType<typeof setup>) {
	const dm = await test.chat.openDM(alice.identities[0]!);
	const post = dm.posts[0]!;
	if (typeof post.content !== "object" || !("card" in post.content))
		throw new Error("Missing card");
	return {
		post,
		approve: post.content.card.actions![0]!.id,
		reject: post.content.card.actions![1]!.id,
	};
}
const approvalWorkflow = (timeout: string | number = "24h", requesterControls = true) =>
	workflow(async (_input, ctx) => {
		const decision = await ctx.waitForApproval("approval", {
			title: "Run SQL?",
			details: "select 1",
			approvers: team,
			timeout,
			requesterControls,
		});
		return decision.status;
	});

describe("routing and lifecycle", () => {
	it("selects specificity, merges triggers, runs observers without a thread and dedupes ingress", async () => {
		const seen: string[] = [];
		const broad = workflow(async () => {
			seen.push("broad");
		});
		const specific = defineWorkflow({
			name: "specific",
			triggers: [
				trigger({ event, filter: { channelId: ["general"] } }),
				trigger({ event, filter: { channelId: ["general"] } }),
			],
			run: async (_input, ctx) => {
				expect(ctx.thread).toBeDefined();
				seen.push("specific");
			},
		});
		const observer = defineWorkflow({
			name: "observer",
			triggers: [trigger({ event, observe: true })],
			run: async (_input, ctx) => {
				expect(ctx.thread).toBeUndefined();
				seen.push("observer");
			},
		});
		const test = setup([broad, specific, observer]);
		await enter(test);
		await test.chat.receive(message("initial"));
		await test.runtime.flush();
		expect(seen.sort()).toEqual(["observer", "specific"]);
	});
	it("honors explicit specificity", async () => {
		const a = workflow(async () => "broad");
		const b = defineWorkflow({
			name: "explicit",
			triggers: [trigger({ event, specificity: 10 })],
			run: async () => "explicit",
		});
		const test = setup([a, b]);
		await enter(test);
		expect(test.runtime.executions("explicit")).toHaveLength(1);
		expect(test.runtime.executions("owner")).toHaveLength(0);
	});
	it.each([
		[{}, {}],
		[{ channelId: ["general"] }, { channelId: [{ prefix: "gen" }] }],
		[
			{ count: [{ numeric: [">", 5] as [string, number] }] },
			{ count: [{ numeric: ["<", 10] as [string, number] }] },
		],
	])("rejects overlapping equal-specificity owners", async (a, b) => {
		const make = (name: string, filter: EventFilter) =>
			defineWorkflow({
				name,
				triggers: [trigger({ event, filter, specificity: 2 })],
				run: async () => {},
			});
		await expect(setup([make("a", a), make("b", b)]).app.start()).rejects.toThrow("Ambiguous");
	});
	it("allows disjoint filters and overlapping triggers on the same workflow", async () => {
		const make = (name: string, channel: string) =>
			defineWorkflow({
				name,
				triggers: [
					trigger({ event, filter: { channelId: [channel] } }),
					trigger({ event, filter: { channelId: [channel] } }),
				],
				run: async () => {},
			});
		await expect(setup([make("a", "a"), make("b", "b")]).app.start()).resolves.toBeUndefined();
	});
	it("dispatches plugin and chat routes, prefixes route emits, and returns 404", async () => {
		const routes = definePlugin({
			id: "source",
			events: { ping: defineEvent({ payload: z.object({ value: z.string() }) }) },
			routes: {
				"POST /ping": async (_request, ctx) => {
					await ctx.emit("ping", { value: "ok" }, { id: "ping" });
					return new Response("ok");
				},
			},
		});
		const test = setup([], { plugins: [routes] });
		test.chat.routes["GET /chat"] = async () => new Response("chat");
		await test.app.start();
		expect(
			await (await test.app.fetch(new Request("https://test/ping", { method: "POST" }))).text(),
		).toBe("ok");
		expect(test.runtime.emitted[0]!.name).toBe("source.ping");
		expect(await (await test.app.fetch(new Request("https://test/chat"))).text()).toBe("chat");
		expect((await test.app.fetch(new Request("https://test/missing"))).status).toBe(404);
	});
	it("ignores bot messages and clears finished interactions", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		const bot = message("bot");
		bot.author.isBot = true;
		expect(await test.chat.receive(bot)).toBe("ignored");
		await test.chat.receive(message("human"));
		await test.runtime.flush();
		expect(await (await test.chat.thread(surface)).getState()).toMatchObject({
			interactionId: null,
			handlerExecutionId: null,
			bufferedReplies: [],
		});
		expect(test.runtime.emitted.filter((e) => e.name === "fabrial.execution.settled")).toHaveLength(
			1,
		);
	});
});

describe("durability and context", () => {
	it("suffixes repeated ids and memoizes effects across replay", async () => {
		let effects = 0;
		const test = setup([
			workflow(async (_input, ctx) => {
				for (let i = 0; i < 3; i++) await ctx.step("x", () => ++effects);
				const ref = await ctx.thread!.post("post", "hello");
				await ctx.thread!.update("update", ref, "updated");
				await ctx.sleep("sleep", "1h");
				return effects;
			}),
		]);
		await enter(test);
		const id = test.runtime.executions()[0]!.executionId;
		await test.runtime.advanceBy(3600000);
		expect(effects).toBe(3);
		expect(test.runtime.stepIds(id)).toEqual(expect.arrayContaining(["x", "x#1", "x#2"]));
		const posts = (await test.chat.thread(surface)).posts;
		expect(posts).toHaveLength(1);
		expect(posts[0]!.updates).toEqual(["updated"]);
	});
	it("rejects empty operation ids", async () => {
		const test = setup([workflow(async (_i, ctx) => ctx.step("", () => 1))]);
		await enter(test);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			status: "failed",
			error: expect.stringContaining("explicit id"),
		});
	});
	it("runs middleware in plugin order on every resume", async () => {
		const seen: string[] = [];
		const p = (id: string) =>
			definePlugin({
				id,
				hooks: {
					workflow: [
						async (_execution, ctx, next) => {
							seen.push(`${id}:before`);
							try {
								return await next(ctx);
							} finally {
								seen.push(`${id}:after`);
							}
						},
					],
				},
			});
		const test = setup(
			[
				workflow(async (_i, ctx) => {
					await ctx.sleep("pause", 1);
				}),
			],
			{ plugins: [p("a"), p("b")] },
		);
		await enter(test);
		await test.runtime.advanceBy(1);
		expect(seen).toEqual([
			"a:before",
			"b:before",
			"b:after",
			"a:after",
			"a:before",
			"b:before",
			"b:after",
			"a:after",
		]);
	});
	it("memoizes evaluation, agents, and shared state", async () => {
		let evaluations = 0;
		const agents = fakeAgents({
			evaluate: async (request) => {
				evaluations++;
				return { stopReason: "stop", model: request.model, answers: {} };
			},
		});
		const counter = defineState({
			name: "count",
			scope: "thread",
			schema: z.object({ count: z.number() }),
			initial: () => ({ count: 0 }),
		});
		const test = setup(
			[
				workflow(async (_i, ctx) => {
					await ctx.state(counter).update("count", (draft) => {
						draft.count++;
					});
					await ctx.evaluate("evaluate", {
						model: { provider: "test", modelId: "test" },
						state: {},
						questions: {},
					});
					const answer = await ctx.agent(
						"agent",
						{ kind: "fabrial.agent", name: "echo" },
						{ input: "echo" },
					);
					await ctx.sleep("sleep", 10);
					return { answer, count: (await ctx.state(counter).get("read")).count };
				}),
			],
			{ agents },
		);
		await enter(test);
		await test.runtime.advanceBy(10);
		expect(evaluations).toBe(1);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			output: { answer: "echo", count: 1 },
		});
	});
	it("loads bounded history durably and asks once", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const history = await ctx.thread!.history("history", { limit: 1 });
				const answer = await ctx.thread!.ask("question", "Question?", { timeout: 100 });
				return { history: history.length, answer: answer!.text };
			}),
		]);
		await enter(test);
		await test.chat.receive(message("answer"));
		await test.runtime.flush();
		expect((await test.chat.thread(surface)).posts).toHaveLength(1);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			output: { history: 1, answer: "answer" },
		});
	});
});

describe("replies and races", () => {
	it("consumes buffered replies, restricts from, and never consumes a message twice", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.sleep("pause", 10);
				const from = await ctx.thread!.waitForReply("from", { from: ctx.actor! });
				const other = await ctx.thread!.waitForReply("other");
				const noDuplicate = await ctx.thread!.waitForReply("third", { timeout: 10 });
				return [from!.messageId, other!.messageId, noDuplicate];
			}),
		]);
		await enter(test);
		await test.chat.receive(message("bob", bob));
		await test.chat.receive(message("alice"));
		await test.chat.receive(message("alice"));
		await test.runtime.advanceBy(10);
		await test.runtime.advanceBy(10);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			status: "completed",
			output: ["alice", "bob", null],
		});
	});
	it("filters a live reply by from", async () => {
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(await ctx.thread!.waitForReply("reply", { from: ctx.actor!, timeout: 100 }))!.text,
			),
		]);
		await enter(test);
		await test.chat.receive(message("bob", bob));
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.status).toBe("suspended");
		await test.chat.receive(message("alice"));
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "alice" });
	});
	it.each(["reply", "timer"])("races replies and timers: %s wins", async (winner) => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const result = await ctx.race("race", {
					reply: ctx.thread!.nextReply(),
					timer: ctx.timer(100),
				});
				return result.key;
			}),
		]);
		await enter(test);
		if (winner === "reply") {
			await test.chat.receive(message("answer"));
			await test.runtime.flush();
		} else await test.runtime.advanceBy(100);
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: winner });
	});
	it("asserts reply timeouts below thread TTL", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.thread!.waitForReply("reply", { timeout: "30d" });
			}),
		]);
		await enter(test);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			status: "failed",
			error: expect.stringContaining("30 days"),
		});
	});
});

describe("approvals", () => {
	it.each(["approved", "rejected"])("handles %s and duplicate clicks", async (status) => {
		const test = setup([approvalWorkflow()]);
		await enter(test);
		const c = await card(test);
		const action = status === "approved" ? c.approve : c.reject;
		await test.chat.click(c.post.ref, action, alice.identities[0]!, { dedupeId: "click" });
		await test.chat.click(c.post.ref, action, alice.identities[0]!, { dedupeId: "click" });
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: status });
		expect(test.runtime.emitted.filter((e) => e.name === "fabrial.approval.decided")).toHaveLength(
			1,
		);
		expect(c.post.updates).toHaveLength(1);
	});
	it("expires and renders cards without actions", async () => {
		const test = setup([approvalWorkflow(100)]);
		await enter(test);
		const c = await card(test);
		await test.runtime.advanceBy(100);
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "expired" });
		expect(c.post.content).toMatchObject({ card: { text: "Expired", actions: [] } });
	});
	it("rejects non-approvers ephemerally and continues waiting with a new wait id", async () => {
		const test = setup([approvalWorkflow()]);
		await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await test.runtime.flush();
		expect(test.chat.ephemeral).toHaveLength(1);
		expect(test.runtime.executions()[0]!.status).toBe("suspended");
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "approved" });
	});
	it("requires assigned approvers to still be members", async () => {
		let members = [alice, bob];
		const rotation = defineGroup({ id: "rotation", resolve: () => members });
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(
						await ctx.waitForApproval("a", {
							title: "Approval",
							details: "x",
							approvers: rotation,
							timeout: 100,
						})
					).status,
			),
		]);
		await enter(test);
		const c = await card(test);
		members = [bob];
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.chat.ephemeral).toHaveLength(1);
		await test.runtime.advanceBy(100);
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "expired" });
	});
	it("allows only the requester to cancel using the status control", async () => {
		const test = setup([approvalWorkflow()]);
		await enter(test);
		const post = (await test.chat.thread(surface)).posts[0]!;
		const action = (post.content as { card: { actions: { id: string }[] } }).card.actions[0]!.id;
		await test.chat.click(post.ref, action, bob.identities[0]!);
		await test.runtime.flush();
		expect(test.chat.ephemeral).toHaveLength(1);
		await test.chat.click(post.ref, action, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "cancelled" });
	});
	it("disables requester controls", async () => {
		const test = setup([approvalWorkflow(100, false)]);
		await enter(test);
		expect((await test.chat.thread(surface)).posts[0]!.content).toMatchObject({
			card: { actions: [] },
		});
	});
	it("supports handle.cancel after a reply wins and renders Superseded", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const handle = await ctx.requestApproval("approval", {
					title: "Approval",
					details: "x",
					approvers: team,
				});
				const winner = await ctx.race("race", {
					decision: handle.decision(),
					reply: ctx.thread!.nextReply(),
				});
				if (winner.key === "reply") await handle.cancel("cancel", "superseded");
				return (await handle.wait("decision")).status;
			}),
		]);
		await enter(test);
		const c = await card(test);
		await test.chat.receive(message("amend"));
		await test.runtime.flush();
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "cancelled" });
		expect(c.post.content).toMatchObject({ card: { text: "Superseded" } });
	});
	it("cancellation of a suspended workflow updates pending cards", async () => {
		const test = setup([approvalWorkflow()]);
		await enter(test);
		const c = await card(test);
		await test.runtime.cancel(test.runtime.executions()[0]!.executionId);
		await test.runtime.flush();
		expect(c.post.content).toMatchObject({ card: { text: "Cancelled", actions: [] } });
		expect(await (await test.chat.thread(surface)).getState()).toMatchObject({
			interactionId: null,
			handlerExecutionId: null,
			bufferedReplies: [],
		});
	});
	it("asserts approval timeouts below 30 days", async () => {
		const test = setup([approvalWorkflow("30d")]);
		await enter(test);
		expect(test.runtime.executions()[0]!.result).toMatchObject({
			status: "failed",
			error: expect.stringContaining("30 days"),
		});
	});
});

describe("children, access and identity", () => {
	it("invokes children with metadata, validates schemas, and starts independently", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({ value: z.number() }),
			run: async (input, ctx) => {
				expect(ctx.actor?.id).toBe("alice");
				expect(ctx.metadata.ownsThread).toBe(false);
				return input.value * 2;
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				const result = await ctx.invoke("child", child, { value: 3 });
				const executionId = await ctx.start("background", child, { value: 4 });
				return { result, executionId };
			}),
			child,
		]);
		await enter(test);
		expect(test.runtime.executions("child")).toHaveLength(2);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: { result: 6 } });
	});
	it.each(["invoke", "start", "handoff"] as const)(
		"validates %s input before starting",
		async (method) => {
			const child = defineWorkflow({
				name: "child",
				input: z.object({ value: z.number() }),
				run: async () => {},
			});
			const test = setup([
				workflow(async (_i, ctx) => {
					await ctx[method]("call", child, { value: "bad" } as never);
				}),
				child,
			]);
			await enter(test);
			expect(test.runtime.executions("child")).toHaveLength(0);
			expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
				status: "failed",
				error: expect.stringContaining("Invalid child input"),
			});
		},
	);
	it("handoff transfers thread ownership and is independent of the caller", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				expect(ctx.metadata.ownsThread).toBe(true);
				return (await ctx.thread!.waitForReply("reply"))!.text;
			},
		});
		const test = setup([workflow(async (_i, ctx) => ctx.handoff("handoff", child, {})), child]);
		await enter(test);
		const childId = test.runtime.executions("child")[0]!.executionId;
		expect((await (await test.chat.thread(surface)).getState())?.handlerExecutionId).toBe(childId);
		await test.chat.receive(message("answer"));
		await test.runtime.flush();
		expect(test.runtime.result(childId)).toMatchObject({ output: "answer" });
		expect(await (await test.chat.thread(surface)).getState()).toMatchObject({
			interactionId: null,
			handlerExecutionId: null,
			bufferedReplies: [],
		});
	});
	it.each([false, true])("structured cancellation, detached=%s", async (detached) => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", 100);
				return "done";
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => ctx.invoke("invoke", child, {}, { detached })),
			child,
		]);
		await enter(test);
		await test.runtime.cancel(test.runtime.executions("owner")[0]!.executionId);
		const childId = test.runtime.executions("child")[0]!.executionId;
		if (detached) {
			expect(test.runtime.result(childId)).toBeUndefined();
			await test.runtime.advanceBy(100);
			expect(test.runtime.result(childId)).toMatchObject({ status: "completed" });
		} else expect(test.runtime.result(childId)).toMatchObject({ status: "cancelled" });
	});
	it("checks access.invoke against the actor", async () => {
		const restricted = defineWorkflow({
			name: "restricted",
			input: z.object({}),
			access: { invoke: defineGroup({ id: "only-bob", members: [bob] }) },
			run: async () => "secret",
		});
		const test = setup([
			workflow(async (_i, ctx) => ctx.invoke("restricted", restricted, {})),
			restricted,
		]);
		await enter(test);
		expect(test.runtime.executions("restricted")).toHaveLength(0);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "failed",
			error: "Not authorized to invoke restricted",
		});
	});
	it("resolves known and unknown identities, static/dynamic groups and names", async () => {
		const lookup = definePlugin({
			id: "chat",
			identity: { lookup: async () => ({ name: "Provider name" }) },
		});
		const test = setup([], { plugins: [lookup] });
		expect(await test.app.host.resolvePrincipal(identity("A"))).toMatchObject({
			id: "alice",
			known: true,
			name: "Alice",
		});
		expect(await test.app.host.resolvePrincipal(identity("unknown"))).toMatchObject({
			id: "chat:workspace:unknown",
			known: false,
			name: "Provider name",
		});
		expect(await test.app.host.directory.identitiesFor("alice")).toEqual(alice.identities);
		expect((await test.app.host.directory.members(team)).map((p) => p.id)).toEqual([
			"alice",
			"bob",
		]);
		const rotation = defineGroup({
			id: "rotation",
			resolve: ({ now }) => [weeklyRotation([alice, bob], { start: "2026-01-05", now })],
		});
		expect((await test.app.host.directory.members(rotation))[0]!.id).toBe("alice");
		await test.runtime.advanceBy(7 * 86400000);
		expect((await test.app.host.directory.members(rotation))[0]!.id).toBe("bob");
		expect(weeklyRotation([alice, bob], { start: "2026-01-05", now: new Date("2025-12-29") })).toBe(
			bob,
		);
	});
	it("memoizes child start ids and enforces a strict mutex", async () => {
		const runtime = new MemoryRuntime();
		let active = 0;
		let max = 0;
		runtime.register({
			events: [],
			workflows: [
				{
					name: "locked",
					triggers: [],
					mutex: () => "key",
					handler: async (_input, execution) => {
						await execution.step("work", async () => {
							active++;
							max = Math.max(max, active);
							await Promise.resolve();
							active--;
						});
					},
				},
			],
		});
		await runtime.start();
		await Promise.all(Array.from({ length: 3 }, () => runtime.invoke("locked", {}, { metadata })));
		await runtime.flush();
		expect(max).toBe(1);
		expect(runtime.executions().every((e) => e.result?.status === "completed")).toBe(true);
	});
});

describe("multi-category chat ingress", () => {
	it("selects one owner across categories and merges observers, including the owner", async () => {
		const seen: { workflow: string; thread: boolean }[] = [];
		const broad = workflow(async () => {
			seen.push({ workflow: "broad", thread: true });
		});
		const specific = defineWorkflow({
			name: "specific",
			triggers: [
				trigger({ event: "chat.mentioned", specificity: 2 }),
				trigger({ event, observe: true }),
			],
			run: async (_i, ctx) => {
				seen.push({ workflow: "specific", thread: !!ctx.thread });
				await ctx.sleep("pause", 10);
			},
		});
		const observer = defineWorkflow({
			name: "observer",
			triggers: [
				trigger({ event, observe: true }),
				trigger({ event: "chat.mentioned", observe: true }),
			],
			run: async (_i, ctx) => {
				seen.push({ workflow: "observer", thread: !!ctx.thread });
			},
		});
		const test = setup([broad, specific, observer]);
		await test.app.start();
		await test.chat.receive(message("multi"), { events: [event, "chat.mentioned"] });
		await test.runtime.flush();
		expect(seen).toEqual(
			expect.arrayContaining([
				{ workflow: "specific", thread: true },
				{ workflow: "observer", thread: false },
			]),
		);
		expect(seen).toHaveLength(2);
		expect(test.runtime.executions("specific")).toHaveLength(1);
		expect(test.runtime.executions("observer")).toHaveLength(1);
		await test.chat.receive(message("reply", bob), { events: [event, "chat.mentioned"] });
		expect(
			(await (await test.chat.thread(surface)).getState())?.bufferedReplies.map((m) => m.messageId),
		).toEqual(["reply"]);
	});
	it("detects dynamic cross-category ownership ties before emitting", async () => {
		const a = workflow(async () => {});
		const b = defineWorkflow({
			name: "other",
			triggers: [trigger({ event: "chat.mentioned" })],
			run: async () => {},
		});
		const test = setup([a, b]);
		await test.app.start();
		await expect(
			test.chat.receive(message("tie"), { events: [event, "chat.mentioned"] }),
		).rejects.toThrow("Ambiguous owner");
		expect(test.runtime.emitted).toHaveLength(0);
	});
	it("dedupes all categories across redelivery after completion", async () => {
		const observer = defineWorkflow({
			name: "observer",
			triggers: [
				trigger({ event, observe: true }),
				trigger({ event: "chat.mentioned", observe: true }),
			],
			run: async () => {},
		});
		const test = setup([workflow(async () => {}), observer]);
		await test.app.start();
		for (let i = 0; i < 2; i++) {
			await test.chat.receive(message("multi"), { events: [event, "chat.mentioned"] });
			await test.runtime.flush();
		}
		expect(test.runtime.executions("owner")).toHaveLength(1);
		expect(test.runtime.executions("observer")).toHaveLength(1);
	});
});

describe("lazy channel threads and cancellation ingress", () => {
	it("rebinds first-post receipts on replay, transfers real refs to children, and clears state", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.thread!.post("child", "child");
				return ctx.thread!.ref.threadId;
			},
		});
		const owner = defineWorkflow({
			name: "owner",
			triggers: [
				trigger({
					event: "source.ping",
					replyTo: { kind: "channel", provider: "chat", channelId: "lazy" },
				}),
			],
			run: async (_i, ctx) => {
				const first = await ctx.thread!.post("first", "first");
				expect(ctx.thread!.ref.threadId).toBe(first.threadId);
				await ctx.sleep("sleep", 10);
				await ctx.thread!.update("update", first, "updated");
				await ctx.thread!.post("second", "second");
				return ctx.invoke("child", child, {});
			},
		});
		const test = setup([owner, child]);
		await test.app.start();
		await test.app.emit("source.ping", {});
		await test.runtime.flush();
		expect(test.chat.threads.size).toBe(1);
		const thread = [...test.chat.threads.values()][0]!;
		expect((await thread.getState())?.handlerExecutionId).toBe(
			test.runtime.executions("owner")[0]!.executionId,
		);
		await test.runtime.advanceBy(10);
		expect(test.chat.threads.size).toBe(1);
		expect(thread.posts.map((p) => p.content)).toEqual(["updated", "second", "child"]);
		expect(test.runtime.executions("child")[0]!.metadata.replyTo).toEqual(thread.ref);
		expect(await thread.getState()).toBeNull();
	});
	it("does not create a channel thread without a post", async () => {
		const test = setup([
			defineWorkflow({
				name: "owner",
				triggers: [
					trigger({ event, replyTo: { kind: "channel", provider: "chat", channelId: "lazy" } }),
				],
				run: async (_i, ctx) => {
					await ctx.sleep("sleep", 10);
				},
			}),
		]);
		await test.app.start();
		await test.app.emit(event, {});
		await test.runtime.flush();
		expect(test.chat.threads.size).toBe(0);
		await test.runtime.advanceBy(10);
		expect(test.chat.threads.size).toBe(0);
	});
	it.each(["requester", "participant"])(
		"allows the %s to stop, rejects outsiders, and ignores duplicates",
		async (actor) => {
			const test = setup([
				workflow(async (_i, ctx) => {
					await ctx.sleep("sleep", 100);
				}),
			]);
			await enter(test);
			await test.chat.cancel(surface, outsider.identities[0]!);
			expect(test.chat.ephemeral).toHaveLength(1);
			expect(test.runtime.executions("owner")[0]!.result).toBeUndefined();
			if (actor === "participant") await test.chat.receive(message("participant", bob));
			const identity = actor === "participant" ? bob.identities[0]! : alice.identities[0]!;
			await test.chat.cancel(surface, identity, "stop");
			await test.chat.cancel(surface, identity, "stop");
			await test.runtime.flush();
			expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ status: "cancelled" });
			expect(await (await test.chat.thread(surface)).getState()).toMatchObject({
				interactionId: null,
				handlerExecutionId: null,
				bufferedReplies: [],
			});
		},
	);
	it("calls chat, agent and plugin lifecycle callbacks", async () => {
		const calls: string[] = [];
		const runtime = new MemoryRuntime();
		const chat = new FakeChat(() => runtime.now());
		const agents = fakeAgents();
		const app = createFabrial({
			plugins: [
				definePlugin({
					id: "lifecycle",
					init: () => {
						calls.push("init");
					},
					shutdown: () => {
						calls.push("shutdown");
					},
				}),
			],
		}).app({
			runtime,
			workflows: [],
			chat: {
				kind: "fabrial.chat",
				connect: (host) => ({
					...chat.connect(host),
					start: async () => {
						calls.push("chat:start");
					},
					stop: async () => {
						calls.push("chat:stop");
					},
				}),
			},
			agents: {
				kind: "fabrial.agents",
				connect: (host) => ({
					...agents.connect(host),
					start: async () => {
						calls.push("agents:start");
					},
					stop: async () => {
						calls.push("agents:stop");
					},
				}),
			},
		});
		await app.start();
		await app.stop();
		expect(calls).toEqual([
			"init",
			"chat:start",
			"agents:start",
			"agents:stop",
			"chat:stop",
			"shutdown",
		]);
	});
});

describe("approval edge cases", () => {
	it("accepts a click emitted during card delivery, before the wait registers", async () => {
		const test = setup([approvalWorkflow()]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = dm.post.bind(dm);
		dm.post = async (content) => {
			const ref = await post(content);
			if (typeof content === "object" && "card" in content)
				await test.chat.click(ref, content.card.actions![0]!.id, alice.identities[0]!);
			return ref;
		};
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "approved" });
	});
	it("free-text replies do not decide or cancel approvals and remain available afterwards", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const approval = await ctx.waitForApproval("approval", {
					title: "Proposal",
					details: {},
					approvers: team,
				});
				return { status: approval.status, reply: (await ctx.thread!.waitForReply("reply"))!.text };
			}),
		]);
		await enter(test);
		const c = await card(test);
		await test.chat.receive(message("never mind"));
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toBeUndefined();
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			output: { status: "approved", reply: "never mind" },
		});
	});
	it("keeps assigned approvers fixed through replay and excludes newly rotated-in members", async () => {
		let members = [alice];
		const group = defineGroup({ id: "rotation", resolve: () => members });
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(
						await ctx.waitForApproval("approval", {
							title: "Proposal",
							details: {},
							approvers: group,
							timeout: 100,
						})
					).status,
			),
		]);
		await enter(test);
		const c = await card(test);
		members = [alice, bob];
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		await test.runtime.flush();
		expect(test.chat.ephemeral).toHaveLength(1);
		expect((await test.chat.openDM(bob.identities[0]!)).posts).toHaveLength(0);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "approved" });
	});
	it("does not extend expiry after invalid decisions and ignores late clicks", async () => {
		const test = setup([approvalWorkflow(100)]);
		await enter(test);
		const c = await card(test);
		await test.runtime.advanceBy(90);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await test.runtime.flush();
		await test.runtime.advanceBy(10);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "expired" });
		expect(c.post.updates).toHaveLength(1);
	});
	it("returns the same decision on repeated handle waits and does not cancel it afterwards", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const handle = await ctx.requestApproval("approval", {
					title: "Proposal",
					details: {},
					approvers: team,
				});
				const first = await handle.wait("first");
				const second = await handle.wait("second");
				await handle.cancel("cancel", "superseded");
				await ctx.sleep("after", 10);
				return [first.status, second.status];
			}),
		]);
		await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		await test.runtime.advanceBy(10);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			output: ["approved", "approved"],
		});
		expect(c.post.updates).toHaveLength(1);
	});
	it("updates every approver card and originating status exactly once across later replay", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.waitForApproval("approval", { title: "Proposal", details: {}, approvers: team });
				await ctx.sleep("after", 10);
			}),
		]);
		await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { dedupeId: "one" });
		await test.runtime.flush();
		await test.chat.click(c.post.ref, c.reject, bob.identities[0]!, { dedupeId: "different" });
		await test.runtime.advanceBy(10);
		for (const thread of test.chat.threads.values())
			for (const post of thread.posts) {
				expect(post.updates).toHaveLength(1);
				expect(post.content).toMatchObject({ card: { text: "Approved by Alice", actions: [] } });
			}
	});
	it("updates cards when a workflow fails or abandons an unawaited approval", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.requestApproval("approval", { title: "Proposal", details: {}, approvers: team });
				throw new Error("failed");
			}),
		]);
		await enter(test);
		const c = await test.chat.openDM(alice.identities[0]!);
		expect(c.posts[0]!.content).toMatchObject({ card: { text: "Cancelled", actions: [] } });
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ status: "failed" });
	});
});

describe("runtime fidelity", () => {
	it("records successful steps across retry after a later failure", async () => {
		let attempts = 0;
		let effects = 0;
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event })],
			retries: { maxAttempts: 2 },
			run: async (_i, ctx) => {
				await ctx.step("effect", () => ++effects);
				if (++attempts === 1) throw new Error("retry");
				return effects;
			},
		});
		const test = setup([owner]);
		await enter(test);
		expect(effects).toBe(1);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "completed",
			output: 1,
		});
	});
	it("redelivers failed settlement hooks without replaying terminal handlers", async () => {
		const runtime = new MemoryRuntime();
		let handlers = 0;
		let deliveries = 0;
		runtime.register({
			events: [],
			workflows: [
				{
					name: "owner",
					triggers: [],
					handler: async () => {
						handlers++;
					},
					onSettled: async () => {
						if (++deliveries === 1) throw new Error("network");
					},
				},
			],
		});
		await runtime.start();
		await runtime.invoke("owner", {}, { metadata });
		await expect(runtime.flush()).rejects.toThrow("network");
		await runtime.flush();
		expect(handlers).toBe(1);
		expect(deliveries).toBe(2);
	});
	it("start survives parent cancellation, while recursive invokes do not", async () => {
		const grandchild = defineWorkflow({
			name: "grandchild",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", 100);
			},
		});
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => ctx.invoke("grandchild", grandchild, {}),
		});
		const background = defineWorkflow({
			name: "background",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", 100);
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.start("background", background, {});
				return ctx.invoke("child", child, {});
			}),
			child,
			grandchild,
			background,
		]);
		await enter(test);
		await test.runtime.cancel(test.runtime.executions("owner")[0]!.executionId);
		await test.runtime.flush();
		expect(test.runtime.executions("child")[0]!.result?.status).toBe("cancelled");
		expect(test.runtime.executions("grandchild")[0]!.result?.status).toBe("cancelled");
		expect(test.runtime.executions("background")[0]!.result).toBeUndefined();
		await test.runtime.advanceBy(100);
		expect(test.runtime.executions("background")[0]!.result?.status).toBe("completed");
	});
	it("initializes state once and validates initial values", async () => {
		let initializations = 0;
		const state = defineState({
			name: "state",
			scope: "interaction",
			schema: z.object({ count: z.number() }),
			initial: () => ({ count: ++initializations }),
		});
		const test = setup(
			[
				workflow(async (_i, ctx) => [
					(await ctx.state(state).get("one")).count,
					(await ctx.state(state).get("two")).count,
				]),
			],
			{ agents: fakeAgents() },
		);
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: [1, 1] });
	});
	it("does not rerun access checks for an already authorized child", async () => {
		let members = [alice];
		const access = defineGroup({ id: "access", resolve: () => members });
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			access: { invoke: access },
			run: async (_i, ctx) => {
				await ctx.sleep("pause", 100);
				return "done";
			},
		});
		const test = setup([workflow(async (_i, ctx) => ctx.invoke("child", child, {})), child]);
		await enter(test);
		members = [];
		await test.runtime.advanceBy(100);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "done" });
	});
});

describe("additional port and correlation guarantees", () => {
	it("finishes an in-flight card step then cancels and cleans up its receipt", async () => {
		const test = setup([approvalWorkflow()]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = dm.post.bind(dm);
		dm.post = async (content) => {
			const receipt = await post(content);
			await test.runtime.cancel(test.runtime.executions("owner")[0]!.executionId);
			return receipt;
		};
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.result?.status).toBe("cancelled");
		expect(dm.posts[0]!.content).toMatchObject({ card: { text: "Cancelled", actions: [] } });
	});
	it("honors scalar filter operators with AND fields and OR alternatives", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [
				trigger({
					event: "source.item",
					filter: {
						level: ["error", "fatal"],
						path: [{ prefix: "api/" }],
						size: [{ numeric: [">=", 5, "<", 10] }],
						optional: [{ exists: false }],
						enabled: [{ "anything-but": false }],
					},
				}),
			],
			run: async () => "matched",
		});
		const test = setup([owner]);
		await test.app.start();
		await test.app.emit("source.item", { level: "error", path: "api/x", size: 6, enabled: true });
		await test.app.emit("source.item", { level: "warning", path: "api/x", size: 6, enabled: true });
		await test.app.emit("source.item", { level: "error", path: "api/x", size: 6, enabled: {} });
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(1);
	});
	it("enforces workflow concurrency without a mutex", async () => {
		const runtime = new MemoryRuntime();
		let active = 0;
		let maximum = 0;
		runtime.register({
			events: [],
			workflows: [
				{
					name: "limited",
					triggers: [],
					concurrency: 1,
					handler: async (_i, execution) => {
						await execution.step("work", async () => {
							active++;
							maximum = Math.max(maximum, active);
							await Promise.resolve();
							active--;
						});
					},
				},
			],
		});
		await runtime.start();
		for (let i = 0; i < 3; i++) await runtime.invoke("limited", {}, { metadata });
		await runtime.flush();
		expect(maximum).toBe(1);
	});
	it("rejects unknown child waits and invalid timers", async () => {
		const runtime = new MemoryRuntime();
		runtime.register({
			events: [],
			workflows: [
				{
					name: "owner",
					triggers: [],
					handler: async (_i, execution) => {
						await execution.waitForAny("wait", {
							child: { kind: "execution", executionId: "missing" },
						});
					},
				},
			],
		});
		await runtime.start();
		const id = await runtime.invoke("owner", {}, { metadata });
		await runtime.flush();
		expect(runtime.result(id)).toMatchObject({
			status: "failed",
			error: "Unknown execution: missing",
		});
	});
	it("registers agent port workflows once and wraps their middleware and terminal settlement", async () => {
		const seen: string[] = [];
		const agents = fakeAgents({
			workflows: [{ name: "agent-run", triggers: [], handler: async () => "agent-result" }],
		});
		const test = setup([], {
			agents,
			plugins: [
				definePlugin({
					id: "hook",
					hooks: {
						workflow: [
							async (execution, ctx, next) => {
								seen.push(execution.workflow);
								return next(ctx);
							},
						],
					},
				}),
			],
		});
		await test.app.start();
		const id = await test.runtime.invoke("agent-run", {}, { metadata });
		await test.runtime.flush();
		expect(test.runtime.result(id)).toMatchObject({ output: "agent-result" });
		expect(seen).toEqual(["agent-run"]);
		expect(
			test.runtime.emitted.filter((event) => event.name === "fabrial.execution.settled"),
		).toHaveLength(1);
	});
	it("recognizes users discovered through dynamic groups and provider-scoped membership", async () => {
		const user = defineUser({ id: "dynamic", identities: [identity("D")] });
		const group = defineGroup({ id: "dynamic-group", resolve: () => [user] });
		const test = setup([], { identity: [group] });
		const before = await test.app.host.resolvePrincipal(identity("D"));
		expect(before.known).toBe(false);
		expect(await test.app.host.directory.isMember(before, group)).toBe(true);
		expect(await test.app.host.resolvePrincipal(identity("D"))).toMatchObject({
			known: true,
			id: "dynamic",
		});
		expect(await test.app.host.directory.identitiesFor("dynamic")).toEqual(user.identities);
	});
	it("forwards agent detached options and leaves agent-active reply buffers for the bridge", async () => {
		const agents = fakeAgents({
			run: async (execution, id, _agent, options) => {
				expect(options.detached).toBe(true);
				const thread = await test.chat.thread(surface);
				const state = await thread.getState();
				if (state)
					await thread.updateState((latest) =>
						latest ? { ...latest, agentActive: true } : latest,
					);
				await execution.sleep(id, 100);
				return "agent-result";
			},
		});
		const test = setup(
			[
				workflow(async (_i, ctx) =>
					ctx.agent(
						"agent",
						{ kind: "fabrial.agent", name: "fake" },
						{ input: {}, detached: true },
					),
				),
			],
			{ agents },
		);
		await enter(test);
		await test.chat.receive(message("steer"));
		await test.runtime.flush();
		expect(
			(await (await test.chat.thread(surface)).getState())?.bufferedReplies.map((m) => m.messageId),
		).toEqual(["steer"]);
	});
	it("handles generated-id collisions deterministically without repeating effects", async () => {
		let effects = 0;
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.step("x", () => ++effects);
				await ctx.step("x:1", () => ++effects);
				await ctx.step("x", () => ++effects);
				await ctx.sleep("sleep", 1);
				return effects;
			}),
		]);
		await enter(test);
		await test.runtime.advanceBy(1);
		expect(effects).toBe(3);
		expect(test.runtime.stepIds(test.runtime.executions("owner")[0]!.executionId)).toEqual(
			expect.arrayContaining(["x", "x:1", "x#1"]),
		);
	});
	it("does not permit an alternate workflow object to bypass registered access", async () => {
		const restricted = defineWorkflow({
			name: "restricted",
			input: z.object({}),
			access: { invoke: defineGroup({ id: "bob-only", members: [bob] }) },
			run: async () => "secret",
		});
		const impostor = defineWorkflow({
			name: "restricted",
			input: z.object({}),
			run: async () => "fake",
		});
		const test = setup([workflow(async (_i, ctx) => ctx.invoke("call", impostor, {})), restricted]);
		await enter(test);
		expect(test.runtime.executions("restricted")).toHaveLength(0);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "failed",
			error: "Not authorized to invoke restricted",
		});
	});
});

describe("ingress tombstones and reservation recovery", () => {
	it.each(["completed", "failed", "cancelled"])(
		"ignores redelivery after %s settlement and routes the next interaction normally",
		async (status) => {
			const test = setup([
				workflow(async (input, ctx) => {
					if ((input as ChatMessage).messageId === "initial") {
						if (status === "failed") throw new Error("failed");
						if (status === "cancelled") await ctx.sleep("pause", "1h");
						return;
					}
					return (await ctx.thread!.waitForReply("answer"))!.messageId;
				}),
			]);
			await enter(test);
			if (status === "cancelled") {
				await test.chat.cancel(surface, alice.identities[0]!, "stop");
				await test.runtime.flush();
			}
			const thread = await test.chat.thread(surface);
			const idle = await thread.getState();
			expect(idle).toMatchObject({ interactionId: null, handlerExecutionId: null });
			expect(test.runtime.executions("owner")[0]!.result?.status).toBe(status);
			expect(await test.chat.receive(message("initial"))).toBe("ignored");
			expect(await thread.getState()).toEqual(idle);
			expect(await test.chat.receive(message("next"))).toBe("new");
			await test.runtime.flush();
			expect(await test.chat.receive(message("initial"))).toBe("ignored");
			expect(await test.chat.receive(message("answer"))).toBe("reply");
			await test.runtime.flush();
			expect(test.runtime.executions("owner")).toHaveLength(2);
			expect(test.runtime.executions("owner")[1]!.result).toMatchObject({ output: "answer" });
			expect(await test.chat.receive(message("answer"))).toBe("ignored");
			expect((await thread.getState())?.interactionId).toBeNull();
		},
	);

	it("only accepts one of two concurrent deliveries of the same ingress", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		expect(
			(
				await Promise.all([test.chat.receive(message("same")), test.chat.receive(message("same"))])
			).sort(),
		).toEqual(["ignored", "new"]);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(1);
		expect((await (await test.chat.thread(surface)).getState())?.ingestedDedupeIds).toEqual([
			{ kind: "message", id: "same", at: test.runtime.now() },
		]);
	});

	it("keeps the last 100 receipts across settlements, including actions", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		for (let i = 0; i < 101; i++) {
			await test.chat.receive(message(`inbound-${i}`));
			await test.runtime.flush();
		}
		const thread = await test.chat.thread(surface);
		expect((await thread.getState())?.ingestedDedupeIds).toHaveLength(100);
		expect((await thread.getState())?.ingestedDedupeIds?.[0]?.id).toBe("inbound-1");
		expect(await test.chat.receive(message("inbound-100"))).toBe("ignored");
		// Beyond the bounded window the runtime still dedupes; the orphaned slot must expire.
		expect(await test.chat.receive(message("inbound-0"))).toBe("new");
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(101);
		expect((await thread.getState())?.handlerExecutionId).toBeNull();
		await test.runtime.advanceBy(10 * 60 * 1000);
		const ref = { ...surface, messageId: "card" };
		let actionEmissions = 0;
		const emit = test.runtime.emit.bind(test.runtime);
		test.runtime.emit = async (...args) => {
			if (args[0] === "fabrial.approval.decided") actionEmissions++;
			return emit(...args);
		};
		await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
			dedupeId: "inbound-100",
		});
		await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
			dedupeId: "inbound-100",
		});
		expect(actionEmissions).toBe(1);
		await test.chat.receive(message("next"));
		await test.runtime.flush();
		await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
			dedupeId: "inbound-100",
		});
		expect(actionEmissions).toBe(1);
		const state = await thread.getState();
		expect(state?.ingestedDedupeIds).toHaveLength(100);
		expect(state?.ingestedDedupeIds).toContainEqual({
			kind: "action",
			id: "inbound-100",
			at: test.runtime.now(),
		});
		state!.ingestedDedupeIds![0]!.id = "mutated snapshot";
		expect((await thread.getState())?.ingestedDedupeIds?.[0]?.id).not.toBe("mutated snapshot");
		await test.runtime.advanceBy(30 * 24 * 60 * 60 * 1000);
		expect(await thread.getState()).toBeNull();
	});

	it.each([undefined, 10 * 60 * 1000])("recovers a stale reservation of age %s", async (age) => {
		const test = setup([
			workflow(async (_i, ctx) => (await ctx.thread!.waitForReply("answer"))!.messageId),
		]);
		await test.app.start();
		const thread = await test.chat.thread(surface);
		await thread.updateState(() => ({
			interactionId: "lost",
			handlerExecutionId: null,
			agentActive: false,
			statusMessageId: "old-status",
			bufferedReplies: [message("old-reply")],
			requesterId: "outsider",
			participantIds: ["outsider"],
			cancellationIds: ["old-stop"],
			...(age === undefined ? {} : { reservedAt: test.runtime.now() - age }),
			ingestedDedupeIds: [{ kind: "message", id: "old", at: test.runtime.now() - 600000 }],
		}));
		expect(await test.chat.receive(message("next"))).toBe("new");
		const reserved = await thread.getState();
		expect(reserved).toMatchObject({
			interactionId: "chat:next",
			reservedAt: test.runtime.now(),
			bufferedReplies: [],
			requesterId: "alice",
			participantIds: ["alice"],
		});
		expect(reserved?.cancellationIds).toBeUndefined();
		expect(await test.chat.receive(message("old"))).toBe("ignored");
		await test.runtime.flush();
		expect(await test.chat.receive(message("answer"))).toBe("reply");
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "answer" });
	});

	it("recovers a lost dispatch after the configured grace without refreshing it for replies", async () => {
		const test = setup([workflow(async () => "recovered")], { reservationTimeout: "1s" });
		await test.app.start();
		const emit = test.runtime.emit.bind(test.runtime);
		test.runtime.emit = async (...args) => {
			if (args[2].id !== "lost") await emit(...args);
		};
		await test.chat.receive(message("lost"));
		await test.runtime.advanceBy(999);
		expect(await test.chat.receive(message("during-grace"))).toBe("reply");
		expect(test.runtime.executions("owner")).toHaveLength(0);
		await test.runtime.advanceBy(1);
		expect(await test.chat.receive(message("recovered"))).toBe("new");
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(1);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "recovered" });
		expect(await test.chat.receive(message("lost"))).toBe("ignored");
		expect(await test.chat.receive(message("during-grace"))).toBe("ignored");
	});

	it("does not expire a bound handler after the reservation grace", async () => {
		const test = setup([workflow(async (_i, ctx) => ctx.sleep("pause", "1h"))]);
		await enter(test);
		await test.runtime.advanceBy(10 * 60 * 1000);
		expect(await test.chat.receive(message("reply"))).toBe("reply");
		expect(test.runtime.executions("owner")).toHaveLength(1);
	});

	it("does not reserve an interaction for observer-only ingress", async () => {
		const test = setup([
			defineWorkflow({
				name: "observer",
				triggers: [trigger({ event, observe: true })],
				run: async () => {},
			}),
		]);
		await test.app.start();
		expect(await test.chat.receive(message("observe"))).toBe("new");
		await test.runtime.flush();
		expect(await test.chat.receive(message("observe"))).toBe("ignored");
		expect(await test.chat.receive(message("next"))).toBe("new");
		await test.runtime.flush();
		expect(test.runtime.executions("observer")).toHaveLength(2);
		expect((await (await test.chat.thread(surface)).getState())?.interactionId).toBeNull();
	});

	it("preserves tombstones when independent cleanup clears a channel binding", async () => {
		const test = setup([]);
		const thread = await test.chat.thread(surface);
		const ref = await thread.post("hello");
		const tombstones = [{ kind: "message" as const, id: "initial", at: test.runtime.now() }];
		await thread.updateState(() => ({
			interactionId: "old",
			handlerExecutionId: "old",
			agentActive: true,
			statusMessageId: "status",
			bufferedReplies: [message("reply")],
			requesterId: "alice",
			ingestedDedupeIds: tombstones,
		}));
		await test.app.start();
		await test.runtime.invoke(
			"fabrial.cleanup",
			{
				executionId: "old",
				presentationId: "p",
				after: "0",
			},
			{ metadata: { ...metadata, interactionId: "old" } },
		);
		await test.runtime.emit(
			"fabrial.presentation",
			{
				presentationId: "p",
				ref: ref as unknown as Json,
			},
			{ metadata },
		);
		await test.runtime.emit("fabrial.execution.settled", { executionId: "old" }, { metadata });
		await test.runtime.flush();
		expect(await thread.getState()).toEqual({
			interactionId: null,
			handlerExecutionId: null,
			agentActive: false,
			statusMessageId: null,
			bufferedReplies: [],
			ingestedDedupeIds: tombstones,
		});
		expect(await test.chat.receive(message("initial"))).toBe("ignored");
	});

	it("lets non-message executions reserve idle state without losing tombstones", async () => {
		const test = setup([
			workflow(async (input, ctx) => {
				if ((input as JsonObject).later) await ctx.sleep("pause", "1h");
			}),
		]);
		await enter(test);
		await test.app.emit(event, { later: true }, { id: "later", replyTo: surface });
		await test.runtime.flush();
		const state = await (await test.chat.thread(surface)).getState();
		expect(state).toMatchObject({
			interactionId: `${event}:later`,
			handlerExecutionId: test.runtime.executions("owner")[1]!.executionId,
		});
		expect(state?.ingestedDedupeIds).toContainEqual({
			kind: "message",
			id: "initial",
			at: test.runtime.now(),
		});
		expect(await test.chat.receive(message("initial"))).toBe("ignored");
	});

	it("uses the final updater decision when a lock retry sees an ingress tombstone", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		const thread = await test.chat.thread(surface);
		const update = thread.updateState.bind(thread);
		thread.updateState = async (fn) => {
			const speculative = fn(null);
			await update(() => speculative);
			return update(fn);
		};
		expect(await test.chat.receive(message("retry"))).toBe("ignored");
		expect(test.runtime.emitted).toHaveLength(0);
		expect((await thread.getState())?.ingestedDedupeIds).toHaveLength(1);
	});

	it.each([-1, Infinity, "30d"])("rejects unbounded reservation grace %s", (reservationTimeout) => {
		expect(() => setup([], { reservationTimeout })).toThrow();
	});
});

describe("ingress redelivery correlation", () => {
	it("never delivers the triggering message again as a reply", async () => {
		const test = setup([
			workflow(
				async (_i, ctx) => (await ctx.thread!.waitForReply("reply", { timeout: 100 }))!.messageId,
			),
		]);
		await enter(test);
		await test.chat.receive(message("initial"));
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toBeUndefined();
		await test.chat.receive(message("answer"));
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "answer" });
	});
});

describe("provider route response context", () => {
	it("passes replyTo and authenticated requestedBy from typed route emits through ingest", async () => {
		const provider = definePlugin({
			id: "provider",
			events: { opened: defineEvent({ payload: z.object({ id: z.string() }) }) },
			routes: {
				"POST /opened": async (_request, ctx) => {
					await ctx.emit(
						"opened",
						{ id: "pr-1" },
						{ id: "delivery", replyTo: surface, requestedBy: alice.identities[0]! },
					);
					return new Response("ok");
				},
			},
		});
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event: "provider.opened" })],
			run: async (_i, ctx) => {
				expect(ctx.actor?.id).toBe("alice");
				expect(ctx.thread!.ref).toEqual(surface);
			},
		});
		const test = setup([owner], { plugins: [provider] });
		await test.app.start();
		await test.app.fetch(new Request("https://test/opened", { method: "POST" }));
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.metadata.requestedBy?.id).toBe("alice");
	});
	it("uses receipt-derived metadata for state and agents after lazy channel posts", async () => {
		const counter = defineState({
			name: "counter",
			scope: "thread",
			schema: z.object({ count: z.number() }),
			initial: () => ({ count: 0 }),
		});
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => (await ctx.state(counter).get("get")).count,
		});
		const owner = defineWorkflow({
			name: "owner",
			triggers: [
				trigger({ event, replyTo: { kind: "channel", provider: "chat", channelId: "lazy" } }),
			],
			run: async (_i, ctx) => {
				const ref = await ctx.thread!.post("first", "first");
				expect(ctx.metadata.replyTo?.threadId).toBe(ref.threadId);
				await ctx.state(counter).update("update", (draft) => {
					draft.count++;
				});
				return ctx.invoke("child", child, {});
			},
		});
		const test = setup([owner, child], { agents: fakeAgents() });
		await test.app.start();
		await test.app.emit(event, {});
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: 1 });
	});
});

describe("thread ownership during parent cancellation", () => {
	it("never clears the receiver's binding when a handoff caller is cancelled", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => (await ctx.thread!.waitForReply("reply"))!.text,
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.handoff("handoff", child, {});
				await ctx.sleep("after-handoff", 100);
			}),
			child,
		]);
		await enter(test);
		const childId = test.runtime.executions("child")[0]!.executionId;
		await test.runtime.cancel(test.runtime.executions("owner")[0]!.executionId);
		await test.runtime.flush();
		expect((await (await test.chat.thread(surface)).getState())?.handlerExecutionId).toBe(childId);
		await test.chat.receive(message("answer"));
		await test.runtime.flush();
		expect(test.runtime.result(childId)).toMatchObject({ output: "answer" });
	});
	it("treats missing prototype-named filter fields as absent", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event, filter: { toString: [{ exists: false }] } })],
			run: async () => {},
		});
		const test = setup([owner]);
		await test.app.start();
		await test.app.emit(event, {});
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(1);
	});
});

describe("review regressions", () => {
	it.each(["fabrial:receipt", "x#1"])("rejects reserved operation id %s", async (id) => {
		const test = setup([workflow(async (_i, ctx) => ctx.step(id, () => 1))]);
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "failed",
			error: expect.stringContaining("reserved"),
		});
	});

	it("delivers approval cards concurrently, durably per card", async () => {
		const test = setup([approvalWorkflow()]);
		const first = await test.chat.openDM(alice.identities[0]!);
		const second = await test.chat.openDM(bob.identities[0]!);
		let secondStarted = false;
		const secondPost = second.post.bind(second);
		second.post = async (content) => {
			secondStarted = true;
			return secondPost(content);
		};
		const firstPost = first.post.bind(first);
		first.post = async (content) => {
			await new Promise((resolve) => setTimeout(resolve, 0));
			expect(secondStarted).toBe(true);
			return firstPost(content);
		};
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.status).toBe("suspended");
		expect(first.posts).toHaveLength(1);
		expect(second.posts).toHaveLength(1);
	});

	it("catches a valid decision emitted while rejecting an invalid click", async () => {
		const test = setup([approvalWorkflow()]);
		await enter(test);
		const c = await card(test);
		const notice = test.chat.postEphemeral.bind(test.chat);
		test.chat.postEphemeral = async (...args) => {
			await notice(...args);
			await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		};
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "approved" });
	});

	it("cleanup catches presentations and settlement emitted between successive waits", async () => {
		const test = setup([approvalWorkflow()]);
		const cleanup = test.runtime.workflows.get("fabrial.cleanup")!;
		const handler = cleanup.handler;
		let extra: Awaited<ReturnType<typeof test.chat.openDM>> | undefined;
		cleanup.handler = (input, execution) =>
			handler(input, {
				...execution,
				waitForAny: async (id, branches) => {
					const result = await execution.waitForAny(id, branches);
					if (!extra && result.kind === "event" && result.key === "presented") {
						extra = await test.chat.openDM(outsider.identities[0]!);
						const ref = await extra.post({ card: { title: "extra", actions: [] } });
						const data = input as JsonObject;
						await execution.emit("fabrial.presentation", {
							presentationId: data.presentationId!,
							ref: ref as unknown as Json,
						});
						await execution.emit("fabrial.execution.settled", { executionId: data.executionId! });
					}
					return result;
				},
			});
		await enter(test);
		expect(extra!.posts[0]!.content).toMatchObject({ card: { text: "Cancelled", actions: [] } });
		expect(test.runtime.executions("fabrial.cleanup")[0]!.result?.status).toBe("completed");
		expect(
			test.runtime
				.stepIds(test.runtime.executions("fabrial.cleanup")[0]!.executionId)
				.every((id) => id.startsWith("fabrial:")),
		).toBe(true);
	});

	it("does not restart a race timer after an invalid approval at 90ms", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const approval = await ctx.requestApproval("a", {
					title: "Proposal",
					details: {},
					approvers: team,
					timeout: 1000,
				});
				return (await ctx.race("r", { approval: approval.decision(), timer: ctx.timer(100) })).key;
			}),
		]);
		await enter(test);
		const c = await card(test);
		await test.runtime.advanceBy(90);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await test.runtime.flush();
		await test.runtime.advanceBy(10);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "timer" });
	});

	it("does not restart a reply deadline after rejecting a duplicate event", async () => {
		const test = setup([
			workflow(async (_i, ctx) => ctx.thread!.waitForReply("reply", { timeout: 100 })),
		]);
		await enter(test);
		await test.runtime.advanceBy(90);
		const interactionId = test.runtime.executions("owner")[0]!.metadata.interactionId;
		await test.runtime.emit(
			"fabrial.reply",
			{ interactionId, message: message("initial") },
			{ metadata },
		);
		await test.runtime.flush();
		await test.runtime.advanceBy(10);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "completed",
			output: null,
		});
	});

	it("atomically reserves an ingress slot before the worker starts and appends concurrent replies", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const a = await ctx.thread!.waitForReply("a");
				const b = await ctx.thread!.waitForReply("b");
				return [a!.messageId, b!.messageId];
			}),
		]);
		await test.app.start();
		expect(
			(
				await Promise.all([
					test.chat.receive(message("initial")),
					test.chat.receive(message("second", bob)),
				])
			).sort(),
		).toEqual(["new", "reply"]);
		await test.chat.receive(message("third"));
		const thread = await test.chat.thread(surface);
		expect((await thread.getState())?.handlerExecutionId).toBeNull();
		expect((await thread.getState())?.bufferedReplies.map((m) => m.messageId)).toEqual([
			"second",
			"third",
		]);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(1);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			output: ["second", "third"],
		});
	});

	it("replay binding and settlement never overwrite a different interaction, even with the same handler id", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.sleep("pause", 10);
			}),
		]);
		await enter(test);
		const thread = await test.chat.thread(surface);
		await thread.updateState((state) => state && { ...state, interactionId: "newer" });
		const newer = await thread.getState();
		await test.runtime.advanceBy(10);
		expect(await thread.getState()).toEqual(newer);
	});

	it("settlement conditionally clears state after status I/O, not from a stale read", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.sleep("pause", 10);
			}),
		]);
		await enter(test);
		const thread = await test.chat.thread(surface);
		thread.setStatus = async () => {
			await thread.updateState((state) => state && { ...state, interactionId: "newer" });
		};
		await test.runtime.advanceBy(10);
		expect((await thread.getState())?.interactionId).toBe("newer");
	});

	it("handoff only transfers from the current handler", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("pause", 100);
			},
		});
		const test = setup([workflow(async (_i, ctx) => ctx.handoff("handoff", child, {})), child]);
		const invoke = test.runtime.invoke.bind(test.runtime);
		test.runtime.invoke = async (...args) => {
			const id = await invoke(...args);
			if (args[0] === "child")
				await (
					await test.chat.thread(surface)
				).updateState((state) => state && { ...state, handlerExecutionId: "replacement" });
			return id;
		};
		await enter(test);
		expect((await (await test.chat.thread(surface)).getState())?.handlerExecutionId).toBe(
			"replacement",
		);
	});

	it.each(["buffer", "event"])(
		"replays a selected %s reply after a crash during acknowledgement, without losing concurrent messages",
		async (source) => {
			const owner = defineWorkflow({
				name: "owner",
				triggers: [trigger({ event })],
				retries: { maxAttempts: 2 },
				run: async (_i, ctx) => {
					if (source === "buffer") await ctx.sleep("pause", 10);
					const first = await ctx.thread!.waitForReply("first");
					const second = await ctx.thread!.waitForReply("second");
					const duplicate = await ctx.thread!.waitForReply("duplicate", { timeout: 10 });
					return [first!.messageId, second!.messageId, duplicate];
				},
			});
			const test = setup([owner]);
			const thread = await test.chat.thread(surface);
			const update = thread.updateState.bind(thread);
			let crashed = false;
			thread.updateState = async (fn) => {
				const state = await update(fn);
				if (!crashed && state?.consumedReplyOperations?.first) {
					crashed = true;
					const id = test.runtime.executions("owner")[0]!.executionId;
					expect(test.runtime.stepIds(id)).toContain(state.consumedReplyOperations.first);
					await test.chat.receive(message("second"));
					throw new Error("crash after acknowledgement");
				}
				return state;
			};
			await enter(test);
			await test.chat.receive(message("first"));
			if (source === "buffer") await test.runtime.advanceBy(10);
			else await test.runtime.flush();
			await test.chat.receive(message("first"));
			await test.runtime.flush();
			await test.runtime.advanceBy(10);
			expect(crashed).toBe(true);
			expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
				status: "completed",
				output: ["first", "second", null],
			});
		},
	);

	it("passes the canonical dispatchOwner on every ingress category", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [
				trigger({ event: "chat.mentioned", specificity: 2 }),
				trigger({ event, observe: true }),
			],
			run: async () => {},
		});
		const test = setup([owner]);
		const emit = test.runtime.emit.bind(test.runtime);
		const owners: unknown[] = [];
		test.runtime.emit = async (name, payload, options) => {
			if (options.dispatchId) owners.push(options.dispatchOwner);
			return emit(name, payload, options);
		};
		await test.app.start();
		await test.chat.receive(message("initial"), { events: [event, "chat.mentioned"] });
		expect(owners).toEqual([
			{ workflow: "owner", event: "chat.mentioned" },
			{ workflow: "owner", event: "chat.mentioned" },
		]);
	});

	it("preserves cron replyTo in the registered runtime contract", () => {
		const cron = defineWorkflow({
			name: "cron",
			cron: [{ schedule: "0 * * * *", replyTo: surface }],
			run: async () => {},
		});
		const test = setup([cron]);
		expect(test.runtime.workflows.get("cron")!.cron).toEqual([
			{ schedule: "0 * * * *", name: "cron", replyTo: surface },
		]);
	});

	it.each(["title", "details"] as const)("rejects changed approval %s on replay", async (field) => {
		let changed = false;
		const test = setup([
			workflow(async (_i, ctx) => {
				return (
					await ctx.waitForApproval("a", {
						title: changed && field === "title" ? "Changed" : "Proposal",
						details: changed && field === "details" ? { sql: "delete" } : { sql: "select" },
						approvers: team,
					})
				).status;
			}),
		]);
		await enter(test);
		const c = await card(test);
		changed = true;
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			status: "failed",
			error: expect.stringContaining("proposal changed"),
		});
	});

	it("treats reordered proposal detail keys as the same proposal", async () => {
		let changed = false;
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(
						await ctx.waitForApproval("a", {
							title: "Proposal",
							details: changed ? { b: 2, a: 1 } : { a: 1, b: 2 },
							approvers: team,
						})
					).status,
			),
		]);
		await enter(test);
		const c = await card(test);
		changed = true;
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await test.runtime.flush();
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({ output: "approved" });
	});

	it.each([false, true])(
		"tries DM identities preferring the origin, with fallback (all unsupported=%s)",
		async (unsupported) => {
			const user = defineUser({
				id: "multi",
				identities: [
					{ ...identity("S"), provider: "slack" },
					{ ...identity("G"), provider: "github" },
				],
			});
			const test = setup(
				[
					workflow(async (_i, ctx) =>
						ctx
							.requestApproval("a", { title: "Proposal", details: {}, approvers: user })
							.then(() => "requested"),
					),
				],
				{ identity: [alice, user] },
			);
			const attempts: string[] = [];
			const open = test.chat.openDM.bind(test.chat);
			test.chat.openDM = async (identity) => {
				attempts.push(identity.provider);
				if (identity.provider === "github" || unsupported) throw new Error("No DM support");
				return open(identity);
			};
			await test.app.start();
			await test.app.emit(event, message("initial"), {
				origin: { provider: "github" },
				replyTo: { ...surface, provider: "slack" },
			});
			await test.runtime.flush();
			expect(attempts).toEqual(["github", "slack"]);
			expect(test.runtime.executions("owner")[0]!.result).toMatchObject(
				unsupported
					? {
							status: "failed",
							error: expect.stringContaining("no identity whose provider can open DMs"),
						}
					: { output: "requested" },
			);
		},
	);
});

describe("updated in-memory port fidelity", () => {
	it.each([false, true])(
		"only explicit after sees events emitted before registration (after=%s)",
		async (explicit) => {
			const runtime = new MemoryRuntime();
			runtime.register({
				events: [],
				workflows: [
					{
						name: "waiter",
						triggers: [],
						handler: async (_i, execution) => {
							const after = await execution.cursor("fabrial:cursor");
							await execution.step("fabrial:emit", () =>
								execution.emit("ping", { value: "early" }),
							);
							const value = await execution.waitForEvent("wait", {
								event: "ping",
								...(explicit ? { after } : {}),
								deadline: runtime.now() + 100,
							});
							return value as unknown as Json;
						},
					},
				],
			});
			await runtime.start();
			const id = await runtime.invoke("waiter", {}, { metadata });
			await runtime.flush();
			if (explicit)
				expect(runtime.result(id)).toMatchObject({
					output: { payload: { value: "early" }, cursor: "1" },
				});
			else {
				expect(runtime.result(id)).toBeUndefined();
				await runtime.emit("ping", { value: "late" }, { metadata });
				await runtime.flush();
				expect(runtime.result(id)).toMatchObject({
					output: { payload: { value: "late" }, cursor: "2" },
				});
			}
		},
	);

	it("memoizes a cursor across replay and continues after the consumed event cursor", async () => {
		const runtime = new MemoryRuntime();
		runtime.register({
			events: [],
			workflows: [
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						const cursor = await execution.cursor("fabrial:cursor");
						await execution.sleep("delay", 10);
						const first = await execution.waitForEvent("first", { event: "ping", after: cursor });
						await execution.step("fabrial:between", () =>
							execution.emit("ping", { value: "second" }),
						);
						const second = await execution.waitForEvent("second", {
							event: "ping",
							after: first!.cursor,
						});
						return [cursor, first!.payload.value!, second!.payload.value!];
					},
				},
			],
		});
		await runtime.start();
		const id = await runtime.invoke("waiter", {}, { metadata });
		await runtime.flush();
		await runtime.emit("ping", { value: "first" }, { metadata });
		await runtime.advanceBy(10);
		expect(runtime.result(id)).toMatchObject({ output: ["0", "first", "second"] });
	});

	it("fires timers at absolute at even when registration is delayed", async () => {
		const runtime = new MemoryRuntime();
		const at = runtime.now() + 100;
		runtime.register({
			events: [],
			workflows: [
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						await execution.sleep("delay", 90);
						return (await execution.waitForAny("timer", { timer: { kind: "timer", at } })).kind;
					},
				},
			],
		});
		await runtime.start();
		const id = await runtime.invoke("waiter", {}, { metadata });
		await runtime.flush();
		await runtime.advanceBy(90);
		expect(runtime.result(id)).toBeUndefined();
		await runtime.advanceBy(10);
		expect(runtime.result(id)).toMatchObject({ output: "timer" });
	});

	it("does not let a late event beat an absolute expired deadline", async () => {
		const runtime = new MemoryRuntime();
		const deadline = runtime.now() + 100;
		runtime.register({
			events: [],
			workflows: [
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						const after = await execution.cursor("cursor");
						await execution.sleep("delay", 200);
						return (await execution.waitForEvent("wait", {
							event: "ping",
							after,
							deadline,
						})) as unknown as Json;
					},
				},
			],
		});
		await runtime.start();
		const id = await runtime.invoke("waiter", {}, { metadata });
		await runtime.flush();
		await runtime.advanceBy(150);
		await runtime.emit("ping", {}, { metadata });
		await runtime.advanceBy(50);
		expect(runtime.result(id)).toMatchObject({ output: null });
	});

	it("suppresses a canonical owner's competing observer emission, even when it arrives first", async () => {
		const runtime = new MemoryRuntime();
		runtime.register({
			events: [],
			workflows: [
				{
					name: "owner",
					triggers: [
						{ event: "observer", role: "observer" },
						{ event: "owned", role: "owner" },
					],
					handler: async () => "ok",
				},
			],
		});
		await runtime.start();
		const options = {
			metadata: { ...metadata, replyTo: surface as InvocationMetadata["replyTo"] },
			dispatchId: "dispatch",
			dispatchOwner: { workflow: "owner", event: "owned" },
		};
		await runtime.emit("observer", {}, options);
		expect(runtime.executions()).toHaveLength(0);
		await runtime.emit("owned", {}, { ...options, owner: "owner" });
		await runtime.flush();
		expect(runtime.executions()).toHaveLength(1);
		expect(runtime.executions()[0]!.metadata.ownsThread).toBe(true);
		expect(runtime.executions()[0]!.metadata.replyTo).toEqual(surface);
	});

	it("fake ChatPort atomically updates state without exposing mutable stored objects", async () => {
		const chat = new FakeChat();
		const thread = await chat.thread(surface);
		await thread.updateState(() => ({
			interactionId: "test",
			handlerExecutionId: null,
			agentActive: false,
			statusMessageId: null,
			bufferedReplies: [],
			count: 0,
		}));
		await Promise.all(
			Array.from({ length: 100 }, () =>
				thread.updateState((state) => state && { ...state, count: Number(state.count) + 1 }),
			),
		);
		const state = await thread.getState();
		expect(state?.count).toBe(100);
		state!.count = 0;
		expect((await thread.getState())?.count).toBe(100);
	});

	it.each(["interaction", "handler"])(
		"cleanup does not clear a thread with a different %s",
		async (changed) => {
			const test = setup([]);
			const thread = await test.chat.thread(surface);
			const ref = await thread.post("hello");
			await thread.updateState(() => ({
				interactionId: changed === "interaction" ? "new" : "old",
				handlerExecutionId: changed === "handler" ? "new" : "old",
				agentActive: false,
				statusMessageId: null,
				bufferedReplies: [],
			}));
			const state = await thread.getState();
			await test.app.start();
			const id = await test.runtime.invoke(
				"fabrial.cleanup",
				{ executionId: "old", presentationId: "p", after: "0" },
				{ metadata: { ...metadata, interactionId: "old" } },
			);
			await test.runtime.emit(
				"fabrial.presentation",
				{ presentationId: "p", ref: ref as unknown as Json },
				{ metadata },
			);
			await test.runtime.emit("fabrial.execution.settled", { executionId: "old" }, { metadata });
			await test.runtime.flush();
			expect(test.runtime.result(id)?.status).toBe("completed");
			expect(await thread.getState()).toEqual(state);
		},
	);
});

describe("registration interleavings", () => {
	it("does not lose an event emitted after registration while the attempt is still running", async () => {
		const runtime = new MemoryRuntime();
		runtime.register({
			events: [],
			workflows: [
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						const pending = execution.waitForEvent("wait", { event: "ping" });
						await execution.step("fabrial:emit", () =>
							execution.emit("ping", { value: "registered" }),
						);
						return (await pending) as unknown as Json;
					},
				},
			],
		});
		await runtime.start();
		const id = await runtime.invoke("waiter", {}, { metadata });
		await runtime.flush();
		expect(runtime.result(id)).toMatchObject({ output: { payload: { value: "registered" } } });
	});

	it("reselects an owner if settlement frees the slot between ingress read and update", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.sleep("pause", 100);
			}),
		]);
		await enter(test);
		const thread = await test.chat.thread(surface);
		const update = thread.updateState.bind(thread);
		let cleared = false;
		thread.updateState = async (fn) => {
			if (!cleared) {
				cleared = true;
				await update(() => null);
			}
			return update(fn);
		};
		expect(await test.chat.receive(message("next"))).toBe("new");
		await test.runtime.flush();
		expect(test.runtime.executions("owner")).toHaveLength(2);
		expect((await thread.getState())?.interactionId).toBe("chat:next");
	});

	it("waitForApproval accepts framework-generated repetition suffixes without relaxing user id validation", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const decisions = [];
				for (let i = 0; i < 2; i++)
					decisions.push(
						(await ctx.waitForApproval("a", { title: "Proposal", details: {}, approvers: alice }))
							.status,
					);
				return decisions;
			}),
		]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = dm.post.bind(dm);
		dm.post = async (content) => {
			const ref = await post(content);
			if (typeof content === "object" && "card" in content)
				await test.chat.click(ref, content.card.actions![0]!.id, alice.identities[0]!);
			return ref;
		};
		await enter(test);
		expect(test.runtime.executions("owner")[0]!.result).toMatchObject({
			output: ["approved", "approved"],
		});
		expect(dm.posts).toHaveLength(2);
	});
});
