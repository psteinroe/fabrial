import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	defineEvent,
	defineGroup,
	definePlugin,
	defineState,
	defineUser,
	defineWorkflow,
	fabrial,
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
function setup(workflows: AnyWorkflow[], extra: Partial<Parameters<typeof fabrial>[0]> = {}) {
	const runtime = new MemoryRuntime();
	const chat = new FakeChat(() => runtime.now());
	const app = fabrial({
		runtime,
		chat,
		plugins: [plugin],
		workflows,
		identity: [alice, bob, outsider, team],
		...extra,
	});
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
		expect(await (await test.chat.thread(surface)).getState()).toBeNull();
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
		expect(test.runtime.stepIds(id)).toEqual(expect.arrayContaining(["x", "x:1", "x:2"]));
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
		expect(await (await test.chat.thread(surface)).getState()).toBeNull();
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
		expect(await (await test.chat.thread(surface)).getState()).toBeNull();
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
			expect(await (await test.chat.thread(surface)).getState()).toBeNull();
		},
	);
	it("calls chat, agent and plugin lifecycle callbacks", async () => {
		const calls: string[] = [];
		const runtime = new MemoryRuntime();
		const chat = new FakeChat(() => runtime.now());
		const agents = fakeAgents();
		const app = fabrial({
			runtime,
			workflows: [],
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
				if (state) await thread.setState({ ...state, agentActive: true });
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
			expect.arrayContaining(["x", "x:1", "x:2"]),
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
