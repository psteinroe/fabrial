import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	defineGroup,
	definePlugin,
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
import { FakeChat, MemoryRuntime, testApp } from "fabrial/testing";
import { StubRuntime } from "./stub-runtime.ts";
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
// Definition, routing-table and identity checks that run no workflow; executions are covered on real
// Conductor in tests/integration/core.
function setup(
	workflows: AnyWorkflow[],
	extra: Partial<import("../../src/app.ts").CoreConfig> = {},
	runtime: StubRuntime | MemoryRuntime = new StubRuntime(),
) {
	const chat = new FakeChat(() => runtime.now());
	const config = {
		runtime,
		chat,
		plugins: [plugin],
		workflows,
		identity: [alice, bob, outsider],
		...extra,
	};
	const { plugins, identity: identities, logger, ...appConfig } = config;
	const app = testApp(createFabrial({ plugins, identity: identities }), { ...appConfig, logger });
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
};

async function enter(test: ReturnType<typeof setup> & { runtime: MemoryRuntime }) {
	await test.app.start();
	await test.chat.receive(message("initial"));
	await test.runtime.flush();
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
const memory = (workflows: AnyWorkflow[]) => {
	const test = setup(workflows, {}, new MemoryRuntime());
	return { ...test, runtime: test.runtime as MemoryRuntime };
};
async function card(test: ReturnType<typeof memory>) {
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

describe("routing table", () => {
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
	it("preserves cron replyTo in the registered runtime contract", () => {
		const cron = defineWorkflow({
			name: "cron",
			cron: [{ schedule: "0 * * * *", replyTo: surface }],
			run: async () => {},
		});
		const test = setup([cron]);
		expect((test.runtime as StubRuntime).workflows.get("cron")!.cron).toEqual([
			{ schedule: "0 * * * *", name: "cron", replyTo: surface },
		]);
	});
});

describe("identity", () => {
	it("resolves known and unknown identities, static/dynamic groups and names", async () => {
		const lookup = definePlugin({
			id: "chat",
			identity: { lookup: async () => ({ name: "Provider name" }) },
		});
		const runtime = new StubRuntime(Date.parse("2026-01-05T00:00:00Z"));
		const test = setup([], { plugins: [lookup] }, runtime);
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
		runtime.time += 7 * 86400000;
		expect((await test.app.host.directory.members(rotation))[0]!.id).toBe("bob");
		expect(weeklyRotation([alice, bob], { start: "2026-01-05", now: new Date("2025-12-29") })).toBe(
			bob,
		);
	});
	it("recognizes users discovered through dynamic groups and provider-scoped membership", async () => {
		const user = defineUser({ id: "dynamic", identities: [identity("D")] });
		const group = defineGroup({ id: "dynamic-group", resolve: () => [user] });
		const test = setup([], { identity: [] });
		const before = await test.app.host.resolvePrincipal(identity("D"));
		expect(before.known).toBe(false);
		expect(await test.app.host.directory.isMember(before, group)).toBe(true);
		expect(await test.app.host.resolvePrincipal(identity("D"))).toMatchObject({
			known: true,
			id: "dynamic",
		});
		expect(await test.app.host.directory.identitiesFor("dynamic")).toEqual(user.identities);
	});
});

describe("fake ports", () => {
	it("fake ChatPort atomically updates state without exposing mutable stored objects", async () => {
		const chat = new FakeChat();
		const thread = await chat.thread(surface);
		await thread.updateState(() => ({ count: 0 }));
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
});

describe("cleanup workflow", () => {
	// PHASE2: task 5. These drive the internal cleanup workflow through MemoryRuntime internals (a
	// handler swapped in its registry, stepIds, cursor "0"); task 9 replaces that workflow and its tests.
	it("cleanup catches presentations and settlement emitted between successive waits", async () => {
		const test = memory([approvalWorkflow()]);
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

	it.each(["first", "second"])("cleanup leaves the thread's routing state alone (%s)", async () => {
		const test = memory([]);
		const thread = await test.chat.thread(surface);
		const ref = await thread.post("hello");
		await thread.updateState(() => ({
			statusMessageId: null,
			follows: [{ session: "chat:thread", agent: "support" }],
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
	});
});

// Executions that stay on the in-memory runtime for now; the rest run on real Conductor in
// tests/integration/core.
describe("in-memory executions", () => {
	// PHASE2: move to real Conductor once conductor fixes the frozen test clock (event created_at == subscription created_at)
	it("expires and renders cards without actions", async () => {
		const test = memory([approvalWorkflow(100)]);
		await enter(test);
		const c = await card(test);
		await test.runtime.advanceBy(100);
		expect(test.runtime.executions()[0]!.result).toMatchObject({ output: "expired" });
		expect(c.post.content).toMatchObject({ card: { text: "Expired", actions: [] } });
	});
	// PHASE2: move to real Conductor once conductor fixes the frozen test clock (event created_at == subscription created_at)
	it("does not extend expiry after invalid decisions and ignores late clicks", async () => {
		const test = memory([approvalWorkflow(100)]);
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
	// PHASE2: move to real Conductor once conductor fixes the frozen test clock (event created_at == subscription created_at)
	it("does not restart a race timer after an invalid approval at 90ms", async () => {
		const test = memory([
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
	// PHASE2: move to real Conductor once conductor fixes the frozen test clock (event created_at == subscription created_at)
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
	// PHASE2: move to real Conductor once conductor fixes the frozen test clock (event created_at == subscription created_at)
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
		const test = memory([owner, child]);
		await test.app.start();
		await test.app.emit("source.ping", {});
		await test.runtime.flush();
		expect(test.chat.threads.size).toBe(1);
		const thread = [...test.chat.threads.values()][0]!;
		await test.runtime.advanceBy(10);
		expect(test.chat.threads.size).toBe(1);
		expect(thread.posts.map((p) => p.content)).toEqual(["updated", "second", "child"]);
		expect(test.runtime.executions("child")[0]!.metadata.replyTo).toEqual(thread.ref);
		expect(await thread.getState()).toBeNull();
	});
	// PHASE2: move to real Conductor with the native race (task 9): Conductor suspends the attempt at the
	// first wait, so the emit in the same attempt never runs while the custom race's wait is pending.
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
});
