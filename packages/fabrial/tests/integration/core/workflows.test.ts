import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineEvent, definePlugin, trigger } from "../../../src/index.ts";
import { fakeAgents } from "../../../src/testing.ts";
import { until } from "../../conductor/helper.ts";
import {
	alice,
	defineState,
	defineWorkflow,
	event,
	message,
	metadata,
	plugin,
	surface,
	useCoreApp,
	workflow,
} from "./setup.ts";

const { db, setup, enter, eventually } = useCoreApp();

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
				seen.push(ctx.thread ? "specific" : "specific without thread");
			},
		});
		const observer = defineWorkflow({
			name: "observer",
			triggers: [trigger({ event, observe: true })],
			run: async (_input, ctx) => {
				seen.push(ctx.thread ? "observer with thread" : "observer");
			},
		});
		const test = setup([broad, specific, observer]);
		await test.app.start();
		await test.chat.receive(message("initial"));
		expect(await test.chat.receive(message("initial"))).toBe("ignored");
		await (await test.execution("specific")).output();
		await (await test.execution("observer")).output();
		expect(seen.sort()).toEqual(["observer", "specific"]);
		expect(test.ids("specific")).toHaveLength(1);
		expect(test.ids("observer")).toHaveLength(1);
	});
	it("honors explicit specificity", async () => {
		const a = workflow(async () => "broad");
		const b = defineWorkflow({
			name: "explicit",
			triggers: [trigger({ event, specificity: 10 })],
			run: async () => "explicit",
		});
		const test = setup([a, b]);
		await test.app.start();
		await test.chat.receive(message("initial"));
		expect(await (await test.execution("explicit")).output()).toBe("explicit");
		expect(test.ids("owner")).toHaveLength(0);
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
		const pinged = defineWorkflow({
			name: "pinged",
			triggers: [trigger<{ value: string }>({ event: "source.ping" })],
			run: async (input) => input.value,
		});
		const test = setup([pinged], { plugins: [routes] });
		test.chat.routes["GET /chat"] = async () => new Response("chat");
		await test.app.start();
		expect(
			await (await test.app.fetch(new Request("https://test/ping", { method: "POST" }))).text(),
		).toBe("ok");
		expect(await (await test.execution("pinged")).output()).toBe("ok");
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
		await (await test.execution()).output();
		expect(test.ids("owner")).toHaveLength(1);
		const thread = await test.chat.thread(surface);
		await eventually("the settled receipt", async () =>
			Boolean((await thread.getState())?.ingestedDedupeIds?.every((receipt) => !receipt.pending)),
		);
		expect((await thread.getState())?.ingestedDedupeIds).toEqual([
			{ kind: "message", id: "human", at: expect.any(Number) },
		]);
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
		const owner = await enter(test);
		await owner.suspended("sleep");
		await db.clock.advance("1h");
		expect(await owner.output()).toBe(3);
		expect(effects).toBe(3);
		const posts = (await test.chat.thread(surface)).posts;
		expect(posts).toHaveLength(1);
		expect(posts[0]!.updates).toEqual(["updated"]);
	});
	it("handles generated-id collisions deterministically without repeating effects", async () => {
		let effects = 0;
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.step("x", () => ++effects);
				await ctx.step("x:1", () => ++effects);
				await ctx.step("x", () => ++effects);
				await ctx.sleep("sleep", "1h");
				return effects;
			}),
		]);
		const owner = await enter(test);
		await owner.suspended("sleep");
		await db.clock.advance("1h");
		expect(await owner.output()).toBe(3);
		expect(effects).toBe(3);
	});
	it("rejects empty operation ids", async () => {
		const test = setup([workflow(async (_i, ctx) => ctx.step("", () => 1))]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("explicit id"),
		});
	});
	it.each(["fabrial:receipt", "x#1"])("rejects reserved operation id %s", async (id) => {
		const test = setup([workflow(async (_i, ctx) => ctx.step(id, () => 1))]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("reserved"),
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
					await ctx.sleep("pause", "1h");
				}),
			],
			{ plugins: [plugin, p("a"), p("b")] },
		);
		const owner = await enter(test);
		await owner.suspended("pause");
		await db.clock.advance("1h");
		await owner.output();
		// Conductor middleware: `next` settles when the attempt suspends, so every attempt is a full round.
		const round = ["a:before", "b:before", "b:after", "a:after"];
		expect(seen).toEqual([...round, ...round]);
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
						model: "test/test",
						state: {},
						questions: {},
					});
					const answer = await ctx.agent(
						"agent",
						{ kind: "fabrial.agent", name: "echo" },
						{ input: "echo" },
					);
					await ctx.sleep("sleep", "1h");
					return { answer, count: (await ctx.state(counter).get("read")).count };
				}),
			],
			{ agents },
		);
		const owner = await enter(test);
		await owner.suspended("sleep");
		await db.clock.advance("1h");
		expect(await owner.output()).toEqual({ answer: "echo", count: 1 });
		expect(evaluations).toBe(1);
	});
	it("loads bounded history durably", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const history = await ctx.thread!.history("history", { limit: 1 });
				return { history: history.length };
			}),
		]);
		expect(await (await enter(test)).output()).toEqual({ history: 1 });
	});
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
		const execution = await enter(test);
		// The retry is scheduled after a backoff; move time past it once the attempt released.
		await until("the retry", async () => {
			if ((await execution.info())?.status !== "pending") return undefined;
			await db.clock.advance("1h");
			return true;
		});
		expect(await execution.settled()).toEqual({ status: "completed", output: 1 });
		expect(effects).toBe(1);
	});
	it("initializes state once and validates initial values", async () => {
		let initializations = 0;
		const state = defineState({
			name: "state",
			scope: "thread",
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
		expect(await (await enter(test)).output()).toEqual([1, 1]);
	});
});

describe("children", () => {
	it("invokes children with metadata, validates schemas, and starts independently", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({ value: z.number() }),
			run: async (input, ctx) => {
				if (ctx.actor?.id !== "alice") throw new Error(`Unexpected actor ${ctx.actor?.id}`);
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
		const output = (await (await enter(test)).output()) as { result: number; executionId: string };
		expect(output.result).toBe(6);
		expect(await db.execution(output.executionId).output()).toBe(8);
		expect(test.ids("child")).toHaveLength(2);
	});
	it.each(["invoke", "start"] as const)("validates %s input before starting", async (method) => {
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
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Invalid child input"),
		});
		expect(test.ids("child")).toHaveLength(0);
	});
	it("does not permit an alternate workflow object to bypass registered input validation", async () => {
		const registered = defineWorkflow({
			name: "registered",
			input: z.object({ count: z.number() }),
			run: async () => "secret",
		});
		const impostor = defineWorkflow({
			name: "registered",
			input: z.object({}),
			run: async () => "fake",
		});
		const test = setup([workflow(async (_i, ctx) => ctx.invoke("call", impostor, {})), registered]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Invalid registered input"),
		});
		expect(test.ids("registered")).toHaveLength(0);
	});
	it("invokes another workflow without a permission check", async () => {
		const other = defineWorkflow({
			name: "other",
			input: z.object({}),
			run: async () => "secret",
		});
		const test = setup([workflow(async (_i, ctx) => ctx.invoke("other", other, {})), other]);
		expect(await (await enter(test)).output()).toBe("secret");
		expect(test.ids("other")).toHaveLength(1);
	});
	it.each([false, true])("structured cancellation, detached=%s", async (detached) => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", "1h");
				return "done";
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => ctx.invoke("invoke", child, {}, { detached })),
			child,
		]);
		const owner = await enter(test);
		const execution = await (await test.execution("child")).suspended("sleep");
		await owner.suspended();
		await test.runtime.cancel(owner.id);
		expect(await owner.settled()).toMatchObject({ status: "cancelled" });
		if (detached) {
			expect(await execution.info()).toMatchObject({ status: "pending" });
			await db.clock.advance("1h");
			expect(await execution.settled()).toMatchObject({ status: "completed" });
		} else expect(await execution.settled()).toMatchObject({ status: "cancelled" });
	});
	it("start survives parent cancellation, while recursive invokes do not", async () => {
		const grandchild = defineWorkflow({
			name: "grandchild",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", "1h");
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
				await ctx.sleep("sleep", "1h");
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
		const owner = await enter(test);
		const leaf = await (await test.execution("grandchild")).suspended("sleep");
		const detached = await (await test.execution("background")).suspended("sleep");
		await (await test.execution("child")).suspended();
		await owner.suspended();
		await test.runtime.cancel(owner.id);
		expect(await (await test.execution("child")).settled()).toMatchObject({ status: "cancelled" });
		expect(await leaf.settled()).toMatchObject({ status: "cancelled" });
		expect(await detached.info()).toMatchObject({ status: "pending" });
		await db.clock.advance("1h");
		expect(await detached.settled()).toMatchObject({ status: "completed" });
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
				await ctx.sleep("pause", "1h");
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
		await (await test.execution("specific")).suspended("pause");
		await (await test.execution("observer")).output();
		expect(seen).toEqual(
			expect.arrayContaining([
				{ workflow: "specific", thread: true },
				{ workflow: "observer", thread: false },
			]),
		);
		expect(seen).toHaveLength(2);
		expect(test.ids("specific")).toHaveLength(1);
		expect(test.ids("observer")).toHaveLength(1);
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
		// Nothing was emitted, so redelivering the same message is still new and still ambiguous.
		await expect(
			test.chat.receive(message("tie"), { events: [event, "chat.mentioned"] }),
		).rejects.toThrow("Ambiguous owner");
		expect([...test.ids("owner"), ...test.ids("other")]).toEqual([]);
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
		await test.chat.receive(message("multi"), { events: [event, "chat.mentioned"] });
		await (await test.execution()).output();
		await (await test.execution("observer")).output();
		await test.chat.receive(message("multi"), { events: [event, "chat.mentioned"] });
		expect(test.ids("owner")).toHaveLength(1);
		expect(test.ids("observer")).toHaveLength(1);
	});
	it("treats missing prototype-named filter fields as absent", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event, filter: { toString: [{ exists: false }] } })],
			run: async () => "matched",
		});
		const test = setup([owner]);
		await test.app.start();
		await test.app.emit(event, {});
		expect(await (await test.execution()).output()).toBe("matched");
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
			run: async (input) => input,
		});
		const test = setup([owner]);
		await test.app.start();
		await test.app.emit("source.item", { level: "warning", path: "api/x", size: 6, enabled: true });
		await test.app.emit("source.item", { level: "error", path: "api/x", size: 6, enabled: {} });
		await test.app.emit("source.item", { level: "error", path: "api/x", size: 6, enabled: true });
		expect(await (await test.execution()).output()).toMatchObject({
			level: "error",
			enabled: true,
		});
		expect(test.ids("owner")).toHaveLength(1);
	});
});

describe("lazy channel threads", () => {
	it("does not create a channel thread without a post", async () => {
		const test = setup([
			defineWorkflow({
				name: "owner",
				triggers: [
					trigger({ event, replyTo: { kind: "channel", provider: "chat", channelId: "lazy" } }),
				],
				run: async (_i, ctx) => {
					await ctx.sleep("sleep", "1h");
				},
			}),
		]);
		await test.app.start();
		await test.app.emit(event, {});
		const execution = await (await test.execution()).suspended("sleep");
		expect(test.chat.threads.size).toBe(0);
		await db.clock.advance("1h");
		await execution.output();
		expect(test.chat.threads.size).toBe(0);
	});
});

describe("ingress tombstones and reservation recovery", () => {
	/** Stored `fabrial.approval.decided` events: an engine fact no workflow observes here. */
	const decided = async () => {
		const [row] = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = 'fabrial.approval.decided'
		`;
		return row!.count;
	};

	it("keeps the last 100 receipts across settlements; actions dedupe in the runtime", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		const thread = await test.chat.thread(surface);
		const settled = async () =>
			Boolean((await thread.getState())?.ingestedDedupeIds?.every((receipt) => !receipt.pending));
		await test.chat.receive(message("inbound-0"));
		await (await test.execution("owner", 0)).output();
		await eventually("the first receipt to settle", settled);
		// Fill the window with the receipts ingress writes, rather than running 99 more executions.
		await thread.updateState((state) => ({
			...state,
			ingestedDedupeIds: [
				...state!.ingestedDedupeIds!,
				...Array.from({ length: 99 }, (_, i) => ({
					kind: "message" as const,
					id: `inbound-${i + 1}`,
					at: state!.ingestedDedupeIds![0]!.at,
				})),
			],
		}));
		await test.chat.receive(message("inbound-100"));
		await (await test.execution("owner", 1)).output();
		await eventually("the newest receipt to settle", settled);
		expect((await thread.getState())?.ingestedDedupeIds).toHaveLength(100);
		expect((await thread.getState())?.ingestedDedupeIds?.[0]?.id).toBe("inbound-1");
		expect(await test.chat.receive(message("inbound-100"))).toBe("ignored");
		// Beyond the bounded window the runtime still dedupes; the orphaned slot must expire.
		expect(await test.chat.receive(message("inbound-0"))).toBe("new");
		await db.clock.advance("10m");
		const ref = { ...surface, messageId: "card" };
		for (let i = 0; i < 2; i++)
			await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
				dedupeId: "inbound-100",
			});
		expect(await decided()).toBe(1);
		await test.chat.receive(message("next"));
		await (await test.execution("owner", 2)).output();
		await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
			dedupeId: "inbound-100",
		});
		expect(await decided()).toBe(1);
		// The redelivered inbound-0 started nothing: the owners ran for inbound-0, inbound-100 and next.
		expect(test.ids("owner")).toHaveLength(3);
		const state = await thread.getState();
		expect(state?.ingestedDedupeIds).toHaveLength(100);
		expect(state?.ingestedDedupeIds?.every((receipt) => receipt.kind === "message")).toBe(true);
		state!.ingestedDedupeIds![0]!.id = "mutated snapshot";
		expect((await thread.getState())?.ingestedDedupeIds?.[0]?.id).not.toBe("mutated snapshot");
		await db.clock.advance("30d");
		expect(await thread.getState()).toBeNull();
	});
	it("uses the final updater decision when a lock retry sees an ingress tombstone", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		const thread = await test.chat.thread(surface);
		const update = thread.updateState.bind(thread);
		let retried = false;
		thread.updateState = async (fn) => {
			if (retried) return update(fn);
			retried = true;
			const speculative = fn(null);
			await update(() => speculative);
			return update(fn);
		};
		// The retry sees the pending receipt of its own attempt and dispatches its route once.
		expect(await test.chat.receive(message("retry"))).toBe("new");
		await (await test.execution()).output();
		expect(test.ids("owner")).toHaveLength(1);
		expect((await thread.getState())?.ingestedDedupeIds).toEqual([
			{ kind: "message", id: "retry", at: expect.any(Number) },
		]);
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
			run: async (_i, ctx) => ({ actor: ctx.actor?.id ?? null, thread: ctx.thread!.ref.threadId }),
		});
		const test = setup([owner], { plugins: [provider] });
		await test.app.start();
		await test.app.fetch(new Request("https://test/opened", { method: "POST" }));
		expect(await (await test.execution()).output()).toEqual({
			actor: "alice",
			thread: surface.threadId,
		});
		expect(test.metadataOf("owner")?.requestedBy?.id).toBe("alice");
	});
	// Pi clears the driver's trigger markers for tool children; they still answer in the driver's thread.
	it("gives a workflow invoked without trigger ownership the thread of its replyTo", async () => {
		const child = defineWorkflow({
			name: "child",
			run: async (_i, ctx) => ({
				thread: ctx.thread?.ref.threadId ?? null,
				owner: ctx.metadata.ownerWorkflow ?? null,
			}),
		});
		const test = setup([workflow(async () => null), child]);
		await test.app.start();
		const id = await test.runtime.invoke(child.name, null, {
			metadata: { ...metadata, replyTo: surface, triggerEvent: null, ownerWorkflow: null },
		});
		expect(await db.execution(id).output()).toEqual({ thread: surface.threadId, owner: null });
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
				if (ctx.metadata.replyTo?.threadId !== ref.threadId) throw new Error("Unbound metadata");
				await ctx.state(counter).update("update", (draft) => {
					draft.count++;
				});
				return ctx.invoke("child", child, {});
			},
		});
		const test = setup([owner, child], { agents: fakeAgents() });
		await test.app.start();
		await test.app.emit(event, {});
		expect(await (await test.execution()).output()).toBe(1);
	});
});

describe("agent port workflows", () => {
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
		expect(await db.execution(id).output()).toBe("agent-result");
		expect(seen).toEqual(["agent-run"]);
		// Terminal settlement is an engine event no app workflow waits on here.
		const settlements = () => db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = 'fabrial.execution.settled'
				and payload -> 'payload' ->> 'executionId' = ${id}
		`;
		await eventually("the settlement event", async () => (await settlements()).length > 0);
		expect(await settlements()).toHaveLength(1);
	});
});
