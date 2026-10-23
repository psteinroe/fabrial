import { metadataOf } from "../../../src/workflows/metadata.ts";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	defineEvent,
	definePlugin,
	trigger,
	type JsonObject,
	type WorkflowContext,
} from "../../internals.ts";
import { reply, scriptedModels } from "../../../src/testing/models.ts";
import { until } from "../../conductor/helper.ts";
import {
	alice,
	defineWorkflow,
	event,
	mention,
	message,
	once,
	metadata,
	plugin,
	surface,
	useCoreApp,
	workflow,
} from "./setup.ts";

const { db, setup, enter } = useCoreApp();

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
		expect(await test.chat.receive(message("initial"))).toBe("accepted");
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
			run: async () => ({ value: "explicit" }),
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
			run: async (input) => ({ value: input.value }),
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
	it("ignores bot messages and leaves no receipts", async () => {
		const test = setup([workflow(async () => {})]);
		await test.app.start();
		const bot = message("bot");
		bot.author.isBot = true;
		expect(await test.chat.receive(bot)).toBe("ignored");
		expect(await test.chat.receive(message("human"))).toBe("accepted");
		await (await test.execution()).output();
		expect(test.ids("owner")).toHaveLength(1);
		expect(await test.routes()).toHaveLength(1);
		// A handled thread is subscribed; routing stores nothing in it.
		const thread = await test.chat.thread(surface);
		expect(await thread.isSubscribed()).toBe(true);
		expect(await thread.getState()).toBeNull();
	});
	it("ignores a message nothing handles: no state, no subscription", async () => {
		const test = setup([
			defineWorkflow({
				name: "owner",
				triggers: [trigger({ event: "chat.mentioned" })],
				run: async () => {},
			}),
		]);
		await test.app.start();
		expect(await test.chat.receive(message("unhandled"))).toBe("accepted");
		expect(await test.route()).toMatchObject({ route: "ignored", owner: null });
		const thread = await test.chat.thread(surface);
		expect(await thread.isSubscribed()).toBe(false);
		expect(await thread.getState()).toBeNull();
		expect(test.ids("owner")).toEqual([]);
	});
});

describe("durability and context", () => {
	it("memoizes effects across replay; loops pass distinct keys", async () => {
		let effects = 0;
		const test = setup([
			workflow(async (_input, ctx) => {
				for (let i = 0; i < 3; i++) await ctx.step(`x:${i}`, () => ++effects);
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
	const outputs = (output: z.ZodType<JsonObject>, result: JsonObject) =>
		defineWorkflow({
			name: "owner",
			triggers: [trigger({ event })],
			retries: once,
			output,
			run: async () => result,
		});
	it("fails a workflow whose result doesn't match its output schema", async () => {
		const test = setup([outputs(z.object({ n: z.number().positive() }), { n: -1 })]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Invalid workflow owner output"),
		});
	});
	it("records a workflow's result as transformed by its output schema", async () => {
		const length = z
			.object({ text: z.string() })
			.transform(({ text }) => ({ length: text.length })) as unknown as z.ZodType<JsonObject>;
		const test = setup([outputs(length, { text: "four" })]);
		expect(await (await enter(test)).settled()).toEqual({
			status: "completed",
			output: { length: 4 },
		});
	});
	it("fails a workflow whose result is not a JSON object", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event })],
			retries: once,
			run: async () => "text" as never,
		});
		const test = setup([owner]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("expected a JSON object"),
		});
	});
	it("rejects input and output schemas that are not object schemas at definition", () => {
		const run = async () => {};
		expect(() => defineWorkflow({ name: "x", input: z.string() as never, run })).toThrow(
			"Workflow x: input must be an object schema (z.object), not string",
		);
		expect(() => defineWorkflow({ name: "x", output: z.array(z.string()) as never, run })).toThrow(
			"Workflow x: output must be an object schema (z.object), not array",
		);
		expect(() =>
			defineWorkflow({ name: "x", input: z.string().pipe(z.any()) as never, run }),
		).toThrow("Workflow x: input must be an object schema (z.object), not string");
		// Transforms, unions with an object and untyped schemas are checked on each value instead.
		const object = z.object({ text: z.string() });
		for (const schema of [object.transform((v) => v), z.union([object, z.string()]), z.any()])
			expect(() => defineWorkflow({ name: "x", input: schema as never, run })).not.toThrow();
	});
	it("invokes an inputless workflow without an argument; it runs with input {}", async () => {
		const child = defineWorkflow({ name: "child", run: async (input) => ({ input }) });
		const test = setup([workflow(async (_i, ctx) => ctx.invokeAndWait("child", child)), child]);
		expect(await (await enter(test)).output()).toEqual({ input: {} });
	});
	it("rejects empty operation ids", async () => {
		const test = setup([workflow(async (_i, ctx) => ctx.step("", () => 1))]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("explicit key"),
		});
	});
	it.each([
		["step", (ctx: WorkflowContext) => ctx.step("fabrial:receipt", () => 1)],
		["sleep", (ctx: WorkflowContext) => ctx.sleep("fabrial:wait", 1)],
	] as const)("rejects the reserved fabrial: key namespace (%s)", async (_name, run) => {
		const test = setup([workflow(async (_i, ctx) => run(ctx))]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("reserved"),
		});
	});
	it("addresses one operation per key: a repeated key replays its receipt", async () => {
		let runs = 0;
		const test = setup([
			workflow(async (_i, ctx) => [
				await ctx.step("x#1", () => ++runs),
				await ctx.step("x#1", () => ++runs),
			]),
		]);
		expect(await (await enter(test)).output()).toEqual([1, 1]);
	});
	it.each([
		["emit", (ctx: WorkflowContext) => ctx.emit("fabrial.ingress", {})],
		[
			"invoke",
			(ctx: WorkflowContext) => ctx.invoke("call", { name: "fabrial.approval.request" } as never),
		],
		[
			"signalWithStart",
			(ctx: WorkflowContext) =>
				ctx.signalWithStart("call", {
					task: { name: "fabrial.approval.decision" } as never,
					key: "k",
					signal: { name: "s", id: "1" },
				}),
		],
		[
			"sendSignal",
			(ctx: WorkflowContext) =>
				ctx.sendSignal(ctx.executionId, { name: "fabrial.approval.cancel", id: "1" }),
		],
	] as const)("rejects reserved destinations before persisting (%s)", async (_name, run) => {
		const test = setup([workflow(async (_i, ctx) => run(ctx))]);
		expect(await (await enter(test)).settled()).toMatchObject({
			status: "failed",
			error: expect.stringMatching(/Reserved|reserved/),
		});
		expect(test.ids("fabrial.approval.request")).toHaveLength(0);
		expect(test.ids("fabrial.approval.decision")).toHaveLength(0);
	});
	it("a failed child is catchable: invoke, then waitForResult", async () => {
		const child = defineWorkflow({
			name: "child",
			run: async () => {
				throw new Error("boom");
			},
			retries: { maxAttempts: 1 },
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				const outcome = await ctx.waitForResult(
					"result",
					await ctx.invoke("child", child, null, { cancelWithParent: true }),
				);
				return outcome.status;
			}),
			child,
		]);
		expect(await (await enter(test)).output()).toBe("failed");
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
								return await next();
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
		const models = scriptedModels();
		const model = models.model("test/model");
		model.respond([reply.text("echo")]);
		models.classifier("judge/classifier", () => {
			evaluations++;
			return { relevant: { probability: 1 } };
		});
		const test = setup(
			(f) => {
				const echo = f.defineAgent({
					name: "echo",
					description: "Echoes its input.",
					model: "test/model",
				});
				return [
					workflow(async (_i, ctx) => {
						const judged = await ctx.evaluate("evaluate", {
							model: "judge/classifier",
							state: {},
							questions: {
								relevant: {
									type: "bool",
									instructions: "Relevant?",
									criteria: { true: "Yes", false: "No" },
								},
							},
						});
						const answer = await ctx.agent("agent", echo, { input: "echo" });
						await ctx.sleep("sleep", "1h");
						return { answer, relevant: (judged.answers.relevant ?? null) as JsonObject | null };
					}),
				];
			},
			{ models },
		);
		const owner = await enter(test);
		await owner.suspended("sleep");
		await db.clock.advance("1h");
		expect(await owner.output()).toEqual({
			answer: "echo",
			relevant: { type: "bool", probability: 1 },
		});
		expect(evaluations).toBe(1);
		expect(model.calls).toBe(1);
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
				return { value: effects };
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
});

describe("children", () => {
	it("invokes children with metadata, validates schemas, and starts independently", async () => {
		const child = defineWorkflow({
			name: "child",
			input: z.object({ value: z.number() }),
			run: async (input, ctx) => {
				if (ctx.actor?.id !== "alice") throw new Error(`Unexpected actor ${ctx.actor?.id}`);
				return { value: input.value * 2 };
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				const result = await ctx.invokeAndWait("child", child, { value: 3 });
				const executionId = await ctx.invoke("background", child, { value: 4 });
				return { result, executionId };
			}),
			child,
		]);
		const output = (await (await enter(test)).output()) as {
			result: { value: number };
			executionId: string;
		};
		expect(output.result).toEqual({ value: 6 });
		expect(await db.execution(output.executionId).output()).toEqual({ value: 8 });
		expect(test.ids("child")).toHaveLength(2);
	});
	it.each(["invokeAndWait", "invoke"] as const)(
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
			expect(await (await enter(test)).settled()).toMatchObject({
				status: "failed",
				error: expect.stringContaining("Invalid child input"),
			});
			expect(test.ids("child")).toHaveLength(0);
		},
	);
	it("does not permit an alternate workflow object to bypass registered input validation", async () => {
		const registered = defineWorkflow({
			name: "registered",
			input: z.object({ count: z.number() }),
			run: async () => ({ value: "secret" }),
		});
		const impostor = defineWorkflow({
			name: "registered",
			input: z.object({}),
			run: async () => ({ value: "fake" }),
		});
		const test = setup([
			workflow(async (_i, ctx) => ctx.invokeAndWait("call", impostor, {})),
			registered,
		]);
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
			run: async () => ({ value: "secret" }),
		});
		const test = setup([workflow(async (_i, ctx) => ctx.invokeAndWait("other", other, {})), other]);
		expect(await (await enter(test)).output()).toEqual({ value: "secret" });
		expect(test.ids("other")).toHaveLength(1);
	});
	it.each([false, true])("structured cancellation, detached=%s", async (detached) => {
		// Native invoke: an attached child (`cancelWithParent`) is cancelled with its parent.
		const child = defineWorkflow({
			name: "child",
			input: z.object({}),
			run: async (_i, ctx) => {
				await ctx.sleep("sleep", "1h");
				return { value: "done" };
			},
		});
		const test = setup([
			workflow(async (_i, ctx) => {
				const id = await ctx.invoke("invoke", child, {}, { cancelWithParent: !detached });
				return ctx.waitForResult("result", id);
			}),
			child,
		]);
		const owner = await enter(test);
		const execution = await (await test.execution("child")).suspended("sleep");
		await owner.suspended();
		await test.client.cancel(owner.id);
		expect(await owner.settled()).toMatchObject({ status: "cancelled" });
		if (detached) {
			expect(await execution.info()).toMatchObject({ status: "pending" });
			await db.clock.advance("1h");
			expect(await execution.settled()).toMatchObject({ status: "completed" });
		} else expect(await execution.settled()).toMatchObject({ status: "cancelled" });
	});
	it("an independent child survives parent cancellation, attached ones do not", async () => {
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
			run: async (_i, ctx) =>
				ctx.waitForResult(
					"result",
					await ctx.invoke("grandchild", grandchild, {}, { cancelWithParent: true }),
				),
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
				await ctx.invoke("background", background, {});
				return ctx.waitForResult(
					"result",
					await ctx.invoke("child", child, {}, { cancelWithParent: true }),
				);
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
		await test.client.cancel(owner.id);
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
		await test.chat.receive(mention("multi"));
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
	it("fails the route of a dynamic cross-category ownership tie without starting anything", async () => {
		const a = workflow(async () => {});
		const b = defineWorkflow({
			name: "other",
			triggers: [trigger({ event: "chat.mentioned" })],
			run: async () => {},
		});
		const test = setup([a, b]);
		await test.app.start();
		expect(await test.chat.receive(mention("tie"))).toBe("accepted");
		// The thread's routing execution decides the message, and retries it.
		await until("the tie", async () => {
			const [row] = await db.sql<{ last_error: string | null }[]>`
				select last_error from pgconductor._private_executions
				where queue = ${test.client.queue} and task_key = 'fabrial.thread'`;
			return row?.last_error?.includes("Ambiguous owner") ? true : undefined;
		});
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
		await test.chat.receive(mention("multi"));
		await (await test.execution()).output();
		await (await test.execution("observer")).output();
		await test.chat.receive(mention("multi"));
		expect(test.ids("owner")).toHaveLength(1);
		expect(test.ids("observer")).toHaveLength(1);
	});
	it("treats missing prototype-named filter fields as absent", async () => {
		const owner = defineWorkflow({
			name: "owner",
			triggers: [trigger({ event, filter: { toString: [{ exists: false }] } })],
			run: async () => ({ value: "matched" }),
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

describe("ingress dedupe", () => {
	/** Stored events of `name`: an engine fact no workflow observes here. */
	const emitted = async (name: string) => {
		const [row] = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = ${name}
		`;
		return row!.count;
	};

	it("routes a redelivered multi-category message once; actions dedupe the same way", async () => {
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
		for (let i = 0; i < 2; i++) expect(await test.chat.receive(mention("multi"))).toBe("accepted");
		expect(await test.route()).toMatchObject({ route: "new", owner: { workflow: "owner" } });
		await (await test.execution()).output();
		await (await test.execution("observer")).output();
		// Redelivered after routing finished: still the one ingress event.
		await test.chat.receive(mention("multi"));
		expect(await emitted("fabrial.ingress")).toBe(1);
		expect(await test.routes()).toHaveLength(1);
		expect(test.ids("owner")).toHaveLength(1);
		expect(test.ids("observer")).toHaveLength(1);
		const ref = { ...surface, messageId: "card" };
		for (let i = 0; i < 2; i++)
			await test.chat.click(ref, "fabrial.approval.approve:a", alice.identities[0]!, {
				dedupeId: "click",
			});
		await test.route(1);
		// No such approval: the click is denied, once, and decides nothing.
		expect(await emitted("fabrial.approval.decided")).toBe(0);
		expect(test.chat.ephemeral).toHaveLength(1);
		expect(await test.routes()).toHaveLength(2);
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
		const test = setup([owner], { plugins: [plugin, provider] });
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
				owner: metadataOf(ctx).ownerWorkflow ?? null,
			}),
		});
		const test = setup([workflow(async () => null), child]);
		await test.app.start();
		const id = await test.client.invoke(child.name, null, {
			metadata: { ...metadata, replyTo: surface, triggerEvent: null, ownerWorkflow: null },
		});
		expect(await db.execution(id).output()).toEqual({ thread: surface.threadId, owner: null });
	});
	it("uses receipt-derived metadata for children after lazy channel posts", async () => {
		const test = setup(
			() => {
				const child = defineWorkflow({
					name: "child",
					input: z.object({}),
					run: async (_i, ctx) => ({ thread: metadataOf(ctx).replyTo?.threadId ?? null }),
				});
				const owner = defineWorkflow({
					name: "owner",
					triggers: [
						trigger({ event, replyTo: { kind: "channel", provider: "chat", channelId: "lazy" } }),
					],
					run: async (_i, ctx) => {
						const ref = await ctx.thread!.post("first", "first");
						if (metadataOf(ctx).replyTo?.threadId !== ref.threadId)
							throw new Error("Unbound metadata");
						const { thread } = await ctx.invokeAndWait("child", child, {});
						return { bound: thread === ref.threadId };
					},
				});
				return [owner, child];
			},
			{ models: scriptedModels() },
		);
		await test.app.start();
		await test.app.emit(event, {});
		expect(await (await test.execution()).output()).toEqual({ bound: true });
	});
});

describe("Pi workflows", () => {
	const hook = (seen: string[]) =>
		definePlugin({
			id: "hook",
			hooks: {
				workflow: [
					async (execution, ctx, next) => {
						seen.push(execution.workflow);
						return next();
					},
				],
			},
		});
	it("registers Pi's workflows once and wraps them in plugin middleware", async () => {
		const seen: string[] = [];
		const test = setup(
			(f) => {
				f.defineAgent({ name: "echo", description: "Echoes its input.", model: "test/model" });
				return [];
			},
			{ models: scriptedModels(), plugins: [hook(seen)] },
		);
		await test.app.start();
		const id = await test.client.invoke("fabrial.sessions.reclaim", null, { metadata });
		expect(await db.execution(id).output()).toMatchObject({ reclaimed: 0 });
		expect(seen).toEqual(["fabrial.sessions.reclaim"]);
	});
	it("decides at start: an agent defined after f.app() runs; defining one while the app runs throws, after stop works", async () => {
		const models = scriptedModels();
		models.model("test/model").respond([reply.text("late")]);
		let late: { kind: "fabrial.agent"; name: string } | undefined;
		const test = setup(
			[workflow(async (_i, ctx) => ctx.agent("agent", late!, { input: "hello" }))],
			{ models },
		);
		late = test.instance.defineAgent({
			name: "late",
			description: "Defined after the app.",
			model: "test/model",
		});
		expect(await (await enter(test)).output()).toBe("late");
		expect(() =>
			test.instance.defineAgent({ name: "later", description: "Too late.", model: "test/model" }),
		).toThrow("Agent later is defined after app.start()");
		expect(() =>
			test.instance.defineState({
				name: "later",
				scope: "session",
				schema: z.object({}),
				initial: () => ({}),
			}),
		).toThrow("State later is defined after app.start()");
		await test.app.stop();
		expect(
			test.instance.defineAgent({ name: "later", description: "In time.", model: "test/model" })
				.name,
		).toBe("later");
		expect(
			test.instance.defineState({
				name: "later",
				scope: "session",
				schema: z.object({}),
				initial: () => ({}),
			}).name,
		).toBe("later");
	});
});
