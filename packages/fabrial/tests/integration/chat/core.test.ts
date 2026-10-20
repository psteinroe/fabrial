// oxlint-disable typescript/unbound-method -- Referenced adapter methods are Vitest mocks.
import { createTestMessage } from "@chat-adapter/tests";
import { createMemoryState } from "@chat-adapter/state-memory";
import {
	createFabrial,
	defineEvent,
	definePlugin,
	trigger,
	type WorkflowContext,
} from "../../../src/index.ts";
import { z } from "zod";
import { testApp } from "../../../src/testing.ts";
import { expect, it, vi } from "vitest";
import { chat, chatTrigger } from "../../../src/chat/index.ts";
import { setup } from "../../chat/helper.ts";
import { until, useConductor } from "../../conductor/helper.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });
const demo = definePlugin({ id: "demo", events: { work: defineEvent({ payload: z.object({}) }) } });

/** Real Conductor in real time: these tests never move the clock. */
const db = useConductor();
/** Names of the events emitted so far, from the rows Conductor dispatches them from. */
const emitted = async () =>
	(
		await db.sql<{ name: string }[]>`
			select payload ->> 'eventKey' as name from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' order by created_at`
	).map((row) => row.name);
/** Every execution of the app on `queue` settled, after every event was dispatched. */
const settled = (queue: string) =>
	until("events to dispatch and executions to settle", async () => {
		const [row] = await db.sql<{ open: number }[]>`
			select count(*)::int as open from pgconductor._private_executions
			where (task_key = 'pgconductor.event-dispatch' or queue = ${queue})
				and task_key != 'pgconductor.maintenance' and completed_at is null and failed_at is null`;
		return row!.open === 0 ? true : undefined;
	});

it("dispatches a mention to its owner and all matching observers without duplicate runs", async () => {
	const env = setup();
	const runtime = db.runtime();
	const owner = vi.fn(async () => {});
	const observer = vi.fn(async () => {});
	const app = testApp(createFabrial({ plugins: env.host.plugins }), {
		runtime,
		chat: chat({ state: createMemoryState() }),
		workflows: [
			defineWorkflow({ name: "owner", triggers: [chatTrigger("slack", "mentioned")], run: owner }),
			defineWorkflow({
				name: "observer",
				triggers: [
					chatTrigger("slack", "message", { observe: true }),
					chatTrigger("slack", "mentioned", { observe: true }),
				],
				run: observer,
			}),
		],
	});
	await app.start();
	try {
		await env.sdk().processMessage(
			env.adapter,
			"slack:C1:root",
			createTestMessage("root", "hello", {
				isMention: true,
				author: { userId: "U1", userName: "alice", fullName: "Alice", isMe: false, isBot: false },
			}),
		);
		await until("the owner and the observer to run", async () =>
			owner.mock.calls.length && observer.mock.calls.length ? true : undefined,
		);
		await settled(runtime.queue);
		expect(owner).toHaveBeenCalledOnce();
		expect(observer).toHaveBeenCalledOnce();
		expect(
			(await emitted())
				.filter((name) => name.startsWith("slack."))
				.sort((a, b) => a.localeCompare(b)),
		).toEqual(["slack.mentioned", "slack.message"]);
	} finally {
		await app.stop();
	}
});

it("rebinds a lazy channel from the durable first-post receipt on replay and child invocation", async () => {
	const env = setup();
	const runtime = db.runtime();
	const child = defineWorkflow({
		name: "child",
		input: z.null(),
		run: async (_input, ctx) => {
			expect(ctx.metadata.replyTo).toEqual({
				kind: "thread",
				provider: "slack",
				threadId: "slack:C1:root-1",
			});
			return null;
		},
	});
	const refs: string[] = [];
	const app = testApp(createFabrial({ plugins: [...env.host.plugins, demo] }), {
		runtime,
		chat: chat({ state: createMemoryState() }),
		workflows: [
			child,
			defineWorkflow({
				name: "owner",
				triggers: [trigger({ event: "demo.work" })],
				retries: { maxAttempts: 1 },
				run: async (_input, ctx: WorkflowContext) => {
					const receipt = await ctx.thread!.post("root", "hello");
					refs.push(ctx.thread!.ref.threadId);
					expect(ctx.thread!.ref.threadId).toBe(receipt.threadId);
					// A real second: the handler replays after the sleep.
					await ctx.sleep("pause", 1000);
					await ctx.thread!.post("reply", "continued");
					await ctx.invoke("child", child, null);
				},
			}),
		],
	});
	await app.start();
	try {
		await app.emit(
			"demo.work",
			{},
			{ replyTo: { kind: "channel", provider: "slack", channelId: "C1" } },
		);
		const owner = await until(
			"the owner to run",
			async () =>
				(
					await db.sql<{ id: string }[]>`
					select id from pgconductor._private_executions
					where queue = ${runtime.queue} and task_key = 'owner'`
				)[0]?.id,
		);
		expect(await db.execution(owner).settled()).toEqual({ status: "completed", output: null });
		// The sleep replays the handler; whether the child's wait replays it again is up to the engine.
		expect(refs.length).toBeGreaterThanOrEqual(2);
		expect(new Set(refs)).toEqual(new Set(["slack:C1:root-1"]));
		expect(env.adapter.postChannelMessage).toHaveBeenCalledOnce();
		expect(env.adapter.postMessage).toHaveBeenCalledExactlyOnceWith("slack:C1:root-1", "continued");
	} finally {
		await app.stop();
	}
});
