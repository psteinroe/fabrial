import { describe, expect, it } from "vitest";
import { z } from "zod";
import { trigger } from "../../../src/index.ts";
import {
	alice,
	approvalWorkflow,
	bob,
	defineWorkflow,
	metadata,
	outsider,
	team,
	useCoreApp,
	workflow,
} from "./setup.ts";

const { db, setup, enter, card, eventually } = useCoreApp();
const cardOf = (content: unknown) => (content as { card: { text?: string } }).card;

/** Executions of `workflow` on `queue`, oldest first: internal workflows are not recorded. */
async function executions(queue: string, workflow: string) {
	const rows = await db.sql<{ id: string }[]>`
		select id from pgconductor._private_executions
		where queue = ${queue} and task_key = ${workflow} order by created_at
	`;
	return rows.map((row) => row.id);
}
/** Every event emitted so far was dispatched. */
const dispatched = () =>
	eventually("events to dispatch", async () => {
		const rows = await db.sql`
			select 1 from pgconductor._private_executions
			where queue = 'pgconductor.internal' and task_key = 'pgconductor.event-dispatch'
				and completed_at is null and failed_at is null
		`;
		return rows.length === 0;
	});

describe("cleanup workflow", () => {
	// Replaces "cleanup catches presentations and settlement emitted between successive waits": each
	// card has its own cleanup, which posts the card itself, so there is no presentation stream to miss.
	it("cleans up each posted card when the caller is cancelled before every card is posted", async () => {
		const test = setup([approvalWorkflow()]);
		const bobDM = await test.chat.openDM(bob.identities[0]!);
		const post = bobDM.post.bind(bobDM);
		let release!: () => void;
		const released = new Promise<void>((resolve) => (release = resolve));
		bobDM.post = async (content) => {
			await released;
			return post(content);
		};
		const owner = await enter(test);
		const c = await card(test);
		await test.runtime.cancel(owner.id);
		expect(await owner.settled()).toMatchObject({ status: "cancelled" });
		await eventually("alice's cancelled card", () => cardOf(c.post.content).text === "Cancelled");
		release();
		await eventually(
			"bob's cancelled card",
			() => !!bobDM.posts[0] && cardOf(bobDM.posts[0].content).text === "Cancelled",
		);
		const cleanups = await executions(test.runtime.queue, "fabrial.cleanup");
		expect(cleanups).toHaveLength(3);
		for (const id of cleanups) expect((await db.execution(id).settled()).status).toBe("completed");
	});
});

// Approvals wait on events and a timer at once. The test clock keeps running from where it was set, so
// the timeouts leave room to deliver the card before the clock moves them to the exact deadline.
describe("expiry and timers", () => {
	const timeout = 60_000;
	const early = 54_000;

	it("expires and renders cards without actions", async () => {
		const test = setup([approvalWorkflow(timeout)]);
		const execution = await enter(test);
		const c = await card(test);
		await execution.suspended();
		await db.clock.advance(timeout);
		expect(await execution.output()).toBe("expired");
		await eventually("the card to expire", () => c.post.updates.length > 0);
		expect(c.post.content).toMatchObject({ card: { text: "Expired", actions: [] } });
	});

	it("does not extend expiry after invalid decisions and ignores late clicks", async () => {
		const test = setup([approvalWorkflow(timeout)]);
		const execution = await enter(test);
		const c = await card(test);
		await execution.suspended();
		await db.clock.advance(early);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await execution.suspended();
		// Unsettled; Fabrial's race poll can be re-claiming it while the clock ticks.
		expect((await execution.info())?.status).not.toMatch(/completed|failed|cancelled/);
		await db.clock.advance(timeout - early);
		expect(await execution.output()).toBe("expired");
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await dispatched();
		await eventually("the card to expire", () => c.post.updates.length > 0);
		expect(await execution.output()).toBe("expired");
		expect(c.post.updates).toHaveLength(1);
	});

	it("does not restart a race timer after an invalid approval", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const approval = await ctx.requestApproval("a", {
					title: "Proposal",
					details: {},
					approvers: team,
					timeout: "1h",
				});
				return (await ctx.race("r", { approval: approval.decision(), timer: ctx.timer(timeout) }))
					.key;
			}),
		]);
		const execution = await enter(test);
		const c = await card(test);
		await execution.suspended();
		await db.clock.advance(early);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await execution.suspended();
		expect((await execution.info())?.status).not.toMatch(/completed|failed|cancelled/);
		await db.clock.advance(timeout - early);
		expect(await execution.output()).toBe("timer");
	});

	it("fires timers at absolute at even when registration is delayed", async () => {
		const at = db.clock.now().getTime() + timeout;
		const runtime = await db.start(
			[
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						await execution.sleep("delay", early);
						// The timeout starts at the first wait: computed from the absolute deadline.
						const never = await execution.subscribe("never", { event: "test.never" });
						return (
							await execution.waitForAny(
								"timer",
								{ never },
								{ timeoutMs: Math.max(0, at - db.clock.now().getTime()) },
							)
						).kind;
					},
				},
			],
			{ events: [{ name: "test.never", filterable: [] }] },
		);
		const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
		await execution.suspended("delay");
		await db.clock.advance(early);
		await execution.suspended();
		expect((await execution.info())?.status).toBe("pending");
		await db.clock.advance(timeout - early);
		expect(await execution.output()).toBe("timeout");
	});

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
		const execution = await test.execution("owner");
		await execution.output();
		expect(test.chat.threads.size).toBe(1);
		const thread = [...test.chat.threads.values()][0]!;
		expect(thread.posts.map((p) => p.content)).toEqual(["updated", "second", "child"]);
		expect(test.metadataOf("child")?.replyTo).toEqual(thread.ref);
		expect(await thread.getState()).toBeNull();
	});

	it("does not lose an event emitted after registration while the attempt is still running", async () => {
		const runtime = await db.start(
			[
				{
					name: "waiter",
					triggers: [],
					handler: async (_i, execution) => {
						const ping = await execution.subscribe("ping", { event: "test.ping" });
						await execution.step("emit", () => execution.emit("test.ping", { n: 1 }));
						const winner = await execution.waitForAny("wait", { ping }, { timeoutMs: 60_000 });
						return winner.kind === "event" ? winner.event.payload : null;
					},
				},
			],
			{ events: [{ name: "test.ping", filterable: [] }] },
		);
		const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
		expect(await execution.output()).toEqual({ n: 1 });
	});
});
