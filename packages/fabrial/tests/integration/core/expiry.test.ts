import { describe, expect, it } from "vitest";
import { z } from "zod";
import { trigger } from "../../../src/index.ts";
import type { Json, JsonObject, RuntimeWorkflow } from "../../../src/index.ts";
import {
	alice,
	approvalWorkflow,
	defineWorkflow,
	metadata,
	outsider,
	surface,
	team,
	useCoreApp,
	workflow,
} from "./setup.ts";

const { db, setup, enter, card, eventually } = useCoreApp();

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
	// PHASE2(task 9): wraps the cleanup handler's waits, which replay on Conductor where the in-memory
	// engine resumed them in place; task 9 replaces the cleanup workflow and this test.
	it.skip("cleanup catches presentations and settlement emitted between successive waits", async () => {
		const test = setup([approvalWorkflow()]);
		let extra: Awaited<ReturnType<typeof test.chat.openDM>> | undefined;
		// Wrap the cleanup handler as it is registered, before the runtime starts.
		const register = test.runtime.register.bind(test.runtime);
		test.runtime.register = (definitions) =>
			register({
				...definitions,
				workflows: definitions.workflows.map((each): RuntimeWorkflow =>
					each.name !== "fabrial.cleanup"
						? each
						: {
								...each,
								handler: (input, execution) =>
									each.handler(input, {
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
												await execution.emit("fabrial.execution.settled", {
													executionId: data.executionId!,
												});
											}
											return result;
										},
									}),
							},
				),
			});
		await enter(test);
		await eventually("the extra card to be cancelled", () => {
			const content = extra?.posts[0]?.content;
			return typeof content === "object" && "card" in content && content.card.text === "Cancelled";
		});
		expect(extra!.posts[0]!.content).toMatchObject({ card: { text: "Cancelled", actions: [] } });
		const [cleanup] = await executions(test.runtime.queue, "fabrial.cleanup");
		expect((await db.execution(cleanup!).settled()).status).toBe("completed");
	});

	it.each(["first", "second"])("cleanup leaves the thread's routing state alone (%s)", async () => {
		const test = setup([]);
		const thread = await test.chat.thread(surface);
		const ref = await thread.post("hello");
		await thread.updateState(() => ({
			statusMessageId: null,
			follows: [{ session: "chat:thread", agent: "support" }],
		}));
		const state = await thread.getState();
		await test.app.start();
		// Without `after`, cleanup starts from the current cursor: emit once it waits.
		const id = await test.runtime.invoke(
			"fabrial.cleanup",
			{ executionId: "old", presentationId: "p" },
			{ metadata: { ...metadata, interactionId: "old" } },
		);
		await db.execution(id).suspended();
		await test.runtime.emit(
			"fabrial.presentation",
			{ presentationId: "p", ref: ref as unknown as Json },
			{ metadata },
		);
		await test.runtime.emit("fabrial.execution.settled", { executionId: "old" }, { metadata });
		expect((await db.execution(id).settled()).status).toBe("completed");
		expect(await thread.getState()).toEqual(state);
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
		const runtime = await db.start([
			{
				name: "waiter",
				triggers: [],
				handler: async (_i, execution) => {
					await execution.sleep("delay", early);
					return (await execution.waitForAny("timer", { timer: { kind: "timer", at } })).kind;
				},
			},
		]);
		const execution = db.execution(await runtime.invoke("waiter", {}, { metadata }));
		await execution.suspended("delay");
		await db.clock.advance(early);
		await execution.suspended();
		expect((await execution.info())?.status).toBe("pending");
		await db.clock.advance(timeout - early);
		expect(await execution.output()).toBe("timer");
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

	// PHASE2(task 9): Conductor suspends the attempt at its first wait, so an emit later in the same
	// attempt never runs while Fabrial's custom race waits. Re-enable with the native race.
	it.todo("does not lose an event emitted after registration while the attempt is still running");
});
