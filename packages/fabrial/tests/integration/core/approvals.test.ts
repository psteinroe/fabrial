import { describe, expect, it } from "vitest";
import { defineGroup, defineUser } from "../../internals.ts";
import type { DurableExecution, Json, RuntimeWorkflow, WorkflowContext } from "../../internals.ts";
import { until } from "../../conductor/helper.ts";
import {
	alice,
	approvalWorkflow,
	bob,
	event,
	identity,
	message,
	outsider,
	surface,
	team,
	useCoreApp,
	workflow,
} from "./setup.ts";

const { db, setup, enter, card, eventually } = useCoreApp();
const DAY = 24 * 60 * 60 * 1000;

/**
 * A runtime whose approval decision executions pause on their first attempt until `release`: before their
 * handler runs (`start`), or before their decision wait registers (`wait`), as after a crash there.
 */
function paused(at: "start" | "cards") {
	const runtime = db.runtime();
	const open = Promise.withResolvers<void>();
	const reached = Promise.withResolvers<void>();
	const pause = async () => {
		reached.resolve();
		await open.promise;
	};
	const hold = (definition: RuntimeWorkflow): RuntimeWorkflow =>
		definition.name !== "fabrial.approval.decision"
			? definition
			: {
					...definition,
					handler: async (input, execution) => {
						if (at === "start") {
							await pause();
							return definition.handler(input, execution);
						}
						// After the cards started, before the decision's wait computes its timeout.
						const start: DurableExecution["start"] = async (id, ...rest) => {
							if (id === "delivery") await pause();
							return execution.start(id, ...rest);
						};
						return definition.handler(
							input,
							Object.assign(Object.create(execution) as DurableExecution, { start }),
						);
					},
				};
	const register = runtime.register.bind(runtime);
	runtime.register = (definition) =>
		register({ ...definition, workflows: definition.workflows.map(hold) });
	return { runtime, reached: reached.promise, release: () => open.resolve() };
}
const cardOf = (content: unknown) =>
	(content as { card: { text?: string; actions: { id: string }[] } }).card;

describe("approvals", () => {
	it.each(["approved", "rejected"])("handles %s and duplicate clicks", async (status) => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		const action = status === "approved" ? c.approve : c.reject;
		await test.chat.click(c.post.ref, action, alice.identities[0]!, { dedupeId: "click" });
		await test.chat.click(c.post.ref, action, alice.identities[0]!, { dedupeId: "click" });
		expect(await owner.output()).toBe(status);
		await eventually("the card update", () => c.post.updates.length > 0);
		expect(c.post.updates).toHaveLength(1);
	});
	it("delivers a click exactly once when its ingress emit fails and the provider retries", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		const emit = test.runtime.emit.bind(test.runtime);
		let failures = 1;
		test.runtime.emit = async (...args) => {
			if (args[0] === "fabrial.ingress" && failures-- > 0) throw new Error("emit failed");
			return emit(...args);
		};
		await expect(
			test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { dedupeId: "click" }),
		).rejects.toThrow("emit failed");
		await owner.suspended();
		for (let i = 0; i < 2; i++)
			await test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { dedupeId: "click" });
		expect(await owner.output()).toBe("approved");
		await eventually("the card update", () => c.post.updates.length > 0);
		expect(c.post.updates).toHaveLength(1);
	});
	it("rejects non-approvers ephemerally and continues waiting", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await owner.suspended();
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		expect(await owner.output()).toBe("approved");
	});
	it("requires assigned approvers to still be members", async () => {
		let members = [alice, bob];
		const rotation = defineGroup({ id: "rotation-removed", resolve: () => members });
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(
						await ctx.waitForApproval("a", {
							title: "Approval",
							details: "x",
							approvers: rotation,
							timeout: "1h",
						})
					).status,
			),
		]);
		const owner = await enter(test);
		const c = await card(test);
		members = [bob];
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await owner.suspended();
		await db.clock.advance("1h");
		expect(await owner.output()).toBe("expired");
	});
	it("allows only the requester to cancel using the status control", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const thread = await test.chat.thread(surface);
		const post = await until("the status card", async () => thread.posts[0]);
		const action = cardOf(post.content).actions[0]!.id;
		await test.chat.click(post.ref, action, bob.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await test.chat.click(post.ref, action, alice.identities[0]!);
		expect(await owner.output()).toBe("cancelled");
	});
	it("disables requester controls", async () => {
		const test = setup([approvalWorkflow(100, false)]);
		await enter(test);
		const thread = await test.chat.thread(surface);
		const post = await until("the status card", async () => thread.posts[0]);
		expect(post.content).toMatchObject({ card: { actions: [] } });
	});
	it("cancellation of a suspended workflow updates pending cards", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		await owner.suspended();
		await test.runtime.cancel(owner.id);
		expect(await owner.settled()).toMatchObject({ status: "cancelled" });
		await eventually("the cancelled card", () => cardOf(c.post.content).text === "Cancelled");
		expect(cardOf(c.post.content).actions).toEqual([]);
	});
	it("asserts approval timeouts below 30 days", async () => {
		const test = setup([approvalWorkflow("30d")]);
		const owner = await enter(test);
		expect(await owner.settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("30 days"),
		});
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
		const owner = await enter(test);
		expect(await owner.output()).toBe("approved");
	});
	it("authorizes the group's current members: a member added after the request may approve", async () => {
		let members = [alice];
		const group = defineGroup({ id: "rotation-added", resolve: () => members });
		const test = setup([
			workflow(async (_i, ctx) => {
				const decision = await ctx.waitForApproval("approval", {
					title: "Proposal",
					details: {},
					approvers: group,
					timeout: "1h",
				});
				return [decision.status, decision.decidedBy?.id ?? null];
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await owner.suspended();
		members = [bob];
		// Cards went to the members when the request was made; authority follows the group.
		expect((await test.chat.openDM(bob.identities[0]!)).posts).toHaveLength(0);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		expect(await owner.output()).toEqual(["approved", "bob"]);
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
				await ctx.sleep("after", "1h");
				return [first.status, second.status];
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await owner.suspended("after");
		await db.clock.advance("1h");
		expect(await owner.output()).toEqual(["approved", "approved"]);
		await eventually("the card update", () => c.post.updates.length > 0);
		expect(c.post.updates).toHaveLength(1);
	});
	it("updates every approver card and originating status exactly once across later replay", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.waitForApproval("approval", { title: "Proposal", details: {}, approvers: team });
				await ctx.sleep("after", "1h");
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { dedupeId: "one" });
		await owner.suspended("after");
		await test.chat.click(c.post.ref, c.reject, bob.identities[0]!, { dedupeId: "different" });
		await db.clock.advance("1h");
		await owner.output();
		const posts = () => [...test.chat.threads.values()].flatMap((thread) => thread.posts);
		await eventually("every card update", () => posts().every((post) => post.updates.length > 0));
		expect(posts()).toHaveLength(3);
		for (const post of posts()) {
			expect(post.updates).toHaveLength(1);
			expect(post.content).toMatchObject({ card: { text: "Approved by Alice", actions: [] } });
		}
	});
	it("records a decision whose approver card was deleted and relabels the remaining cards", async () => {
		const warnings: string[] = [];
		const logger = { debug() {}, info() {}, error() {}, warn: (m: string) => warnings.push(m) };
		const test = setup([approvalWorkflow()], { logger });
		const owner = await enter(test);
		const aliceDM = await test.chat.openDM(alice.identities[0]!);
		const bobDM = await test.chat.openDM(bob.identities[0]!);
		await until("both cards", async () => (aliceDM.posts[0] && bobDM.posts[0]) || undefined);
		aliceDM.posts.splice(0);
		const approve = cardOf(bobDM.posts[0]!.content).actions[0]!.id;
		await test.chat.click(bobDM.posts[0]!.ref, approve, bob.identities[0]!);
		expect(await owner.output()).toBe("approved");
		const status = (await test.chat.thread(surface)).posts[0]!;
		await eventually("the relabelled cards", () =>
			[bobDM.posts[0]!, status].every((post) => cardOf(post.content).text === "Approved by Bob"),
		);
		expect(cardOf(status.content).actions).toEqual([]);
		await eventually("the failed update warning", () =>
			warnings.includes("Approval card update failed"),
		);
	});
	it("resumes card updates after an interruption without repeating or cancelling them", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		const bobDM = await test.chat.openDM(bob.identities[0]!);
		await until("bob's card", async () => bobDM.posts[0]);
		const update = bobDM.update.bind(bobDM);
		let interrupted = false;
		bobDM.update = async (...args) => {
			if (!interrupted) {
				interrupted = true;
				throw new Error("interrupted");
			}
			return update(...args);
		};
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
		const posts = () => [...test.chat.threads.values()].flatMap((thread) => thread.posts);
		await eventually("every card update", () => posts().every((post) => post.updates.length > 0));
		expect(interrupted).toBe(true);
		for (const post of posts()) {
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
		const owner = await enter(test);
		expect(await owner.settled()).toMatchObject({ status: "failed" });
		// The card may already be cancelled, without actions, by the time it is read.
		const post = () => [...test.chat.threads.values()].flatMap((thread) => thread.posts)[0];
		await eventually(
			"the cancelled card",
			() => post() !== undefined && cardOf(post()!.content).text === "Cancelled",
		);
		expect(cardOf(post()!.content).actions).toEqual([]);
	});
	it("finishes an in-flight card step then cancels and cleans up its receipt", async () => {
		const test = setup([approvalWorkflow()]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = dm.post.bind(dm);
		dm.post = async (content) => {
			const receipt = await post(content);
			await test.runtime.cancel((await test.execution()).id);
			return receipt;
		};
		const owner = await enter(test);
		expect(await owner.settled()).toMatchObject({ status: "cancelled" });
		await eventually("the cancelled card", () => cardOf(dm.posts[0]?.content).text === "Cancelled");
		expect(cardOf(dm.posts[0]!.content).actions).toEqual([]);
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
			// Sequential delivery would never start the second card while the first one waits here.
			await until("the second card's delivery", async () => secondStarted || undefined, 5_000);
			return firstPost(content);
		};
		await enter(test);
		await eventually("both cards", () => first.posts.length === 1 && second.posts.length === 1);
	});
	it("catches a valid decision emitted while rejecting an invalid click", async () => {
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		const notice = test.chat.postEphemeral.bind(test.chat);
		test.chat.postEphemeral = async (...args) => {
			await notice(...args);
			await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		};
		await test.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		expect(await owner.output()).toBe("approved");
	});
	it.each(["title", "details"] as const)("rejects changed approval %s on replay", async (field) => {
		let changed = false;
		const test = setup([
			workflow(
				async (_i, ctx) =>
					(
						await ctx.waitForApproval("a", {
							title: changed && field === "title" ? "Changed" : "Proposal",
							details: changed && field === "details" ? { sql: "delete" } : { sql: "select" },
							approvers: team,
						})
					).status,
			),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await owner.suspended();
		changed = true;
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.settled()).toMatchObject({
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
		const owner = await enter(test);
		const c = await card(test);
		await owner.suspended();
		changed = true;
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
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
			const owner = await test.execution();
			// Cards are posted by their cleanups: an unreachable approver fails only its card.
			expect(await owner.settled()).toMatchObject({ status: "completed", output: "requested" });
			const cleanup = await until("the card's cleanup", async () => {
				const [row] = await db.sql<{ id: string }[]>`
					select id from pgconductor._private_executions
					where queue = ${test.runtime.queue} and task_key = 'fabrial.cleanup' order by created_at`;
				return row;
			});
			// Its retries back off on the test clock: the first failed attempt is enough here.
			if (unsupported)
				await until("the card's delivery failure", async () =>
					(await db.execution(cleanup!.id).info())?.error?.includes(
						"no identity whose provider can open DMs",
					)
						? true
						: undefined,
				);
			else expect((await db.execution(cleanup!.id).settled()).status).toBe("completed");
			expect(attempts.slice(0, 2)).toEqual(["github", "slack"]);
		},
	);
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
		const owner = await enter(test);
		expect(await owner.output()).toEqual(["approved", "approved"]);
		expect(dm.posts).toHaveLength(2);
	});
});

describe("native approval waits", () => {
	const cleanups = (queue: string) =>
		db.sql<{ id: string; failed: boolean }[]>`
			select id, failed_at is not null as failed from pgconductor._private_executions
			where queue = ${queue} and task_key = 'fabrial.cleanup' order by created_at`;

	it("decides on a click before every card is posted and labels the later cards with it", async () => {
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
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
		release();
		await eventually(
			"bob's late card",
			() => !!bobDM.posts[0] && cardOf(bobDM.posts[0].content).text === "Approved by Alice",
		);
		expect(cardOf(bobDM.posts[0]!.content).actions).toEqual([]);
	});

	// Cleanup retries back off on the test clock: advance it until every unreachable card gives up.
	const givenUp = (queue: string, count: number) =>
		until("the unreachable cards to be given up", async () => {
			if ((await cleanups(queue)).filter((row) => row.failed).length >= count) return true;
			await db.clock.advance("1m");
			return undefined;
		});

	it("decides with the cards that were delivered when one approver cannot be reached", async () => {
		const warnings: string[] = [];
		const logger = { debug() {}, info() {}, error() {}, warn: (m: string) => warnings.push(m) };
		const test = setup([approvalWorkflow()], { logger });
		const open = test.chat.openDM.bind(test.chat);
		test.chat.openDM = async (identity) => {
			if (identity.subjectId === bob.identities[0]!.subjectId) throw new Error("No DM support");
			return open(identity);
		};
		const owner = await enter(test);
		const c = await card(test);
		await eventually("the delivery warning", () =>
			warnings.includes("Approval card delivery failed"),
		);
		// Bob's cleanup is retried, then gives up; the approval keeps waiting on Alice's card.
		await givenUp(test.runtime.queue, 1);
		await owner.suspended();
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
		await eventually(
			"alice's relabelled card",
			() => cardOf(c.post.content).text === "Approved by Alice",
		);
		expect(await cleanups(test.runtime.queue)).toHaveLength(3);
	});

	it("fails at once when no approver can be reached", async () => {
		const test = setup([approvalWorkflow()]);
		test.chat.openDM = async () => {
			throw new Error("No DM support");
		};
		const started = db.clock.now().getTime();
		const owner = await enter(test);
		const thread = await test.chat.thread(surface);
		await until("the status card", async () => thread.posts[0]);
		// The requester's status card was delivered, but it is not an approver card.
		await givenUp(test.runtime.queue, 2);
		expect(await owner.settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("Approval failed: No approver could be reached"),
		});
		// Long before the 24h deadline.
		expect(db.clock.now().getTime() - started).toBeLessThan(60 * 60_000);
		await eventually(
			"the relabelled status card",
			() => cardOf(thread.posts[0]!.content).text === "No approver could be reached",
		);
		expect(cardOf(thread.posts[0]!.content).actions).toEqual([]);
	});

	it("races an approval without closing it and never extends its deadline across resumes", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const approval = await ctx.requestApproval("a", {
					title: "Proposal",
					details: {},
					approvers: team,
					timeout: 60_000,
				});
				const first = await ctx.race("r", {
					approval: approval.decision(),
					timer: ctx.timer(10_000),
				});
				return [first.key, (await approval.wait("again")).status];
			}),
		]);
		const owner = await enter(test);
		await card(test);
		await owner.suspended();
		await db.clock.advance(10_000);
		await owner.suspended("fabrial:again:wait");
		await db.clock.advance(50_000);
		expect(await owner.output()).toEqual(["timer", "expired"]);
		const [row] = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_custom_event_subscriptions
			where execution_id = ${owner.id}::uuid`;
		expect(row!.count).toBe(0);
	});

	it("cancels an approval whose decision execution has not run yet", async () => {
		const decision = paused("start");
		const test = setup(
			[
				workflow(async (_i, ctx) => {
					const approval = await ctx.requestApproval("a", {
						title: "Proposal",
						details: {},
						approvers: team,
						timeout: "1h",
					});
					await approval.cancel("cancel", "Withdrawn");
					const { status, comment } = await approval.wait("decision");
					return [status, comment ?? null];
				}),
			],
			{ runtime: decision.runtime },
		);
		const owner = await enter(test);
		await decision.reached;
		// The cancel is sent and the caller waits, before the decision execution first runs.
		await owner.suspended("fabrial:decision:wait");
		decision.release();
		expect(await owner.output()).toEqual(["cancelled", "Withdrawn"]);
		const dm = await test.chat.openDM(alice.identities[0]!);
		await eventually("the cancelled card", () => cardOf(dm.posts[0]?.content).text === "Cancelled");
	});

	it("returns a decision first awaited after maintenance ran past 60 days", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				const approval = await ctx.requestApproval("a", {
					title: "Proposal",
					details: {},
					approvers: team,
					timeout: "1h",
				});
				await ctx.sleep("later", 90 * DAY);
				return (await approval.wait("decision")).status;
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await eventually("the final card", () => c.post.updates.length > 0);
		await owner.suspended("later");
		await db.clock.advance(61 * DAY);
		// Conductor's maintenance: removes settled executions older than their task's retention.
		const removed = await db.sql<{ task_key: string }[]>`
			delete from pgconductor._private_executions e using pgconductor._private_tasks t
			where t.key = e.task_key and t.queue = e.queue and e.queue = ${test.runtime.queue}
			and ((e.completed_at is not null and t.remove_on_complete_days > 0
					and e.completed_at < pgconductor._private_current_time() - t.remove_on_complete_days * interval '1 day')
				or (e.failed_at is not null and t.remove_on_fail_days > 0
					and e.failed_at < pgconductor._private_current_time() - t.remove_on_fail_days * interval '1 day'))
			returning e.task_key`;
		expect(removed.map((row) => row.task_key)).toContain("fabrial.approval.request");
		await db.clock.advance(29 * DAY);
		expect(await owner.output()).toBe("approved");
	});

	it("denies a click after the deadline while the decision has not registered its timeout", async () => {
		const decision = paused("cards");
		const test = setup([approvalWorkflow("1h")], { runtime: decision.runtime });
		const owner = await enter(test);
		await decision.reached;
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = await until("alice's approval card", async () => dm.posts[0]);
		await db.clock.advance(60 * 60 * 1000 + 1000);
		await test.chat.click(post.ref, cardOf(post.content).actions[0]!.id, alice.identities[0]!);
		await eventually("the expiry notice", () => test.chat.ephemeral.length === 1);
		expect(test.chat.ephemeral[0]).toMatchObject({ text: "This approval has expired." });
		// Denied before it was emitted: the approval's subscription never holds it.
		const approvalId = cardOf(post.content).actions[0]!.id.slice(
			"fabrial.approval.approve:".length,
		);
		const [emitted] = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = 'fabrial.approval.decided'
			and payload -> 'payload' ->> 'approvalId' = ${approvalId}`;
		expect(emitted!.count).toBe(0);
		expect((await owner.info())?.status).not.toBe("completed");
		// Resumed past its deadline, the decision expires: the late click never approves it.
		decision.release();
		expect(await owner.output()).toBe("expired");
	});

	it("keeps executions that cleanup waits on past the longest approval", async () => {
		const test = setup([approvalWorkflow()]);
		await test.app.start();
		const rows = await db.sql<{ key: string; complete: number | null; fail: number | null }[]>`
			select key, remove_on_complete_days as complete, remove_on_fail_days as fail
			from pgconductor._private_tasks where queue = ${test.runtime.queue}
			and key in ('fabrial.approval.decision', 'owner')`;
		// Callers may first wait on a decision any time later: it is never removed.
		expect(rows.find((row) => row.key === "fabrial.approval.decision")).toMatchObject({
			complete: null,
			fail: null,
		});
		// Callers are app workflows: kept until the app sets a retention.
		expect(rows.find((row) => row.key === "owner")).toMatchObject({ complete: null, fail: null });
	});
});

describe("approval authorization", () => {
	const decidedBy = () =>
		workflow(async (_i, ctx) => {
			const decision = await ctx.waitForApproval("approval", {
				title: "Proposal",
				details: {},
				approvers: team,
				timeout: "1h",
			});
			return [decision.status, decision.decidedBy?.id ?? null];
		});

	it("decides with the first authorized click, after an unauthorized one", async () => {
		const test = setup([decidedBy()]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.reject, outsider.identities[0]!, { dedupeId: "outsider" });
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!, { dedupeId: "bob" });
		await test.chat.click(c.post.ref, c.reject, alice.identities[0]!, { dedupeId: "alice" });
		expect(await owner.output()).toEqual(["approved", "bob"]);
		// The router decided each click once: an unauthorized one never reaches the approval.
		const decisions = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = 'pgconductor.event-dispatch' and payload ->> 'eventKey' = 'fabrial.approval.decided'
			and payload -> 'payload' ->> 'actor' is not null and payload -> 'payload' -> 'actor' ->> 'id' = 'outsider'`;
		expect(decisions[0]!.count).toBe(0);
	});

	it("denies forged and unknown approval ids without deciding the approval", async () => {
		const test = setup([decidedBy()]);
		const owner = await enter(test);
		const c = await card(test);
		const forged = [
			"fabrial.approval.approve:not-a-uuid",
			"fabrial.approval.approve:00000000-0000-4000-8000-000000000000",
			// An execution, but not an approval request.
			`fabrial.approval.approve:${owner.id}`,
			`fabrial.approval.cancel:${owner.id}`,
		];
		for (const [index, action] of forged.entries())
			await test.chat.click(c.post.ref, action, alice.identities[0]!, {
				dedupeId: `forged-${index}`,
			});
		await eventually("every denial", () => test.chat.ephemeral.length === forged.length);
		await owner.suspended();
		// Unsettled; Fabrial's race poll can be re-claiming it.
		expect((await owner.info())?.status).not.toMatch(/completed|failed|cancelled/);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toEqual(["approved", "alice"]);
	});

	it("authorizes a click after a restart between the request and the click", async () => {
		const first = setup([decidedBy()]);
		const owner = await enter(first);
		const c = await card(first);
		await owner.suspended();
		await first.app.stop();
		const second = setup([decidedBy()], {
			runtime: db.runtime({ queue: first.runtime.queue }),
			chat: first.chat,
		});
		await second.app.start();
		await first.chat.click(c.post.ref, c.approve, outsider.identities[0]!);
		await eventually("the ephemeral rejection", () => first.chat.ephemeral.length === 1);
		await first.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		expect(await owner.output()).toEqual(["approved", "bob"]);
	});

	it("rejects a group id defined twice at once, keeping the first registration", async () => {
		const one = defineGroup({ id: "twice", members: [alice] });
		expect(() => defineGroup({ id: "twice", members: [outsider] })).toThrow(
			"Group twice is already defined",
		);
		const test = setup([
			workflow(async (_i, ctx) => {
				const decision = await ctx.waitForApproval("approval", {
					title: "Proposal",
					details: {},
					approvers: one,
				});
				return decision.decidedBy?.id ?? null;
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("alice");
	});

	it("keeps the request readable past the longest approval", async () => {
		const test = setup([decidedBy()]);
		const owner = await enter(test);
		const c = await card(test);
		const approvalId = c.approve.slice("fabrial.approval.approve:".length);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toEqual(["approved", "alice"]);
		const request = await test.runtime.getExecution(approvalId);
		expect(request).toMatchObject({
			workflow: "fabrial.approval.request",
			result: {
				status: "completed",
				output: {
					callerId: owner.id,
					operationId: "approval",
					assigned: { users: [], groups: ["team"] },
					requester: { id: "alice" },
					title: "Proposal",
				},
			},
		});
		const [retention] = await db.sql<{ complete: number | null; fail: number | null }[]>`
			select remove_on_complete_days as complete, remove_on_fail_days as fail
			from pgconductor._private_tasks where key = 'fabrial.approval.request' and queue = ${test.runtime.queue}`;
		// Approvals last below 30 days; waits and clicks on the request must find it until then.
		expect(retention!.complete).toBeGreaterThan(30);
		expect(retention!.fail).toBeGreaterThan(30);
	});

	it("rejects ingested events in Fabrial's namespace", async () => {
		const test = setup([decidedBy()]);
		await test.app.start();
		await expect(
			test.app.emit("fabrial.approval.decided", { approvalId: "x", status: "approved" }),
		).rejects.toThrow("Reserved event name");
	});
});

describe("approval cards", () => {
	type Shown = {
		title: string;
		text?: string;
		fields: { label: string; value: string }[];
		code?: { language?: string; content: string };
		footer?: string;
		actions: { id: string }[];
	};
	const shown = (content: unknown) => (content as { card: Shown }).card;
	const field = (content: unknown, label: string) =>
		shown(content).fields.find((each) => each.label === label)?.value;
	const decisionWorkflow = (
		options: Partial<Parameters<WorkflowContext["waitForApproval"]>[1]> = {},
	) =>
		workflow(async (_input, ctx) => {
			const decision = await ctx.waitForApproval("approval", {
				title: "Run SQL?",
				details: { sql: "delete from users where id = 1" },
				approvers: team,
				timeout: "24h",
				...options,
			});
			return decision as unknown as Json;
		});

	it("shows the exact proposal, the verified requester, the source thread and the deadline", async () => {
		const test = setup([decisionWorkflow()]);
		await enter(test);
		const c = await card(test);
		const thread = await test.chat.thread(surface);
		const status = await until("the status card", async () => thread.posts[0]);
		for (const content of [c.post.content, status.content]) {
			expect(shown(content)).toMatchObject({
				title: "Run SQL?",
				code: {
					language: "json",
					content: JSON.stringify({ sql: "delete from users where id = 1" }, null, 2),
				},
			});
			expect(field(content, "Requested by")).toBe("Alice (alice)");
			// The authenticated input: the chat message; replies go to its thread.
			expect(field(content, "Source")).toBe("chat messageId initial");
			expect(field(content, "Reply thread")).toBe(`https://chat.test/chat/${surface.threadId}`);
			expect(field(content, "Expires")).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
		}
		expect(shown(c.post.content).text).toBe("Approval requested");
		expect(shown(status.content).text).toBe("Waiting for approval");
	});
	it("shows the input that prompted the request as its source, apart from the reply thread", async () => {
		const test = setup([decisionWorkflow()]);
		await test.app.start();
		// As a followed GitHub review delivers to a chat-hosted conversation: GitHub origin, chat reply thread.
		const pr = { provider: "github", installationId: "1", repo: "acme/app", number: 5 };
		await test.app.emit(event, message("review"), {
			origin: { ...pr, threadId: "github:acme/app:5" },
			replyTo: surface,
			requestedBy: identity("Z"),
		});
		await test.execution();
		const c = await card(test);
		expect(field(c.post.content, "Source")).toBe(
			"https://chat.test/github/github:acme/app:5 (github repo acme/app number 5)",
		);
		expect(field(c.post.content, "Reply thread")).toBe(
			`https://chat.test/chat/${surface.threadId}`,
		);
		// A display name is not an identity: the stable id shows, marked when no user is defined for it.
		expect(field(c.post.content, "Requested by")).toBe("unmapped chat:workspace:Z");
	});
	it("adds custom content in its own labelled section, never among the verified fields", async () => {
		const test = setup([
			decisionWorkflow({
				card: { text: "Nothing to see here", fields: [{ label: "Ticket", value: "OPS-1" }] },
			}),
		]);
		await enter(test);
		const c = await card(test);
		const content = shown(c.post.content);
		expect(content.title).toBe("Run SQL?");
		expect(content.code?.content).toContain("delete from users where id = 1");
		expect(content.fields.map((each) => each.label)).toEqual([
			"Requested by",
			"Source",
			"Reply thread",
			"Expires",
		]);
		expect(content.fields[0]!.value).toBe("Alice (alice)");
		expect((content as { section?: unknown }).section).toEqual({
			title: "From the requesting app (not verified by Fabrial)",
			text: "Nothing to see here",
			fields: [{ label: "Ticket", value: "OPS-1" }],
		});
	});
	it.each(["Requested by", " source ", "Expires", "Reply thread"])(
		"rejects the reserved card field label %j",
		async (label) => {
			const test = setup([decisionWorkflow({ card: { fields: [{ label, value: "Mallory" }] } })]);
			const owner = await enter(test);
			expect(await owner.settled()).toMatchObject({
				status: "failed",
				error: expect.stringContaining("is reserved"),
			});
		},
	);
	it("keeps the proposal and adds the decision, its time and reason on final cards", async () => {
		const test = setup([decisionWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		// A reason comes from the reject modal's submit, which is authorized like any click.
		await test.chat.click(c.post.ref, c.reject, outsider.identities[0]!, { value: "No" });
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await owner.suspended();
		await test.chat.click(c.post.ref, c.reject, bob.identities[0]!, { value: " Too risky " });
		expect(await owner.output()).toMatchObject({
			status: "rejected",
			comment: "Too risky",
			decidedBy: { id: "bob" },
		});
		await eventually("the final card", () => c.post.updates.length > 0);
		const content = c.post.content;
		expect(shown(content)).toMatchObject({
			title: "Run SQL?",
			text: "Rejected by Bob",
			actions: [],
		});
		expect(shown(content).code?.content).toContain("delete from users where id = 1");
		expect(field(content, "Requested by")).toBe("Alice (alice)");
		expect(field(content, "Reason")).toBe("Too risky");
		expect(field(content, "Closed")).toMatch(/Z$/);
		expect(field(content, "Expires")).toBeUndefined();
	});
	it("rejects without a reason where the provider has no modal", async () => {
		const test = setup([decisionWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.reject, alice.identities[0]!);
		const decision = (await owner.output()) as { status: string; comment?: string };
		expect(decision.status).toBe("rejected");
		expect(decision.comment).toBeUndefined();
		await eventually("the final card", () => c.post.updates.length > 0);
		expect(field(c.post.content, "Reason")).toBeUndefined();
	});
	it("ignores a value on approve", async () => {
		const test = setup([decisionWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { value: "spoofed" });
		const decision = (await owner.output()) as { status: string; comment?: string };
		expect(decision).toMatchObject({ status: "approved" });
		expect(decision.comment).toBeUndefined();
		await eventually("the final card", () => c.post.updates.length > 0);
		expect(shown(c.post.content)).toMatchObject({ text: "Approved by Alice", actions: [] });
		expect(field(c.post.content, "Requested by")).toBe("Alice (alice)");
	});
	it("keeps the proposal on expired cards", async () => {
		const test = setup([decisionWorkflow({ timeout: "1h" })]);
		const owner = await enter(test);
		const c = await card(test);
		await owner.suspended();
		await db.clock.advance("1h");
		expect(await owner.output()).toMatchObject({ status: "expired" });
		await eventually("the expired card", () => shown(c.post.content).text === "Expired");
		expect(shown(c.post.content).code?.content).toContain("delete from users");
		expect(field(c.post.content, "Requested by")).toBe("Alice (alice)");
	});
	it("posts to a destination, where only assigned approvers may decide", async () => {
		const destination = { kind: "channel", provider: "chat", channelId: "approvals" } as const;
		const test = setup([decisionWorkflow({ approvers: alice, destination })]);
		const owner = await enter(test);
		const post = await until("the channel card", async () =>
			[...test.chat.threads.values()]
				.find((thread) => thread.channelId === "approvals")
				?.posts.at(0),
		);
		const [approve] = shown(post.content).actions;
		expect(shown(post.content).text).toBe("Approval requested");
		expect(field(post.content, "Requested by")).toBe("Alice (alice)");
		// No DMs: the destination replaces them.
		expect((await test.chat.openDM(alice.identities[0]!)).posts).toHaveLength(0);
		await owner.suspended();
		await test.chat.click(post.ref, approve!.id, bob.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await owner.suspended();
		await test.chat.click(post.ref, approve!.id, alice.identities[0]!);
		expect(await owner.output()).toMatchObject({ status: "approved", decidedBy: { id: "alice" } });
		await eventually("the final card", () => post.updates.length > 0);
		expect(shown(post.content)).toMatchObject({ text: "Approved by Alice", actions: [] });
	});
});
