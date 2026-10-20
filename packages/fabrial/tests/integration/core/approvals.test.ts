import { describe, expect, it } from "vitest";
import { defineGroup, defineUser } from "../../../src/index.ts";
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
		const c = await card(test);
		await eventually("the cancelled card", () => cardOf(c.post.content).text === "Cancelled");
		expect(cardOf(c.post.content).actions).toEqual([]);
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

	// PHASE2(task 9, unresolved): the late card's cleanup does not relabel it within the test's wait.
	it.skip("decides on a click before every card is posted and labels the later cards with it", async () => {
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
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
		await eventually(
			"alice's relabelled card",
			() => cardOf(c.post.content).text === "Approved by Alice",
		);
		// Bob's cleanup is retried, then gives up; it never blocks the others.
		expect(await cleanups(test.runtime.queue)).toHaveLength(3);
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

	it("keeps executions that cleanup waits on past the longest approval", async () => {
		const test = setup([approvalWorkflow()]);
		await test.app.start();
		const rows = await db.sql<{ key: string; complete: number | null; fail: number | null }[]>`
			select key, remove_on_complete_days as complete, remove_on_fail_days as fail
			from pgconductor._private_tasks where queue = ${test.runtime.queue}
			and key in ('fabrial.approval.decision', 'owner')`;
		const decision = rows.find((row) => row.key === "fabrial.approval.decision")!;
		expect(decision.complete).toBeGreaterThan(30);
		expect(decision.fail).toBeGreaterThan(30);
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

	it("fails an approval assigned to a group id defined more than once", async () => {
		const one = defineGroup({ id: "twice", members: [alice] });
		defineGroup({ id: "twice", members: [outsider] });
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.waitForApproval("approval", { title: "Proposal", details: {}, approvers: one });
			}),
		]);
		const owner = await enter(test);
		expect(await owner.settled()).toMatchObject({
			status: "failed",
			error: expect.stringContaining("defined once with defineGroup"),
		});
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
