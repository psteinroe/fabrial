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

const { db, setup, enter, card, eventually, realTime } = useCoreApp();
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
	it("delivers a click exactly once when its first emit fails and the provider retries", async () => {
		await realTime();
		const test = setup([approvalWorkflow()]);
		const owner = await enter(test);
		const c = await card(test);
		const emit = test.runtime.emit.bind(test.runtime);
		let failures = 1;
		test.runtime.emit = async (...args) => {
			if (args[0] === "fabrial.approval.decided" && failures-- > 0) throw new Error("emit failed");
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
		await realTime();
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
		const owner = await enter(test);
		const c = await card(test);
		members = [bob];
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		await owner.suspended();
		await db.clock.advance(100);
		expect(await owner.output()).toBe("expired");
	});
	it("allows only the requester to cancel using the status control", async () => {
		await realTime();
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
		await realTime();
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
		const owner = await enter(test);
		const c = await card(test);
		members = [alice, bob];
		await test.chat.click(c.post.ref, c.approve, bob.identities[0]!);
		await eventually("the ephemeral rejection", () => test.chat.ephemeral.length === 1);
		expect((await test.chat.openDM(bob.identities[0]!)).posts).toHaveLength(0);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		expect(await owner.output()).toBe("approved");
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
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!);
		await owner.suspended("after");
		await db.clock.advance(10);
		expect(await owner.output()).toEqual(["approved", "approved"]);
		await eventually("the card update", () => c.post.updates.length > 0);
		expect(c.post.updates).toHaveLength(1);
	});
	it("updates every approver card and originating status exactly once across later replay", async () => {
		const test = setup([
			workflow(async (_i, ctx) => {
				await ctx.waitForApproval("approval", { title: "Proposal", details: {}, approvers: team });
				await ctx.sleep("after", 10);
			}),
		]);
		const owner = await enter(test);
		const c = await card(test);
		await test.chat.click(c.post.ref, c.approve, alice.identities[0]!, { dedupeId: "one" });
		await owner.suspended("after");
		await test.chat.click(c.post.ref, c.reject, bob.identities[0]!, { dedupeId: "different" });
		await db.clock.advance(10);
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
		await realTime();
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
		const owner = await enter(test);
		await owner.suspended();
		expect(first.posts).toHaveLength(1);
		expect(second.posts).toHaveLength(1);
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
		await realTime();
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
		await realTime();
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
			expect(await owner.settled()).toMatchObject(
				unsupported
					? {
							status: "failed",
							error: expect.stringContaining("no identity whose provider can open DMs"),
						}
					: { status: "completed", output: "requested" },
			);
			expect(attempts).toEqual(["github", "slack"]);
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
