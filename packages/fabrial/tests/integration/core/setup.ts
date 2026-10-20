import { beforeEach } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	defineGroup,
	definePlugin,
	defineUser,
	trigger,
} from "../../../src/index.ts";
import type {
	AnyWorkflow,
	ChatMessage,
	ExternalIdentity,
	InvocationMetadata,
	Json,
	JsonObject,
	WorkflowContext,
} from "../../../src/index.ts";
import type { CoreConfig } from "../../../src/app.ts";
import type { ExecutionInfo } from "../../../src/workflow.ts";
import { FakeChat, testApp } from "../../../src/testing.ts";
import { until, useConductor } from "../../conductor/helper.ts";

export const { defineState, defineWorkflow } = createFabrial({ plugins: [] });
export const identity = (subjectId: string): ExternalIdentity => ({
	provider: "chat",
	installationId: "workspace",
	subjectId,
});
export const alice = defineUser({ id: "alice", name: "Alice", identities: [identity("A")] });
export const bob = defineUser({ id: "bob", name: "Bob", identities: [identity("B")] });
export const outsider = defineUser({ id: "outsider", identities: [identity("O")] });
export const team = defineGroup({ id: "team", members: [alice, bob] });
export const surface = {
	kind: "thread",
	provider: "chat",
	threadId: "channel:general:thread",
} as const;
export const event = "chat.message";
export const metadata: InvocationMetadata = {
	interactionId: "test",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
export const message = (id: string, author = alice, text = id): ChatMessage => ({
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
const payload = z.record(z.string(), z.json());
export const plugin = definePlugin({
	id: "chat",
	events: {
		message: defineEvent({ payload, filterable: ["channelId", "toString"] }),
		mentioned: defineEvent({ payload, filterable: ["channelId"] }),
	},
});
/** Events of a non-chat provider. */
export const source = definePlugin({
	id: "source",
	events: {
		item: defineEvent({
			payload,
			filterable: ["level", "path", "size", "optional", "enabled"],
		}),
		ping: defineEvent({ payload }),
	},
});
/**
 * One attempt, as on the in-memory runtime: Conductor retries failed attempts after a backoff (three
 * attempts by default), which the test clock reaches only when a test moves it.
 */
export const once = { maxAttempts: 1 };
export const workflow = (
	run: (input: Json, ctx: WorkflowContext) => Promise<Json | void>,
	name = "owner",
) => defineWorkflow({ name, triggers: [trigger({ event })], retries: once, run });
export const approvalWorkflow = (timeout: string | number = "24h", requesterControls = true) =>
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

/**
 * Core apps on real Conductor with FakeChat. Database time is set at the start of each test, keeps
 * running from there, and jumps by `db.clock.advance` after the wait it should end is registered. Every attempt of an app
 * workflow is recorded by a middleware, which is how tests find their executions.
 */
export function useCoreApp() {
	const db = useConductor();
	beforeEach(() => db.clock.set(new Date()));

	function setup(workflows: AnyWorkflow[], extra: Partial<CoreConfig> = {}) {
		const runtime = db.runtime();
		const chat = new FakeChat(() => runtime.now!());
		const attempts: ExecutionInfo[] = [];
		const recorder = definePlugin({
			id: "recorder",
			hooks: {
				workflow: [
					async (execution, ctx, next) => {
						attempts.push(execution);
						return next(ctx);
					},
				],
			},
		});
		const {
			plugins = [plugin, source],
			identity: identities = [alice, bob, outsider],
			logger,
			...config
		} = { workflows, ...extra };
		const app = testApp(createFabrial({ plugins: [...plugins, recorder], identity: identities }), {
			runtime,
			chat,
			...config,
			logger,
		});
		const routes = async () =>
			(
				await db.sql<{ id: string }[]>`
					select id from pgconductor._private_executions
					where queue = ${runtime.queue} and task_key = 'fabrial.route'
					order by created_at, id`
			).map((row) => row.id);
		/** Executions of `name` in the order they first ran. */
		const ids = (name: string) => [
			...new Set(attempts.filter((a) => a.workflow === name).map((a) => a.executionId)),
		];
		return {
			runtime,
			chat,
			app,
			ids,
			/** The `index`th execution of `name`, once it ran. */
			execution: async (name = "owner", index = 0) =>
				db.execution(await until(`${name} #${index} to run`, async () => ids(name)[index])),
			/** Executions of the router, oldest first: it runs without middleware, so it isn't recorded. */
			routes,
			/** The `index`th routing decision, once the router finished it. */
			route: async (index = 0) =>
				(await db
					.execution(await until(`route #${index} to run`, async () => (await routes())[index]))
					.output()) as { route: string; owner: { workflow: string } | null },
			/** Invocation metadata of the first execution of `name`. */
			metadataOf: (name: string) => attempts.find((a) => a.workflow === name)?.metadata,
		};
	}
	type Test = ReturnType<typeof setup>;

	/** Start the app and route one message to its owner. */
	async function enter(test: Test) {
		await test.app.start();
		await test.chat.receive(message("initial"));
		return test.execution();
	}
	/** Alice's approval card, once delivered. */
	async function card(test: Test) {
		const dm = await test.chat.openDM(alice.identities[0]!);
		const post = await until("alice's approval card", async () => dm.posts[0]);
		if (typeof post.content !== "object" || !("card" in post.content))
			throw new Error("Missing card");
		return {
			post,
			approve: post.content.card.actions![0]!.id,
			reject: post.content.card.actions![1]!.id,
		};
	}
	/** Resolves once `probe` holds. */
	const eventually = (description: string, probe: () => boolean | Promise<boolean>) =>
		until(description, async () => ((await probe()) ? true : undefined));

	return { db, setup, enter, card, eventually };
}
