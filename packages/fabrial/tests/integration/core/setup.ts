import { metadataOf } from "../../../src/workflows/metadata.ts";
import { afterAll, beforeEach } from "vitest";
import postgres, { type Sql } from "postgres";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	defineGroup,
	definePlugin,
	defineUser,
	trigger,
} from "../../internals.ts";
import type {
	AnyWorkflow,
	ChatMessage,
	ExternalIdentity,
	InvocationMetadata,
	Json,
	JsonObject,
	WorkflowContext,
} from "../../internals.ts";
import type { CoreConfig } from "../../../src/app.ts";
import type { AnyPlugin } from "../../../src/plugin.ts";
import type { ExecutionInfo } from "../../../src/workflows/definition.ts";
import type { Fabrial } from "../../../src/instance.ts";
import type { ScriptedModels } from "../../../src/testing/models.ts";
import { FakeChat } from "../../../src/testing/chat.ts";
import { scriptedPi } from "../../../src/testing/models.ts";
import { testApp } from "../../../src/testing/app.ts";
import { testClient, until, useConductor, type ExecutionHandle } from "../../conductor/helper.ts";

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
	threadId: "chat:general:thread",
} as const;
export const event = "chat.message";
export const metadata: InvocationMetadata = {
	requestId: "test",
	origin: null,
	replyTo: null,
	requestedBy: null,
};
/** A message in `surface`; FakeChat's SDK ingests it as `chat.message`. */
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
	isMention: false,
	isDM: false,
	sentAt: "2026-01-05T00:00:00Z",
});
/** A message mentioning the bot: `chat.message` and `chat.mentioned`. */
export const mention = (id: string, author = alice, text = id): ChatMessage => ({
	...message(id, author, text),
	isMention: true,
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
/** A routed test workflow. Outputs are objects: it returns `{ value }`, which `execution().output()` unwraps. */
export const workflow = (
	run: (input: JsonObject, ctx: WorkflowContext) => Promise<Json | void>,
	name = "owner",
) =>
	defineWorkflow({
		name,
		triggers: [trigger({ event })],
		retries: once,
		run: async (input, ctx) => ({ value: (await run(input, ctx)) ?? null }),
	});
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
	// Pi's storage, off the test clock's connection; opened by the first app on scripted models.
	let pool: Sql | undefined;
	afterAll(async () => {
		await pool?.end();
	});

	/**
	 * An app of `workflows`, or of those `define` returns after defining agents and state on the app's instance.
	 * With `models`, Pi runs on them.
	 */
	function setup(
		workflows: AnyWorkflow[] | ((f: Fabrial<readonly AnyPlugin[]>) => AnyWorkflow[]),
		extra: Omit<Partial<CoreConfig>, "agents" | "conductor"> & {
			models?: ScriptedModels;
			/** Another app's queue, to run as its next deploy. */
			queue?: string;
			/** Replaces the test worker settings (fast polling, concurrency 8). */
			worker?: CoreConfig["conductor"]["worker"];
		} = {},
	) {
		const conductor = {
			...db.conductor(extra.queue ? { queue: extra.queue } : {}),
			...(extra.worker && { worker: extra.worker }),
		};
		const chat = new FakeChat();
		const attempts: (ExecutionInfo & { metadata: InvocationMetadata })[] = [];
		const recorder = definePlugin({
			id: "recorder",
			hooks: {
				workflow: [
					async (execution, ctx, next) => {
						attempts.push({ ...execution, metadata: metadataOf(ctx) });
						return next();
					},
				],
			},
		});
		const {
			plugins = [plugin, source],
			identity: identities = [alice, bob, outsider],
			logger,
			models,
			queue: _,
			worker: __,
			...config
		} = extra;
		const f = createFabrial({ plugins: [...plugins, recorder], identity: identities });
		const app = db.own(
			testApp(f, {
				conductor,
				chat,
				...config,
				workflows: typeof workflows === "function" ? workflows(f) : workflows,
				logger,
				...(models && {
					agents: {
						models: scriptedPi.get(models)!,
						sql: (pool ??= postgres(db.url, { max: 4, onnotice: () => {} })),
						settings: { retry: { enabled: false } },
					},
				}),
			}),
		);
		const client = testClient(conductor.queue, () => app.host.conductor);
		const routes = async () =>
			(
				await db.sql<{ id: string }[]>`
					select id from pgconductor._private_executions
					where queue = ${conductor.queue} and task_key = 'fabrial.route'
					order by created_at, id`
			).map((row) => row.id);
		/** Executions of `name` in the order they first ran. */
		const ids = (name: string) => [
			...new Set(attempts.filter((a) => a.workflow === name).map((a) => a.executionId)),
		];
		return {
			/** The app's instance: agents and state defined on it before `app.start()` are the app's. */
			instance: f,
			/** Conductor calls on the app's queue, once it started. */
			client,
			chat,
			app,
			ids,
			/** The `index`th execution of `name`, once it ran. */
			execution: async (name = "owner", index = 0) => {
				const handle = db.execution(
					await until(`${name} #${index} to run`, async () => ids(name)[index]),
				);
				// `workflow()` returns `{ value }`: the handle unwraps it, from any of its methods.
				const unwrap = (output: Json) =>
					output !== null && typeof output === "object" && "value" in output
						? (output.value as Json)
						: output;
				const unwrapped: ExecutionHandle = {
					...handle,
					running: async () => (await handle.running(), unwrapped),
					suspended: async (step) => (await handle.suspended(step), unwrapped),
					settled: async () => {
						const result = await handle.settled();
						return result.status === "completed"
							? { ...result, output: unwrap(result.output) }
							: result;
					},
					output: async () => unwrap(await handle.output()),
				};
				return unwrapped;
			},
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
		// The decision computes its timeout from the deadline when its wait registers: tests advance the
		// clock only after that, or after the decision settled.
		await until("the approval decision to wait", async () => {
			const [row] = await db.sql<{ ready: boolean }[]>`
				select (completed_at is not null or failed_at is not null
					or (waiting_step_key = 'decision' and locked_by is null)) as ready
				from pgconductor._private_executions
				where queue = ${test.client.queue} and task_key = 'fabrial.approval.decision'
				order by created_at desc limit 1`;
			return row?.ready ? true : undefined;
		});
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
