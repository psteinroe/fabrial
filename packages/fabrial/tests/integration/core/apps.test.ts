import { testClient } from "../../conductor/helper.ts";
import { updateRoutingState } from "../../../src/chat/index.ts";
import type { AnyWorkflow } from "../../../src/workflow.ts";
import { until } from "../../conductor/helper.ts";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	definePlugin,
	defineUser,
	trigger,
	type ChatMessage,
	type ExternalIdentity,
	type JsonObject,
} from "../../internals.ts";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { createModels } from "@earendil-works/pi-ai/models";
import postgres, { type Sql } from "postgres";
import { PostgresStorage } from "../../../src/agents/index.ts";
import { FakeChat, sessionSignals, testApp } from "../../../src/testing-internal.ts";
import { useConductor } from "../../conductor/helper.ts";

// App-level runs on real Conductor and real time: instances, follow records.
const db = useConductor();
const message = (id: string, threadId: string, text = id): ChatMessage => ({
	provider: "chat",
	threadId,
	channelId: "general",
	messageId: id,
	text,
	author: {
		identity: { provider: "chat", installationId: "w", subjectId: "A" } as ExternalIdentity &
			JsonObject,
		name: "Alice",
		isBot: false,
	},
	isMention: false,
	isDM: false,
	sentAt: "2026-01-05T00:00:00Z",
});

describe("instances", () => {
	const database = definePlugin((value: string) => ({
		id: "database",
		clients: () => ({ database: { query: () => value } }),
	}));
	const f = createFabrial({ plugins: [database("production")] });
	const user = defineUser({ id: "alice", identities: [] });
	const group = f.defineGroup({
		id: "readers",
		resolve() {
			return [user];
		},
	});
	const workflow = f.defineWorkflow({
		name: "query",
		async run(_input, ctx) {
			return { rows: ctx.clients.database.query() };
		},
	});

	it("reuses definitions across isolated apps with definition-time plugins and spied clients", async () => {
		const fixture = () => {
			const conductor = db.conductor();
			const app = db.own(testApp(f, { workflows: [workflow], conductor, chat: new FakeChat() }));
			return { client: testClient(conductor.queue, () => app.host.conductor), app };
		};
		const production = fixture();
		const test = fixture();
		vi.spyOn(test.app.host, "clients").mockReturnValue({ database: { query: () => "fake" } });
		expect(f.plugins[0]!.clients!().database.query()).toBe("production");
		expect(Object.isFrozen(f.plugins)).toBe(true);
		for (const [fixture, value] of [
			[production, "production"],
			[test, "fake"],
		] as const) {
			await fixture.app.start();
			try {
				const id = await fixture.client.invoke(workflow.name, null, {
					metadata: {
						requestId: "query",
						origin: null,
						replyTo: null,
						requestedBy: null,
					},
				});
				expect(await db.execution(id).output()).toEqual({ rows: value });
				// This group was never registered in identity.
				expect(await fixture.app.host.directory.members(group)).toMatchObject([{ id: "alice" }]);
				expect(fixture.app.host.clients().database.query()).toBe(value);
			} finally {
				await fixture.app.stop();
			}
		}
	});
});

describe("follow records", () => {
	// Pi's storage, off the test clock's connection.
	let pool!: Sql;
	beforeAll(async () => {
		pool = postgres(db.url, { max: 4, onnotice: () => {} });
		await PostgresStorage.migrate(pool);
	});
	afterAll(async () => {
		await pool.end();
	});
	const plugin = definePlugin({
		id: "chat",
		events: {
			message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
		},
	});
	const f = createFabrial({ plugins: [plugin] });
	const agent = f.defineAgent({
		name: "support",
		description: "Answers test questions.",
		model: "test/model",
		triggers: [trigger({ event: "chat.message", filter: { channelId: ["general"] } })],
	});
	const ref = { kind: "thread", provider: "chat", threadId: "chat:general:t1" } as const;
	/** Pi on a scripted model that answers every request; the tests observe the signals its Sessions receive. */
	const start = (workflows: AnyWorkflow[] = []) => {
		const conductor = db.conductor();
		const faux = fauxProvider({ provider: "test", models: [{ id: "model" }] });
		faux.setResponses(Array.from({ length: 20 }, () => fauxAssistantMessage("ok")));
		const models = createModels();
		models.setProvider(faux.provider);
		const chat = new FakeChat({ installationId: "w" });
		const agents = { models, sql: pool, settings: { retry: { enabled: false } } };
		const app = db.own(testApp(f, { workflows, routedAgents: [agent], agents, conductor, chat }));
		const signals = () => sessionSignals(db.sql, conductor.queue);
		const sent = async () => (await signals()).filter((signal) => signal.kind === "input");
		return { app, chat, signals, sent };
	};

	it("writes a follow record for a thread session and routes the next message by it", async () => {
		const { app, chat, sent } = start();
		await app.start();
		expect(await chat.receive(message("m1", ref.threadId))).toBe("accepted");
		const session = (await until("the delivery", async () => (await sent())[0]))!.session;
		expect(session).toBe("chat:chat:general:t1");
		const thread = await chat.thread(ref);
		await until("the follow record", async () => (await thread.getState())?.follows);
		expect((await thread.getState())?.follows).toEqual([{ session, agent: "support" }]);
		// The follow record, not the trigger, routes the next message. (A thread's channel is its id's: a
		// message in it always matches the trigger's channel.)
		await chat.receive(message("m2", ref.threadId));
		await until("the second delivery", async () => (await sent())[1]);
		expect((await sent()).map((each) => [each.session, each.agent])).toEqual([
			[session, "support"],
			[session, "support"],
		]);
		await app.stop();
	});

	it("a stop cancels the current run of every conversation following the thread; anyone there may stop", async () => {
		const { app, chat, signals, sent } = start();
		await app.start();
		await chat.receive(message("m1", ref.threadId));
		const thread = await chat.thread(ref);
		await until("the follow record", async () => (await thread.getState())?.follows);
		// Bob never wrote in the thread; he may stop it all the same. A redelivered stop is dropped.
		const bob = { provider: "chat", installationId: "w", subjectId: "B" };
		await chat.cancel(ref, bob, "stop-1");
		await chat.cancel(ref, bob, "stop-1");
		const stops = async () => (await signals()).filter((signal) => signal.kind === "stop");
		await until("the stop", async () => (await stops())[0]);
		await chat.receive(message("m2", ref.threadId));
		await until("the next delivery", async () => (await sent())[1]);
		// The production dedupe id of Slack's stop, whose streamed message ids FakeChat sends as ["stop-1"].
		const dedupeId = JSON.stringify([
			"chat",
			"agent-session-stopped",
			ref.threadId,
			"B",
			["stop-1"],
		]);
		expect(await stops()).toEqual([
			{
				session: "chat:chat:general:t1",
				id: `fabrial.stop:${dedupeId}:support`,
				kind: "stop",
				agent: "support",
				reason: expect.stringContaining("Stopped by"),
			},
		]);
		await app.stop();
	});

	it("retries a partial delivery: the router resumes after the signals it sent", async () => {
		const { app, chat, sent } = start();
		const attempts: string[] = [];
		await app.start();
		const conductor = app.host.conductor;
		const signalWithStart = conductor.signalWithStart.bind(conductor);
		conductor.signalWithStart = async (options) => {
			attempts.push(options.key);
			// The second conversation's first delivery fails, after the first one was signalled.
			if (attempts.length === 2) throw new Error("agents unavailable");
			return signalWithStart(options);
		};
		const connection = app.host.chat()!;
		await updateRoutingState(connection, await connection.thread(ref), () => ({
			follows: [
				{ session: "app:one", agent: "support" },
				{ session: "app:two", agent: "support" },
			],
		}));
		await chat.receive(message("m1", ref.threadId));
		await chat.receive(message("m1", ref.threadId));
		await until("the failed delivery", async () => attempts[1]);
		// Conductor retries the router after its backoff, scheduled once the failed attempt is released.
		await until("the router's retry to be scheduled", async () => {
			const [row] = await db.sql<{ id: string }[]>`
				select id from pgconductor._private_executions
				where task_key = 'fabrial.route' and last_error is not null and locked_at is null`;
			return row?.id;
		});
		await db.clock.advance("1m");
		await until("the retried delivery", async () => (await sent())[1]);
		expect((await sent()).map((each) => each.session)).toEqual(["app:one", "app:two"]);
		expect(attempts.slice(0, 3)).toEqual(["app:one", "app:two", "app:two"]);
		await app.stop();
	});

	it("observers receive messages in a followed thread; the followers steer", async () => {
		const seen: unknown[] = [];
		const observer = f.defineWorkflow({
			name: "observer",
			triggers: [trigger({ event: "chat.message", observe: true })],
			run: async (input, ctx) => {
				seen.push([(input as ChatMessage).messageId, ctx.thread]);
			},
		});
		const { app, chat, sent } = start([observer]);
		await app.start();
		await chat.receive(message("m1", ref.threadId));
		await until("the first delivery", async () => (await sent())[0]);
		await until(
			"the follow record",
			async () => (await (await chat.thread(ref)).getState())?.follows,
		);
		await chat.receive(message("m2", ref.threadId));
		await until("the observer in the followed thread", async () => seen[1]);
		await until("the second delivery", async () => (await sent())[1]);
		// Observers run without the thread, once per message.
		expect(seen).toEqual([
			["m1", undefined],
			["m2", undefined],
		]);
		await app.stop();
	});
});
