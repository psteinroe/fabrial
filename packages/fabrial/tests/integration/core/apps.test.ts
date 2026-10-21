import type { AnyWorkflow } from "../../../src/workflow.ts";
import { until } from "../../conductor/helper.ts";
import { describe, expect, it, vi } from "vitest";
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
import { FakeChat, fakeAgents, testApp } from "../../../src/testing-internal.ts";
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
			return ctx.clients.database.query();
		},
	});

	it("reuses definitions across isolated apps with definition-time plugins and spied clients", async () => {
		const fixture = () => {
			const runtime = db.runtime();
			return { runtime, app: testApp(f, { workflows: [workflow], runtime, chat: new FakeChat() }) };
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
				const id = await fixture.runtime.invoke(workflow.name, null, {
					metadata: {
						requestId: "query",
						origin: null,
						replyTo: null,
						requestedBy: null,
					},
				});
				expect(await db.execution(id).output()).toBe(value);
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
		triggers: [trigger({ event: "chat.message", filter: { channelId: ["general"] } })],
	});
	const ref = { kind: "thread", provider: "chat", threadId: "chat:general:t1" } as const;
	const start = (agents: ReturnType<typeof fakeAgents>, workflows: AnyWorkflow[] = []) => {
		const runtime = db.runtime();
		const chat = new FakeChat({ installationId: "w" });
		const app = testApp(f, { workflows, routedAgents: [agent], agents, runtime, chat });
		return { app, chat };
	};

	it("writes a follow record for a thread session and routes the next message by it", async () => {
		const agents = fakeAgents();
		const { app, chat } = start(agents);
		await app.start();
		expect(await chat.receive(message("m1", ref.threadId))).toBe("accepted");
		const session = (await until("the delivery", async () => agents.sent[0]))!.session;
		expect(session).toBe("chat:chat:general:t1");
		const thread = await chat.thread(ref);
		await until("the follow record", async () => (await thread.getState())?.follows);
		expect((await thread.getState())?.follows).toEqual([{ session, agent: "support" }]);
		// The follow record, not the trigger, routes the next message. (A thread's channel is its id's: a
		// message in it always matches the trigger's channel.)
		await chat.receive(message("m2", ref.threadId));
		await until("the second delivery", async () => agents.sent[1]);
		expect(agents.sent.map((each) => [each.session, each.agent])).toEqual([
			[session, "support"],
			[session, "support"],
		]);
		await app.stop();
	});

	it("a stop cancels the current run of every conversation following the thread; anyone there may stop", async () => {
		const agents = fakeAgents();
		const connect = agents.connect.bind(agents);
		const stops: unknown[][] = [];
		agents.connect = (host) => {
			const connected = connect(host);
			connected.agents.stop = async (...args) => {
				stops.push(args);
			};
			return connected;
		};
		const { app, chat } = start(agents);
		await app.start();
		await chat.receive(message("m1", ref.threadId));
		const thread = await chat.thread(ref);
		await until("the follow record", async () => (await thread.getState())?.follows);
		// Bob never wrote in the thread; he may stop it all the same. A redelivered stop is dropped.
		const bob = { provider: "chat", installationId: "w", subjectId: "B" };
		await chat.cancel(ref, bob, "stop-1");
		await chat.cancel(ref, bob, "stop-1");
		await until("the stop", async () => stops[0]);
		await chat.receive(message("m2", ref.threadId));
		await until("the next delivery", async () => agents.sent[1]);
		expect(stops).toEqual([
			[
				{ session: "chat:chat:general:t1", agent: "support" },
				// The production dedupe id of Slack's stop, whose streamed message ids FakeChat sends as ["stop-1"].
				JSON.stringify(["chat", "agent-session-stopped", ref.threadId, "B", ["stop-1"]]),
				expect.stringContaining("Stopped by"),
			],
		]);
		await app.stop();
	});

	it("retries a partial delivery: the router resumes after the signals it sent", async () => {
		const agents = fakeAgents();
		const connect = agents.connect.bind(agents);
		const attempts: string[] = [];
		agents.connect = (host) => {
			const connected = connect(host);
			const deliver = connected.agents.deliver!.bind(connected.agents);
			connected.agents.deliver = async (target, request) => {
				attempts.push(request.session);
				// The second conversation's first delivery fails, after the first one was signalled.
				if (attempts.length === 2) throw new Error("agents unavailable");
				return deliver(target, request);
			};
			return connected;
		};
		const { app, chat } = start(agents);
		await app.start();
		const thread = await app.host.chat()!.thread(ref);
		await thread.updateState(() => ({
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
		await until("the retried delivery", async () => agents.sent[1]);
		expect(agents.sent.map((each) => each.session)).toEqual(["app:one", "app:two"]);
		expect(attempts).toEqual(["app:one", "app:two", "app:two"]);
		await app.stop();
	});

	it("observers receive messages in a followed thread; the followers steer", async () => {
		const agents = fakeAgents();
		const seen: unknown[] = [];
		const observer = f.defineWorkflow({
			name: "observer",
			triggers: [trigger({ event: "chat.message", observe: true })],
			run: async (input, ctx) => {
				seen.push([(input as ChatMessage).messageId, ctx.thread]);
			},
		});
		const { app, chat } = start(agents, [observer]);
		await app.start();
		await chat.receive(message("m1", ref.threadId));
		await until("the first delivery", async () => agents.sent[0]);
		await until(
			"the follow record",
			async () => (await (await chat.thread(ref)).getState())?.follows,
		);
		await chat.receive(message("m2", ref.threadId));
		await until("the observer in the followed thread", async () => seen[1]);
		await until("the second delivery", async () => agents.sent[1]);
		// Observers run without the thread, once per message.
		expect(seen).toEqual([
			["m1", undefined],
			["m2", undefined],
		]);
		await app.stop();
	});
});
