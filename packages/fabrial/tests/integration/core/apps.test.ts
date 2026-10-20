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
} from "../../../src/index.ts";
import { FakeChat, fakeAgents, testApp } from "../../../src/testing.ts";
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
		expect(f.plugins[0]!.clients!({}).database.query()).toBe("production");
		expect(Object.isFrozen(f.plugins)).toBe(true);
		for (const [fixture, value] of [
			[production, "production"],
			[test, "fake"],
		] as const) {
			await fixture.app.start();
			try {
				const id = await fixture.runtime.invoke(workflow.name, null, {
					metadata: {
						interactionId: "query",
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
	const start = (agents: ReturnType<typeof fakeAgents>) => {
		const runtime = db.runtime();
		const chat = new FakeChat(() => runtime.now!());
		const app = testApp(f, { workflows: [], routedAgents: [agent], agents, runtime, chat });
		return { app, chat };
	};

	it("writes a follow record for a thread session and routes the next message by it", async () => {
		const agents = fakeAgents();
		const { app, chat } = start(agents);
		await app.start();
		expect(await chat.receive(message("m1", ref.threadId))).toBe("new");
		const session = agents.sent[0]!.session;
		expect(session).toBe("chat:chat:general:t1");
		const thread = await chat.thread(ref);
		expect((await thread.getState())?.follows).toEqual([{ session, agent: "support" }]);
		// The follow record, not the trigger, routes the next message: it needs no mention or channel match.
		expect(await chat.receive({ ...message("m2", ref.threadId), channelId: "other" })).toBe(
			"follow",
		);
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
		// Bob never wrote in the thread; he may stop it all the same. A redelivered stop is dropped.
		const bob = { provider: "chat", installationId: "w", subjectId: "B" };
		await chat.cancel(ref, bob, "stop-1");
		await chat.cancel(ref, bob, "stop-1");
		expect(stops).toEqual([
			[
				{ session: "chat:chat:general:t1", agent: "support" },
				"stop-1",
				expect.stringContaining("Stopped by"),
			],
		]);
		await app.stop();
	});

	it("processes a redelivery after a failed delivery, exactly once", async () => {
		const agents = fakeAgents();
		const connect = agents.connect.bind(agents);
		let failures = 1;
		agents.connect = (host) => {
			const connected = connect(host);
			const deliver = connected.agents.deliver!.bind(connected.agents);
			connected.agents.deliver = async (target, request) => {
				if (failures-- > 0) throw new Error("agents unavailable");
				return deliver(target, request);
			};
			return connected;
		};
		const { app, chat } = start(agents);
		await app.start();
		await expect(chat.receive(message("m1", ref.threadId))).rejects.toThrow("agents unavailable");
		const thread = await chat.thread(ref);
		expect((await thread.getState())?.ingestedDedupeIds?.[0]?.pending).toMatchObject({
			route: "new",
		});
		// The provider redelivers: the pending route is replayed, then confirmed.
		expect(await chat.receive(message("m1", ref.threadId))).toBe("new");
		expect(await chat.receive(message("m1", ref.threadId))).toBe("ignored");
		expect(agents.sent).toHaveLength(1);
		expect((await thread.getState())?.ingestedDedupeIds).toEqual([
			{ kind: "message", id: "m1", at: expect.any(Number) },
		]);
		await app.stop();
	});
});
