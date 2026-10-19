import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	definePlugin,
	trigger,
	type ChatMessage,
	type ExternalIdentity,
	type JsonObject,
} from "../../src/index.ts";
import { deliveryFor, escapeXml, renderInput } from "../../src/send.ts";
import { createTestApp, fakeAgents } from "../../src/testing.ts";

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

describe("renderInput", () => {
	it("renders attributed messages and escapes XML-unsafe content", () => {
		const input = message("m1", 'chat:general:"t1"', "a < b & c > d");
		expect(renderInput({ input, kind: "message" })).toBe(
			`<message from="Alice" thread="chat:general:&quot;t1&quot;">a &lt; b &amp; c &gt; d</message>`,
		);
		expect(renderInput({ input: "hi", kind: "message", from: "Bob" })).toBe(
			`<message from="Bob">hi</message>`,
		);
	});
	it("renders notifications with source, kind and priority", () => {
		expect(
			renderInput({
				input: { state: "changes_requested" },
				kind: "notification",
				priority: "urgent",
				source: "github",
				event: "github.review",
			}),
		).toBe(
			`<notification source="github" kind="github.review" priority="urgent">{&quot;state&quot;:&quot;changes_requested&quot;}</notification>`,
		);
		expect(escapeXml(`'&'`)).toBe("&apos;&amp;&apos;");
	});
});

describe("deliveryFor", () => {
	it("maps kinds and priorities to Pi admission", () => {
		expect(deliveryFor("message")).toBe("steer");
		expect(deliveryFor("message", "low")).toBe("steer");
		expect(deliveryFor("notification", "urgent")).toBe("steer");
		expect(deliveryFor("notification")).toBe("followUp");
		expect(deliveryFor("notification", "low")).toBe("write");
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

	it("writes a follow record for a thread session and routes the next message by it", async () => {
		const agents = fakeAgents();
		const { app, chat } = createTestApp(f, { workflows: [], routedAgents: [agent], agents });
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
		const { app, chat } = createTestApp(f, { workflows: [], routedAgents: [agent], agents });
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
		const { app, chat } = createTestApp(f, { workflows: [], routedAgents: [agent], agents });
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
