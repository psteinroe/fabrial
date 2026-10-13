import { createHash } from "node:crypto";
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
import {
	claimRequestAlias,
	deliveryFor,
	escapeXml,
	renderInput,
	withRequestAlias,
} from "../../src/send.ts";
import { createTestApp, fakeAgents } from "../../src/testing.ts";

/** Short request id the model sees: the first 8 hex digits of its SHA-256. */
const short = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 8);

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
		expect(renderInput({ input, kind: "message", requestId: "r1" })).toBe(
			`<message from="Alice" thread="chat:general:&quot;t1&quot;" request="${short("r1")}">a &lt; b &amp; c &gt; d</message>`,
		);
		expect(renderInput({ input: "hi", kind: "message", requestId: "r2", from: "Bob" })).toBe(
			`<message from="Bob" request="${short("r2")}">hi</message>`,
		);
	});
	it("renders notifications with source, kind and priority", () => {
		expect(
			renderInput({
				input: { state: "changes_requested" },
				kind: "notification",
				priority: "urgent",
				requestId: "r3",
				source: "github",
				event: "github.review",
			}),
		).toBe(
			`<notification source="github" kind="github.review" priority="urgent" request="${short("r3")}">{&quot;state&quot;:&quot;changes_requested&quot;}</notification>`,
		);
		expect(escapeXml(`'&'`)).toBe("&apos;&amp;&apos;");
	});
});

describe("request aliases", () => {
	// Two request ids whose hashes share their first 8 hex digits.
	const first = "request-105084";
	const second = "request-110778";
	it("extends the alias of a request whose short id is taken in the Session", async () => {
		expect(short(first)).toBe(short(second));
		const aliases = new Map<string, string>();
		const claim = (requestId: string) =>
			claimRequestAlias(requestId, async (alias) => {
				if (!aliases.has(alias)) aliases.set(alias, requestId);
				return aliases.get(alias)!;
			});
		expect(await claim(first)).toBe("edd91ebd");
		expect(await claim(second)).toBe("edd91ebd1");
		// Re-admitting either request keeps its alias.
		expect(await claim(first)).toBe("edd91ebd");
		expect(await claim(second)).toBe("edd91ebd1");
		expect(aliases.get("edd91ebd")).toBe(first);
	});
	it("renders the claimed alias as the request attribute", () => {
		const content = renderInput({ input: "hi", kind: "message", requestId: second, from: "Bob" });
		expect(withRequestAlias(content, second, "edd91ebd1")).toBe(
			`<message from="Bob" request="edd91ebd1">hi</message>`,
		);
		expect(withRequestAlias(content, second, short(second))).toBe(content);
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

describe("static subscriptions", () => {
	const plugin = definePlugin({
		id: "chat",
		events: {
			message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
		},
	});
	const f = createFabrial({ plugins: [plugin] });
	const agent = f.defineAgent({ name: "support", description: "Answers test questions." });

	it("sends every message without holding the thread, keyed by channel or a computed key", async () => {
		const channel = f.subscribe({
			name: "support-channel",
			source: trigger({ event: "chat.message", filter: { channelId: ["general"] } }),
			agent,
			session: "channel",
		});
		const tickets = f.subscribe({
			name: "tickets",
			source: trigger({ event: "chat.message", filter: { channelId: ["tickets"] } }),
			agent,
			session: (event) => `ticket:${event.text as string}`,
		});
		const agents = fakeAgents();
		const { app, runtime, chat } = createTestApp(f, {
			workflows: [],
			subscriptions: [channel, tickets],
			agents,
		});
		await app.start();
		expect(await chat.receive(message("m1", "chat:general:t1"))).toBe("new");
		// The compiled workflow may still be running: the routing slot must stay free.
		expect(await chat.receive(message("m2", "chat:general:t1"))).toBe("new");
		expect(await chat.receive(message("m3", "chat:general:t2"))).toBe("new");
		expect(await chat.receive(message("m3", "chat:general:t2"))).toBe("ignored");
		await chat.receive({ ...message("m4", "chat:tickets:t9", "42"), channelId: "tickets" });
		await runtime.flush();
		const sent = [...agents.sent].sort((a, b) => a.requestId.localeCompare(b.requestId));
		expect(sent.map((each) => [each.session, each.agent, each.delivery])).toEqual([
			["chat:channel:general", "support", "steer"],
			["chat:channel:general", "support", "steer"],
			["chat:channel:general", "support", "steer"],
			["app:ticket:42", "support", "steer"],
		]);
		expect(sent[2]!.metadata).toMatchObject({
			replyTo: { kind: "thread", threadId: "chat:general:t2" },
			requestedBy: { id: "chat:w:A" },
			send: { kind: "message", priority: "normal" },
			ownsThread: false,
		});
		expect(sent[0]!.content).toContain('<message from="Alice" thread="chat:general:t1"');
		const thread = await chat.thread(sent[0]!.metadata.replyTo as never);
		expect((await thread.getState())?.interactionId ?? null).toBeNull();
		await app.stop();
	});
});
