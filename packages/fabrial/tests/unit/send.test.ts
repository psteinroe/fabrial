import { describe, expect, it } from "vitest";
import type { ChatMessage, ExternalIdentity, JsonObject } from "../internals.ts";
import { deliveryFor, escapeXml, renderInput } from "../../src/send.ts";

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
