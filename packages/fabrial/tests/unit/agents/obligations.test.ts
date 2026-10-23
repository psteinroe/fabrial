import { describe, expect, it } from "vitest";
import { type Obligation, prunable } from "../../../src/sessions/obligations.ts";

const reply = (fulfilled: boolean, extra: Partial<Obligation> = {}): Obligation => ({
	conversationId: 1,
	owes: "reply",
	...(fulfilled ? { fulfilled: true } : {}),
	...extra,
});

describe("prunable", () => {
	it("keeps a long Session's settled obligations to the newest `keep`", () => {
		const requests: Record<string, Obligation> = {};
		for (let turn = 0; turn < 1_000; turn++) {
			requests[`r${turn}`] = reply(true);
			for (const requestId of prunable(requests, 100)) delete requests[requestId];
		}
		expect(Object.keys(requests)).toHaveLength(100);
		expect(Object.keys(requests)[0]).toBe("r900");
	});

	it("keeps what is still owed, a pending post, a call's outcome, and the origin of a child's task", () => {
		const requests: Record<string, Obligation> = {
			open: reply(false),
			posting: reply(true, {
				deliveries: { post: { executionId: "e", status: "pending" } },
			}),
			call: { conversationId: 1, owes: "call", fulfilled: true },
			lead: reply(true),
			task: { conversationId: 2, owes: "task", origin: "lead" },
			posted: reply(true, { deliveries: { post: { executionId: "e", status: "posted" } } }),
			stopped: reply(true, { stopped: true }),
		};
		expect(prunable(requests, 0)).toEqual(["posted", "stopped"]);
		expect(prunable(requests, 1)).toEqual(["posted"]);
	});
});
