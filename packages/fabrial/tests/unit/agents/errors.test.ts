import { describe, expect, it } from "vitest";
import { newErrorId, sentErrorReply } from "../../../src/sessions/obligations.ts";

describe("sentErrorReply", () => {
	it("tells the requester to retry later when a transient error exhausted the retries", () => {
		for (const detail of ["overloaded", "429 Too Many Requests", "fetch failed"])
			expect(sentErrorReply("model_error", detail, "abc123")).toBe(
				"I couldn't reach the model provider while handling your request. Please try again in a few minutes.\n\n_Error id: `abc123`_",
			);
	});
	it("reports other model errors and a missing model without the provider's text", () => {
		const failed =
			"I hit an error while handling your request. Please try again, rephrase, or reach out if it keeps failing.\n\n_Error id: `abc123`_";
		expect(
			sentErrorReply("model_error", "400 invalid_request_error: bad tool schema", "abc123"),
		).toBe(failed);
		// Quota exhaustion is not transient, even on a 429.
		expect(sentErrorReply("model_error", "429 insufficient_quota", "abc123")).toBe(failed);
		expect(sentErrorReply("model_error", undefined, "abc123")).toBe(failed);
		expect(sentErrorReply("no_model", undefined, "abc123")).toBe(failed);
	});
	it("does not reply to input ended on purpose", () => {
		for (const reason of ["aborted", "reset", "stale"])
			expect(sentErrorReply(reason, "overloaded", "abc123")).toBeUndefined();
	});
	it("generates distinct short error ids", () => {
		expect(newErrorId()).toMatch(/^[0-9a-f]{12}$/);
		expect(newErrorId()).not.toBe(newErrorId());
	});
});
