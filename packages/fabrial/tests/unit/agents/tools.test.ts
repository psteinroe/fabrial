import { expect, it } from "vitest";
import { z } from "zod";
import { toJsonSchema, toolContent } from "../../../src/agents/index.ts";

it("converts a Standard Schema input to JSON Schema for the model", () => {
	const schema = toJsonSchema(
		z.object({ text: z.string(), count: z.number().optional() }),
		"Tool t",
	);
	expect(schema).toEqual({
		type: "object",
		properties: { text: { type: "string" }, count: { type: "number" } },
		required: ["text"],
	});
	expect(schema).not.toHaveProperty("$schema");
});

it("rejects schemas without Standard JSON Schema or that are not objects", () => {
	const plain = {
		"~standard": { version: 1, vendor: "x", validate: (value: unknown) => ({ value }) },
	};
	expect(() => toJsonSchema(plain as never, "Tool t")).toThrow("Standard JSON Schema");
	expect(() => toJsonSchema(z.string(), "Tool t")).toThrow("must describe an object");
});

it("passes strings through and serializes other JSON tool results", () => {
	expect(toolContent("done")).toBe("done");
	expect(toolContent({ rows: [1, 2] })).toBe('{"rows":[1,2]}');
	expect(toolContent(undefined)).toBe("null");
});
