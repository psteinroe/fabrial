import { expectTypeOf, it } from "vitest";
import { z } from "zod";
import type { AgentRef, WorkflowContext } from "../../src/index.ts";

// Compile-only: `pnpm typecheck` checks these; the functions never run.

it("ctx.agent infers its result from the output schema", () => {
	const check = async (ctx: WorkflowContext, agent: AgentRef) => {
		const output = z.object({ severity: z.enum(["low", "high"]), summary: z.string() });
		expectTypeOf(ctx.agent("triage", agent, { input: "x", output })).resolves.toEqualTypeOf<{
			severity: "low" | "high";
			summary: string;
		}>();
		expectTypeOf(
			ctx.agent("count", agent, { input: "x", output: z.number() }),
		).resolves.toEqualTypeOf<number>();
	};
	expectTypeOf(check).toBeFunction();
});

it("ctx.agent without output returns the answer text", () => {
	const check = async (ctx: WorkflowContext, agent: AgentRef) => {
		expectTypeOf(ctx.agent("answer", agent, { input: "x" })).resolves.toEqualTypeOf<string>();
		expectTypeOf(
			ctx.agent("answer", agent, { input: "x", detached: true, timeout: "1m" }),
		).resolves.toEqualTypeOf<string>();
	};
	expectTypeOf(check).toBeFunction();
});

it("ctx.agent rejects mismatched usage", () => {
	const check = async (ctx: WorkflowContext, agent: AgentRef) => {
		// @ts-expect-error The result type comes from the schema, not from the caller.
		await ctx.agent<string>("answer", agent, { input: "x" });
		// @ts-expect-error Without output the result is a string.
		const answer: number = await ctx.agent("answer", agent, { input: "x" });
		const parsed = await ctx.agent("count", agent, { input: "x", output: z.number() });
		// @ts-expect-error The schema's output is a number.
		const text: string = parsed;
		// @ts-expect-error `output` must be a Standard Schema.
		await ctx.agent("answer", agent, { input: "x", output: "number" });
		return [answer, text];
	};
	expectTypeOf(check).toBeFunction();
});
