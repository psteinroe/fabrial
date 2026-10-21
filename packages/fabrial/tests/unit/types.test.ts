import { expectTypeOf, it } from "vitest";
import { z } from "zod";
import type { AgentRef, ToolContext, WorkflowContext } from "../../src/index.ts";
import { createFabrial } from "../../src/index.ts";
import { defineState } from "../../src/state.ts";

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

it("tools use state of both scopes; workflows have none", () => {
	const schema = z.object({ count: z.number() });
	const session = defineState({
		name: "s",
		scope: "session",
		schema,
		initial: () => ({ count: 0 }),
	});
	const agent = defineState({ name: "a", scope: "agent", schema, initial: () => ({ count: 0 }) });
	const check = async (ctx: WorkflowContext, tool: ToolContext) => {
		// @ts-expect-error State belongs to tools; workflows pass data to agents with `ctx.send`.
		ctx.state(session);
		expectTypeOf(await tool.state(session).get()).toEqualTypeOf<{ count: number }>();
		expectTypeOf(await tool.state(agent).get()).toEqualTypeOf<{ count: number }>();
	};
	expectTypeOf(check).toBeFunction();
});

it("inputless workflows are invoked and started without an argument", () => {
	const f = createFabrial({ plugins: [] });
	const hello = f.defineWorkflow({
		name: "hello",
		run: async (): Promise<{ greeting: string }> => ({ greeting: "hi" }),
	});
	const greet = f.defineWorkflow({
		name: "greet",
		input: z.object({ name: z.string() }),
		run: async ({ name }) => ({ name }),
	});
	const check = async (ctx: WorkflowContext, tool: ToolContext) => {
		expectTypeOf(ctx.invokeAndWait("hello", hello)).resolves.toEqualTypeOf<{ greeting: string }>();
		expectTypeOf(
			ctx.invoke("hello", hello, null, { cancelWithParent: true }),
		).resolves.toEqualTypeOf<string>();
		// @ts-expect-error a workflow without input takes none
		await ctx.invokeAndWait("hello", hello, { name: "x" });
		expectTypeOf(tool.invoke("hello", hello)).resolves.toEqualTypeOf<{ greeting: string }>();
		expectTypeOf(tool.start("hello", hello)).resolves.toEqualTypeOf<string>();
		// @ts-expect-error a workflow without input takes none
		await ctx.invoke("hello", hello, { name: "x" });
		// @ts-expect-error a workflow with input requires it
		await ctx.invoke("greet", greet);
		// @ts-expect-error a workflow with input requires it
		await tool.start("greet", greet);
		expectTypeOf(ctx.invoke("greet", greet, { name: "x" })).resolves.toEqualTypeOf<string>();
	};
	expectTypeOf(check).toBeFunction();
});
