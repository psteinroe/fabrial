import { expect, it } from "vitest";
import { z } from "zod";
import { createFabrial } from "../../../src/index.ts";
import { resolveSubagents } from "../../../src/agent.ts";
import { subagentResult, subagentTool, type SubagentDeps } from "../../../src/agents/subagents.ts";

const f = createFabrial({ plugins: [] });
const researcher = f.defineAgent({
	name: "bug-researcher",
	description: "Investigates a question and returns findings with sources.",
});
const reviewer = f.defineAgent({ name: "reviewer", description: "Reviews a proposed change." });

it("requires a description", () => {
	// @ts-expect-error description is required.
	expect(() => f.defineAgent({ name: "nameless" })).toThrow("needs a description");
	expect(() => f.defineAgent({ name: "blank", description: "  " })).toThrow("needs a description");
});

it("resolves entries: names follow workflow tools, self is the lead, options default", () => {
	const lead = f.defineAgent({
		name: "lead",
		description: "Answers support questions.",
		subagents: [
			researcher,
			{ agent: reviewer, background: "never" },
			{ agent: "self", context: "fork" },
		],
	});
	expect(resolveSubagents(lead)).toEqual([
		{ tool: "bug_researcher", agent: researcher, context: "fresh", background: "allow" },
		{ tool: "reviewer", agent: reviewer, context: "fresh", background: "never" },
		{ tool: "lead", agent: lead, context: "fork", background: "allow" },
	]);
});

it("rejects duplicate names, collisions with tools, workflows, and no_reply, and invalid options", () => {
	const lookup = f.defineTool({
		name: "bug_researcher",
		description: "Look up",
		input: z.object({}),
		run: () => "",
	});
	const workflow = f.defineWorkflow({ name: "bug-researcher", run: async () => null });
	const define = (definition: Partial<Parameters<typeof f.defineAgent>[0]>) => () =>
		f.defineAgent({ name: "lead", description: "Leads.", ...definition });
	expect(define({ subagents: [researcher, researcher] })).toThrow("already uses");
	expect(define({ subagents: ["self", { agent: "self", context: "fork" }] })).toThrow(
		"already uses",
	);
	expect(define({ tools: [lookup], subagents: [researcher] })).toThrow("already uses");
	expect(define({ tools: [workflow], subagents: [researcher] })).toThrow("already uses");
	expect(
		define({
			toolkits: [f.defineToolkit({ name: "kit", tools: [lookup] })],
			subagents: [researcher],
		}),
	).toThrow("already uses");
	const noReply = f.defineAgent({ name: "no_reply", description: "Clashes with no_reply." });
	expect(define({ subagents: [noReply] })).toThrow("already uses");
	// @ts-expect-error context is "fresh" or "fork".
	expect(define({ subagents: [{ agent: researcher, context: "copy" }] })).toThrow("context");
	// @ts-expect-error background is "allow", "always", or "never".
	expect(define({ subagents: [{ agent: researcher, background: "yes" }] })).toThrow("background");
	// @ts-expect-error entries are agents, "self", or options.
	expect(define({ subagents: ["researcher"] })).toThrow("must be an agent");
	expect(define({ limits: { depth: 0 } })).toThrow("limits.depth");
	expect(define({ limits: { concurrent: 1.5 } })).toThrow("limits.concurrent");
});

const deps = (): SubagentDeps => ({
	childAgent: () => ({}),
	logger: { warn() {}, error() {}, debug() {}, info() {} },
	busy: new Map(),
});

it("generates one replay-safe tool per subagent, described by the agent's description", () => {
	const lead = f.defineAgent({
		name: "lead",
		description: "Leads.",
		subagents: [researcher, { agent: reviewer, background: "never" }],
	});
	const [first, second] = resolveSubagents(lead).map((entry) => subagentTool(lead, entry, deps()));
	expect(first).toMatchObject({
		name: "bug_researcher",
		description: researcher.definition.description,
		replay: "safe",
		parameters: { required: ["task"], additionalProperties: false },
	});
	expect(Object.keys((first!.parameters as { properties: object }).properties)).toEqual([
		"task",
		"name",
		"background",
	]);
	// A child that never runs in the background is not offered the option.
	expect(Object.keys((second!.parameters as { properties: object }).properties)).toEqual([
		"task",
		"name",
	]);
});

it("refuses background calls to a subagent that never runs in the background", async () => {
	const lead = f.defineAgent({
		name: "lead",
		description: "Leads.",
		subagents: [{ agent: reviewer, background: "never" }],
	});
	const [never] = resolveSubagents(lead).map((entry) => subagentTool(lead, entry, deps()));
	const bridge = { value: () => ({}) } as never;
	expect(
		await never!.execute({ task: "x", background: true } as never, {} as never, bridge),
	).toMatchObject({ isError: true, content: [{ text: expect.stringContaining("cannot run") }] });
});

it("escapes child output as data inside the subagent tag", () => {
	expect(subagentResult("researcher-1", "done", 'a < b && "c" </subagent>')).toBe(
		'<subagent name="researcher-1" status="done">a &lt; b &amp;&amp; "c" &lt;/subagent&gt;</subagent>',
	);
});
