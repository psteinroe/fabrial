import { expect, expectTypeOf, it } from "vitest";
import { z } from "zod";
import { createFabrial, definePlugin } from "../../internals.ts";

const f = createFabrial({
	plugins: [
		definePlugin({
			id: "example",
			clients: () => ({ example: { read: (): string => "value" } }),
			toolkit: (clients) => {
				expectTypeOf(clients.example.read()).toEqualTypeOf<string>();
				return { tools: [] };
			},
		}),
		definePlugin({ id: "plain", clients: () => ({}) }),
		definePlugin((id: "factory") => ({ id, toolkit: () => ({ tools: [] }) }))("factory"),
		definePlugin((id: "plain-factory") => ({ id }))("plain-factory"),
	],
});

const tool = f.defineTool({
	name: "read",
	description: "Read an example value",
	input: z.object({ key: z.string() }),
	async run({ key }, ctx) {
		expectTypeOf(key).toEqualTypeOf<string>();
		expectTypeOf(ctx.clients.example.read()).toEqualTypeOf<string>();
		// @ts-expect-error No open index signature for misspelled client names.
		void ctx.clients.exmaple;
		return ctx.clients.example.read();
	},
});
const toolkit = f.defineToolkit({
	name: "example-toolkit",
	tools: [tool],
	instructions: (ctx) => {
		expectTypeOf(ctx.clients.example.read()).toEqualTypeOf<string>();
		// @ts-expect-error Instruction clients are bound to the instance too.
		void ctx.clients.exmaple;
		return ctx.clients.example.read();
	},
});

function invalidDefinitions() {
	// @ts-expect-error Only this catalog's toolkit plugin ids are permitted.
	f.defineAgent({ name: "wrong", description: "Answers test questions.", toolkits: ["exmaple"] });
	// @ts-expect-error Plugins without a toolkit cannot be selected.
	f.defineAgent({ name: "wrong", description: "Answers test questions.", toolkits: ["plain"] });
	f.defineAgent({
		name: "wrong",
		description: "Answers test questions.",
		// @ts-expect-error Plugin factories record a missing toolkit too.
		toolkits: ["plain-factory"],
	});
	f.defineAgent({ name: "factory", description: "Answers test questions.", toolkits: ["factory"] });
	const other = createFabrial({ plugins: [] });
	other.defineAgent({
		name: "wrong",
		description: "Answers test questions.",
		// @ts-expect-error Other instances do not inherit this catalog's ids.
		toolkits: ["example"],
	});
	// @ts-expect-error Thinking levels are Fabrial's.
	other.defineAgent({ name: "wrong", description: "Answers test questions.", thinking: "xhigh" });
	other.defineTool({
		name: "empty",
		description: "No clients",
		input: z.object({}),
		async run(_input, ctx) {
			// @ts-expect-error Other instances do not inherit this catalog's clients.
			void ctx.clients.example;
		},
	});
}
void invalidDefinitions;

it("returns plain Fabrial definitions bound to an explicit catalog", () => {
	const agent = f.defineAgent({
		name: "example",
		description: "Answers test questions.",
		model: "anthropic/claude-sonnet-5",
		thinking: "medium",
		instructions: ["Static", (ctx) => `Requested by ${ctx.actor?.name}`],
		toolkits: ["example", toolkit],
		tools: [tool],
	});
	expect(agent.definition.tools).toEqual([tool]);
	expect(agent.definition.toolkits).toEqual(["example", toolkit]);
	expect(tool.kind).toBe("fabrial.tool");
	expect(toolkit.kind).toBe("fabrial.toolkit");
	expect(() =>
		f.defineAgent({ name: "bad", description: "Answers test questions.", model: "no-provider" }),
	).toThrow("provider/model");
	expect(() =>
		f.defineTool({ name: "bad name", description: "", input: z.object({}), run: () => null }),
	).toThrow("Invalid tool name");
});
