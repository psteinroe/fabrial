import { Type } from "typebox";
import { expect, expectTypeOf, it } from "vitest";
import { createFabrial, definePlugin } from "fabrial";
import { withPi } from "../../src/index.ts";

const f = createFabrial({
	plugins: [
		definePlugin({
			id: "example",
			clients: () => ({ example: { read: (): string => "value" } }),
			extension: () => ({}),
		}),
	],
});
const { defineTool, defineAgent, section } = withPi(f);

const tool = defineTool({
	name: "read",
	description: "Read an example value",
	parameters: Type.Object({}),
	async execute(_args, ctx) {
		expectTypeOf(ctx.clients.example.read()).toEqualTypeOf<string>();
		// @ts-expect-error No open index signature for misspelled client names.
		void ctx.clients.exmaple;
		return { content: [{ type: "text", text: ctx.clients.example.read() }] };
	},
});
const prompt = section("example", (_input, ctx) => {
	expectTypeOf(ctx.clients.example.read()).toEqualTypeOf<string>();
	// @ts-expect-error Section clients are bound to the instance too.
	void ctx.clients.exmaple;
	return ctx.clients.example.read();
});

function invalidDefinitions() {
	// @ts-expect-error Only this catalog's plugin ids are permitted.
	defineAgent({ name: "wrong", extensions: ["exmaple"] });
	const other = withPi(createFabrial({ plugins: [] }));
	// @ts-expect-error Other instances do not inherit this catalog's ids.
	other.defineAgent({ name: "wrong", extensions: ["example"] });
	other.defineTool({
		name: "empty",
		description: "No clients",
		parameters: Type.Object({}),
		async execute(_args, ctx) {
			// @ts-expect-error Other instances do not inherit this catalog's clients.
			void ctx.clients.example;
			return {};
		},
	});
}
void invalidDefinitions;

it("returns plain native Pi definitions bound to an explicit catalog", () => {
	const agent = defineAgent({ name: "example", extensions: ["example"], tools: [tool] });
	expect(agent.definition.tools).toEqual([tool]);
	expect(agent.definition.extensions).toEqual(["example"]);
	expect(prompt).toBeDefined();
});
