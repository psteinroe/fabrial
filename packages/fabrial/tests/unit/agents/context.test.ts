import { expectTypeOf, it } from "vitest";
import type { ToolContext } from "../../../src/index.ts";

it("exposes Fabrial's tool context, not Pi's tool API", () => {
	expectTypeOf<keyof ToolContext>().toEqualTypeOf<
		| "actor"
		| "requestedBy"
		| "clients"
		| "interaction"
		| "thread"
		| "state"
		| "invoke"
		| "start"
		| "evaluate"
		| "send"
		| "follow"
		| "unfollow"
		| "signal"
	>();
});
