import { expectTypeOf, it } from "vitest";
import type { ToolContext } from "../../internals.ts";

it("exposes Fabrial's tool context, not Pi's tool API", () => {
	expectTypeOf<keyof ToolContext>().toEqualTypeOf<
		| "actor"
		| "clients"
		| "thread"
		| "state"
		| "invoke"
		| "start"
		| "evaluate"
		| "send"
		| "follow"
		| "unfollow"
		| "signal"
		| "idempotencyKey"
	>();
});
