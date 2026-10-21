import { expect, test } from "vitest";
import { generate, written } from "../docs/snippets.ts";

test("README snippets are generated for typecheck", () => {
	// Stale: run `node examples/acme/tests/docs/snippets.ts`.
	expect(Object.fromEntries(written())).toEqual(Object.fromEntries(generate()));
});
