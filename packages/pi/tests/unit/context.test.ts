import { expectTypeOf, it } from "vitest";
import type { ToolExecutionApi, PromptInput } from "@earendil-works/pi-durable";
import type { BoundToolApi, FabrialFields } from "../../src/index.ts";

type AssertNever<T extends never> = T;
type ToolCollision = AssertNever<Extract<keyof ToolExecutionApi, keyof FabrialFields>>;
type SectionCollision = AssertNever<Extract<keyof PromptInput, keyof FabrialFields>>;
type SimulatedReservedName = Extract<keyof ToolExecutionApi | "actor", keyof FabrialFields>;
// @ts-expect-error A hypothetical upstream actor field must fail the reserved-name assertion.
type SimulatedCollision = AssertNever<SimulatedReservedName>;

it("keeps reserved Fabrial fields disjoint from native Pi APIs", () => {
	expectTypeOf<ToolCollision>().toEqualTypeOf<never>();
	expectTypeOf<SectionCollision>().toEqualTypeOf<never>();
	expectTypeOf<SimulatedCollision>().toEqualTypeOf<"actor">();
	expectTypeOf<BoundToolApi>().toExtend<ToolExecutionApi>();
});
