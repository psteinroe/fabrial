import { createModels } from "@earendil-works/pi-ai/models";
import { typesafeProvider } from "@earendil-works/pi-ai/providers/typesafe";
import { expect, it } from "vitest";
import { evaluate } from "../../../src/models.ts";

it.skipIf(!process.env.TYPESAFE_API_KEY)(
	"classifies with TypeSafe Jev when explicitly configured",
	async () => {
		const models = createModels();
		models.setProvider(typesafeProvider());
		const model = models.getModelsOfType("classifier", "typesafe")[0]!;
		const result = await evaluate(
			models,
			{
				model: `${model.provider}/${model.id}`,
				state: { statement: "Two plus two equals four." },
				questions: {
					correct: {
						type: "bool",
						instructions: "Is the statement correct?",
						criteria: { true: "Yes", false: "No" },
					},
				},
			},
			AbortSignal.timeout(30_000),
		);
		expect(result.stopReason).toBe("stop");
		expect(result.answers.correct).toMatchObject({ type: "bool" });
		expect((result.answers.correct as { probability: number }).probability).toBeGreaterThan(0.5);
	},
	40_000,
);
