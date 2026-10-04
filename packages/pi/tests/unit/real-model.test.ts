import { createModels } from "@earendil-works/pi-ai/models";
import { typesafeProvider } from "@earendil-works/pi-ai/providers/typesafe";
import { expect, it } from "vitest";
import { createEvaluator } from "../../src/index.ts";

it.skipIf(!process.env.TYPESAFE_API_KEY)(
	"classifies with TypeSafe Jev when explicitly configured",
	async () => {
		const models = createModels();
		models.setProvider(typesafeProvider());
		const model = models.getModelsOfType("classifier", "typesafe")[0]!;
		const result = await createEvaluator(models).evaluate(
			{
				model: { provider: model.provider, modelId: model.id },
				state: { statement: "Two plus two equals four." },
				questions: { correct: { type: "boolean", instructions: "Is the statement correct?" } },
			},
			AbortSignal.timeout(30_000),
		);
		expect(result.stopReason).toBe("stop");
		expect(result.answers.correct?.value).toBe(true);
	},
	40_000,
);
