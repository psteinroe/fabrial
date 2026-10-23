import { createModels, createProvider } from "@earendil-works/pi-ai/models";
import type { ClassifierContext, ClassifierModel, ClassifierQuestion } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { evaluate } from "../../../src/models.ts";

const ref = "test/classifier";
function fixture() {
	const models = createModels();
	let received: ClassifierContext | undefined;
	models.setProvider(
		createProvider({
			id: "test",
			auth: { apiKey: { name: "Test", resolve: async () => ({ auth: {} }) } },
			models: [
				{
					type: "classifier",
					id: "classifier",
					name: "Test",
					api: "test-classifier",
					provider: "test",
					baseUrl: "http://example.test",
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				} as ClassifierModel<string>,
			],
			classifiers: {
				"test-classifier": {
					classify: async (_model, context) => {
						received = context;
						return {
							api: "test-classifier",
							provider: "test",
							model: "classifier",
							timestamp: 0,
							stopReason: "stop",
							answers: {
								ok: { type: "bool", probability: 0.8 },
								route: {
									type: "choice",
									choice: "a",
									probabilities: { a: 0.7, b: 0.3 },
									confidence: 0.4,
								},
								score: { type: "score", score: 1, confidence: 0.6 },
							},
						};
					},
				},
			},
		}),
	);
	return { models, received: () => received };
}

describe("ctx.evaluate", () => {
	it("passes Pi AI's questions and returns its result as is", async () => {
		const { models, received } = fixture();
		const questions = {
			ok: { type: "bool" as const, instructions: "Good?", criteria: { true: "Yes", false: "No" } },
			route: { type: "choice", instructions: "Route", criteria: { a: "A", b: "B" } },
			score: { type: "score", instructions: "Score", criteria: ["Low", "High"] },
		} satisfies Record<string, ClassifierQuestion>;
		const result = await evaluate(models, { model: ref, state: { text: "hello" }, questions });
		expect(received()).toEqual({ state: { text: "hello" }, questions });
		expect(result.answers).toEqual({
			ok: { type: "bool", probability: 0.8 },
			route: { type: "choice", choice: "a", probabilities: { a: 0.7, b: 0.3 }, confidence: 0.4 },
			score: { type: "score", score: 1, confidence: 0.6 },
		});
	});
	it("returns an error result rather than rejecting for unknown classifiers", async () => {
		const result = await evaluate(createModels(), { model: ref, state: {}, questions: {} });
		expect(result).toMatchObject({
			stopReason: "error",
			errorMessage: "Unknown classifier test/classifier",
			provider: "test",
			model: "classifier",
			answers: {},
		});
	});
});
