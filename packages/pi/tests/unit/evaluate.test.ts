import { createModels, createProvider } from "@earendil-works/pi-ai/models";
import type { ClassifierContext, ClassifierModel } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { createEvaluator } from "../../src/index.ts";

const ref = { provider: "test", modelId: "classifier" };
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
	return { evaluator: createEvaluator(models), received: () => received };
}

describe("Pi classifier adapter", () => {
	it("maps boolean, choice and ordered score rubric without inventing score probabilities", async () => {
		const { evaluator, received } = fixture();
		const result = await evaluator.evaluate(
			{
				model: ref,
				state: "hello",
				questions: {
					ok: { type: "boolean", instructions: "Good?" },
					route: { type: "choice", instructions: "Route", criteria: { a: "A", b: "B" } },
					score: { type: "score", instructions: "Score", rubric: { low: "Low", high: "High" } },
				},
			},
			new AbortController().signal,
		);
		expect(received()?.questions.ok?.type).toBe("bool");
		expect(received()?.state).toEqual({ value: "hello" });
		expect(result.answers.ok).toEqual({ type: "boolean", value: true, probability: 0.8 });
		expect(result.answers.route).toMatchObject({
			choice: "a",
			probability: 0.7,
			confidence: 0.4,
			probabilities: { a: 0.7, b: 0.3 },
		});
		expect(result.answers.score).toEqual({
			type: "score",
			score: "high",
			probabilities: {},
			confidence: 0.6,
		});
	});
	it("returns errors rather than rejecting for unavailable classifiers", async () => {
		const result = await createEvaluator(createModels()).evaluate(
			{ model: ref, state: {}, questions: {} },
			new AbortController().signal,
		);
		expect(result.stopReason).toBe("error");
		expect(result.error).toContain("Unknown classifier");
	});
});
