import type { ClassifierQuestion, Models } from "@earendil-works/pi-ai";
import type { EvaluateQuestion, EvaluateRequest, EvaluateResult, Evaluator, Json } from "fabrial";

export function createEvaluator(models: Models): Evaluator {
	return {
		async evaluate<Q extends Record<string, EvaluateQuestion>>(
			request: EvaluateRequest<Q>,
			signal: AbortSignal,
		): Promise<EvaluateResult<Q>> {
			try {
				const model = models.getModelOfType(
					"classifier",
					request.model.provider,
					request.model.modelId,
				);
				if (!model)
					throw new Error(`Unknown classifier ${request.model.provider}/${request.model.modelId}`);
				const questions: Record<string, ClassifierQuestion> = {};
				const scoreKeys: Record<string, string[]> = {};
				for (const [key, question] of Object.entries(request.questions)) {
					if (question.type === "boolean")
						questions[key] = {
							type: "bool",
							instructions: question.instructions,
							criteria: { true: "Yes", false: "No" },
						};
					else if (question.type === "score") {
						scoreKeys[key] = Object.keys(question.rubric);
						questions[key] = {
							type: "score",
							instructions: question.instructions,
							criteria: Object.values(question.rubric),
						};
					} else questions[key] = question;
				}
				const state =
					request.state !== null &&
					typeof request.state === "object" &&
					!Array.isArray(request.state)
						? request.state
						: { value: request.state };
				const result = await models.classify(model, { state, questions }, { signal });
				const answers: Record<string, unknown> = {};
				for (const [key, answer] of Object.entries(result.answers)) {
					if (answer.type === "bool")
						answers[key] = {
							type: "boolean",
							value: answer.probability >= 0.5,
							probability: answer.probability,
						};
					else if (answer.type === "choice")
						answers[key] = { ...answer, probability: answer.probabilities[answer.choice] ?? 0 };
					else {
						const keys = scoreKeys[key];
						if (!keys || !Number.isInteger(answer.score) || !keys[answer.score])
							throw new Error(`Invalid classifier score for ${key}: ${answer.score}`);
						// Pi exposes no score distribution. Do not invent probabilities.
						answers[key] = {
							type: "score",
							score: keys[answer.score],
							probabilities: {},
							confidence: answer.confidence,
						};
					}
				}
				return {
					stopReason: result.stopReason === "stop" ? "stop" : "error",
					...(result.stopReason === "stop"
						? {}
						: { error: result.errorMessage ?? result.stopReason }),
					answers: answers as EvaluateResult<Q>["answers"],
					model: request.model,
					...(result.usage ? { usage: result.usage as unknown as Json } : {}),
				};
			} catch (error) {
				return {
					stopReason: "error",
					error: error instanceof Error ? error.message : String(error),
					answers: {},
					model: request.model,
				};
			}
		},
	};
}
