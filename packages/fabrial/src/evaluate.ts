import type { Json } from "./json.ts";

/** Questions follow the Vercel AI SDK `evaluate` / TypeSafe Jev shape. */
export type EvaluateQuestion =
	| { type: "choice"; instructions: string; criteria: Record<string, string> }
	| { type: "score"; instructions: string; rubric: Record<string, string> }
	| { type: "boolean"; instructions: string };

export interface EvaluateRequest<
	Q extends Record<string, EvaluateQuestion> = Record<string, EvaluateQuestion>,
> {
	/** A classifier model reference, resolved by the configured evaluator (Pi AI models). */
	model: ModelRef;
	state: Json;
	questions: Q;
}

export interface ModelRef {
	provider: string;
	modelId: string;
}

export type EvaluateAnswer<Q extends EvaluateQuestion> = Q extends {
	type: "choice";
	criteria: infer C;
}
	? {
			type: "choice";
			choice: keyof C & string;
			/** Probability of the selected option. */
			probability: number;
			probabilities: Record<keyof C & string, number>;
			/** Distribution statistic; not the selected option's probability. */
			confidence: number;
		}
	: Q extends { type: "score"; rubric: infer R }
		? {
				type: "score";
				score: keyof R & string;
				probabilities: Record<keyof R & string, number>;
				confidence: number;
			}
		: { type: "boolean"; value: boolean; probability: number };

export interface EvaluateResult<Q extends Record<string, EvaluateQuestion>> {
	stopReason: "stop" | "error";
	error?: string;
	answers: { [K in keyof Q]?: EvaluateAnswer<Q[K]> };
	model: ModelRef;
	usage?: Json;
}

/** Port: runs a classification. Implemented by `@fabrial/pi` on `pi-ai` `models.classify()`. */
export interface Evaluator {
	evaluate<Q extends Record<string, EvaluateQuestion>>(
		request: EvaluateRequest<Q>,
		signal: AbortSignal,
	): Promise<EvaluateResult<Q>>;
}
