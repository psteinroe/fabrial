import type { StandardSchemaV1 } from "@standard-schema/spec";

export type { StandardSchemaV1 };

/** Any Standard Schema (zod, valibot, arktype, …). */
export type Schema<T = unknown> = StandardSchemaV1<unknown, T>;
export type InferOutput<S> = S extends StandardSchemaV1<unknown, infer O> ? O : never;

export class SchemaValidationError extends Error {
	constructor(
		message: string,
		readonly issues: ReadonlyArray<StandardSchemaV1.Issue>,
	) {
		super(message);
		this.name = "SchemaValidationError";
	}
}

/** Validate `value` against a Standard Schema and return the parsed output. */
export async function parseSchema<T>(
	schema: Schema<T>,
	value: unknown,
	label = "value",
): Promise<T> {
	const result = await schema["~standard"].validate(value);
	if (result.issues) {
		const details = result.issues.map((issue) => issue.message).join("; ");
		throw new SchemaValidationError(`Invalid ${label}: ${details}`, result.issues);
	}
	return result.value;
}
