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
export type Json = string | number | boolean | null | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };

/**
 * JSON Schema for the model, from the Standard JSON Schema interface (`~standard.jsonSchema`).
 * Schema libraries without it (or schemas it cannot express) are rejected when the app starts.
 */
export function toJsonSchema(schema: Schema, label: string): Record<string, unknown> {
	const json = jsonSchemaOf(schema, `${label}: the input schema`);
	if (json.type !== "object") throw new Error(`${label}: the input schema must describe an object`);
	return json;
}

/** The JSON Schema of the values `schema` accepts; it must implement Standard JSON Schema. */
export function jsonSchemaOf(schema: Schema, label: string): Record<string, unknown> {
	const converter = (
		schema["~standard"] as {
			jsonSchema?: { input(options: { target: string }): Record<string, unknown> };
		}
	).jsonSchema;
	if (!converter)
		throw new Error(
			`${label} must implement Standard JSON Schema (~standard.jsonSchema), e.g. zod >= 4.2`,
		);
	const { $schema: _dialect, ...json } = converter.input({ target: "draft-07" });
	return json;
}
export const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
