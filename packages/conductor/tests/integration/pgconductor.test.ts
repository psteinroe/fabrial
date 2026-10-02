import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import postgres from "postgres";
import { afterAll, beforeAll, expect, test } from "vitest";
import { z } from "zod";
import { Conductor, Orchestrator, TaskSchemas, defineTask } from "../../src/index.ts";

// Fabrial runs on Node while Conductor is developed and tested on Bun. This guards the assumption
// that its source runs unchanged on Node against a real Postgres.

let container: StartedPostgreSqlContainer;
let sql: postgres.Sql;

beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17").start();
	sql = postgres(container.getConnectionUri(), { max: 5 });
});

afterAll(async () => {
	await sql?.end();
	await container?.stop();
});

test("runs a task with a memoized step on Node", async () => {
	const greet = defineTask({ name: "greet", payload: z.object({ name: z.string() }) });
	const conductor = Conductor.create({
		sql,
		tasks: TaskSchemas.fromSchema([greet]),
		context: {},
	});

	const greetings: string[] = [];
	let stepRuns = 0;
	const task = conductor.createTask({ name: "greet" }, { invocable: true }, async (event, ctx) => {
		const greeting = await ctx.step("compose", () => {
			stepRuns++;
			return `Hello ${event.payload.name}`;
		});
		greetings.push(greeting);
	});

	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});
	await orchestrator.start();

	await conductor.invoke({ name: "greet" }, { name: "Fabrial" });
	await expect.poll(() => greetings, { timeout: 10_000 }).toEqual(["Hello Fabrial"]);

	await orchestrator.stop();
	await orchestrator.stopped;
	expect(stepRuns).toBe(1);
});
