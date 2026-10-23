import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { TestProject } from "vitest/node";

/** One Postgres per Vitest run; each test file creates its own database on it (see `helper.ts`). */
export default async function setup(project: TestProject) {
	const external = process.env.FABRIAL_TEST_DATABASE_URL;
	if (external) {
		project.provide("fabrialDatabaseUrl", external);
		return;
	}
	const container = await new PostgreSqlContainer("postgres:17").start();
	project.provide("fabrialDatabaseUrl", container.getConnectionUri());
	return async () => {
		await container.stop();
	};
}

declare module "vitest" {
	export interface ProvidedContext {
		fabrialDatabaseUrl: string;
	}
}
