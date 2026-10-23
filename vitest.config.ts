import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// Every integration file runs Postgres-polling workers; more files at once on a loaded machine only
		// slow each other past their timeouts.
		maxWorkers: 4,
		projects: [
			{
				extends: true,
				test: {
					name: "unit",
					include: ["packages/*/tests/unit/**/*.test.ts"],
				},
			},
			{
				extends: true,
				test: {
					name: "integration",
					include: ["packages/*/tests/integration/**/*.test.ts"],
					globalSetup: ["./packages/fabrial/tests/conductor/global-setup.ts"],
					testTimeout: 60_000,
					hookTimeout: 120_000,
				},
			},
			{
				extends: true,
				test: {
					name: "examples",
					include: ["examples/*/tests/**/*.test.ts"],
					globalSetup: ["./examples/acme/tests/global-setup.ts"],
					testTimeout: 60_000,
					hookTimeout: 120_000,
				},
			},
		],
	},
});
