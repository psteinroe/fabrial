import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
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
