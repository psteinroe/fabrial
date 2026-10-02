import { defineConfig } from "vitest/config";

export default defineConfig({
	server: {
		deps: {
			// SHIM(conductor#9): pgconductor-js is TypeScript source, so Vite must transform it.
			inline: [/pgconductor-js/],
		},
	},
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
		],
	},
});
