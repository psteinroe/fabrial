import { createFabrial } from "fabrial";
import { StubRuntime, testApp } from "fabrial/testing";
import postgres from "postgres";
import { afterEach, expect, it, vi } from "vitest";
import { databasePlugin } from "../../src/database.ts";

vi.mock("postgres", () => ({ default: vi.fn() }));
afterEach(() => {
	vi.unstubAllEnvs();
	vi.resetAllMocks();
	vi.resetModules();
});

it("imports Acme's definitions and app with unset env without creating clients or pools", async () => {
	for (const key of Object.keys(process.env)) {
		if (/^(SLACK_|GITHUB_|LINEAR_|APP_DATABASE_URL$)/.test(key)) vi.stubEnv(key, undefined);
	}
	vi.resetModules();
	const { app } = await import("../../src/app.ts");
	expect(postgres).not.toHaveBeenCalled();
	await expect(app.start()).rejects.toThrow("Slack requires workspace");
	expect(postgres).not.toHaveBeenCalled();
});

it("creates the application pool only on start, closes it on stop, and can restart", async () => {
	const end = vi.fn(async () => {});
	vi.mocked(postgres).mockReturnValue({ end } as never);
	const url = "postgres://localhost/acme";
	const options = { max: 2 };
	const plugin = databasePlugin(url, options);
	const app = testApp(createFabrial({ plugins: [plugin] }), {
		runtime: new StubRuntime(),
		workflows: [],
	});
	const client = app.host.clients().database;
	expect(postgres).not.toHaveBeenCalled();
	await expect(client.query("acme", "SELECT 1")).rejects.toThrow("not started");
	await app.start();
	expect(postgres).toHaveBeenCalledExactlyOnceWith(url, options);
	await app.stop();
	expect(end).toHaveBeenCalledOnce();
	await expect(client.query("acme", "SELECT 1")).rejects.toThrow("not started");
	await app.start();
	expect(postgres).toHaveBeenCalledTimes(2);
	await app.stop();
	expect(end).toHaveBeenCalledTimes(2);
});

it("defers missing application URL validation until startup", async () => {
	const app = testApp(createFabrial({ plugins: [databasePlugin(undefined)] }), {
		runtime: new StubRuntime(),
		workflows: [],
	});
	await expect(app.start()).rejects.toThrow("APP_DATABASE_URL");
	expect(postgres).not.toHaveBeenCalled();
});
