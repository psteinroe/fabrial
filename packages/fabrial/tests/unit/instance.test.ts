import { createMemoryState } from "@chat-adapter/state-memory";
import { expect, expectTypeOf, it, vi } from "vitest";
import { createFabrial, definePlugin, defineUser } from "../internals.ts";
import { testApp } from "../../src/testing/app.ts";
import postgres from "postgres";

const database = definePlugin((value: string) => ({
	id: "database",
	clients: () => ({ database: { query: () => value } }),
}));
const f = createFabrial({ plugins: [database("production")] });
const user = defineUser({ id: "alice", identities: [] });
const group = f.defineGroup({
	id: "readers",
	resolve(ctx) {
		expectTypeOf(ctx.clients.database.query()).toEqualTypeOf<string>();
		// @ts-expect-error Clients are closed, not globally augmented or string-indexed.
		void ctx.clients.databse;
		return [user];
	},
});
const workflow = f.defineWorkflow({
	name: "query",
	async run(_input, ctx) {
		expectTypeOf(ctx.clients.database.query()).toEqualTypeOf<string>();
		// @ts-expect-error A misspelled client must not compile.
		void ctx.clients.databse;
		return { rows: ctx.clients.database.query() };
	},
});

// Compiled only: plugins have one configuration point, at definition.
function invalidConfig() {
	// @ts-expect-error Plugins cannot be passed to f.app().
	f.app({ workflows: [], plugins: { database: database("fake") } });
	// @ts-expect-error Groups are referenced by workflows, not registered as users.
	createFabrial({ plugins: [], identity: [group] });
	// @ts-expect-error The introspection catalog cannot be mutated.
	f.plugins.push(database("fake"));
}
void invalidConfig;
void workflow;

it("isolates instance client types and supports client-less catalogs", () => {
	const other = createFabrial({
		plugins: [
			definePlugin({ id: "other", clients: () => ({ other: 42 }) }),
			definePlugin({ id: "second", clients: () => ({ second: "merged" }) }),
			{ id: "client-less" },
		],
	});
	other.defineWorkflow({
		name: "other",
		async run(_input, ctx) {
			expectTypeOf(ctx.clients.other).toEqualTypeOf<number>();
			expectTypeOf(ctx.clients.second).toEqualTypeOf<string>();
			// @ts-expect-error No database client from another instance leaks here.
			void ctx.clients.database;
		},
	});
	createFabrial({ plugins: [] }).defineWorkflow({
		name: "empty",
		async run(_input, ctx) {
			expectTypeOf(ctx.clients).toEqualTypeOf<{}>();
			// @ts-expect-error Empty catalog has no clients.
			void ctx.clients.database;
		},
	});
});

it("does not connect chat adapters until startup", async () => {
	const connect = vi.fn(() => undefined);
	const instance = createFabrial({
		plugins: [{ id: "chat", chat: { installationId: "", adapter: vi.fn() } }],
	});
	const app = testApp(instance, {
		// Reaches no database: the start fails when Conductor connects, after chat connected. A missing
		// socket is not an error Conductor retries.
		conductor: { sql: postgres({ host: "/nonexistent-fabrial", max: 1 }) },
		workflows: [],
		chat: { state: createMemoryState(), capability: connect },
	});
	expect(connect).not.toHaveBeenCalled();
	await expect(app.start()).rejects.toThrow(/ENOENT/);
	expect(connect).toHaveBeenCalledOnce();
	await app.stop();
});
