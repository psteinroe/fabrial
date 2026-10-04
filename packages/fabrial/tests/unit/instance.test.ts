import { expect, expectTypeOf, it } from "vitest";
import { createFabrial, definePlugin, defineUser, type AnyPlugin } from "../../src/index.ts";
import { createTestApp, MemoryRuntime } from "fabrial/testing";

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
		return ctx.clients.database.query();
	},
});

// Compiled only: overrides must retain the registered id and client contract.
function invalidOverrides() {
	const runtime = new MemoryRuntime();
	// @ts-expect-error Unknown override key.
	f.app({ runtime, workflows: [], plugins: { unknown: database("fake") } });
	f.app({
		runtime,
		workflows: [],
		// @ts-expect-error Different plugin id, even with identical clients.
		plugins: { database: { id: "other", clients: () => ({ database: { query: () => "fake" } }) } },
	});
	f.app({
		runtime,
		workflows: [],
		plugins: {
			// @ts-expect-error Same id with an incompatible client contract.
			database: definePlugin({
				id: "database",
				clients: () => ({ database: { query: () => 42 } }),
			}),
		},
	});
	const overrides = { database: database("fake"), unknown: database("fake") };
	// @ts-expect-error Unknown keys must fail for variables too, not only fresh object literals.
	f.app({ runtime, workflows: [], plugins: overrides });
	// @ts-expect-error The introspection catalog cannot be mutated.
	f.plugins.push(database("fake"));
}
void invalidOverrides;

it("reuses plain definitions across isolated apps with typed plugin overrides", async () => {
	const production = createTestApp(f, { workflows: [workflow] });
	const test = createTestApp(f, { workflows: [workflow], plugins: { database: database("fake") } });
	expect(f.plugins[0]!.clients!({}).database.query()).toBe("production");
	expect(Object.isFrozen(f.plugins)).toBe(true);
	for (const [fixture, value] of [
		[production, "production"],
		[test, "fake"],
	] as const) {
		await fixture.app.start();
		try {
			const id = await fixture.runtime.invoke(workflow.name, null, {
				metadata: {
					interactionId: "query",
					origin: null,
					replyTo: null,
					requestedBy: null,
					ownsThread: false,
				},
			});
			await fixture.runtime.flush();
			expect(fixture.runtime.result(id)).toEqual({ status: "completed", output: value });
			expect(await fixture.app.host.directory.members(group)).toMatchObject([{ id: "alice" }]);
			expect(fixture.app.host.clients().database.query()).toBe(value);
		} finally {
			await fixture.app.stop();
		}
	}
});

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

it("rejects unknown and mismatched override ids at runtime for untyped callers", () => {
	const config = { runtime: new MemoryRuntime(), workflows: [] };
	const untyped = f.app as (config: {
		runtime: MemoryRuntime;
		workflows: [];
		plugins: Record<string, AnyPlugin>;
	}) => unknown;
	expect(() =>
		untyped({ ...config, workflows: [], plugins: { unknown: database("fake") } }),
	).toThrow("Unknown plugin override: unknown");
	expect(() =>
		f.app({
			...config,
			plugins: { database: { id: "other" } as unknown as ReturnType<typeof database> },
		}),
	).toThrow("Plugin override database has id other");
});
