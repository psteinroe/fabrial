import { expect, expectTypeOf, it, vi } from "vitest";
import { createFabrial, definePlugin, defineUser } from "../../src/index.ts";
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

// Compiled only: plugins have one configuration point, at definition.
function invalidConfig() {
	const runtime = new MemoryRuntime();
	// @ts-expect-error Plugins cannot be passed to f.app().
	f.app({ runtime, workflows: [], plugins: { database: database("fake") } });
	// @ts-expect-error Groups are referenced by workflows, not registered as users.
	createFabrial({ plugins: [], identity: [group] });
	// @ts-expect-error The introspection catalog cannot be mutated.
	f.plugins.push(database("fake"));
}
void invalidConfig;

it("reuses definitions across isolated apps with definition-time plugins and spied clients", async () => {
	const production = createTestApp(f, { workflows: [workflow] });
	const test = createTestApp(f, { workflows: [workflow] });
	vi.spyOn(test.app.host, "clients").mockReturnValue({ database: { query: () => "fake" } });
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
			// This group was never registered in identity.
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

it("does not connect chat adapters until startup", async () => {
	const connect = vi.fn(() => ({ port: {} as never, routes: {} }));
	const instance = createFabrial({
		plugins: [{ id: "chat", chat: { installationId: "", adapter: vi.fn() } }],
	});
	const app = instance.app({
		runtime: new MemoryRuntime(),
		workflows: [],
		chat: { kind: "fabrial.chat", connect },
	});
	expect(connect).not.toHaveBeenCalled();
	await app.start();
	expect(connect).toHaveBeenCalledOnce();
	await app.stop();
});
