import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineEvent,
	defineGroup,
	definePlugin,
	defineUser,
	trigger,
	weeklyRotation,
} from "../internals.ts";
import type { AnyWorkflow, EventFilter, ExternalIdentity } from "../internals.ts";
import { FakeChat, StubRuntime, testApp } from "../../src/testing-internal.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

const identity = (subjectId: string): ExternalIdentity => ({
	provider: "chat",
	installationId: "workspace",
	subjectId,
});
const alice = defineUser({ id: "alice", name: "Alice", identities: [identity("A")] });
const bob = defineUser({ id: "bob", name: "Bob", identities: [identity("B")] });
const outsider = defineUser({ id: "outsider", identities: [identity("O")] });
const team = defineGroup({ id: "team", members: [alice, bob] });
const surface = { kind: "thread", provider: "chat", threadId: "channel:general:thread" } as const;
const event = "chat.message";
const plugin = definePlugin({
	id: "chat",
	events: {
		message: defineEvent({ payload: z.record(z.string(), z.json()), filterable: ["channelId"] }),
	},
});
// Definition, routing-table and identity checks that run no workflow; executions are covered on real
// Conductor in tests/integration/core.
function setup(
	workflows: AnyWorkflow[],
	extra: Partial<import("../../src/app.ts").CoreConfig> = {},
	runtime: StubRuntime = new StubRuntime(),
) {
	const chat = new FakeChat(() => runtime.now());
	const config = {
		runtime,
		chat,
		plugins: [plugin],
		workflows,
		identity: [alice, bob, outsider],
		...extra,
	};
	const { plugins, identity: identities, logger, ...appConfig } = config;
	const app = testApp(createFabrial({ plugins, identity: identities }), { ...appConfig, logger });
	return { runtime, chat, app };
}
describe("routing table", () => {
	it.each([
		[{}, {}],
		[{ channelId: ["general"] }, { channelId: [{ prefix: "gen" }] }],
		[
			{ count: [{ numeric: [">", 5] as [string, number] }] },
			{ count: [{ numeric: ["<", 10] as [string, number] }] },
		],
	])("rejects overlapping equal-specificity owners", async (a, b) => {
		const make = (name: string, filter: EventFilter) =>
			defineWorkflow({
				name,
				triggers: [trigger({ event, filter, specificity: 2 })],
				run: async () => {},
			});
		await expect(setup([make("a", a), make("b", b)]).app.start()).rejects.toThrow("Ambiguous");
	});
	it("allows disjoint filters and overlapping triggers on the same workflow", async () => {
		const make = (name: string, channel: string) =>
			defineWorkflow({
				name,
				triggers: [
					trigger({ event, filter: { channelId: [channel] } }),
					trigger({ event, filter: { channelId: [channel] } }),
				],
				run: async () => {},
			});
		await expect(setup([make("a", "a"), make("b", "b")]).app.start()).resolves.toBeUndefined();
	});
	it("preserves cron replyTo in the registered runtime contract", () => {
		const cron = defineWorkflow({
			name: "cron",
			cron: [{ schedule: "0 * * * *", replyTo: surface }],
			run: async () => {},
		});
		const test = setup([cron]);
		expect((test.runtime as StubRuntime).workflows.get("cron")!.cron).toEqual([
			{ schedule: "0 * * * *", name: "cron", replyTo: surface },
		]);
	});
});

describe("identity", () => {
	it("resolves known and unknown identities, static/dynamic groups and names", async () => {
		const lookup = definePlugin({
			id: "chat",
			identity: { lookup: async () => ({ name: "Provider name" }) },
		});
		const runtime = new StubRuntime(Date.parse("2026-01-05T00:00:00Z"));
		const test = setup([], { plugins: [lookup] }, runtime);
		expect(await test.app.host.resolvePrincipal(identity("A"))).toMatchObject({
			id: "alice",
			known: true,
			name: "Alice",
		});
		expect(await test.app.host.resolvePrincipal(identity("unknown"))).toMatchObject({
			id: "chat:workspace:unknown",
			known: false,
			name: "Provider name",
		});
		expect(await test.app.host.directory.identitiesFor("alice")).toEqual(alice.identities);
		expect((await test.app.host.directory.members(team)).map((p) => p.id)).toEqual([
			"alice",
			"bob",
		]);
		const rotation = defineGroup({
			id: "rotation",
			resolve: ({ now }) => [weeklyRotation([alice, bob], { start: "2026-01-05", now })],
		});
		expect((await test.app.host.directory.members(rotation))[0]!.id).toBe("alice");
		runtime.time += 7 * 86400000;
		expect((await test.app.host.directory.members(rotation))[0]!.id).toBe("bob");
		expect(weeklyRotation([alice, bob], { start: "2026-01-05", now: new Date("2025-12-29") })).toBe(
			bob,
		);
	});
	it("recognizes users discovered through dynamic groups and provider-scoped membership", async () => {
		const user = defineUser({ id: "dynamic", identities: [identity("D")] });
		const group = defineGroup({ id: "dynamic-group", resolve: () => [user] });
		const test = setup([], { identity: [] });
		const before = await test.app.host.resolvePrincipal(identity("D"));
		expect(before.known).toBe(false);
		expect(await test.app.host.directory.isMember(before, group)).toBe(true);
		expect(await test.app.host.resolvePrincipal(identity("D"))).toMatchObject({
			known: true,
			id: "dynamic",
		});
		expect(await test.app.host.directory.identitiesFor("dynamic")).toEqual(user.identities);
	});
});

describe("fake ports", () => {
	it("fake ChatPort atomically updates state without exposing mutable stored objects", async () => {
		const chat = new FakeChat();
		const thread = await chat.thread(surface);
		await thread.updateState(() => ({ count: 0 }));
		await Promise.all(
			Array.from({ length: 100 }, () =>
				thread.updateState((state) => state && { ...state, count: Number(state.count) + 1 }),
			),
		);
		const state = await thread.getState();
		expect(state?.count).toBe(100);
		state!.count = 0;
		expect((await thread.getState())?.count).toBe(100);
	});
});
