import { connectChat } from "../../../src/chat/index.ts";
import { randomUUID } from "node:crypto";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { expect, it, vi } from "vitest";
import {
	createFabrial,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeWorkflow,
} from "../../internals.ts";
import { FakeChat } from "../../../src/testing.ts";
import {
	callSession,
	connectAgents,
	PostgresStorage,
	type PiOptions,
} from "../../../src/agents/index.ts";
import { until } from "../../conductor/helper.ts";
import { useAgents } from "./setup.ts";

const definitions = createFabrial({ plugins: [] });
const { defineAgent, defineWorkflow } = definitions;

/**
 * `fabrial.sessions.reclaim` on real Conductor with the test clock: it deletes the Pi storage of `ctx.agent` calls
 * whose Session execution settled at least `retainCallSessionsDays` ago. Tests run it directly (no schedule).
 */
const { db, sql, all } = useAgents();
const RECLAIM = "fabrial.sessions.reclaim";
const GO = "reclaim-test.go";

function fixture(options: Partial<PiOptions> = {}) {
	const runtime = db.runtime();
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		requestId: randomUUID(),
		origin: null,
		replyTo: {
			kind: "thread",
			provider: "slack-test",
			threadId: `slack-test:general:${randomUUID()}`,
		},
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const chat = new FakeChat({ installationId: "w" });
	const host = {
		definitionScope: definitions,
		plugins: [],
		workflows: [],
		runtime,
		clients: () => ({}),
		chat: () => chat.connection(),
		logger: { warn() {}, error() {}, debug() {}, info() {} },
		directory: { isMember: async () => true },
		resolvePrincipal: async () => null,
	} as unknown as FabrialHost;
	// The fake platform behind `slack-test`, the provider of the tests' threads.
	connectChat(chat, { ...host, plugins: [...host.plugins, { id: "slack-test" }] } as FabrialHost);
	const integration = connectAgents(host, {
		models,
		sql: sql(),
		settings: { retry: { enabled: false } },
		reclaimSchedule: false,
		...options,
	});
	const agent = defineAgent({
		name: randomUUID(),
		description: "Answers test questions.",
		model: `${faux.provider.id}/${faux.getModel().id}`,
	});
	async function start(workflows: RuntimeWorkflow[], events: string[] = []) {
		runtime.register({
			workflows: [...workflows, ...integration.workflows],
			events: [
				...[GO, ...events].map((name) => ({ name, filterable: [] })),
				...(integration.events ?? []),
			],
		});
		await integration.start();
		await runtime.start();
	}
	/** Runs the reclaim job once and returns its result. */
	async function reclaim() {
		const id = await runtime.invoke(RECLAIM, null, { metadata });
		const result = await db.execution(id).settled();
		if (result.status !== "completed") throw new Error(JSON.stringify(result));
		return result.output as { reclaimed: number; batches: number };
	}
	return { runtime, faux, integration, metadata, host, agent, start, reclaim };
}

const stored = async (key: string) => {
	const [session] = await sql()`SELECT 1 FROM fabrial_pi.sessions WHERE key = ${key}`;
	const [commits] = await sql()<{ n: number }[]>`
		SELECT count(*)::int AS n FROM fabrial_pi.commits WHERE session_key = ${key}`;
	return { session: !!session, commits: commits!.n };
};

it("reclaims a completed call's Session once its execution settled 7 days ago, never a thread's; a replayed caller returns the stored answer without Pi", async () => {
	const f = fixture();
	f.faux.setResponses([fauxAssistantMessage("forty-two")]);
	const caller: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			const answer = await f.integration.run(execution, "answer", f.agent, {
				input: "Question",
			});
			const go = await execution.subscribe("go", { event: GO });
			await execution.waitForAny("later", { go });
			return answer;
		},
	};
	await f.start([caller]);
	const id = await f.runtime.invoke(caller.name, null, { metadata: f.metadata });
	await db.execution(id).suspended("later");
	const key = callSession(id, "answer");
	const [call] = await all(f.runtime.queue, "fabrial.session");
	expect((await db.execution(call!).settled()).status).toBe("completed");
	expect((await stored(key)).commits).toBeGreaterThan(0);
	// A thread's Session, last hosted by the same settled execution, is never a call's.
	const thread = `slack-test:${randomUUID()}`;
	await (
		await PostgresStorage.open(sql(), thread, { executionId: call! })
	).close(BACKGROUND_CONTEXT);

	expect((await f.reclaim()).reclaimed).toBe(0);
	await db.clock.advance("167h");
	expect((await f.reclaim()).reclaimed).toBe(0);
	expect((await stored(key)).session).toBe(true);
	await db.clock.advance("2h");
	expect((await f.reclaim()).reclaimed).toBe(1);
	expect(await stored(key)).toEqual({ session: false, commits: 0 });
	expect((await stored(thread)).session).toBe(true);

	// The caller replays from its stored call result: Pi is neither asked nor reopened.
	await f.runtime.emit(GO, {}, { metadata: f.metadata });
	expect(await db.execution(id).settled()).toEqual({ status: "completed", output: "forty-two" });
	expect(f.faux.state.callCount).toBe(1);
	expect((await stored(key)).session).toBe(false);
	expect((await f.reclaim()).reclaimed).toBe(0);
	await f.integration.stop();
});

it("reclaims a call cancelled while suspended on a workflow tool, but not while someone holds its lease", async () => {
	const f = fixture();
	const tool = defineWorkflow({ name: randomUUID(), run: async () => null });
	(f.host.workflows as unknown as (typeof tool)[]).push(tool);
	const agent = defineAgent({
		name: randomUUID(),
		description: "Runs a tool.",
		model: `${f.faux.provider.id}/${f.faux.getModel().id}`,
		tools: [tool],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall(tool.name.replaceAll(/[^a-zA-Z0-9_]/g, "_"), {}), {
			stopReason: "toolUse",
		}),
	]);
	const caller: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) => f.integration.run(execution, "agent", agent, { input: "Wait" }),
	};
	await f.start(
		[
			caller,
			{
				name: tool.name,
				triggers: [],
				handler: async (_input, execution) => {
					const never = await execution.subscribe("never", { event: `never-${tool.name}` });
					await execution.waitForAny("approval", { never });
					return "approved";
				},
			},
		],
		[`never-${tool.name}`],
	);
	const id = await f.runtime.invoke(caller.name, null, { metadata: f.metadata });
	const child = db.execution(
		await until(
			"the workflow tool to start",
			async () => (await all(f.runtime.queue, tool.name))[0],
		),
	);
	await child.suspended("approval");
	const [call] = await all(f.runtime.queue, "fabrial.session");
	await db.execution(call!).suspended();
	await f.runtime.cancel(id, "user cancelled");
	expect((await db.execution(call!).settled()).status).toBe("cancelled");
	const key = callSession(id, "agent");
	expect((await stored(key)).commits).toBeGreaterThan(0);

	await db.clock.advance("8d");
	// A live lease (here: someone else's) keeps the Session, whatever its execution did.
	await sql()`UPDATE fabrial_pi.sessions SET owner = gen_random_uuid(),
		lease_until = clock_timestamp() + interval '1 hour' WHERE key = ${key}`;
	expect((await f.reclaim()).reclaimed).toBe(0);
	expect((await stored(key)).session).toBe(true);
	await sql()`UPDATE fabrial_pi.sessions SET lease_until = clock_timestamp() - interval '1 second'
		WHERE key = ${key}`;
	expect((await f.reclaim()).reclaimed).toBe(1);
	expect(await stored(key)).toEqual({ session: false, commits: 0 });
	await f.integration.stop();
});

it("examines at most reclaimBatchSize Sessions per step", async () => {
	const f = fixture({ reclaimBatchSize: 2 });
	const calls = ["a", "b", "c", "d", "e"];
	f.faux.setResponses(calls.map((call) => fauxAssistantMessage(call)));
	const caller: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			for (const call of calls) await f.integration.run(execution, call, f.agent, { input: call });
			return null;
		},
	};
	await f.start([caller]);
	const id = await f.runtime.invoke(caller.name, null, { metadata: f.metadata });
	expect((await db.execution(id).settled()).status).toBe("completed");
	await db.clock.advance("8d");
	const reclaimable = vi.spyOn(PostgresStorage, "reclaimable");
	try {
		const { reclaimed, batches } = await f.reclaim();
		expect(reclaimed).toBe(5);
		expect(batches).toBeGreaterThanOrEqual(3);
		expect(reclaimable.mock.calls.every(([, , , limit]) => limit === 2)).toBe(true);
		for (const result of reclaimable.mock.results)
			expect((await result.value).length).toBeLessThanOrEqual(2);
	} finally {
		reclaimable.mockRestore();
	}
	for (const call of calls) expect((await stored(callSession(id, call))).session).toBe(false);
	await f.integration.stop();
});

it("schedules the reclaim daily by default; call Session executions outlive the retention", () => {
	const workflows = (options: Partial<PiOptions>) =>
		connectAgents(
			{
				definitionScope: definitions,
				plugins: [],
			} as unknown as FabrialHost,
			{ models: createModels(), sql: sql(), ...options },
		).workflows;
	const reclaim = workflows({}).find((workflow) => workflow.name === RECLAIM)!;
	expect(reclaim.cron).toEqual([{ schedule: "30 3 * * *", name: RECLAIM }]);
	// Callers replay from a call's result and reclaim reads when it settled: never removed before both.
	const session = workflows({}).find((workflow) => workflow.name === "fabrial.session")!;
	expect(session.retentionDays).toBeUndefined();
	const off = workflows({ reclaimSchedule: false }).find((workflow) => workflow.name === RECLAIM)!;
	expect(off.cron).toEqual([]);
});
