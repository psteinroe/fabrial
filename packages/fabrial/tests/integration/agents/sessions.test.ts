import { randomUUID } from "node:crypto";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import type { Context as TranscriptContext } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import { z } from "zod";
import {
	createFabrial,
	defineUser,
	type InvocationMetadata,
	type Json,
	type ThreadRef,
} from "../../../src/index.ts";
import { toolName } from "../../../src/agent.ts";
import { pi, PostgresStorage } from "../../../src/agents/index.ts";
import { conductor } from "../../../src/runtime/conductor/index.ts";
import { FakeChat, testApp } from "../../../src/testing.ts";

/**
 * Sessions hosted in keyed Conductor executions: every case runs the real runtime, and the cross-process cases two
 * app instances (two orchestrators, two pools) on one database, since signals reach only a shared runtime.
 */
const alice = defineUser({
	id: "alice",
	name: "Alice",
	identities: [{ provider: "slack-test", installationId: "w", subjectId: "alice" }],
});
const f = createFabrial({ plugins: [], identity: [alice] });
const probe = fauxProvider();
const model = `${probe.provider.id}/${probe.getModel().id}`;
const debug = process.env.SESSIONS_DEBUG ? (...args: unknown[]) => console.log(...args) : () => {};
const logger = { debug() {}, info() {}, warn: debug, error: debug };
const poll = { timeout: 60_000, interval: 50 };
const SESSION = "fabrial.session";

let container: StartedPostgreSqlContainer;
let admin: Sql;
let url: string;
let sql: Sql;
const stops: (() => Promise<void>)[] = [];
beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	admin = postgres(container.getConnectionUri(), { onnotice: () => {} });
});
beforeEach(async () => {
	const database = `test_${randomUUID().replaceAll("-", "")}`;
	await admin.unsafe(`create database ${database}`);
	const uri = new URL(container.getConnectionUri());
	uri.pathname = database;
	url = uri.toString();
	sql = postgres(url, { max: 4, onnotice: () => {} });
	requests.length = 0;
	script = () => say("unscripted");
});
afterEach(async () => {
	for (const stop of stops.splice(0).reverse()) await stop();
	await sql.end();
});
afterAll(async () => {
	await admin?.end();
	await container?.stop();
});

type Reply = ReturnType<typeof fauxAssistantMessage>;
/** A model request: the instance that sent it and the transcript it carried. */
interface Request {
	instance: string;
	context: TranscriptContext;
	signal?: AbortSignal;
}
const requests: Request[] = [];
let script: (request: Request) => Reply | Promise<Reply>;
const use = (tool: string, args: object) =>
	fauxAssistantMessage(fauxToolCall(tool, args as never), { stopReason: "toolUse" });
const say = (text: string) => fauxAssistantMessage(text);
const lastText = (context: TranscriptContext) => {
	const last = context.messages.findLast((message) => (message.role as string) !== "system")!;
	const content =
		typeof last.content === "string" ? [{ type: "text", text: last.content }] : last.content;
	return content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
};
const transcript = (request: Request) => JSON.stringify(request.context.messages);

/** Opens when the test says so; `entered` resolves once something waits on it. */
function gate() {
	let open!: () => void;
	let enter!: () => void;
	const opened = new Promise<void>((resolve) => (open = resolve));
	const entered = new Promise<void>((resolve) => (enter = resolve));
	return {
		open,
		entered,
		/** Waits until opened or until `signal` aborts. */
		pass: async (signal?: AbortSignal) => {
			enter();
			await Promise.race([
				opened,
				new Promise<void>((resolve) => {
					if (signal?.aborted) resolve();
					signal?.addEventListener("abort", () => resolve(), { once: true });
				}),
			]);
		},
	};
}

/** One app instance on the test database: its own pool, orchestrator, chat, and model provider. */
async function launch(name: string, workflows: Parameters<typeof testApp>[1]["workflows"]) {
	const pool = postgres(url, { max: 10, onnotice: () => {} });
	const runtime = conductor(
		{ sql: pool },
		{
			logger,
			worker: { pollIntervalMs: 20, flushIntervalMs: 20, concurrency: 8, fetchBatchSize: 10 },
			pollIntervalMs: 50,
		},
	);
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	faux.setResponses(
		Array.from(
			{ length: 100 },
			() => (context: TranscriptContext, options?: { signal?: AbortSignal }) => {
				const request = {
					instance: name,
					context: structuredClone(context),
					signal: options?.signal,
				};
				requests.push(request);
				return script(request);
			},
		),
	);
	const chat = new FakeChat();
	const app = testApp(f, {
		workflows,
		runtime,
		chat,
		logger,
		agents: pi({ models, sql: pool, settings: { retry: { enabled: false } } }),
	});
	await app.start();
	let stopped = false;
	const stop = async () => {
		if (stopped) return;
		stopped = true;
		await app.stop();
		await pool.end();
	};
	stops.push(stop);
	return { name, runtime, chat, app, stop };
}

const metadataOf = (): InvocationMetadata => ({
	interactionId: randomUUID(),
	origin: null,
	replyTo: null,
	requestedBy: { id: alice.id, known: true, identities: [], name: "Alice" },
	ownerWorkflow: null,
	triggerEvent: null,
});
const short = () => randomUUID().slice(0, 8);
const thread = (): ThreadRef => ({
	kind: "thread",
	provider: "slack-test",
	threadId: randomUUID(),
});

/** Session executions: how many exist and how many run (are locked by a worker) now. */
async function sessionExecutions() {
	const rows = await sql<{ id: string; running: boolean; done: boolean }[]>`
		select id, locked_by is not null as running, completed_at is not null as done
		from pgconductor._private_executions where task_key = ${SESSION}`;
	return rows;
}
async function lease(key: string): Promise<string | null> {
	const [row] = await sql<{ owner: string | null }[]>`
		select owner from fabrial_pi.sessions where key = ${key}`;
	return row?.owner ?? null;
}
async function execution(id: string) {
	const [row] = await sql<{ done: boolean; cancelled: boolean; failed: boolean }[]>`
		select completed_at is not null as done, cancelled, failed_at is not null as failed
		from pgconductor._private_executions where id = ${id}::uuid`;
	return row;
}
async function output(id: string): Promise<Json> {
	const [row] = await sql<{ result: { result: Json } }[]>`
		select result from pgconductor._private_steps where execution_id = ${id}::uuid and key = '__fabrial:output'`;
	return row?.result.result ?? null;
}
/** Request ids Pi accepted in a Session, in order, read from its commit log without taking the lease. */
async function submitted(key: string): Promise<string[]> {
	const rows =
		await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${key} ORDER BY seq`;
	const ids: string[] = [];
	for (const row of rows)
		for (const write of JSON.parse(String(row.writes)) as StorageWrite[])
			if (
				write.type === "submission" &&
				write.value.requestId &&
				!ids.includes(write.value.requestId)
			)
				ids.push(write.value.requestId);
	return ids;
}
/** The key of the test's only Pi Session. */
async function only(): Promise<string> {
	const rows = await sql<{ key: string }[]>`select key from fabrial_pi.sessions`;
	expect(rows).toHaveLength(1);
	return rows[0]!.key;
}
/** Every Session execution has returned, and the Session's lease is free. */
async function released(key: string) {
	await expect
		.poll(async () => (await sessionExecutions()).every((row) => row.done), poll)
		.toBe(true);
	expect(await lease(key)).toBeNull();
}

it("a message ingested on another instance steers the run its host is busy with; never two Session executions run at once", async () => {
	const look = f.defineTool({
		name: "look",
		description: "Looking around",
		input: z.object({}),
		run: async () => "nothing",
	});
	const lead = f.defineAgent({
		name: `lead-${short()}`,
		description: "Answers.",
		model,
		tools: [look],
	});
	const session = `ops-${short()}`;
	const replyTo = thread();
	const deliver = f.defineWorkflow({
		name: `deliver-${short()}`,
		input: z.string(),
		run: async (input, ctx) => {
			// Plain workflow input is a notification; urgent ones steer a busy run like a chat message.
			await ctx.send("send", lead, {
				session,
				input,
				priority: "urgent",
				...(replyTo ? { home: replyTo } : {}),
			});
		},
	});
	const busy = gate();
	script = async ({ signal }) => {
		if (requests.length === 1) {
			await busy.pass(signal);
			return use("look", {});
		}
		return say("done");
	};
	const a = await launch("a", [deliver]);
	await a.runtime.invoke(deliver.name, "first question", { metadata: metadataOf() });
	await busy.entered;
	// B starts while A hosts the Session, so A's worker holds its execution.
	const b = await launch("b", [deliver]);
	const [hosting] = await sessionExecutions();
	expect(hosting).toMatchObject({ running: true, done: false });
	// Never two Session executions running at once (sampled throughout).
	let concurrent = 0;
	const sampler = setInterval(() => {
		void sessionExecutions().then(
			(rows) => (concurrent = Math.max(concurrent, rows.filter((row) => row.running).length)),
			() => {},
		);
	}, 25);
	await b.runtime.invoke(deliver.name, "second question", { metadata: metadataOf() });
	await expect.poll(async () => (await submitted(await only())).length, poll).toBe(2);
	expect(await sessionExecutions()).toEqual([hosting]);
	expect(requests).toHaveLength(1);
	busy.open();
	await released(await only());
	clearInterval(sampler);
	// The steer lands in the same run's next request, after the tool round; the one answer posts once.
	expect(requests).toHaveLength(2);
	expect(requests[0]!.instance).toBe("a");
	expect(transcript(requests[0]!)).not.toContain("second question");
	expect(transcript(requests[1]!)).toContain('"text":"nothing"');
	expect(transcript(requests[1]!)).toContain("second question");
	expect(concurrent).toBe(1);
	const posts = [...(await a.chat.thread(replyTo)).posts, ...(await b.chat.thread(replyTo)).posts];
	expect(posts.map((post) => post.content)).toEqual(["done"]);
});

it("a message is answered while only a background child works; the child's report comes after", async () => {
	const researcher = f.defineAgent({ name: `researcher-${short()}`, description: "Fixes.", model });
	const lead = f.defineAgent({
		name: `lead-${short()}`,
		description: "Leads.",
		model,
		subagents: [researcher],
	});
	const tool = toolName(researcher.name);
	const session = `ticket-${short()}`;
	const replyTo = thread();
	const deliver = f.defineWorkflow({
		name: `deliver-${short()}`,
		input: z.string(),
		run: async (input, ctx) => {
			await ctx.send("send", lead, { session, input, ...(replyTo ? { home: replyTo } : {}) });
		},
	});
	const child = gate();
	script = async ({ context }) => {
		const text = lastText(context);
		if (text === "Fix it") {
			await child.pass();
			return say("Fixed.");
		}
		if (text.includes("Fix the row")) return use(tool, { task: "Fix it", background: true });
		if (text.startsWith("Started")) return say("On it.");
		if (text.includes("Status?")) return say("Still working.");
		if (text.startsWith("<subagent")) return say("Done.");
		return say(`unexpected: ${text}`);
	};
	const a = await launch("a", [deliver]);
	const posts = async () =>
		(await a.chat.thread(replyTo)).posts.flatMap((post) =>
			typeof post.content === "string" ? [post.content] : [],
		);
	await a.runtime.invoke(deliver.name, "Fix the row", { metadata: metadataOf() });
	await child.entered;
	await expect.poll(posts, poll).toEqual(["On it."]);
	await a.runtime.invoke(deliver.name, "Status?", { metadata: metadataOf() });
	// Answered while the child's model call is still held.
	await expect.poll(posts, poll).toEqual(["On it.", "Still working."]);
	expect(requests.filter((request) => lastText(request.context).startsWith("<subagent"))).toEqual(
		[],
	);
	child.open();
	await expect.poll(posts, poll).toEqual(["On it.", "Still working.", "Done."]);
	await released(await only());
});

for (const from of ["the same", "another"] as const)
	it(`cancelling a ctx.agent caller on ${from} instance aborts the run in Pi and settles the call cancelled`, async () => {
		const lead = f.defineAgent({ name: `lead-${short()}`, description: "Answers.", model });
		const ask = f.defineWorkflow({
			name: `ask-${short()}`,
			input: z.string(),
			run: (input, ctx) => ctx.agent("answer", lead, { input }),
		});
		const held = gate();
		let aborted = false;
		script = async ({ signal }) => {
			await held.pass(signal);
			aborted = !!signal?.aborted;
			return say("too late");
		};
		const a = await launch("a", [ask]);
		const metadata = metadataOf();
		const caller = await a.runtime.invoke(ask.name, "Take your time", { metadata });
		await held.entered;
		const canceller = from === "another" ? (await launch("b", [ask])).runtime : a.runtime;
		await canceller.cancel(caller, "user cancelled");
		await expect.poll(() => aborted, poll).toBe(true);
		await expect.poll(() => execution(caller), poll).toMatchObject({ cancelled: true });
		await released(metadata.interactionId);
		expect(requests.map((request) => request.instance)).toEqual(["a"]);
		// The run is done: a later call in the Session gets a fresh run and answers.
		script = () => say("fresh");
		const next = await a.runtime.invoke(ask.name, "Again", { metadata });
		await expect.poll(() => execution(next), poll).toMatchObject({ done: true, cancelled: false });
		expect(await output(next)).toBe("fresh");
	});

it("a run resumes on a fresh instance after its host stops mid-run; each model request is sent once", async () => {
	let lookups = 0;
	let slows = 0;
	const slow = gate();
	const lookup = f.defineTool({
		name: "lookup",
		description: "Looks up.",
		input: z.object({}),
		run: () => `found ${++lookups}`,
	});
	const wait = f.defineTool({
		name: "wait",
		description: "Waits.",
		input: z.object({}),
		idempotent: true,
		run: async (_input, ctx) => {
			// The first call is interrupted by the stop; the replay on the fresh instance completes.
			if (++slows === 1) await slow.pass(ctx.signal);
			return "waited";
		},
	});
	const lead = f.defineAgent({
		name: `lead-${short()}`,
		description: "Answers.",
		model,
		tools: [lookup, wait],
	});
	const ask = f.defineWorkflow({
		name: `ask-${short()}`,
		input: z.string(),
		run: (input, ctx) => ctx.agent("answer", lead, { input }),
	});
	script = ({ context }) => {
		const text = lastText(context);
		if (text === "Go") return use("lookup", {});
		if (text.startsWith("found")) return use("wait", {});
		if (text === "waited") return say("all done");
		return say(`unexpected: ${text}`);
	};
	const a = await launch("a", [ask]);
	const metadata = metadataOf();
	const caller = await a.runtime.invoke(ask.name, "Go", { metadata });
	await slow.entered;
	await a.stop();
	expect(await lease(metadata.interactionId)).toBeNull();
	await launch("b", [ask]);
	await expect.poll(() => execution(caller), poll).toMatchObject({ done: true });
	expect(await output(caller)).toBe("all done");
	expect(requests.map((request) => [request.instance, lastText(request.context)])).toEqual([
		["a", "Go"],
		["a", "found 1"],
		["b", "waited"],
	]);
	expect(lookups).toBe(1);
	expect(slows).toBe(2);
	await released(metadata.interactionId);
});

it("input redelivered with the same request id, also through another instance, is processed once", async () => {
	const lead = f.defineAgent({ name: `lead-${short()}`, description: "Answers.", model });
	const session = `ops-${short()}`;
	const busy = gate();
	script = async ({ context }) => {
		if (requests.length === 1) await busy.pass(requests[0]!.signal);
		return say(`answered ${lastText(context)}`);
	};
	const a = await launch("a", []);
	const b = await launch("b", []);
	const metadata = { ...metadataOf(), piSession: session } as InvocationMetadata;
	const input = (requestId: string, content: string) => ({
		workflow: SESSION,
		key: session,
		signal: {
			name: "input",
			id: requestId,
			payload: {
				kind: "input",
				agent: lead.name,
				requestId,
				content,
				delivery: "followUp",
				metadata,
			} as unknown as Json,
		},
		input: { session },
		metadata,
	});
	const first = await a.runtime.signalWithStart(input("r1", "one"));
	await busy.entered;
	// The same signal through both instances, and the same request under another signal id.
	expect(await b.runtime.signalWithStart(input("r1", "one"))).toBe(first);
	expect(await a.runtime.signalWithStart(input("r1", "one"))).toBe(first);
	await b.runtime.signalWithStart(input("r2", "two"));
	await b.runtime.signalWithStart({
		...input("r1", "one"),
		signal: { ...input("r1", "one").signal, id: "r1:again" },
	});
	await expect.poll(async () => (await submitted(session)).length, poll).toBe(2);
	busy.open();
	await released(session);
	// After the Session closed, a redelivery starts a new execution that admits nothing.
	await b.runtime.signalWithStart(input("r1", "one"));
	await expect.poll(async () => (await sessionExecutions()).length, poll).toBe(2);
	await released(session);
	expect(await submitted(session)).toEqual(["r1", "r2"]);
	expect(requests.map((request) => lastText(request.context))).toEqual(["one", "two"]);
	const users = requests.at(-1)!.context.messages.filter((message) => message.role === "user");
	expect(users.filter((message) => JSON.stringify(message.content).includes("one"))).toHaveLength(
		1,
	);
});

it("a host that loses its Session lease during a model call closes its harness and sends nothing more", async () => {
	let lookups = 0;
	const lookup = f.defineTool({
		name: "lookup",
		description: "Looks up.",
		input: z.object({}),
		run: () => `found ${++lookups}`,
	});
	const lead = f.defineAgent({
		name: `lead-${short()}`,
		description: "Answers.",
		model,
		tools: [lookup],
	});
	const session = `ops-${short()}`;
	const replyTo = thread();
	const deliver = f.defineWorkflow({
		name: `deliver-${short()}`,
		input: z.string(),
		run: async (input, ctx) => {
			await ctx.send("send", lead, { session, input, ...(replyTo ? { home: replyTo } : {}) });
		},
	});
	const held = gate();
	script = async ({ context }) => {
		const text = lastText(context);
		if (requests.length === 1) await held.pass(requests[0]!.signal);
		if (text.startsWith("found")) return say("done");
		return use("lookup", {});
	};
	const a = await launch("a", [deliver]);
	await a.runtime.invoke(deliver.name, "Go", { metadata: metadataOf() });
	await held.entered;
	const owner = await lease(await only());
	expect(owner).not.toBeNull();
	// Another owner takes the Session over once A's lease has expired.
	await sql`UPDATE fabrial_pi.sessions SET lease_until = clock_timestamp() - interval '1 second' WHERE key = ${await only()}`;
	const thief = await PostgresStorage.open(sql, await only());
	try {
		held.open();
		// A's attempt ends: committing the response fails, and the harness closes without running the tool.
		await expect
			.poll(async () => (await sessionExecutions()).some((row) => row.running), poll)
			.toBe(false);
		expect(requests).toHaveLength(1);
		expect(lookups).toBe(0);
		expect(await lease(await only())).toBe(thief.owner);
	} finally {
		await thief.close(BACKGROUND_CONTEXT);
	}
	// The retried attempt resumes the run: the uncommitted request is sent again, then the run completes once.
	await released(await only());
	expect(requests.map((request) => lastText(request.context).replace(/<[^>]*>/g, ""))).toEqual([
		"Go",
		"Go",
		"found 1",
	]);
	expect(lookups).toBe(1);
	expect(owner).not.toBe(thief.owner);
});

it("a workflow tool waiting on approval ends the Session execution; the click resumes the tool and the agent answers", async () => {
	const runSql = f.defineWorkflow({
		name: `run-sql-${short()}`,
		description: "Runs SQL after approval",
		input: z.object({ sql: z.string() }),
		async run(input, ctx) {
			const approval = await ctx.waitForApproval("approve", {
				title: "Run SQL?",
				details: input.sql,
				approvers: alice,
			});
			return approval.status === "approved" ? "ran" : "declined";
		},
	});
	const lead = f.defineAgent({
		name: `lead-${short()}`,
		description: "Runs SQL.",
		model,
		tools: [runSql],
	});
	const ask = f.defineWorkflow({
		name: `ask-${short()}`,
		input: z.string(),
		run: (input, ctx) => ctx.agent("answer", lead, { input }),
	});
	script = ({ context }) => {
		const text = lastText(context);
		if (text === "Fix the row") return use(toolName(runSql.name), { sql: "update x" });
		if (text === '"ran"') return say("Ran it.");
		return say(`unexpected: ${text}`);
	};
	const a = await launch("a", [runSql, ask]);
	const dm = await a.chat.openDM(alice.identities[0]!);
	const post = dm.post.bind(dm);
	let approve: (() => Promise<void>) | undefined;
	dm.post = async (content) => {
		const ref = await post(content);
		const action =
			typeof content === "object" && "card" in content
				? content.card.actions?.find((each) => each.id.startsWith("fabrial.approval.approve:"))
				: undefined;
		if (action) approve = () => a.chat.click(ref, action.id, alice.identities[0]!);
		return ref;
	};
	const metadata = metadataOf();
	const caller = await a.runtime.invoke(ask.name, "Fix the row", { metadata });
	await expect.poll(() => !!approve, poll).toBe(true);
	// Waiting on the approval, no Session execution runs and the lease is free.
	await released(metadata.interactionId);
	expect(requests).toHaveLength(1);
	await approve!();
	await expect.poll(() => execution(caller), poll).toMatchObject({ done: true });
	expect(await output(caller)).toBe("Ran it.");
	expect(requests.map((request) => lastText(request.context))).toEqual(["Fix the row", '"ran"']);
	await released(metadata.interactionId);
});
