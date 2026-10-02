import { randomUUID } from "node:crypto";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { defineExtension, hook, GenerationTask } from "@earendil-works/pi-durable";
import { Type } from "typebox";
import {
	defineState,
	defineWorkflow,
	type FabrialHost,
	type InvocationMetadata,
	type RuntimeWorkflow,
} from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import postgres, { type Sql } from "postgres";
import { afterAll, beforeAll, expect, it } from "vitest";
import { z } from "zod";
import {
	abortOrphans,
	checkOrphans,
	defineAgent,
	defineTool,
	pi,
	PostgresStorage,
	section,
	sessionKey,
} from "../../src/index.ts";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";

let sql: Sql;
let container: Awaited<ReturnType<PostgreSqlContainer["start"]>>;
beforeAll(async () => {
	container = await new PostgreSqlContainer("postgres:17-alpine").start();
	sql = postgres(container.getConnectionUri());
	await PostgresStorage.migrate(sql);
});
afterAll(async () => {
	await sql?.end();
	await container?.stop();
});

function fixture(plugins: FabrialHost["plugins"] = []) {
	const runtime = new MemoryRuntime();
	const chat = new FakeChat();
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const metadata: InvocationMetadata = {
		interactionId: randomUUID(),
		origin: null,
		replyTo: { kind: "thread", provider: "slack-test", threadId: randomUUID() },
		requestedBy: { id: "alice", known: true, identities: [], name: "Alice" },
		ownsThread: true,
		ownerWorkflow: null,
		triggerEvent: null,
	};
	const host = {
		plugins,
		workflows: [],
		runtime,
		clients: () => ({ example: "client" }),
		chat: () => chat,
		logger: { warn() {}, error() {}, debug() {}, info() {} },
		directory: { isMember: async () => true },
	} as unknown as FabrialHost;
	const integration = pi({ models, sql, settings: { retry: { enabled: false } } }).connect(host);
	return { runtime, chat, faux, integration, metadata, host };
}
async function execute(f: ReturnType<typeof fixture>, workflow: RuntimeWorkflow) {
	runtimeRegister(f, workflow);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	await f.runtime.flush();
	return { id, result: f.runtime.result(id) };
}
function runtimeRegister(
	f: ReturnType<typeof fixture>,
	workflow: RuntimeWorkflow,
	others: RuntimeWorkflow[] = [],
) {
	f.runtime.register({ workflows: [workflow, ...f.integration.workflows, ...others], events: [] });
}

it("runs durable structured answers, memoizes submissions, and continues one agent conversation", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage('{"answer":42}'), fauxAssistantMessage("continued")]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			const first = await f.integration.agents.run(execution, "answer", agent, {
				input: "Question",
				output: z.object({ answer: z.number() }),
			});
			const again = await f.integration.agents.run(execution, "again", agent, {
				input: "Continue",
			});
			return { first, again };
		},
	};
	const { result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { first: { answer: 42 }, again: "continued" },
	});
	expect(f.faux.state.callCount).toBe(2);
	const commits =
		await sql`SELECT writes FROM fabrial_pi.commits WHERE session_key = ${sessionKey(f.metadata)} ORDER BY seq`;
	const conversations = commits
		.flatMap((row) => JSON.parse(String(row.writes)) as { type: string }[])
		.filter((write) => write.type === "conversation");
	expect(conversations).toHaveLength(1);
	await f.integration.stop();
});

it("binds adapter identity, clients, sections and durable posts; commits tool state with a result receipt", async () => {
	const f = fixture();
	const state = defineState({
		name: randomUUID(),
		scope: "thread",
		schema: z.object({ notes: z.array(z.string()) }),
		initial: () => ({ notes: [] as string[] }),
		render: (value) => value.notes.join(","),
	});
	let actor: string | undefined;
	const tool = defineTool({
		name: "record_note",
		description: "Record a note",
		parameters: Type.Object({ text: Type.String() }),
		execute: async ({ text }, ctx) => {
			actor = ctx.actor?.id;
			expect(ctx.clients).toEqual({ example: "client" });
			await ctx.state(state).update((draft) => {
				draft.notes.push(text);
			});
			await ctx.thread!.post("post", "Recorded");
			return { content: [{ type: "text", text: "done" }] };
		},
	});
	const extension = defineExtension({
		name: randomUUID(),
		tools: [tool],
		sections: [section("requester", (_input, ctx) => `Requester ${ctx.actor?.name}`)],
	});
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
		extensions: [extension],
		state: [state],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall("record_note", { text: "hello" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("finished"),
	]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			await f.integration.state.update(execution, "seed", state, (draft) => {
				draft.notes.push("seed");
			});
			const answer = await f.integration.agents.run(execution, "agent", agent, { input: "Do it" });
			return { answer, state: await f.integration.state.get(execution, "read", state) };
		},
	};
	const { result } = await execute(f, workflow);
	expect(result).toEqual({
		status: "completed",
		output: { answer: "finished", state: { notes: ["seed", "hello"] } },
	});
	expect(actor).toBe("alice");
	const io = await f.chat.resolve(f.metadata.replyTo!);
	const recorded = await f.chat.thread(io.ref);
	expect(recorded.posts).toHaveLength(1);
	expect(recorded.statuses).toContain("Thinking…");
	expect(recorded.statuses.at(-1)).toBeNull();
	const rows = await sql`SELECT writes FROM fabrial_pi.commits`;
	expect(
		rows.some((row) => {
			const writes = JSON.parse(String(row.writes)) as {
				type: string;
				record?: { kind: string };
			}[];
			return (
				writes.some((write) => write.record?.kind === "fabrial.tool-result") &&
				writes.some((write) => write.type === "document.change")
			);
		}),
	).toBe(true);
	await f.integration.stop();
});

it("releases the Session while a workflow tool waits and reconnects with a stable child id", async () => {
	const f = fixture();
	const child = defineWorkflow({
		name: randomUUID(),
		input: z.object({ value: z.string() }),
		run: async () => "unused",
	});
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
		tools: [child.asTool()],
	});
	f.faux.setResponses([
		fauxAssistantMessage(fauxToolCall(child.asTool().name, { value: "ok" }), {
			stopReason: "toolUse",
		}),
		fauxAssistantMessage("child finished"),
	]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Run child" }),
	};
	runtimeRegister(f, workflow, [
		{
			name: child.name,
			triggers: [],
			handler: async () => {
				const storage = await PostgresStorage.open(sql, sessionKey(f.metadata));
				await storage.close(BACKGROUND_CONTEXT);
				return "ok";
			},
		},
	]);
	await f.integration.start();
	await f.runtime.start();
	const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
	await f.runtime.flush();
	expect(f.runtime.result(id)).toEqual({ status: "completed", output: "child finished" });
	expect(f.runtime.executions(child.name)).toHaveLength(1);
	await f.integration.stop();
});

it("rejects call-time configure and invalid structured answers", async () => {
	const f = fixture();
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage('{"answer":"not a number"}')]);
	const workflow: RuntimeWorkflow = {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, {
				input: "Answer",
				output: z.object({ answer: z.number() }),
			}),
	};
	const { result } = await execute(f, workflow);
	expect(result).toMatchObject({
		status: "failed",
		error: expect.stringContaining("Invalid agent"),
	});
	await f.integration.stop();
});

it("detects missing live task kinds and explicitly aborts their ownership tree", async () => {
	const storage = await PostgresStorage.open(sql, randomUUID());
	const id = await storage.mintId();
	await storage.commit(
		[
			{ type: "conversation", value: { id: 1 as never } },
			{
				type: "task",
				value: {
					id: id as never,
					conversationId: 1 as never,
					kind: "removed.extension",
					version: 1,
					input: {},
					background: false,
					abortRequested: false,
					state: { status: "pending", checkpoint: { phase: "run" } },
				},
			},
		],
		BACKGROUND_CONTEXT,
	);
	await storage.close(BACKGROUND_CONTEXT);
	await expect(checkOrphans(sql, ["pi.generation", "pi.tool", "pi.compaction"])).rejects.toThrow(
		"abortOrphans",
	);
	expect(await abortOrphans(sql, ["removed.extension"])).toBe(1);
	await expect(
		checkOrphans(sql, ["pi.generation", "pi.tool", "pi.compaction"]),
	).resolves.toBeUndefined();
});

it.each([false, true])(
	"honors detached=%s when the agent caller is cancelled",
	async (detached) => {
		const f = fixture();
		const child = defineWorkflow({ name: randomUUID(), run: async () => null });
		const agent = defineAgent({
			name: randomUUID(),
			model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
			tools: [child.asTool()],
		});
		f.faux.setResponses([
			fauxAssistantMessage(fauxToolCall(child.asTool().name, {}), { stopReason: "toolUse" }),
			fauxAssistantMessage("approved"),
		]);
		const workflow: RuntimeWorkflow = {
			name: randomUUID(),
			triggers: [],
			handler: (_input, execution) =>
				f.integration.agents.run(execution, "agent", agent, { input: "Wait", detached }),
		};
		runtimeRegister(f, workflow, [
			{
				name: child.name,
				triggers: [],
				handler: async (_input, execution) => {
					await execution.sleep("approval", 86_400_000);
					return "approved";
				},
			},
		]);
		await f.integration.start();
		await f.runtime.start();
		const id = await f.runtime.invoke(workflow.name, null, { metadata: f.metadata });
		await f.runtime.flush();
		expect(f.runtime.executions(child.name)[0]?.status).toBe("suspended");
		await f.runtime.cancel(id, "User stopped");
		// Cancellation queues onSettled; flush delivers the bridge's persisted-child cleanup.
		await f.runtime.flush();
		expect(f.runtime.result(id)?.status).toBe("cancelled");
		if (detached) {
			expect(f.runtime.executions(child.name)[0]?.status).toBe("suspended");
			expect(f.runtime.executions("fabrial.pi.run")[0]?.status).toBe("suspended");
			await f.runtime.advanceBy(86_400_000);
			expect(f.runtime.executions("fabrial.pi.run")[0]?.result).toEqual({
				status: "completed",
				output: "approved",
			});
		} else {
			expect(f.runtime.executions(child.name)[0]?.result?.status).toBe("cancelled");
			expect(f.runtime.executions("fabrial.pi.run")[0]?.result?.status).toBe("cancelled");
		}
		await f.integration.stop();
	},
);

it("installs plugin hooks globally but does not select unrequested plugin extensions", async () => {
	let requests = 0;
	const f = fixture([
		{
			id: "unselected",
			extension: () =>
				defineExtension({
					name: "unselected",
					sections: [
						section("hidden", () => {
							throw new Error("Unselected extension rendered");
						}),
					],
				}),
			hooks: {
				agent: [
					hook(GenerationTask, {
						beforeRequest: () => {
							requests++;
						},
					}),
				],
			},
		},
	]);
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage("ok")]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Hello" }),
	});
	expect(result).toEqual({ status: "completed", output: "ok" });
	expect(requests).toBe(1);
	await f.integration.stop();
});

it("consumes buffered steering replies with stable request ids", async () => {
	const f = fixture();
	const io = await f.chat.resolve(f.metadata.replyTo!);
	await io.setState({
		interactionId: f.metadata.interactionId,
		handlerExecutionId: null,
		agentActive: false,
		statusMessageId: null,
		bufferedReplies: [
			{
				provider: "slack-test",
				threadId: io.ref.threadId,
				channelId: io.channelId,
				messageId: "steer",
				text: "Use the new account",
				author: {
					identity: { provider: "slack-test", installationId: "test", subjectId: "alice" },
					name: "Alice",
					isBot: false,
				},
				isMention: false,
				isDM: false,
				sentAt: new Date().toISOString(),
			},
		],
	});
	const agent = defineAgent({
		name: randomUUID(),
		model: { provider: f.faux.provider.id, modelId: f.faux.getModel().id },
	});
	f.faux.setResponses([fauxAssistantMessage("ok"), fauxAssistantMessage("steered")]);
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: (_input, execution) =>
			f.integration.agents.run(execution, "agent", agent, { input: "Hello" }),
	});
	expect(result?.status).toBe("completed");
	expect((await io.getState())?.bufferedReplies).toHaveLength(0);
	expect((await io.getState())?.agentActive).toBe(false);
	await f.integration.stop();
});

it("does not promote an unposted channel locator to a Pi Session key", async () => {
	const f = fixture();
	f.metadata.replyTo = { kind: "channel", provider: "slack-test", channelId: "C1" };
	const state = defineState({
		name: randomUUID(),
		scope: "thread",
		schema: z.object({ value: z.string() }),
		initial: () => ({ value: "initial" }),
	});
	const { result } = await execute(f, {
		name: randomUUID(),
		triggers: [],
		handler: async (_input, execution) => {
			await expect(f.integration.state.get(execution, "state", state)).rejects.toThrow(
				"ctx.thread.post",
			);
			return f.integration.agents.run(
				execution,
				"agent",
				{ kind: "fabrial.agent", name: "unused" },
				{ input: "Hello" },
			);
		},
	});
	expect(result).toMatchObject({
		status: "failed",
		error: expect.stringContaining("ctx.thread.post"),
	});
	expect(f.runtime.executions("fabrial.pi.run")).toHaveLength(0);
	expect(f.chat.threads.size).toBe(0);
	await f.integration.stop();
});
