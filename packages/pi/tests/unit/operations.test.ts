import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT, withContextValue } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	createRegistry,
	Harness,
	MemoryStorage,
	type ToolExecutionApi,
} from "@earendil-works/pi-durable";
import {
	defineWorkflow,
	type DurableExecution,
	type FabrialHost,
	type InvocationMetadata,
} from "fabrial";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import { BridgeKey, fields, type BridgeFrame } from "../../src/context.ts";
import { Binding, ChildIntents, ChildResults, Invocation, Runs } from "../../src/documents.ts";

async function fixture(workflows: FabrialHost["workflows"] = []) {
	const harness = await Harness.open(
		new MemoryStorage(),
		{ models: createModels(), registry: createRegistry() },
		BACKGROUND_CONTEXT,
	);
	const conversation = await harness.createConversation(
		{ ownership: { kind: "ownerless" } },
		BACKGROUND_CONTEXT,
	);
	const metadata: InvocationMetadata = {
		interactionId: randomUUID(),
		origin: { provider: "slack" },
		replyTo: { kind: "thread", provider: "slack", threadId: "thread" },
		requestedBy: { id: "alice", known: true, identities: [] },
		ownsThread: true,
		triggerEvent: "slack.mentioned",
		ownerWorkflow: "router",
	};
	await harness.commit(async (tx) => {
		(await tx.doc(Binding, conversation.id)).requestId = "request";
		await tx.doc(Invocation, "request", metadata);
		(await tx.doc(Runs)).runs.a = { conversationId: conversation.id, children: {} };
		(await tx.doc(Runs)).runs.b = { conversationId: conversation.id, children: {} };
	}, BACKGROUND_CONTEXT);
	const start = vi.fn(async (key: string) => {
		// The start intent must already be durable when the child can begin.
		expect(await harness.snapshot(ChildIntents, key, BACKGROUND_CONTEXT)).toMatchObject({
			executionId: null,
		});
		const child = `child:${key}`;
		await harness.commit(async (tx) => {
			(await tx.doc(ChildResults)).results[child] = {
				status: "completed",
				output: { messageId: "posted" },
			};
		}, BACKGROUND_CONTEXT);
		return child;
	});
	const bridge = {
		harness,
		children: new Map(),
		host: {
			workflows,
			clients: () => ({}),
			directory: { isMember: vi.fn(async () => false) },
		} as unknown as FabrialHost,
		execution: {
			executionId: "a",
			metadata: {
				...metadata,
				interactionId: "driver-interaction",
				requestedBy: { id: "driver", known: true, identities: [] },
				replyTo: null,
			},
			start,
		} as unknown as DurableExecution,
		thread: {} as BridgeFrame["thread"],
	} as BridgeFrame;
	const context = withContextValue(BridgeKey, bridge, BACKGROUND_CONTEXT);
	const api = { taskId: 42, conversationId: conversation.id } as ToolExecutionApi;
	return { harness, bridge, context, api, start, metadata, conversation };
}

it("ordinary tool children carry request identity, not the driver's trigger ownership", async () => {
	const workflow = defineWorkflow({ name: "child", run: async () => null });
	const f = await fixture([workflow]);
	try {
		await (
			await fields(f.context, f.conversation.id, f.api)
		).start("child", workflow, undefined as never);
		expect(f.start.mock.calls[0]).toBeDefined();
		const key = f.start.mock.calls[0]![0];
		const intent = await f.harness.snapshot(ChildIntents, key, BACKGROUND_CONTEXT);
		expect(intent?.metadata).toMatchObject({
			...f.metadata,
			ownsThread: false,
			triggerEvent: null,
			ownerWorkflow: null,
		});
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("checks canonical registered permissions and input, rejecting forged workflow definitions", async () => {
	const restricted = defineWorkflow({
		name: "restricted",
		access: { invoke: { kind: "fabrial.group", id: "admins", members: [] } },
		run: async () => null,
	});
	const canonical = defineWorkflow({
		name: "validated",
		input: z.object({ count: z.number() }),
		run: async () => null,
	});
	const f = await fixture([restricted, canonical]);
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
		await expect(
			ctx.start(
				"restricted",
				defineWorkflow({ name: "restricted", run: async () => null }),
				undefined as never,
			),
		).rejects.toThrow("Not authorized");
		await expect(
			ctx.start(
				"invalid",
				defineWorkflow({
					name: "validated",
					input: z.object({ count: z.string() }),
					run: async () => null,
				}),
				{ count: "wrong" },
			),
		).rejects.toThrow("Invalid validated input");
		await expect(
			ctx.start(
				"unknown",
				defineWorkflow({ name: "unknown", run: async () => null }),
				undefined as never,
			),
		).rejects.toThrow("not registered");
		expect(f.start).not.toHaveBeenCalled();
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("managed effects reconnect across drivers without executing another post", async () => {
	const f = await fixture();
	try {
		const first = await fields(f.context, f.conversation.id, f.api);
		expect(await first.thread!.post("post", "hello")).toEqual({ messageId: "posted" });
		f.bridge.execution = { ...f.bridge.execution, executionId: "b" };
		const replay = await fields(f.context, f.conversation.id, f.api);
		expect(await replay.thread!.post("post", "hello")).toEqual({ messageId: "posted" });
		expect(f.start).toHaveBeenCalledTimes(1);
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("reconciles a lost child-start receipt from its persisted intent and stable global key", async () => {
	const workflow = defineWorkflow({ name: "child", run: async () => null });
	const f = await fixture([workflow]);
	try {
		f.start.mockImplementationOnce(async () => {
			throw new Error("receipt lost");
		});
		await expect(
			(await fields(f.context, f.conversation.id, f.api)).start(
				"start",
				workflow,
				undefined as never,
			),
		).rejects.toThrow("receipt lost");
		const key = f.start.mock.calls[0]![0];
		const intent = await f.harness.snapshot(ChildIntents, key, BACKGROUND_CONTEXT);
		expect((await f.harness.snapshot(Runs, BACKGROUND_CONTEXT))?.runs.a?.intents).toContain(key);
		expect(intent?.executionId).toBeNull();
		f.bridge.execution = { ...f.bridge.execution, executionId: "b" };
		await (
			await fields(f.context, f.conversation.id, f.api)
		).start("start", workflow, undefined as never);
		expect(f.start.mock.calls[1]![0]).toBe(key);
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("uses collision-free # suffixes and rejects framework-reserved ids", async () => {
	const f = await fixture();
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
		await ctx.thread!.post("x", "one");
		await ctx.thread!.post("x:1", "two");
		await ctx.thread!.post("x", "three");
		const keys = f.start.mock.calls.map(([key]) => key);
		expect(new Set(keys).size).toBe(3);
		expect(keys[2]).toMatch(/:x#1$/);
		await expect(ctx.thread!.post("fabrial:receipt", "bad")).rejects.toThrow(
			"Reserved operation id",
		);
		await expect(ctx.thread!.post("x#1", "bad")).rejects.toThrow("Reserved operation id");
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});
