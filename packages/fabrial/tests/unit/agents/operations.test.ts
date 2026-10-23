import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT, withContextValue } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import {
	createRegistry,
	Harness,
	MemoryStorage,
	type ToolExecutionApi,
} from "@earendil-works/pi-durable";
import { createFabrial, type FabrialHost, type InvocationMetadata } from "../../internals.ts";
import { TaskContext } from "pgconductor-js";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import { BridgeKey, fields, type BridgeFrame } from "../../../src/agents/tool-context.ts";
import { cancelChildren } from "../../../src/sessions/workflows.ts";
import { Binding, Invocation, Obligations } from "../../../src/sessions/obligations.ts";
import { ChildIntents, ChildResults } from "../../../src/sessions/workflows.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

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
		requestId: randomUUID(),
		origin: { provider: "slack" },
		replyTo: { kind: "thread", provider: "slack", threadId: "thread" },
		requestedBy: { id: "alice", known: true, identities: [] },
		triggerEvent: "slack.mentioned",
		ownerWorkflow: "router",
	};
	await harness.commit(async (tx) => {
		(await tx.doc(Binding, conversation.id)).requestId = "request";
		await tx.doc(Invocation, "request", metadata);
		(await tx.doc(Obligations)).requests.request = {
			conversationId: conversation.id,
			owes: "reply",
		};
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
			chat: () => ({}),
		} as unknown as FabrialHost,
		session: "app:operations",
		// Conductor's context of the Session's execution: Fabrial starts children with its prototype's `invoke`.
		ctx: Object.assign(Object.create(conductorMethods(start)) as object, {
			executionId: "a",
			queue: "default",
			metadata: {
				...metadata,
				requestId: "driver-interaction",
				requestedBy: { id: "driver", known: true, identities: [] },
				replyTo: null,
			},
			onSignal() {},
		}),
	} as unknown as BridgeFrame;
	const context = withContextValue(BridgeKey, bridge, BACKGROUND_CONTEXT);
	const api = { taskId: 42, conversationId: conversation.id } as ToolExecutionApi;
	return { harness, bridge, context, api, start, metadata, conversation };
}

/** Conductor's context methods (not its getters, which the fixture's plain fields replace), with `invoke` stubbed. */
function conductorMethods(invoke: unknown): object {
	const proto = TaskContext.prototype as unknown as Record<string, unknown>;
	const methods = Object.getOwnPropertyNames(proto).filter(
		(name) => typeof Object.getOwnPropertyDescriptor(proto, name)!.value === "function",
	);
	return { ...Object.fromEntries(methods.map((name) => [name, proto[name]])), invoke };
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
			triggerEvent: null,
			ownerWorkflow: null,
		});
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("checks canonical registered input, rejecting forged workflow definitions", async () => {
	const other = defineWorkflow({ name: "other", run: async () => null });
	const canonical = defineWorkflow({
		name: "validated",
		input: z.object({ count: z.number() }),
		run: async () => null,
	});
	const f = await fixture([other, canonical]);
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
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
		await ctx.start("other", other, undefined as never);
		expect(f.start).toHaveBeenCalledTimes(1);
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("managed effects reconnect across drivers without executing another post", async () => {
	const f = await fixture();
	try {
		const first = await fields(f.context, f.conversation.id, f.api);
		expect(await first.thread!.post("post", "hello")).toEqual({ messageId: "posted" });
		f.bridge.ctx = Object.assign(
			Object.create(Object.getPrototypeOf(f.bridge.ctx) as object),
			f.bridge.ctx,
			{
				executionId: "b",
			},
		);
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
		expect(
			(await f.harness.snapshot(Obligations, BACKGROUND_CONTEXT))?.requests.request?.intents,
		).toContain(key);
		expect(intent?.executionId).toBeNull();
		f.bridge.ctx = Object.assign(
			Object.create(Object.getPrototypeOf(f.bridge.ctx) as object),
			f.bridge.ctx,
			{
				executionId: "b",
			},
		);
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

it("a stop that races a child's start still cancels it, and later starts for the stopped request are refused", async () => {
	const workflow = defineWorkflow({ name: "child", run: async () => null });
	const f = await fixture([workflow]);
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
		// The intent committed, but its start did not return (the stop runs before it, or it crashed).
		f.start.mockRejectedValueOnce(new Error("start interrupted"));
		await expect(ctx.start("racing", workflow, null as never)).rejects.toThrow("start interrupted");
		await f.harness.commit(async (tx) => {
			(await tx.doc(Obligations)).requests.request!.stopped = true;
		}, BACKGROUND_CONTEXT);
		const cancel = vi.fn(async () => {});
		await cancelChildren(f.bridge, cancel, ["request"], "Stopped", BACKGROUND_CONTEXT);
		// Started (or found) by the same start key, then cancelled.
		const [first, again] = f.start.mock.calls.map(([key]) => key);
		expect(again).toBe(first);
		expect(cancel.mock.calls).toEqual([[`child:${first}`, "Stopped"]]);
		await expect(ctx.start("later", workflow, null as never)).rejects.toThrow(
			"Request request was stopped",
		);
		expect(f.start).toHaveBeenCalledTimes(2);
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("stopping a request cancels the attached workflows its tools started or waited on, never detached ones", async () => {
	const workflow = defineWorkflow({ name: "child", run: async () => null });
	const f = await fixture([workflow]);
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
		await ctx.start("attached", workflow, null as never);
		// Waited: started behind a reporter, its intent linked to the request like any start.
		await expect(ctx.invoke("waited", workflow, null as never)).resolves.toEqual({
			messageId: "posted",
		});
		await (
			await fields(f.context, f.conversation.id, f.api, undefined, true)
		).start("detached", workflow, null as never);
		const [attached, waited, detached] = f.start.mock.calls.map(([key]) => `child:${key}`);
		const cancel = vi.fn(async () => {});
		// Another request's children are not linked to this one.
		await cancelChildren(f.bridge, cancel, ["other"], "Stopped", BACKGROUND_CONTEXT);
		expect(cancel).not.toHaveBeenCalled();
		await cancelChildren(f.bridge, cancel, ["request"], "Stopped", BACKGROUND_CONTEXT);
		expect(cancel.mock.calls).toEqual([
			[attached, "Stopped"],
			[waited, "Stopped"],
		]);
		expect(detached).toBeDefined();
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});

it("a stop whose cancel failed partway cancels the rest when redelivered, and never cancels a child twice", async () => {
	const workflow = defineWorkflow({ name: "child", run: async () => null });
	const f = await fixture([workflow]);
	try {
		const ctx = await fields(f.context, f.conversation.id, f.api);
		await ctx.start("first", workflow, null as never);
		await ctx.start("second", workflow, null as never);
		const [first, second] = f.start.mock.calls.map(([key]) => `child:${key}`);
		const cancel = vi.fn(async (executionId: string) => {
			if (executionId === second && cancel.mock.calls.length === 2)
				throw new Error("Conductor unavailable");
		});
		await expect(
			cancelChildren(f.bridge, cancel, ["request"], "Stopped", BACKGROUND_CONTEXT),
		).rejects.toThrow("Conductor unavailable");
		await cancelChildren(f.bridge, cancel, ["request"], "Stopped", BACKGROUND_CONTEXT);
		await cancelChildren(f.bridge, cancel, ["request"], "Stopped", BACKGROUND_CONTEXT);
		expect(cancel.mock.calls).toEqual([
			[first, "Stopped"],
			[second, "Stopped"],
			[second, "Stopped"],
		]);
	} finally {
		await f.harness.close(BACKGROUND_CONTEXT);
	}
});
