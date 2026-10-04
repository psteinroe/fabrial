import { createHmac } from "node:crypto";
import { createFabrial, type WorkflowContext } from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import { describe, expect, it, vi } from "vitest";
import { linear, issueCreated, issueUpdated } from "../../src/index.ts";
import { options, request } from "../helper.ts";
const { defineWorkflow } = createFabrial({ plugins: [] });

const payload = (action = "create") => ({
	type: "Issue",
	action,
	organizationId: "org",
	webhookTimestamp: Date.now(),
	actor: { id: "sender" },
	data: {
		id: "issue",
		creatorId: "alice",
		identifier: "ENG-1",
		title: "Fix it",
		description: "Please",
		url: "https://linear.app/acme/issue/ENG-1",
		team: { key: "ENG" },
		state: { type: "started" },
	},
	updatedFrom: action === "update" ? { title: "Old title" } : {},
});

describe("Linear lifecycle webhooks", () => {
	it.each(["create", "update"])(
		"ingests %s with origin, actor, reply destination and durable dedupe",
		async (action) => {
			const plugin = linear(options);
			vi.spyOn(plugin.identity!, "lookup").mockResolvedValue({ name: "Alice" });
			const runtime = new MemoryRuntime();
			const chat = new FakeChat();
			const trigger = action === "create" ? issueCreated : issueUpdated;
			const run = vi.fn(async (_input: unknown, ctx: WorkflowContext) => {
				await ctx.thread!.post("reply", "Thanks!");
			});
			const app = createFabrial({ plugins: [plugin] }).app({
				runtime,
				chat,
				workflows: [
					defineWorkflow({
						name: "owner",
						triggers: [trigger({ team: "ENG", stateType: "started" })],
						run,
					}),
				],
			});
			await app.start();
			try {
				const make = () =>
					new Request("https://example.com/linear/webhook", request(payload(action)));
				expect((await app.fetch(make())).status).toBe(200);
				expect((await app.fetch(make())).status).toBe(200);
				await runtime.flush();
				expect(run).toHaveBeenCalledTimes(1);
				expect(runtime.emitted.filter((entry) => entry.name.startsWith("linear."))).toHaveLength(1);
				expect(runtime.emitted[0]).toMatchObject({
					name: `linear.${action === "create" ? "issueCreated" : "issueUpdated"}`,
					payload: {
						issueId: "issue",
						teamKey: "ENG",
						stateType: "started",
						threadId: "linear:issue",
						updatedFrom: action === "update" ? { title: "Old title" } : {},
					},
				});
				expect(runtime.executions("owner")[0]!.metadata).toMatchObject({
					origin: {
						provider: "linear",
						installationId: "org",
						issueId: "issue",
						teamKey: "ENG",
						threadId: "linear:issue",
					},
					replyTo: { kind: "thread", provider: "linear", threadId: "linear:issue" },
					requestedBy: {
						identities: [
							{
								provider: "linear",
								installationId: "org",
								subjectId: action === "create" ? "alice" : "sender",
							},
						],
					},
				});
			} finally {
				await app.stop();
			}
		},
	);
	it("verifies signatures, timestamps, organization and required delivery IDs", async () => {
		const plugin = linear(options);
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /linear/webhook"]!;
		expect((await route(request(payload(), "bad"), ctx)).status).toBe(401);
		expect(
			(await route(request({ ...payload(), webhookTimestamp: Date.now() - 120_000 }), ctx)).status,
		).toBe(401);
		expect((await route(request({ ...payload(), organizationId: "other" }), ctx)).status).toBe(403);
		const noId = request(payload());
		noId.headers.delete("linear-delivery");
		expect((await route(noId, ctx)).status).toBe(400);
		expect((await route(request({ ...payload(), data: {} }), ctx)).status).toBe(400);
		expect(emit).not.toHaveBeenCalled();
	});
	it("rejects verified malformed JSON without emitting", async () => {
		const plugin = linear(options);
		const emit = vi.fn();
		const malformed = new Request("https://example.com/linear/webhook", {
			method: "POST",
			body: "{",
			headers: {
				"linear-signature": createHmac("sha256", "secret").update("{").digest("hex"),
			},
		});
		expect(
			(
				await plugin.routes!["POST /linear/webhook"]!(malformed, {
					emit,
					clients: plugin.clients!({}),
				})
			).status,
		).toBe(400);
		expect(emit).not.toHaveBeenCalled();
	});
	it("ignores other types/actions", async () => {
		const plugin = linear(options);
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /linear/webhook"]!;
		expect((await route(request({ ...payload(), type: "Project" }), ctx)).status).toBe(200);
		expect((await route(request(payload("remove")), ctx)).status).toBe(200);
		expect(emit).not.toHaveBeenCalled();
	});
	it("resolves team and state IDs when webhooks lack expanded objects", async () => {
		const plugin = linear(options);
		const clients = plugin.clients!({});
		const emit = vi.fn();
		vi.spyOn(clients.linear, "team").mockResolvedValue({ key: "ENG" } as never);
		vi.spyOn(clients.linear, "workflowState").mockResolvedValue({ type: "started" } as never);
		const input = {
			...payload(),
			data: {
				...payload().data,
				team: undefined,
				state: undefined,
				teamId: "team-id",
				stateId: "state-id",
			},
		};
		expect(
			(await plugin.routes!["POST /linear/webhook"]!(request(input), { emit, clients })).status,
		).toBe(200);
		expect(emit).toHaveBeenCalledWith(
			"issueCreated",
			expect.objectContaining({ teamKey: "ENG", stateType: "started" }),
			expect.any(Object),
		);
	});
	it("honors custom verified bodies and fails closed on verifier exceptions", async () => {
		const verify = vi
			.fn()
			.mockResolvedValueOnce(JSON.stringify(payload()))
			.mockRejectedValueOnce(new Error("rejected"));
		const plugin = linear({ ...options, webhookVerifier: verify });
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /linear/webhook"]!;
		expect((await route(request({}, "bad"), ctx)).status).toBe(200);
		expect((await route(request(payload()), ctx)).status).toBe(401);
		expect(emit).toHaveBeenCalledTimes(1);
	});
});

it("uses an expanded issue creator without a webhook actor and rejects unattributed creation", async () => {
	const plugin = linear(options);
	const clients = plugin.clients!({});
	const emit = vi.fn();
	const route = plugin.routes!["POST /linear/webhook"]!;
	const source = { ...payload().data, creatorId: undefined, creator: { id: "author" } };
	expect(
		(await route(request({ ...payload(), actor: undefined, data: source }), { emit, clients }))
			.status,
	).toBe(200);
	expect(emit).toHaveBeenCalledWith(
		"issueCreated",
		expect.any(Object),
		expect.objectContaining({
			requestedBy: { provider: "linear", installationId: "org", subjectId: "author" },
			replyTo: { kind: "thread", provider: "linear", threadId: "linear:issue" },
		}),
	);
	emit.mockClear();
	expect(
		(
			await route(
				request({ ...payload(), actor: undefined, data: { ...source, creator: undefined } }),
				{ emit, clients },
			)
		).status,
	).toBe(400);
	expect(emit).not.toHaveBeenCalled();
});
