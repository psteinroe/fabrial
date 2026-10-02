import { createHmac } from "node:crypto";
import { GitHubAdapter } from "@chat-adapter/github";
import { defineWorkflow, fabrial, type WorkflowContext } from "fabrial";
import { FakeChat, MemoryRuntime } from "fabrial/testing";
import { describe, expect, it, vi } from "vitest";
import { github, pullRequestOpened, issueOpened } from "../../src/index.ts";
import { alice, options, repository, request } from "../helper.ts";

const conversation = {
	id: 3,
	number: 5,
	title: "Fix it",
	body: null,
	html_url: "https://github.com/acme/app/pull/5",
	user: alice,
};
const payload = (event: string, action = "opened") => ({
	action,
	repository,
	sender: { id: 2, login: "sender" },
	[event === "issues" ? "issue" : "pull_request"]: conversation,
});

describe("GitHub lifecycle webhooks", () => {
	it.each(["pull_request", "issues"])(
		"ingests %s with dedupe, origin, actor, thread and observers",
		async (event) => {
			const plugin = github(options);
			vi.spyOn(plugin.identity!, "lookup").mockResolvedValue({ name: "Alice" });
			const runtime = new MemoryRuntime();
			const chat = new FakeChat();
			const trigger = event === "issues" ? issueOpened : pullRequestOpened;
			const run = vi.fn(async (_input: unknown, ctx: WorkflowContext) => {
				await ctx.thread!.post("reply", "Thanks!");
			});
			const observer = vi.fn(async (_input: unknown, ctx: WorkflowContext) => {
				expect(ctx.thread).toBeUndefined();
			});
			const app = fabrial({
				runtime,
				chat,
				plugins: [plugin],
				workflows: [
					defineWorkflow({ name: "owner", triggers: [trigger({ repo: "acme/app" })], run }),
					defineWorkflow({
						name: "observer",
						triggers: [trigger({ repo: "acme/app", observe: true })],
						run: observer,
					}),
				],
			});
			await app.start();
			try {
				const make = () => {
					const req = request(payload(event), event);
					return new Request("https://example.com/github/webhook", req);
				};
				expect((await app.fetch(make())).status).toBe(200);
				expect((await app.fetch(make())).status).toBe(200);
				await runtime.flush();
				expect(run).toHaveBeenCalledTimes(1);
				expect(observer).toHaveBeenCalledTimes(1);
				const threadId = event === "issues" ? "github:acme/app:issue:5" : "github:acme/app:5";
				expect(runtime.emitted.filter((entry) => entry.name.startsWith("github."))).toHaveLength(1);
				expect(runtime.emitted[0]).toMatchObject({
					name: `github.${event === "issues" ? "issueOpened" : "pullRequestOpened"}`,
					payload: { repo: "acme/app", number: 5, authorId: "1", authorLogin: "alice", threadId },
				});
				expect(runtime.executions("owner")[0]!.metadata).toMatchObject({
					origin: {
						provider: "github",
						installationId: "default",
						repo: "acme/app",
						number: 5,
						threadId,
					},
					replyTo: { kind: "thread", provider: "github", threadId },
					requestedBy: {
						identities: [{ provider: "github", installationId: "default", subjectId: "1" }],
					},
				});
			} finally {
				await app.stop();
			}
		},
	);
	it("rejects bad signatures, JSON, missing delivery IDs and incomplete payloads", async () => {
		const plugin = github(options);
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /github/webhook"]!;
		expect((await route(request(payload("issues"), "issues", "bad"), ctx)).status).toBe(401);
		expect((await route(request({}, "issues"), ctx)).status).toBe(400);
		const noId = request(payload("issues"), "issues");
		noId.headers.delete("x-github-delivery");
		expect((await route(noId, ctx)).status).toBe(400);
		expect(emit).not.toHaveBeenCalled();
	});
	it("rejects verified malformed JSON without emitting", async () => {
		const plugin = github(options);
		const emit = vi.fn();
		const malformed = new Request("https://example.com/github/webhook", {
			method: "POST",
			body: "{",
			headers: {
				"x-github-event": "issues",
				"x-hub-signature-256": `sha256=${createHmac("sha256", "secret").update("{").digest("hex")}`,
			},
		});
		expect(
			(
				await plugin.routes!["POST /github/webhook"]!(malformed, {
					emit,
					clients: plugin.clients!({}),
				})
			).status,
		).toBe(400);
		expect(emit).not.toHaveBeenCalled();
	});
	it("rejects other App installations on both ingress paths", async () => {
		const plugin = github({
			appId: "1",
			privateKey: "unused-test-key",
			installationId: 42,
			webhookSecret: "secret",
			botUserId: 99,
			logger: options.logger,
		});
		const emit = vi.fn();
		const wrong = { ...payload("issues"), installation: { id: 43 } };
		expect(
			(
				await plugin.routes!["POST /github/webhook"]!(request(wrong, "issues"), {
					emit,
					clients: plugin.clients!({}),
				})
			).status,
		).toBe(403);
		const adapter = plugin.chat!.adapter() as GitHubAdapter;
		expect((await adapter.handleWebhook(request(wrong, "issues"))).status).toBe(401);
		expect((await adapter.handleWebhook(request(wrong, "issues", "bad"))).status).toBe(401);
		expect(emit).not.toHaveBeenCalled();
	});
	it("ignores unsupported events/actions and PR-shaped issue events", async () => {
		const plugin = github(options);
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /github/webhook"]!;
		expect((await route(request({}, "push"), ctx)).status).toBe(200);
		expect((await route(request(payload("issues", "closed"), "issues"), ctx)).status).toBe(200);
		expect(
			(
				await route(
					request({ ...payload("issues"), issue: { ...conversation, pull_request: {} } }, "issues"),
					ctx,
				)
			).status,
		).toBe(200);
		expect(emit).not.toHaveBeenCalled();
	});
	it("supports custom verification and fails closed on verifier exceptions", async () => {
		const verify = vi.fn().mockResolvedValueOnce(true).mockRejectedValueOnce(new Error("rejected"));
		const plugin = github({ ...options, webhookVerifier: verify });
		const emit = vi.fn();
		const ctx = { emit, clients: plugin.clients!({}) };
		const route = plugin.routes!["POST /github/webhook"]!;
		expect((await route(request(payload("issues"), "issues", "bad"), ctx)).status).toBe(200);
		expect((await route(request(payload("issues"), "issues"), ctx)).status).toBe(401);
		expect(emit).toHaveBeenCalledTimes(1);
	});
});
