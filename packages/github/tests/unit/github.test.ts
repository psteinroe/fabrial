// oxlint-disable typescript/unbound-method -- Referenced host methods are Vitest mocks.
import { GitHubAdapter } from "@chat-adapter/github";
import { describe, expect, it, vi } from "vitest";
import { github, mentioned, pullRequestOpened, issueOpened, identity } from "../../src/index.ts";
import { connect, options, alice, repository, request } from "../helper.ts";

describe("GitHub provider", () => {
	it("declares SDK capability, canonical client and lifecycle events", () => {
		const plugin = github(options);
		expect(plugin.chat!.adapter()).toBeInstanceOf(GitHubAdapter);
		expect(plugin.chat!.adapter()).toBe(plugin.chat!.adapter());
		expect(Object.keys(plugin.events!)).toEqual([
			"mentioned",
			"message",
			"dm",
			"pullRequestOpened",
			"issueOpened",
		]);
		expect(plugin.clients!({}).github).toBe((plugin.chat!.adapter() as GitHubAdapter).octokit);
		expect(github.mentioned).toBe(mentioned);
	});
	it("builds scoped triggers and observer specificity", () => {
		expect(mentioned()).toMatchObject({ event: "github.mentioned", specificity: 0 });
		expect(mentioned({ repo: "acme/app" })).toMatchObject({
			filter: { channelId: ["acme/app"] },
			specificity: 1,
		});
		expect(mentioned({ repo: "acme/app", thread: "github:acme/app:2" })).toMatchObject({
			specificity: 2,
		});
		expect(pullRequestOpened({ repo: "acme/app", observe: true })).toMatchObject({
			event: "github.pullRequestOpened",
			filter: { repo: ["acme/app"] },
			observe: true,
			specificity: 1,
		});
		expect(issueOpened()).toMatchObject({ event: "github.issueOpened", specificity: 0 });
	});
	it("creates installation-scoped numeric and login identities", () => {
		expect(identity({ userId: 1 })).toEqual({
			provider: "github",
			installationId: "default",
			subjectId: "1",
		});
		expect(identity({ installationId: 42, login: "alice" })).toEqual({
			provider: "github",
			installationId: "42",
			subjectId: "alice",
		});
	});
	it("looks up numeric IDs and logins using the exposed client", async () => {
		const plugin = github(options);
		const clients = plugin.clients!({});
		const numeric = vi
			.spyOn(clients.github, "request")
			.mockResolvedValue({ data: { name: "Alice", login: "alice" } } as never);
		const login = vi
			.spyOn(clients.github.rest.users, "getByUsername")
			.mockResolvedValue({ data: { name: null, login: "alice" } } as never);
		expect(await plugin.identity!.lookup!(identity({ userId: 1 }), clients)).toEqual({
			name: "Alice",
		});
		expect(numeric).toHaveBeenCalledWith("GET /user/{account_id}", { account_id: 1 });
		expect(await plugin.identity!.lookup!(identity({ login: "alice" }), clients)).toEqual({
			name: "alice",
		});
		expect(login).toHaveBeenCalledWith({ username: "alice" });
		expect(
			await plugin.identity!.lookup!(identity({ installationId: "other", userId: 1 }), clients),
		).toBeUndefined();
	});
	it("returns undefined for missing users, but propagates outages", async () => {
		const plugin = github(options);
		const clients = plugin.clients!({});
		const lookup = vi
			.spyOn(clients.github, "request")
			.mockRejectedValueOnce(Object.assign(new Error("missing"), { status: 404 }))
			.mockRejectedValueOnce(new Error("outage"));
		expect(await plugin.identity!.lookup!(identity({ userId: 1 }), clients)).toBeUndefined();
		await expect(plugin.identity!.lookup!(identity({ userId: 1 }), clients)).rejects.toThrow(
			"outage",
		);
		expect(lookup).toHaveBeenCalledTimes(2);
	});
	it("requires explicit webhook verification", () => {
		expect(() => github({ token: "test" })).toThrow("webhookSecret");
	});
});

describe("GitHub comment ingress", () => {
	it.each([false, true])(
		"normalizes conversation comments (PR=%s) and preserves retry dedupe",
		async (pr) => {
			const plugin = github(options);
			const adapter = plugin.chat!.adapter() as GitHubAdapter;
			vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
				Object.assign(adapter, { chat: sdk });
			});
			const env = connect(plugin);
			const payload = {
				action: "created",
				repository,
				sender: alice,
				issue: { number: 5, title: "Issue", ...(pr ? { pull_request: {} } : {}) },
				comment: {
					id: 10,
					body: "@bot hello",
					user: alice,
					created_at: "2026-01-01T00:00:00Z",
					updated_at: "2026-01-01T00:00:00Z",
					html_url: "https://github.com/acme/app/issues/5#issuecomment-10",
				},
			};
			const route = env.connection.routes["POST /github/events"]!;
			expect((await route(request(payload))).status).toBe(200);
			expect((await route(request(payload))).status).toBe(200);
			const calls = vi.mocked(env.host.receiveMessage).mock.calls;
			expect(calls).toHaveLength(2);
			expect(calls[0]![0]).toMatchObject({
				provider: "github",
				channelId: "acme/app",
				threadId: pr ? "github:acme/app:5" : "github:acme/app:issue:5",
				messageId: "10",
				isMention: true,
				isNewThread: false,
				author: { identity: identity({ userId: 1 }) },
			});
			expect(calls[0]![1]).toMatchObject({
				events: ["github.message", "github.mentioned"],
				dedupeId: JSON.stringify(["github", "acme/app", "10"]),
			});
			expect(calls[1]![1].dedupeId).toBe(calls[0]![1].dedupeId);
		},
	);
	it.each([undefined, 10])(
		"maps review roots/replies to one review thread (parent=%s)",
		async (parent) => {
			const plugin = github(options);
			const adapter = plugin.chat!.adapter() as GitHubAdapter;
			vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
				Object.assign(adapter, { chat: sdk });
			});
			const env = connect(plugin);
			const payload = {
				action: "created",
				repository,
				sender: alice,
				pull_request: { number: 5 },
				comment: {
					id: parent ? 11 : 10,
					in_reply_to_id: parent,
					body: "hello",
					user: alice,
					created_at: "2026-01-01T00:00:00Z",
					updated_at: "2026-01-01T00:00:00Z",
				},
			};
			expect(
				(
					await env.connection.routes["POST /github/events"]!(
						request(payload, "pull_request_review_comment"),
					)
				).status,
			).toBe(200);
			expect(vi.mocked(env.host.receiveMessage).mock.calls[0]![0]).toMatchObject({
				threadId: "github:acme/app:5:rc:10",
				isNewThread: !parent,
				isMention: false,
			});
		},
	);
	it("ignores comments authored by the configured bot", async () => {
		const plugin = github(options);
		const adapter = plugin.chat!.adapter() as GitHubAdapter;
		vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
			Object.assign(adapter, { chat: sdk });
		});
		const env = connect(plugin);
		const bot = { id: 99, login: "bot", type: "Bot" };
		const payload = {
			action: "created",
			repository,
			sender: bot,
			issue: { number: 5 },
			comment: {
				id: 10,
				body: "@bot hello",
				user: bot,
				created_at: "2026-01-01T00:00:00Z",
				updated_at: "2026-01-01T00:00:00Z",
			},
		};
		expect((await env.connection.routes["POST /github/events"]!(request(payload))).status).toBe(
			200,
		);
		expect(env.host.receiveMessage).not.toHaveBeenCalled();
	});
	it("rejects bad comment signatures before receiving messages", async () => {
		const plugin = github(options);
		const adapter = plugin.chat!.adapter() as GitHubAdapter;
		vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
			Object.assign(adapter, { chat: sdk });
		});
		const env = connect(plugin);
		expect(
			(await env.connection.routes["POST /github/events"]!(request({}, "issue_comment", "bad")))
				.status,
		).toBe(401);
		expect(env.host.receiveMessage).not.toHaveBeenCalled();
	});
});

it("rejects signed foreign repository owners for PATs on both ingress paths", async () => {
	const plugin = github(options);
	const adapter = plugin.chat!.adapter() as GitHubAdapter;
	const emit = vi.fn();
	const payload = {
		action: "opened",
		sender: alice,
		repository: { ...repository, full_name: "other/app", owner: { ...alice, login: "other" } },
		issue: {
			id: 10,
			number: 5,
			title: "Issue",
			body: null,
			html_url: "https://github.com/other/app/issues/5",
			user: alice,
		},
	};
	expect((await adapter.handleWebhook(request(payload))).status).toBe(401);
	expect(
		(
			await plugin.routes!["POST /github/webhook"]!(request(payload, "issues"), {
				emit,
				clients: plugin.clients!({}),
			})
		).status,
	).toBe(403);
	expect(emit).not.toHaveBeenCalled();
});

it("defaults PAT owner isolation to the token's authenticated user", async () => {
	const adapter = github({ ...options, owner: undefined }).chat!.adapter() as GitHubAdapter;
	const auth = vi
		.spyOn(adapter.octokit.rest.users, "getAuthenticated")
		.mockResolvedValue({ data: { login: "acme" } } as never);
	const payload = { action: "ignored", repository };
	expect((await adapter.handleWebhook(request(payload, "issues"))).status).toBe(200);
	expect(
		(
			await adapter.handleWebhook(
				request({ ...payload, repository: { ...repository, full_name: "other/app" } }, "issues"),
			)
		).status,
	).toBe(401);
	expect(auth).toHaveBeenCalledOnce();
});

it("does not treat an App config with an explicitly undefined token as a PAT", async () => {
	const plugin = github({
		appId: "1",
		privateKey: "key",
		installationId: 42,
		token: undefined,
		webhookSecret: "secret",
	});
	const adapter = plugin.chat!.adapter() as GitHubAdapter;
	expect(
		(await adapter.handleWebhook(request({ installation: { id: 43 }, repository }, "issues")))
			.status,
	).toBe(401);
	expect((await adapter.handleWebhook(request({ repository }, "issues"))).status).toBe(401);
	expect(
		(await adapter.handleWebhook(request({ installation: { id: 42 }, repository }, "issues")))
			.status,
	).toBe(200);
});
