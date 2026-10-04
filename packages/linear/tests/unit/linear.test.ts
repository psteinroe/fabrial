// oxlint-disable typescript/unbound-method -- Referenced host methods are Vitest mocks.
import { LinearAdapter } from "@chat-adapter/linear";
import { describe, expect, it, vi } from "vitest";
import {
	linear,
	mentioned,
	issueCreated,
	issueUpdated,
	identity,
	triageResponsibility,
} from "../../src/index.ts";
import { connect, options, request } from "../helper.ts";

describe("Linear provider", () => {
	it("declares comment capability, canonical client and lifecycle events", () => {
		const plugin = linear(options);
		expect(plugin.chat!.adapter()).toBeInstanceOf(LinearAdapter);
		expect(Object.keys(plugin.events!)).toEqual([
			"mentioned",
			"message",
			"dm",
			"issueCreated",
			"issueUpdated",
		]);
		expect(plugin.clients!({}).linear).toBe((plugin.chat!.adapter() as LinearAdapter).linearClient);
		expect(linear.mentioned).toBe(mentioned);
	});
	it("builds team/state filters with increasing specificity and observer support", () => {
		expect(mentioned()).toMatchObject({ event: "linear.mentioned", specificity: 0 });
		expect(mentioned({ team: "ENG" })).toMatchObject({
			filter: { channelId: ["ENG"] },
			specificity: 1,
		});
		expect(mentioned({ team: "ENG", thread: "linear:issue:c:root" })).toMatchObject({
			specificity: 2,
		});
		expect(issueCreated({ team: "ENG", observe: true })).toMatchObject({
			event: "linear.issueCreated",
			filter: { teamKey: ["ENG"] },
			specificity: 1,
			observe: true,
		});
		expect(issueUpdated({ team: "ENG", stateType: "started" })).toMatchObject({
			event: "linear.issueUpdated",
			filter: { teamKey: ["ENG"], stateType: ["started"] },
			specificity: 2,
		});
		expect(issueUpdated()).toMatchObject({ specificity: 0 });
	});
	it("creates organization-scoped identities and looks up display names", async () => {
		const plugin = linear(options);
		const clients = plugin.clients!({});
		const lookup = vi
			.spyOn(clients.linear, "user")
			.mockResolvedValue({ displayName: "Alice", name: "Alice Full" } as never);
		const subject = identity({ organizationId: "org", userId: "alice" });
		expect(subject).toEqual({ provider: "linear", installationId: "org", subjectId: "alice" });
		expect(await plugin.identity!.lookup!(subject, clients)).toEqual({ name: "Alice" });
		expect(lookup).toHaveBeenCalledExactlyOnceWith("alice");
		expect(
			await plugin.identity!.lookup!({ ...subject, installationId: "other" }, clients),
		).toBeUndefined();
	});
	it("falls back to full names and propagates lookup failures", async () => {
		const plugin = linear(options);
		const clients = plugin.clients!({});
		vi.spyOn(clients.linear, "user")
			.mockResolvedValueOnce({ displayName: "", name: "Alice Full" } as never)
			.mockRejectedValueOnce(new Error("outage"));
		const subject = identity({ organizationId: "org", userId: "alice" });
		expect(await plugin.identity!.lookup!(subject, clients)).toEqual({ name: "Alice Full" });
		await expect(plugin.identity!.lookup!(subject, clients)).rejects.toThrow("outage");
	});
	it("requires organization and webhook verification", () => {
		const plugin = linear({ apiKey: "", organizationId: "org" });
		expect(() => plugin.init!()).toThrow("webhookSecret");
		const unconfigured = linear({ ...options, organizationId: "" });
		expect(() => unconfigured.init!()).toThrow("organizationId");
	});
});

describe("Linear triage responsibility", () => {
	it("resolves the live current user from the exposed client", async () => {
		const clients = linear(options).clients!({});
		const team = vi
			.spyOn(clients.linear, "teams")
			.mockResolvedValueOnce({
				nodes: [
					{
						triageResponsibility: Promise.resolve({
							currentUser: Promise.resolve({ id: "alice" }),
						}),
					},
				],
			} as never)
			.mockResolvedValueOnce({
				nodes: [
					{
						triageResponsibility: Promise.resolve({ currentUser: Promise.resolve({ id: "bob" }) }),
					},
				],
			} as never);
		const group = triageResponsibility({ id: "triage", team: "ENG" });
		const ctx = { clients, now: new Date(), principal: vi.fn() };
		expect(await group.resolve!(ctx)).toEqual([
			identity({ organizationId: "org", userId: "alice" }),
		]);
		expect(await group.resolve!(ctx)).toEqual([identity({ organizationId: "org", userId: "bob" })]);
		expect(team).toHaveBeenCalledTimes(2);
	});
	it("fails closed for missing responsibility or client", async () => {
		const clients = linear(options).clients!({});
		vi.spyOn(clients.linear, "teams").mockResolvedValue({
			nodes: [{ triageResponsibility: undefined }],
		} as never);
		const group = triageResponsibility({ id: "triage", team: "ENG" });
		await expect(group.resolve!({ clients, now: new Date(), principal: vi.fn() })).rejects.toThrow(
			"no current triage",
		);
		await expect(
			group.resolve!({ clients: {}, now: new Date(), principal: vi.fn() }),
		).rejects.toThrow("clients.linear");
	});
});

describe("Linear comment ingress", () => {
	it.each([undefined, "root"])(
		"normalizes nested comments (parent=%s), team routing and retry dedupe",
		async (parentId) => {
			const plugin = linear(options);
			const adapter = plugin.chat!.adapter() as LinearAdapter;
			vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
				Object.assign(adapter, {
					chat: sdk,
					defaultBotUserId: "bot-id",
					defaultOrganizationId: "org",
				});
			});
			vi.spyOn(adapter.linearClient, "issue").mockResolvedValue({
				id: "issue",
				team: Promise.resolve({ key: "ENG" }),
			} as never);
			const env = connect(plugin);
			const payload = {
				type: "Comment",
				action: "create",
				organizationId: "org",
				webhookTimestamp: Date.now(),
				url: "https://linear.app/acme/issue/ENG-1",
				data: {
					id: "comment",
					issueId: "issue",
					parentId,
					body: "@bot hello",
					createdAt: "2026-01-01T00:00:00Z",
					updatedAt: "2026-01-01T00:00:00Z",
					user: {
						id: "alice",
						name: "Alice",
						email: "alice@example.com",
						url: "https://linear.app/acme/profiles/alice",
					},
				},
			};
			const route = env.connection.routes["POST /linear/events"]!;
			expect((await route(request(payload))).status).toBe(200);
			expect((await route(request(payload))).status).toBe(200);
			const calls = vi.mocked(env.host.receiveMessage).mock.calls;
			expect(calls).toHaveLength(2);
			expect(calls[0]![0]).toMatchObject({
				provider: "linear",
				channelId: "ENG",
				threadId: `linear:issue:c:${parentId ?? "comment"}`,
				messageId: "comment",
				isMention: true,
				isNewThread: !parentId,
				author: { identity: identity({ organizationId: "org", userId: "alice" }) },
			});
			expect(calls[0]![1]).toMatchObject({
				events: ["linear.message", "linear.mentioned"],
				dedupeId: JSON.stringify(["linear", "ENG", "comment"]),
			});
			expect(calls[1]![1].dedupeId).toBe(calls[0]![1].dedupeId);
		},
	);
	it("rejects bad signatures without looking up issues or receiving messages", async () => {
		const plugin = linear(options);
		const adapter = plugin.chat!.adapter() as LinearAdapter;
		vi.spyOn(adapter, "initialize").mockImplementation(async (sdk) => {
			Object.assign(adapter, { chat: sdk });
		});
		const issue = vi.spyOn(adapter.linearClient, "issue");
		const env = connect(plugin);
		expect((await env.connection.routes["POST /linear/events"]!(request({}, "bad"))).status).toBe(
			401,
		);
		expect(issue).not.toHaveBeenCalled();
		expect(env.host.receiveMessage).not.toHaveBeenCalled();
	});
});

it.each(["Comment", "Reaction", "OAuthApp", "AgentSessionEvent"])(
	"rejects signed foreign-organization %s before dispatch",
	async (type) => {
		const plugin = linear(options);
		const adapter = plugin.chat!.adapter() as LinearAdapter;
		const issue = vi.spyOn(adapter.linearClient, "issue");
		const response = await adapter.handleWebhook(
			request({
				type,
				action: "create",
				organizationId: "other",
				webhookTimestamp: Date.now(),
				data: {},
			}),
		);
		expect(response.status).toBe(401);
		expect(issue).not.toHaveBeenCalled();
	},
);

it("scopes custom-verifier rewritten bodies as well", async () => {
	const adapter = linear({
		...options,
		webhookVerifier: () => JSON.stringify({ type: "OAuthApp", organizationId: "other" }),
	}).chat!.adapter() as LinearAdapter;
	expect((await adapter.handleWebhook(request({ organizationId: "org" }))).status).toBe(401);
});
