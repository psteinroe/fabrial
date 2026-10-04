import { WebClient } from "@slack/web-api";
import type { GroupResolveContext } from "fabrial";
import { describe, expect, it, vi } from "vitest";
import {
	slack,
	mentioned,
	message,
	newThread,
	dm,
	channel,
	identity,
	userGroup,
} from "../../src/index.ts";

const options = { botToken: "xoxb-test", signingSecret: "secret", workspace: "acme" };

describe("Slack plugin", () => {
	it("declares the adapter, standard events, canonical client and installation", () => {
		const plugin = slack(options);
		expect(plugin.id).toBe("slack");
		expect(plugin.chat!.installationId).toBe(options.workspace);
		expect(Object.keys(plugin.events!)).toEqual(["mentioned", "message", "dm"]);
		expect(plugin.chat!.adapter()).toMatchObject({ name: "slack" });
		expect(plugin.clients!({}).slack).toBeInstanceOf(WebClient);
		expect(slack.mentioned).toBe(mentioned);
	});

	it("builds typed filters with workspace < channel < channel+thread specificity", () => {
		expect(mentioned()).toMatchObject({ event: "slack.mentioned", specificity: 0 });
		expect(mentioned({ channel: "C1" })).toMatchObject({
			filter: { channelId: ["C1"] },
			specificity: 1,
		});
		expect(mentioned({ channel: "C1", thread: "slack:C1:123" })).toMatchObject({
			filter: { channelId: ["C1"], threadId: ["slack:C1:123"] },
			specificity: 2,
		});
		expect(message({ channel: "C1", observe: true })).toMatchObject({
			event: "slack.message",
			filter: { channelId: ["C1"] },
			observe: true,
		});
		expect(newThread({ channel: "C1" })).toMatchObject({
			event: "slack.message",
			filter: { channelId: ["C1"], isNewThread: [true] },
		});
		expect(dm()).toMatchObject({ event: "slack.dm", specificity: 0 });
	});

	it("creates serializable surfaces and installation-scoped identities", () => {
		expect(channel("C1")).toEqual({ kind: "channel", provider: "slack", channelId: "C1" });
		expect(identity({ workspace: "acme", userId: "U1" })).toEqual({
			provider: "slack",
			installationId: "acme",
			subjectId: "U1",
		});
	});

	it("looks up display names using the configured client and rejects other installations", async () => {
		const plugin = slack(options);
		const clients = plugin.clients!({});
		const info = vi.spyOn(clients.slack.users, "info").mockResolvedValue({
			ok: true,
			user: { profile: { display_name: "Alice", real_name: "Alice Full" } },
		});
		expect(
			await plugin.identity!.lookup!(identity({ workspace: "acme", userId: "U1" }), clients),
		).toEqual({ name: "Alice" });
		expect(info).toHaveBeenCalledExactlyOnceWith({ user: "U1" });
		expect(
			await plugin.identity!.lookup!(identity({ workspace: "other", userId: "U1" }), clients),
		).toBeUndefined();
	});

	it("falls back to real names for users without a display name", async () => {
		const plugin = slack(options);
		const clients = plugin.clients!({});
		vi.spyOn(clients.slack.users, "info").mockResolvedValue({
			ok: true,
			user: { real_name: "Alice Full" },
		});
		expect(
			await plugin.identity!.lookup!(identity({ workspace: "acme", userId: "U1" }), clients),
		).toEqual({ name: "Alice Full" });
	});
});

describe("Slack user groups", () => {
	it("resolves current members using the plugin client and configured workspace", async () => {
		const clients = slack(options).clients!({});
		const list = vi.spyOn(clients.slack.usergroups, "list").mockResolvedValue({
			ok: true,
			usergroups: [{ id: "S1", handle: "triage", team_id: "T_NATIVE" }],
		});
		const members = vi
			.spyOn(clients.slack.usergroups.users, "list")
			.mockResolvedValueOnce({ ok: true, users: ["U1", "U2"] })
			.mockResolvedValueOnce({ ok: true, users: ["U3"] });
		const ctx: GroupResolveContext<typeof clients> = {
			clients,
			now: new Date(),
			principal: vi.fn(),
		};
		const group = userGroup({ id: "triage-approvers", handle: "@triage" });
		expect(group.kind).toBe("fabrial.group");
		expect(await group.resolve!(ctx)).toEqual([
			identity({ workspace: "acme", userId: "U1" }),
			identity({ workspace: "acme", userId: "U2" }),
		]);
		expect(await group.resolve!(ctx)).toEqual([identity({ workspace: "acme", userId: "U3" })]);
		expect(list).toHaveBeenCalledTimes(2);
		expect(members).toHaveBeenCalledWith({ usergroup: "S1" });
	});

	it("does not silently grant access for missing groups or clients", async () => {
		const clients = slack(options).clients!({});
		vi.spyOn(clients.slack.usergroups, "list").mockResolvedValue({ ok: true, usergroups: [] });
		const group = userGroup({ id: "triage", handle: "missing" });
		await expect(group.resolve!({ clients, now: new Date(), principal: vi.fn() })).rejects.toThrow(
			"not found",
		);
		await expect(
			// @ts-expect-error Deliberately missing the required Slack client.
			group.resolve!({ clients: {}, now: new Date(), principal: vi.fn() }),
		).rejects.toThrow("clients.slack");
	});
});
