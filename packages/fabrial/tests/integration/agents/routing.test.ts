import { randomUUID } from "node:crypto";
import { createModels } from "@earendil-works/pi-ai/models";
import type { Message } from "@earendil-works/pi-ai";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";
import { z } from "zod";
import {
	chatEvents,
	chatTrigger,
	createFabrial,
	defineEvent,
	definePlugin,
	trigger,
	type Agent,
	type AnyWorkflow,
	type ChatMessage,
	type ExternalIdentity,
	type JsonObject,
} from "../../../src/index.ts";
import { pi } from "../../../src/agents/index.ts";
import { FakeChat } from "../../../src/testing.ts";
import { useAgents } from "./setup.ts";

const { sql, launch, idle } = useAgents();

const chat = definePlugin({ id: "chat", events: chatEvents() });
const gh = definePlugin({ id: "gh", events: chatEvents() });
const sentry = definePlugin({
	id: "sentry",
	events: {
		issueCreated: defineEvent({
			payload: z.object({ id: z.string(), title: z.string() }),
			filterable: ["id"],
		}),
	},
});
const f = createFabrial({ plugins: [chat, gh, sentry] });

const text = (messages: readonly Message[]) => JSON.stringify(messages);
type Response = Parameters<ReturnType<typeof fauxProvider>["setResponses"]>[0][number];

/** A fresh channel and model; `build` defines the agents and workflows of one app. */
async function setup(
	build: (ctx: { channel: string; model: string }) => {
		agents: Agent[];
		workflows?: AnyWorkflow[];
	},
) {
	const channel = `c${randomUUID().slice(0, 8)}`;
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const model = `${faux.provider.id}/${faux.getModel().id}`;
	const { agents, workflows = [] } = build({ channel, model });
	const {
		runtime,
		chat: fake,
		app,
	} = await launch(f, {
		workflows,
		routedAgents: agents,
		agents: pi({ models, sql: sql(), settings: { retry: { enabled: false } } }),
	});
	const settle = () => idle(runtime.queue);
	const threadId = (thread: string, provider = "chat") =>
		provider === "chat" ? `chat:${channel}:${thread}` : `${provider}:${thread}`;
	const message = (
		id: string,
		thread: string,
		body: string,
		options: { mention?: boolean; provider?: string; author?: string } = {},
	): ChatMessage => {
		const provider = options.provider ?? "chat";
		const author = options.author ?? id;
		return {
			provider,
			threadId: threadId(thread, provider),
			channelId: channel,
			messageId: id,
			text: body,
			author: {
				identity: { provider, installationId: "w", subjectId: author } as ExternalIdentity &
					JsonObject,
				name: author,
				isBot: false,
			},
			isMention: options.mention ?? false,
			isDM: false,
			isNewThread: false,
			sentAt: new Date().toISOString(),
		};
	};
	/** Receive a message as the chat integration does: a mention is both a message and a mention. */
	const receive = (m: ChatMessage) =>
		fake.receive(m, {
			events: [`${m.provider}.message`, ...(m.isMention ? [`${m.provider}.mentioned`] : [])],
		});
	const posts = async (thread: string, provider = "chat") =>
		(
			await fake.thread({ kind: "thread", provider, threadId: threadId(thread, provider) })
		).posts.map((post) => post.content);
	const respond = (responses: Response[]) => faux.setResponses(responses);
	return {
		channel,
		model,
		faux,
		runtime,
		chat: fake,
		app,
		settle,
		message,
		receive,
		posts,
		respond,
	};
}

const asked = (seen: string[]) => (context: { messages: readonly Message[] }) => {
	seen.push(text(context.messages));
	return fauxAssistantMessage(`Answer ${seen.length}`);
};

it("use case 1: a mention in #help starts a conversation that follows its thread; an unmentioned reply starts the next run", async () => {
	const seen: string[] = [];
	const t = await setup(({ channel, model }) => ({
		agents: [
			f.defineAgent({
				name: `support-${channel}`,
				description: "Answers questions in #help.",
				model,
				triggers: [chatTrigger("chat", "mentioned", { channel })],
			}),
		],
	}));
	t.respond([asked(seen), asked(seen)]);
	expect(
		await t.receive(t.message("m1", "t1", "@bot how do refunds work?", { mention: true })),
	).toBe("new");
	await t.settle();
	// Bob doesn't mention the bot: the thread is followed, so the message reaches the same conversation.
	expect(await t.receive(t.message("m2", "t1", "and for annual plans?", { author: "bob" }))).toBe(
		"follow",
	);
	expect(await t.receive(t.message("m2", "t1", "and for annual plans?", { author: "bob" }))).toBe(
		"ignored",
	);
	await t.settle();
	expect(t.faux.state.callCount).toBe(2);
	expect(seen[1]).toContain("how do refunds work?");
	expect(seen[1]).toContain('from=\\"bob\\"');
	expect(await t.posts("t1")).toEqual(["Answer 1", "Answer 2"]);
	// An unmentioned message in another thread reaches nobody.
	expect(await t.receive(t.message("m3", "t2", "hello?"))).toBe("new");
	await t.settle();
	expect(t.faux.state.callCount).toBe(2);
	expect(await t.posts("t2")).toEqual([]);
});

it("use case 2: every message in a channel reaches the agent in its thread; irrelevant ones end in no_reply", async () => {
	const t = await setup(({ channel, model }) => ({
		agents: [
			f.defineAgent({
				name: `watcher-${channel}`,
				description: "Answers every message in a channel.",
				model,
				triggers: [chatTrigger("chat", "message", { channel })],
			}),
		],
	}));
	t.respond([
		fauxAssistantMessage("Restart the worker."),
		fauxAssistantMessage(fauxToolCall("no_reply", { reason: "chatter" }), {
			stopReason: "toolUse",
		}),
	]);
	expect(await t.receive(t.message("m1", "t1", "the queue is stuck"))).toBe("new");
	await t.settle();
	expect(await t.receive(t.message("m2", "t2", "lol"))).toBe("new");
	await t.settle();
	expect(t.faux.state.callCount).toBe(2);
	expect(await t.posts("t1")).toEqual(["Restart the worker."]);
	expect(await t.posts("t2")).toEqual([]);
	// Two threads, two conversations: one Session per thread, each recorded as the thread's follower.
	for (const thread of ["t1", "t2"]) {
		const io = await t.chat.thread({
			kind: "thread",
			provider: "chat",
			threadId: `chat:${t.channel}:${thread}`,
		});
		expect((await io.getState())?.follows).toEqual([
			{ session: `chat:chat:${t.channel}:${thread}`, agent: `watcher-${t.channel}` },
		]);
	}
});

it('use case 3: session "channel" puts many threads in one conversation, answers each in its thread, and follows each', async () => {
	const seen: string[] = [];
	let agent!: Agent;
	const t = await setup(({ channel, model }) => {
		agent = f.defineAgent({
			name: `ops-${channel}`,
			description: "One conversation for #ops.",
			model,
			triggers: [chatTrigger("chat", "mentioned", { channel })],
			session: "channel",
		});
		return { agents: [agent] };
	});
	t.respond([asked(seen), asked(seen), asked(seen)]);
	expect(await t.receive(t.message("m1", "t1", "@bot deploy status?", { mention: true }))).toBe(
		"new",
	);
	await t.settle();
	expect(await t.receive(t.message("m2", "t2", "@bot disk usage?", { mention: true }))).toBe("new");
	await t.settle();
	// Unmentioned, but t1 is followed: a follow record in its thread state.
	expect(await t.receive(t.message("m3", "t1", "thanks, and staging?"))).toBe("follow");
	await t.settle();
	expect(seen[2]).toContain("deploy status?");
	expect(seen[2]).toContain("disk usage?");
	expect(await t.posts("t1")).toEqual(["Answer 1", "Answer 3"]);
	expect(await t.posts("t2")).toEqual(["Answer 2"]);
	const session = `chat:channel:${t.channel}`;
	for (const thread of ["t1", "t2"]) {
		const io = await t.chat.thread({
			kind: "thread",
			provider: "chat",
			threadId: `chat:${t.channel}:${thread}`,
		});
		expect((await io.getState())?.follows).toEqual([{ session, agent: agent.name }]);
	}
});

it("use case 5: started on GitHub, the home is the PR and replies post there", async () => {
	const t = await setup(({ channel, model }) => ({
		agents: [
			f.defineAgent({
				name: `reviewer-${channel}`,
				description: "Checks pull requests.",
				model,
				triggers: [chatTrigger("gh", "mentioned")],
			}),
		],
	}));
	t.respond([fauxAssistantMessage("Looks good."), fauxAssistantMessage("Fixed now.")]);
	const pr = `acme/app#${t.channel}`;
	expect(
		await t.receive(
			t.message("c1", pr, "@bot can you check this?", { mention: true, provider: "gh" }),
		),
	).toBe("new");
	await t.settle();
	expect(await t.receive(t.message("c2", pr, "pushed a fix", { provider: "gh" }))).toBe("follow");
	await t.settle();
	expect(await t.posts(pr, "gh")).toEqual(["Looks good.", "Fixed now."]);
});

it("use case 6: an event without a thread starts a conversation in its home; the first reply creates the thread, followed after", async () => {
	const seen: string[] = [];
	let agent!: Agent;
	const t = await setup(({ channel, model }) => {
		agent = f.defineAgent({
			name: `bugs-${channel}`,
			description: "Investigates Sentry issues.",
			model,
			triggers: [trigger({ event: "sentry.issueCreated", filter: { id: [channel] } })],
			home: { kind: "channel", provider: "chat", channelId: channel },
		});
		return { agents: [agent] };
	});
	t.respond([asked(seen), asked(seen)]);
	await t.app.emit(
		"sentry.issueCreated",
		{ id: t.channel, title: "TypeError in checkout" },
		{
			id: `issue-${t.channel}`,
		},
	);
	await t.settle();
	expect(seen[0]).toContain("<notification");
	expect(seen[0]).toContain("TypeError in checkout");
	const created = [...t.chat.threads.values()].find((thread) =>
		thread.ref.threadId.startsWith(`channel:${t.channel}:root-`),
	)!;
	expect(created.posts.map((post) => post.content)).toEqual(["Answer 1"]);
	expect((await created.getState())?.follows).toEqual([
		{ session: `sentry.issueCreated:issue-${t.channel}`, agent: agent.name },
	]);
	// A reply in the created thread reaches the same conversation and is answered there.
	const reply = {
		...t.message("m1", "x", "is this the checkout bug?"),
		threadId: created.ref.threadId,
	};
	expect(await t.receive(reply)).toBe("follow");
	await t.settle();
	expect(seen[1]).toContain("TypeError in checkout");
	expect(created.posts.map((post) => post.content)).toEqual(["Answer 1", "Answer 2"]);
});

it("a ctx.agent call runs in its own Session and never follows the thread: later replies don't reach it", async () => {
	const seen: string[] = [];
	const t = await setup(({ channel, model }) => {
		const investigator = f.defineAgent({
			name: `investigator-${channel}`,
			description: "Investigates bugs.",
			model,
		});
		const intake = f.defineWorkflow({
			name: `intake-${channel}`,
			triggers: [chatTrigger("chat", "mentioned", { channel })],
			async run(message, ctx) {
				const answer = await ctx.agent<string>("investigate", investigator, {
					input: message.text,
				});
				await ctx.thread!.post("analysis", answer);
			},
		});
		return { agents: [investigator], workflows: [intake] };
	});
	t.respond([asked(seen), asked(seen)]);
	expect(await t.receive(t.message("m1", "b1", "@bot checkout is broken", { mention: true }))).toBe(
		"new",
	);
	await t.settle();
	expect(await t.posts("b1")).toEqual(["Answer 1"]);
	const io = await t.chat.thread({
		kind: "thread",
		provider: "chat",
		threadId: `chat:${t.channel}:b1`,
	});
	expect((await io.getState())?.follows).toBeUndefined();
	// No conversation follows the thread and nothing triggers on an unmentioned reply: it reaches no one.
	await t.receive(t.message("m2", "b1", "it fails on step 2"));
	await t.settle();
	expect(t.faux.state.callCount).toBe(1);
	expect(await t.posts("b1")).toEqual(["Answer 1"]);
	const [thread] =
		await sql()`SELECT 1 FROM fabrial_pi.sessions WHERE key = ${`chat:chat:${t.channel}:b1`}`;
	expect(thread).toBeUndefined();
});

it("a router workflow picks an agent with ctx.send; later replies go straight to it, and a more specific agent trigger wins over the router", async () => {
	const seen: string[] = [];
	let routed = 0;
	const t = await setup(({ channel, model }) => {
		const support = f.defineAgent({
			name: `help-${channel}`,
			description: "Answers in #help.",
			model,
			triggers: [chatTrigger("chat", "mentioned", { channel })],
		});
		const bugs = f.defineAgent({
			name: `bug-${channel}`,
			description: "Investigates bugs.",
			model,
		});
		const router = f.defineWorkflow({
			name: `router-${channel}`,
			triggers: [chatTrigger("chat", "mentioned")],
			async run(message, ctx) {
				routed++;
				await ctx.send("deliver", bugs, { input: message });
			},
		});
		return { agents: [support], workflows: [router] };
	});
	t.respond([asked(seen), asked(seen), asked(seen), asked(seen)]);
	const elsewhere = (id: string, body: string, mention = false): ChatMessage => ({
		...t.message(id, "x", body, { mention }),
		channelId: `other-${t.channel}`,
		threadId: `chat:other-${t.channel}:r1`,
	});
	// Less specific than #help's trigger: the router runs and hands the thread to the bug agent.
	expect(await t.receive(elsewhere("m1", "@bot checkout is broken", true))).toBe("new");
	await t.settle();
	expect(routed).toBe(1);
	expect(await t.receive(elsewhere("m2", "it fails on step 2"))).toBe("follow");
	await t.settle();
	expect(await t.receive(elsewhere("m3", "@bot any update?", true))).toBe("follow");
	await t.settle();
	expect(routed).toBe(1);
	const other = await t.chat.thread({
		kind: "thread",
		provider: "chat",
		threadId: `chat:other-${t.channel}:r1`,
	});
	expect(other.posts.map((post) => post.content)).toEqual(["Answer 1", "Answer 2", "Answer 3"]);
	expect(seen[2]).toContain("it fails on step 2");
	expect(seen[2]).toContain("any update?");
	// A mention in #help matches the agent's channel trigger, which is more specific than the router's.
	expect(await t.receive(t.message("m4", "h1", "@bot hi", { mention: true }))).toBe("new");
	await t.settle();
	expect(routed).toBe(1);
	expect(await t.posts("h1")).toEqual(["Answer 4"]);
	expect(seen[3]).not.toContain("checkout is broken");
});

it("a message naming another agent in a followed thread goes only to that agent, which then follows the thread", async () => {
	const seen: { lead: string[]; expert: string[] } = { lead: [], expert: [] };
	const t = await setup(({ channel, model }) => {
		const lead = f.defineAgent({
			name: `lead-${channel}`,
			description: "Answers first.",
			model,
			instructions: "You are lead.",
			triggers: [chatTrigger("chat", "mentioned", { channel })],
		});
		const expert = f.defineAgent({
			name: `expert-${channel}`,
			description: "Answers hard questions.",
			model,
			instructions: "You are expert.",
		});
		return { agents: [lead, expert] };
	});
	// Instructions reach the model as a system entry of the transcript.
	const who = (context: { messages: readonly Message[] }) =>
		text(context.messages).includes("You are expert.") ? "expert" : "lead";
	const answer = (context: { messages: readonly Message[] }) => {
		const name = who(context);
		seen[name].push(text(context.messages));
		return fauxAssistantMessage(`${name} ${seen[name].length}`);
	};
	t.respond([answer, answer, answer, answer]);
	await t.receive(t.message("m1", "t1", "@bot question", { mention: true }));
	await t.settle();
	expect(await t.receive(t.message("m2", "t1", `@expert-${t.channel} can you take this?`))).toBe(
		"follow",
	);
	await t.settle();
	expect(seen.lead).toHaveLength(1);
	expect(seen.expert).toHaveLength(1);
	expect(seen.expert[0]).toContain("can you take this?");
	expect(await t.posts("t1")).toEqual(["lead 1", "expert 1"]);
	// Both follow the thread now: an unaddressed message reaches both conversations.
	expect(await t.receive(t.message("m3", "t1", "thanks both"))).toBe("follow");
	await t.settle();
	expect(seen.lead).toHaveLength(2);
	expect(seen.expert).toHaveLength(2);
	expect(seen.lead[1]).not.toContain("can you take this?");
});

it("equal specificity between an agent's and a workflow's trigger fails at startup", async () => {
	const channel = `c${randomUUID().slice(0, 8)}`;
	const agent = f.defineAgent({
		name: `tie-${channel}`,
		description: "Ties.",
		triggers: [chatTrigger("chat", "mentioned", { channel })],
	});
	const workflow = f.defineWorkflow({
		name: `tie-${channel}`,
		triggers: [chatTrigger("chat", "mentioned", { channel })],
		async run() {},
	});
	await expect(
		launch(f, {
			workflows: [workflow],
			routedAgents: [agent],
			chat: new FakeChat(),
			agents: pi({ models: createModels(), sql: sql() }),
		}),
	).rejects.toThrow(
		`Ambiguous trigger owners: tie-${channel}, agent tie-${channel} on chat.mentioned`,
	);
});
