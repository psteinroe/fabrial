import { github } from "@fabrial/github";
import { z } from "zod";
import { f } from "../fabrial.ts";
import { supportModel } from "./support.ts";

/** Changes requested on a PR steer the agent even mid-run; everything else waits for its turn. */
export const prReviewPolicy = f.defineSubscriptionPolicy({
	name: "acme-pr-review",
	priority: (event) =>
		event.kind === "review" && event.attributes.state === "changes_requested"
			? "urgent"
			: undefined,
});

/** Open a pull request and follow it in this conversation until it is merged or closed. */
export const openPullRequest = f.defineTool({
	name: "open_pull_request",
	description: "Open a pull request on acme/app from a pushed branch and follow its reviews.",
	input: z.object({ title: z.string(), head: z.string(), body: z.string().optional() }),
	async run({ title, head, body }, ctx) {
		const { data } = await ctx.clients.github.rest.pulls.create({
			owner: "acme",
			repo: "app",
			title,
			head,
			base: "main",
			body,
		});
		await ctx.subscribe("watch-pr", github.pullRequest({ repo: "acme/app", number: data.number }), {
			preset: "review",
			policy: prReviewPolicy,
			until: "merged-or-closed",
		});
		return `Opened ${data.html_url}; reviews will arrive here.`;
	},
});

/** A read-only code search; the researcher's only tool. */
export const searchCode = f.defineTool({
	name: "search_code",
	description: "Search acme/app's code for a query and return matching paths.",
	input: z.object({ query: z.string() }),
	async run({ query }, ctx) {
		const { data } = await ctx.clients.github.rest.search.code({ q: `${query} repo:acme/app` });
		return data.items.map((item) => item.path).join("\n") || "No matches.";
	},
});

/** Investigates for the developer without changing anything: it has only read-only tools. */
export const researcherAgent = f.defineAgent({
	name: "acme-researcher",
	description: "Investigates a code question in acme/app and returns findings with file paths.",
	model: supportModel,
	instructions: "You investigate code questions. Answer with what you found and where.",
	tools: [searchCode],
});

export const developerAgent = f.defineAgent({
	name: "acme-developer",
	description: "Makes code changes, opens pull requests, and follows their reviews.",
	model: supportModel,
	instructions:
		"You make code changes for the team. Your final answer is posted in the conversation. When a pull request you follow gets a review, address it and say what you changed.",
	tools: [openPullRequest],
	toolkits: ["github"],
	// The `acme_researcher` tool: in the foreground for a quick lookup, in the background for longer digging.
	subagents: [researcherAgent],
});
