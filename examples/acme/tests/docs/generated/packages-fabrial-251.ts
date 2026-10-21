// Generated from packages/fabrial/README.md:252 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, lookupOrder } from "../prelude.ts";
const researcher = f.defineAgent({
	name: "researcher",
	description: "Investigates a question and returns findings.",
	model: "anthropic/claude-haiku-4-5",
	tools: [lookupOrder],
});
const lead = f.defineAgent({
	name: "lead",
	description: "Answers hard order questions.",
	model: "anthropic/claude-sonnet-5",
	subagents: [researcher, { agent: "self", context: "fork" }],
	limits: { depth: 1, concurrent: 5 }, // the defaults
});
export {};
