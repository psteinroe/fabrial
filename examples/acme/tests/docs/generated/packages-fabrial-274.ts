// Generated from packages/fabrial/README.md:275 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, z } from "../prelude.ts";
const Notes = f.defineState({
	name: "notes",
	scope: "session",
	schema: z.object({ notes: z.array(z.string()) }),
	initial: () => ({ notes: [] }),
	render: (s) => (s.notes.length ? `Known so far:\n- ${s.notes.join("\n- ")}` : undefined),
});
const note = f.defineTool({
	name: "note",
	description: "Remember a finding.",
	input: z.object({ text: z.string() }),
	async run({ text }, ctx) {
		await ctx.state(Notes).update((s) => {
			s.notes.push(text);
		}); // committed with the tool result
		return "noted";
	},
});
export {};
