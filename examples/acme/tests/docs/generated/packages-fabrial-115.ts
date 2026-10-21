// Generated from packages/fabrial/README.md:116 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, support, z } from "../prelude.ts";
const triage = f.defineWorkflow({
	name: "triage",
	input: z.object({ text: z.string() }),
	async run({ text }, ctx) {
		const verdict = await ctx.agent("classify", support, {
			input: text,
			output: z.object({ urgent: z.boolean(), summary: z.string() }),
		});
		return { summary: verdict.urgent ? `Urgent: ${verdict.summary}` : verdict.summary };
	},
});
export {};
