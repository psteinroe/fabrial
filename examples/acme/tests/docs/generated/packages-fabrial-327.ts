// Generated from packages/fabrial/README.md:328 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { slack, z } from "../prelude.ts";
import { defineEvent, definePlugin, trigger } from "fabrial/plugin";

const issue = z.object({ id: z.string(), title: z.string(), level: z.string() });
export const sentry = definePlugin((options: { secret: string }) => ({
	id: "sentry",
	events: { issueCreated: defineEvent({ payload: issue, filterable: ["level"] }) },
	routes: {
		"POST /sentry/webhook": async (request, ctx) => {
			if (request.headers.get("x-secret") !== options.secret)
				return new Response(null, { status: 401 });
			const body = issue.parse(await request.json());
			await ctx.emit("issueCreated", body, { id: `sentry:${body.id}` }); // the dedupe key
			return new Response(null, { status: 200 });
		},
	},
}));
export const issueCreated = (level?: string) =>
	trigger<z.infer<typeof issue>>({
		event: "sentry.issueCreated",
		filter: level ? { level: [level] } : {},
		replyTo: slack.channel("C_BUGS"),
	});
export {};
