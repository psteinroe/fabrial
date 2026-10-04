import type { Logger } from "fabrial";
import { conductor } from "@fabrial/conductor";
import { pi, type PiOptions } from "@fabrial/pi";
import { chat } from "@fabrial/chat";
import { slack, type SlackOptions } from "@fabrial/slack";
import { github, type GitHubOptions } from "@fabrial/github";
import { linear, type LinearOptions } from "@fabrial/linear";
import type { Models } from "@earendil-works/pi-ai";
import type { StateAdapter } from "chat";
import type { Sql } from "postgres";
import { databasePlugin } from "./database.ts";
import { f } from "./fabrial.ts";
import { generalAssistant } from "./workflows/general-assistant.ts";
import { bugIntake } from "./workflows/bug-intake.ts";
import { runSql } from "./workflows/run-sql.ts";
import { prReview } from "./workflows/pr-review.ts";
import { changelog } from "./workflows/changelog.ts";
import { linearTriage } from "./workflows/linear-triage.ts";

export interface AppDependencies {
	sql: Sql;
	/** Optional separate least-privilege application database pool. */
	databaseSql?: Sql;
	state: StateAdapter;
	models: Models;
	slack: SlackOptions;
	github: GitHubOptions;
	linear: LinearOptions;
	logger?: Logger;
	runtimeOptions?: Parameters<typeof conductor>[1];
	piSettings?: PiOptions["settings"];
}
export const workflows = [generalAssistant, bugIntake, runSql, prReview, changelog, linearTriage];
export function createApp(deps: AppDependencies) {
	return f.app({
		runtime: conductor({ sql: deps.sql }, deps.runtimeOptions),
		chat: chat({ state: deps.state }),
		agents: pi({ models: deps.models, sql: deps.sql, settings: deps.piSettings }),
		plugins: {
			slack: slack(deps.slack),
			github: github(deps.github),
			linear: linear(deps.linear),
			database: databasePlugin(deps.databaseSql ?? deps.sql),
		},
		workflows,
		logger: deps.logger,
	});
}
