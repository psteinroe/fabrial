import { fabrial, type Logger } from "fabrial";
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
import { preserveToolResponse } from "./tool-response-workaround.ts";
import { identity } from "./identity.ts";
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
type Plugins = readonly [
	ReturnType<typeof slack>,
	ReturnType<typeof github>,
	ReturnType<typeof linear>,
	ReturnType<typeof databasePlugin>,
];
declare module "fabrial" {
	interface Register {
		plugins: Plugins;
	}
}
export const workflows = [generalAssistant, bugIntake, runSql, prReview, changelog, linearTriage];
export function createApp(deps: AppDependencies) {
	const database = databasePlugin(deps.databaseSql ?? deps.sql);
	const plugins = [slack(deps.slack), github(deps.github), linear(deps.linear), database] as const;
	return fabrial({
		runtime: preserveToolResponse(conductor({ sql: deps.sql }, deps.runtimeOptions)),
		chat: chat({ state: deps.state }),
		agents: pi({ models: deps.models, sql: deps.sql, settings: deps.piSettings }),
		plugins,
		workflows,
		identity,
		logger: deps.logger,
	});
}
