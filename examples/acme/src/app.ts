import { f } from "./fabrial.ts";
import { generalAssistant } from "./workflows/general-assistant.ts";
import { runSql } from "./workflows/run-sql.ts";
import { prReview } from "./workflows/pr-review.ts";
import { changelog } from "./workflows/changelog.ts";
import { linearTriage } from "./workflows/linear-triage.ts";
import { supportAgent } from "./agents/support.ts";
import { developerAgent } from "./agents/developer.ts";

export const workflows = [generalAssistant, runSql, prReview, changelog, linearTriage];
/** Agents whose triggers receive events: #help and #dev messages. */
export const agents = [supportAgent, developerAgent];
export const app = f.app({ workflows, agents });
