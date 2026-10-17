import { f } from "./fabrial.ts";
import { generalAssistant } from "./workflows/general-assistant.ts";
import { bugIntake } from "./workflows/bug-intake.ts";
import { runSql } from "./workflows/run-sql.ts";
import { prReview } from "./workflows/pr-review.ts";
import { changelog } from "./workflows/changelog.ts";
import { linearTriage } from "./workflows/linear-triage.ts";
import { devThreads, supportChannel } from "./subscriptions.ts";

export const workflows = [generalAssistant, bugIntake, runSql, prReview, changelog, linearTriage];
export const subscriptions = [supportChannel, devThreads];
export const app = f.app({ workflows, subscriptions });
