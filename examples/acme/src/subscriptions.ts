import { f } from "./fabrial.ts";
import { slack } from "@fabrial/slack";
import { supportAgent } from "./agents/support.ts";

/** One conversation for the whole help channel, steered by whoever writes there. */
export const supportChannel = f.subscribe({
	name: "support-channel",
	source: slack.message({ channel: "C_HELP" }),
	agent: supportAgent,
	session: "channel",
});
