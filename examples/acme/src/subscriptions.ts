import { f } from "./fabrial.ts";
import { slack } from "@fabrial/slack";
import { supportAgent } from "./agents/support.ts";
import { developerAgent } from "./agents/developer.ts";

/** One conversation for the whole help channel, steered by whoever writes there. */
export const supportChannel = f.subscribe({
	name: "support-channel",
	source: slack.message({ channel: "C_HELP" }),
	agent: supportAgent,
	session: "channel",
});

/** One conversation per #dev thread; its agent follows the pull requests it opens (use case 3). */
export const devThreads = f.subscribe({
	name: "dev-threads",
	source: slack.message({ channel: "C_DEV" }),
	agent: developerAgent,
});
