import { randomBytes } from "node:crypto";
import { isRetryableAssistantError, type AssistantMessage } from "@earendil-works/pi-ai";

/** Status of a sent run whose generation is backing off before a durable retry. */
export const WAITING_STATUS = "Waiting for the model provider…";

const UNREACHABLE =
	"I couldn't reach the model provider while handling your request. Please try again in a few minutes.";
const FAILED =
	"I hit an error while handling your request. Please try again, rephrase, or reach out if it keeps failing.";

/** Id of one failed sent request, shown in its error reply and logged with the provider's error text. */
export const newErrorId = () => randomBytes(6).toString("hex");

/**
 * The error reply for sent input Pi settled `unanswered`, or undefined for none. A model error whose text
 * is transient exhausted the durable retry budget; any other model error, or no model, is not retried.
 * Aborted, reset, and stale input was ended on purpose and gets no reply. The provider's text stays in logs.
 */
export function sentErrorReply(
	reason: string,
	detail: string | undefined,
	errorId: string,
): string | undefined {
	if (reason !== "model_error" && reason !== "no_model") return undefined;
	const transient =
		reason === "model_error" &&
		isRetryableAssistantError({ stopReason: "error", errorMessage: detail } as AssistantMessage);
	return `${transient ? UNREACHABLE : FAILED}\n\n_Error id: \`${errorId}\`_`;
}
