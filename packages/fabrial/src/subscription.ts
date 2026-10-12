import type { AgentRef } from "./runtime.ts";
import { isChatMessage, type InputKind, type InputPriority, type SessionOption } from "./send.ts";
import type { TriggerPayload, TriggerSpec } from "./trigger.ts";
import { defineWorkflow, type AnyWorkflow } from "./workflow.ts";

export interface SubscriptionDefinition<TTrigger extends TriggerSpec<any> = TriggerSpec<any>> {
	/** Unique among workflows and subscriptions; names the compiled workflow. */
	name: string;
	/** An ordinary trigger: ownership, specificity, and dedup work as for workflows. */
	source: TTrigger;
	agent: AgentRef;
	/** `"thread"` (default), `"channel"`, a custom key, or one computed from the event. */
	session?: SessionOption | ((event: TriggerPayload<TTrigger>) => SessionOption);
	/** Default: `"message"` for chat messages, otherwise `"notification"`. */
	kind?: InputKind;
	/** Notifications only; default `"normal"`. */
	priority?: InputPriority;
}

export interface Subscription {
	readonly kind: "fabrial.subscription";
	readonly name: string;
	readonly definition: SubscriptionDefinition;
	/** @internal The workflow that sends each event; registered by `f.app`. */
	readonly workflow: AnyWorkflow;
}

/** Compile a static subscription to a workflow whose run sends the event to the agent. */
export function subscribe<const TTrigger extends TriggerSpec<any>>(
	definition: SubscriptionDefinition<TTrigger>,
): Subscription {
	const workflow = defineWorkflow({
		name: definition.name,
		triggers: [definition.source],
		async run(event, ctx) {
			const { session } = definition;
			await ctx.send("deliver", definition.agent, {
				session:
					typeof session === "function" ? session(event as TriggerPayload<TTrigger>) : session,
				input: event,
				kind: definition.kind ?? (isChatMessage(event) ? "message" : "notification"),
				priority: definition.priority,
			});
		},
	});
	return {
		kind: "fabrial.subscription",
		name: definition.name,
		definition: definition as unknown as SubscriptionDefinition,
		workflow: { ...workflow, subscription: true },
	};
}
