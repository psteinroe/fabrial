import type {
	AgentChange,
	Conversation,
	Extension,
	ToolRegistration,
} from "@earendil-works/pi-durable";
import type { AgentRef, StateDefinition, WorkflowTool } from "fabrial";

export interface AgentDefinition<TId extends string = string> extends Omit<
	AgentChange,
	"extensions" | "tools"
> {
	name: string;
	extensions?: readonly (TId | Extension)[];
	tools?: readonly (ToolRegistration | WorkflowTool)[];
	state?: readonly StateDefinition<any>[];
	/** Process-local configuration. Call-time configure cannot cross a durable child boundary. */
	configure?: (conversation: Conversation) => void | Promise<void>;
}
export interface DefinedAgent extends AgentRef {
	readonly definition: AgentDefinition;
}

/** Discovery is scoped to the Fabrial instance, not to the importing process. */
export const definedAgents = new WeakMap<object, Set<DefinedAgent>>();
export function defineAgent(definition: AgentDefinition, scope: object): DefinedAgent {
	if (!definition.name) throw new Error("Agent name must not be empty");
	const agent: DefinedAgent = { kind: "fabrial.agent", name: definition.name, definition };
	let agents = definedAgents.get(scope);
	if (!agents) definedAgents.set(scope, (agents = new Set()));
	agents.add(agent);
	return agent;
}
