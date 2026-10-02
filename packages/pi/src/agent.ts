import type {
	AgentChange,
	Conversation,
	Extension,
	ToolRegistration,
} from "@earendil-works/pi-durable";
import type { AgentRef, StateDefinition, WorkflowTool } from "fabrial";

export interface AgentDefinition extends Omit<AgentChange, "extensions" | "tools"> {
	name: string;
	extensions?: readonly (string | Extension)[];
	tools?: readonly (ToolRegistration | WorkflowTool)[];
	state?: readonly StateDefinition<any>[];
	/** Process-local configuration. Call-time configure cannot cross a durable child boundary. */
	configure?: (conversation: Conversation) => void | Promise<void>;
}
export interface DefinedAgent extends AgentRef {
	readonly definition: AgentDefinition;
}

/** Core currently has no declaration of the agents captured by a workflow closure. */
export const definedAgents = new Set<DefinedAgent>();
export function defineAgent(definition: AgentDefinition): DefinedAgent {
	if (!definition.name) throw new Error("Agent name must not be empty");
	const agent: DefinedAgent = { kind: "fabrial.agent", name: definition.name, definition };
	definedAgents.add(agent);
	return agent;
}
