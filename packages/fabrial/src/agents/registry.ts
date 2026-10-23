// The Pi registry of an app's agents: their toolkits, tools, instructions and subagents, compiled into Pi extensions
// once, when the app starts.
import {
	type AgentChange,
	createRegistry,
	defineExtension,
	type Extension,
	GenerationTask,
	hook,
	section as piSection,
	type ToolRegistration,
	ToolTask,
} from "@earendil-works/pi-durable";
import {
	type Agent,
	type AgentObservers,
	definedAgents,
	isWorkflow,
	ownToolNames,
	resolveSubagents,
	type Toolkit,
} from "./definition.ts";
import type { FabrialHost } from "../app.ts";
import { agentOf, fields } from "./tool-context.ts";
import type { JsonObject } from "../schema.ts";
import { instructionsSection, noReplyTool, piToolOf, workflowTool } from "./tools.ts";
import { stopTool, type SubagentDeps, subagentTasks, subagentTool } from "./subagents.ts";
import { parseModel } from "../models.ts";

/** Fabrial observers as Pi hooks on the built-in generation and tool tasks. */
function observerHooks(observers: AgentObservers[], host: FabrialHost) {
	const notify = async (run: (observer: AgentObservers) => void | Promise<void> | undefined) => {
		for (const observer of observers)
			try {
				await run(observer);
			} catch (error) {
				host.logger.warn("Agent observer failed", { error });
			}
	};
	return [
		hook(GenerationTask, {
			async afterResponse(message, api, context) {
				const agent = await agentOf(api, api.conversationId, context);
				await notify((observer) =>
					observer.onModelResponse?.({
						agent,
						model: { provider: message.provider, modelId: message.model },
						text: message.content
							.flatMap((block) => (block.type === "text" ? [block.text] : []))
							.join("\n"),
						toolCalls: message.content.flatMap((block) =>
							block.type === "toolCall"
								? [{ id: block.id, name: block.name, arguments: block.arguments as JsonObject }]
								: [],
						),
						stopReason: message.stopReason,
						...(message.errorMessage ? { error: message.errorMessage } : {}),
						...(message.usage
							? {
									usage: {
										input: message.usage.input,
										output: message.usage.output,
										totalTokens: message.usage.totalTokens,
									},
								}
							: {}),
					}),
				);
			},
		}),
		hook(ToolTask, {
			async afterTool(call, result, api, context) {
				const agent = await agentOf(api, api.conversationId, context);
				await notify((observer) =>
					observer.onToolCall?.({
						agent,
						callId: call.id,
						name: call.name,
						arguments: call.arguments as JsonObject,
						result: (result.content ?? [])
							.flatMap((block) => (block.type === "text" ? [block.text] : []))
							.join("\n"),
						isError: !!result.isError,
					}),
				);
				return undefined;
			},
		}),
	];
}

/** Compiles the instance's agents, their toolkits, tools and instructions into one Pi registry, once, when the app starts. */
export function agentRegistry(host: FabrialHost) {
	const registry = createRegistry();
	const agents = new Map<string, Agent>();
	const configs = new Map<string, AgentChange>();
	const observers = host.plugins.flatMap((plugin) =>
		plugin.hooks?.agent ? [plugin.hooks.agent] : [],
	);
	const noReply = noReplyTool();
	const subagentDeps: SubagentDeps = { childAgent };
	const internal = defineExtension({
		name: "fabrial.internal",
		tools: [noReply],
		// Background subagent tasks resume after a restart wherever a Session is opened.
		tasks: subagentTasks,
		hooks: observers.length ? observerHooks(observers, host) : [],
	});
	registry.install(internal);
	const pluginToolkits = new Map<string, Extension>();
	const appToolkits = new Map<string, { toolkit: Toolkit; extension: Extension }>();
	/** Each agent's extensions without, and its subagent tools as, an extension. */
	const extensionsOf = new Map<string, { base: Extension[]; subagents?: Extension }>();
	/** A child's agent: its own definition, subagent tools only with levels left, and never `no_reply`. */
	function childAgent(agent: Agent, depth: number): AgentChange {
		const { model, thinking } = agent.definition;
		const parts = extensionsOf.get(agent.name)!;
		return {
			// Unset fields keep the copy of the lead's agent the child starts with.
			...(model ? { model: parseModel(model) } : {}),
			...(thinking ? { thinkingLevel: thinking } : {}),
			instructions: null,
			extensions: [...parts.base, ...(depth > 0 && parts.subagents ? [parts.subagents] : [])],
			tools: { remove: [noReply] },
		};
	}

	/** Workflow tools whose workflow the app does not register drop out. */
	function tools(owner: string, list: Toolkit["tools"] = []): ToolRegistration[] {
		return list.flatMap((tool) =>
			!isWorkflow(tool)
				? [piToolOf(tool)]
				: host.workflows.some((candidate) => candidate.name === tool.name)
					? [workflowTool(tool, owner)]
					: [],
		);
	}

	function toolkit(entry: string | Toolkit, agent: string): Extension {
		if (typeof entry === "string") {
			const resolved = pluginToolkits.get(entry);
			if (!resolved)
				throw new Error(`Agent ${agent} selects plugin ${entry}, which provides no toolkit`);
			return resolved;
		}
		const existing = appToolkits.get(entry.name);
		if (existing && existing.toolkit !== entry)
			throw new Error(`Duplicate Fabrial toolkit name: ${entry.name}`);
		if (existing) return existing.extension;
		const section = instructionsSection(`toolkit-${entry.name}`, entry.instructions);
		const extension = defineExtension({
			name: `fabrial.toolkit.${entry.name}`,
			tools: tools(`toolkit ${entry.name}`, entry.tools),
			sections: section ? [section] : [],
		});
		appToolkits.set(entry.name, { toolkit: entry, extension });
		registry.install(extension);
		return extension;
	}

	function install(agent: Agent): void {
		const previous = agents.get(agent.name);
		if (previous && previous !== agent)
			throw new Error(`Duplicate Fabrial agent name: ${agent.name}`);
		agents.set(agent.name, agent);
		const { name, model, thinking, instructions, state = [] } = agent.definition;
		const selected = (agent.definition.toolkits ?? []).map((entry) => toolkit(entry, name));
		const subagents = resolveSubagents(agent);
		const taken = new Set([
			...ownToolNames(agent.definition),
			...selected.flatMap((extension) => (extension.tools ?? []).map((tool) => tool.name)),
		]);
		for (const subagent of subagents) {
			if (taken.has(subagent.tool))
				throw new Error(
					`Agent ${name}: subagent ${subagent.agent.name} is called ${subagent.tool}, which a toolkit tool already uses`,
				);
			if (subagent.agent !== agent) install(subagent.agent);
		}
		const section = instructionsSection("agent-instructions", instructions);
		const own = defineExtension({
			name: `fabrial.agent.${name}`,
			tools: tools(`agent ${name}`, agent.definition.tools),
			sections: [
				...(section ? [section] : []),
				...state
					.filter((definition) => definition.render)
					.map((definition) =>
						piSection(
							`state-${definition.name.replaceAll(/[^a-z0-9_-]/g, "-")}`,
							async (input, context) => {
								const ctx = await fields(context, input.conversationId);
								return definition.render!(await ctx.state(definition).get());
							},
						),
					),
			],
		});
		registry.install(own);
		const delegates = subagents.length
			? defineExtension({
					name: `fabrial.subagents.${name}`,
					tools: [
						...subagents.map((subagent) => subagentTool(agent, subagent, subagentDeps)),
						stopTool(),
					],
				})
			: undefined;
		if (delegates) registry.install(delegates);
		extensionsOf.set(name, { base: [internal, ...selected, own], subagents: delegates });
		configs.set(name, {
			model: model ? parseModel(model) : null,
			thinkingLevel: thinking ?? null,
			instructions: null,
			extensions: [internal, ...selected, own, ...(delegates ? [delegates] : [])],
			tools: null,
		});
	}

	/** Plugin toolkits, then the instance's agents. */
	function installAll(scope: object) {
		for (const plugin of host.plugins)
			if (plugin.toolkit) {
				const built = plugin.toolkit(host.clients());
				const section = instructionsSection(`plugin-${plugin.id}`, built.instructions);
				const extension = defineExtension({
					name: `fabrial.plugin.${plugin.id}`,
					tools: tools(`plugin ${plugin.id}`, built.tools),
					sections: section ? [section] : [],
				});
				pluginToolkits.set(plugin.id, extension);
				registry.install(extension);
			}
		for (const agent of definedAgents.get(scope) ?? []) install(agent);
	}
	return { registry, agents, configs, noReply, internal, installAll };
}
