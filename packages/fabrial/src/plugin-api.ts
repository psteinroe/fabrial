// `fabrial/plugin`: what plugin authors add to the root API (events, routes, chat adapters, hooks).
export {
	definePlugin,
	resource,
	type AnyPlugin,
	type ChatCapability,
	type IdentityCapability,
	type NotificationAttributes,
	type PluginClients,
	type PluginDefinition,
	type PluginEvent,
	type PluginToolkit,
	type PluginToolkitFactory,
	type ResourceKind,
	type ResourceNotification,
	type RouteContext,
	type RouteHandler,
	type ToolkitPlugin,
} from "./plugin.ts";
export { defineEvent, type EventDefinition, type InferEventPayload } from "./events.ts";
export { trigger } from "./trigger.ts";
export {
	chatCapability,
	chatEvents,
	chatIdentity,
	chatTrigger,
	type ChatPluginCapability,
} from "./chat/events.ts";
export { literalCard } from "./chat/port.ts";
export type { AgentObservers, ModelResponse, ToolCall } from "./agent.ts";
export type { ExecutionInfo, WorkflowMiddleware } from "./workflow.ts";
