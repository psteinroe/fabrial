import type { DurableRuntime } from "fabrial";

/**
 * Temporary shim: Pi's start() inherits triggerEvent/ownerWorkflow. Core then
 * mistakes a tool child for a trigger observer and strips its replyTo.
 * Clear only the trigger ownership markers, retaining actor, response and identity.
 * Remove when @fabrial/pi supplies these overrides itself.
 */
export function preserveToolResponse(runtime: DurableRuntime): DurableRuntime {
	const register = runtime.register.bind(runtime);
	runtime.register = (catalog) =>
		register({
			...catalog,
			workflows: catalog.workflows.map((workflow) => ({
				...workflow,
				handler: (input, execution) =>
					workflow.handler(input, {
						...execution,
						start: (id, name, payload, options) =>
							execution.start(
								id,
								name,
								payload,
								name === "run-sql"
									? {
											...options,
											metadata: { ...options?.metadata, triggerEvent: null, ownerWorkflow: null },
										}
									: options,
							),
					}),
			})),
		});
	return runtime;
}
