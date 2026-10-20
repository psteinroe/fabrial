import type { DurableRuntime, RuntimeEvent, RuntimeWorkflow } from "../../src/index.ts";

/** A runtime port for tests that never run a workflow: it records registration and keeps the time. */
export class StubRuntime implements DurableRuntime {
	workflows = new Map<string, RuntimeWorkflow>();
	events: RuntimeEvent[] = [];
	constructor(public time = Date.now()) {}
	now(): number {
		return this.time;
	}
	register(definition: { workflows: RuntimeWorkflow[]; events: RuntimeEvent[] }): void {
		for (const workflow of definition.workflows) this.workflows.set(workflow.name, workflow);
		this.events.push(...definition.events);
	}
	async start(): Promise<void> {}
	async stop(): Promise<void> {}
	emit(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	invoke(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	cancel(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	signalWithStart(): never {
		throw new Error("StubRuntime runs no workflows");
	}
	signal(): never {
		throw new Error("StubRuntime runs no workflows");
	}
}
