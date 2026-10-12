import type { Context, JsonValue } from "@earendil-works/chord";
import type { ToolExecutionApi, Tx } from "@earendil-works/pi-durable";

type ImplicitContext<F> = F extends (...args: infer A) => infer R
	? A extends [...infer Head, Context]
		? (...args: Head) => R
		: F
	: F;
type ImplicitApi<D extends JsonValue> = {
	[K in keyof ToolExecutionApi<D>]: ImplicitContext<ToolExecutionApi<D>[K]>;
};

/** Native signatures remain available; these common operations also accept the bound Context implicitly. */
export type BoundToolApi<D extends JsonValue = JsonValue> = ToolExecutionApi<D> &
	ImplicitApi<D> & {
		commit<T>(change: (tx: Tx) => T | Promise<T>): Promise<T>;
		memo<T extends JsonValue>(name: string): Promise<T | undefined>;
		memo<T extends JsonValue>(name: string, candidate: T): Promise<T>;
		agent(): ReturnType<ToolExecutionApi["agent"]>;
		details(value: D): Promise<void>;
	};

export function bindApi<D extends JsonValue>(
	api: ToolExecutionApi<D>,
	context: Context,
): BoundToolApi<D> {
	const methods: Record<string, (...args: unknown[]) => unknown> = {};
	for (const name of [
		"agent",
		"details",
		"commit",
		"memo",
		"createTask",
		"getTask",
		"waitForTask",
		"conversation",
		"snapshot",
		"snapshotAsOf",
		"watchDoc",
	] as const) {
		methods[name] = (...args) => {
			const last = args.at(-1);
			const hasContext =
				last !== null && typeof last === "object" && "abortSignal" in last && "value" in last;
			return Reflect.apply(api[name], api, hasContext ? args : [...args, context]);
		};
	}
	return Object.assign({}, api, methods) as unknown as BoundToolApi<D>;
}
