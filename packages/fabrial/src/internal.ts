import type { EventFilter, FilterAlternative } from "./events.ts";
import type { JsonObject } from "./json.ts";

export const THREAD_TTL = 30 * 24 * 60 * 60 * 1000;

export function duration(value: string | number): number {
	if (typeof value === "number") {
		if (!Number.isFinite(value) || value < 0) throw new Error("Invalid duration");
		return value;
	}
	const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/.exec(value);
	if (!match) throw new Error(`Invalid duration: ${value}`);
	return duration(
		Number(match[1]) *
			{ ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[match[2]!]!,
	);
}

export function boundedTimeout(value: string | number = "24h"): number {
	const ms = duration(value);
	if (ms >= THREAD_TTL) throw new Error("Timeout must be below 30 days");
	return ms;
}

function compare(value: number, operator: string, bound: number): boolean {
	switch (operator) {
		case "=":
			return value === bound;
		case ">":
			return value > bound;
		case ">=":
			return value >= bound;
		case "<":
			return value < bound;
		case "<=":
			return value <= bound;
		default:
			throw new Error(`Invalid numeric filter operator: ${operator}`);
	}
}

export function matchesAlternative(value: unknown, alternative: FilterAlternative): boolean {
	if (alternative === null || typeof alternative !== "object") return value === alternative;
	if ("exists" in alternative) return (value !== undefined) === alternative.exists;
	if ("prefix" in alternative)
		return typeof value === "string" && value.startsWith(alternative.prefix);
	if ("anything-but" in alternative)
		return (
			value !== undefined &&
			(value === null || ["string", "number", "boolean"].includes(typeof value)) &&
			value !== alternative["anything-but"]
		);
	const [op, bound, op2, bound2] = alternative.numeric;
	return (
		typeof value === "number" &&
		compare(value, op, bound) &&
		(op2 === undefined || compare(value, op2, bound2!))
	);
}

export function matchesFilter(payload: JsonObject, filter: EventFilter = {}): boolean {
	return Object.entries(filter).every(([field, alternatives]) =>
		alternatives?.some((a) =>
			matchesAlternative(Object.hasOwn(payload, field) ? payload[field] : undefined, a),
		),
	);
}

/** Conservative overlap check: disjoint literals/prefixes/ranges are proven; uncertain pairs overlap. */
export function filtersOverlap(a: EventFilter = {}, b: EventFilter = {}): boolean {
	return Object.keys(a).every((field) => {
		if (!Object.hasOwn(b, field)) return true;
		return a[field]!.some((x) => b[field]!.some((y) => alternativesOverlap(x, y)));
	});
}

function alternativesOverlap(a: FilterAlternative, b: FilterAlternative): boolean {
	const objectA = a !== null && typeof a === "object";
	const objectB = b !== null && typeof b === "object";
	if (!objectA) return matchesAlternative(a, b);
	if (!objectB) return matchesAlternative(b, a);
	if ("exists" in a) return a.exists || ("exists" in b && !b.exists);
	if ("exists" in b) return b.exists;
	if ("prefix" in a && "prefix" in b)
		return a.prefix.startsWith(b.prefix) || b.prefix.startsWith(a.prefix);
	if (("prefix" in a && "numeric" in b) || ("numeric" in a && "prefix" in b)) return false;
	if ("numeric" in a && "numeric" in b) {
		const bounds = [...a.numeric, ...b.numeric];
		const points = bounds.filter((v): v is number => typeof v === "number");
		points.push(-Infinity, Infinity);
		points.sort((x, y) => x - y);
		const candidates = [...points, ...points.slice(1).map((p, i) => (p + points[i]!) / 2)];
		return candidates.some((n) => matchesAlternative(n, a) && matchesAlternative(n, b));
	}
	return true;
}
