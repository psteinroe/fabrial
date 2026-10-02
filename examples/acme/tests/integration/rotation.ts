import { weeklyRotation } from "fabrial";
import { alice, bob, engineeringTriage } from "../../src/identity.ts";
export { bob, engineeringTriage };
export function weeklyRotationForTest() {
	return weeklyRotation([bob, alice], {
		start: "2026-01-05",
		now: new Date("2026-01-05T00:00:00Z"),
	});
}
