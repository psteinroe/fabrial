import { weeklyRotation } from "fabrial";
import { alice, bob } from "../../src/users.ts";
import { engineeringTriage } from "../../src/groups.ts";
export { bob, engineeringTriage };
export function weeklyRotationForTest() {
	return weeklyRotation([bob, alice], {
		start: "2026-01-05",
		now: new Date("2026-01-05T00:00:00Z"),
	});
}
