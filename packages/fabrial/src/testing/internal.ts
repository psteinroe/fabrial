// `fabrial/internal/testing`: test apps below the public API, for Fabrial's own tests and its plugins' tests. Not
// public: the dev-only entry, omitted from the published exports.
export {
	buildTestApp as createTestApp,
	internal,
	sessionSignals,
	testApp,
	testingApp,
	type SessionSignal,
} from "./app.ts";
export { FakeChat } from "./chat.ts";
export { scriptedPi } from "./models.ts";
/** Conductor's test clock; pass it as the app's `conductor: { sql, clock }` over a single-connection pool. */
export { createTestClock, type TestClock } from "pgconductor-js/testing";
