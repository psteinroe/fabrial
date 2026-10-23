// `fabrial/testing`: apps on real Conductor with fake chat or scripted models.
export {
	createIntegrationApp,
	createTestApp,
	type ModelRetryOptions,
	type TestApp,
	type TestAppConfig,
	type TestingApp,
	type ThreadFollowers,
} from "./testing/app.ts";
export { FakeChat, type FakePost } from "./testing/chat.ts";
export {
	reply,
	scriptedModels,
	type ScriptedAnswer,
	type ScriptedMessage,
	type ScriptedModel,
	type ScriptedModels,
	type ScriptedReply,
	type ScriptedStep,
} from "./testing/models.ts";
/** Conductor's test clock, as `TestApp.clock`. */
export type { TestClock } from "pgconductor-js/testing";
