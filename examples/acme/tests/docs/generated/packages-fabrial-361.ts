// Generated from packages/fabrial/README.md:362 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f } from "../prelude.ts";
import { createTestApp } from "fabrial/testing";

const hello = f.defineWorkflow({ name: "hello", run: async () => ({ greeting: "hi" }) });
const { app, invoke, settled, stop } = createTestApp(f, {
	database: process.env.DATABASE_URL!,
	workflows: [hello],
});
await app.start();
const id = await invoke(hello);
console.log(await settled(id)); // { status: "completed", output: { greeting: "hi" } }
await stop();
export {};
