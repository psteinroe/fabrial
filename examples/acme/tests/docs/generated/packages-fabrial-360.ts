// Generated from packages/fabrial/README.md:361 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f } from "../prelude.ts";
import { createTestApp } from "fabrial/testing";

const hello = f.defineWorkflow({ name: "hello", run: async () => "hi" });
const { app, runtime, settled, stop } = createTestApp(f, {
	database: process.env.DATABASE_URL!,
	workflows: [hello],
});
await app.start();
const id = await runtime.invoke("hello", null, {
	metadata: { requestId: "r1", origin: null, replyTo: null, requestedBy: null },
});
console.log(await settled(id)); // { status: "completed", output: "hi" }
await stop();
export {};
