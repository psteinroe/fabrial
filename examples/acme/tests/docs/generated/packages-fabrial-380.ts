// Generated from packages/fabrial/README.md:381 by ../snippets.ts. Do not edit.
/* oxlint-disable */
import { f, support } from "../prelude.ts";
import { createIntegrationApp, reply, scriptedModels } from "fabrial/testing";

const models = scriptedModels();
models
	.model("anthropic/claude-sonnet-5")
	.respond([
		reply.toolCall("lookup_order", { orderId: "o1" }),
		({ messages }) => reply.text(`Seen ${messages.length} messages`),
	]);
models.classifier("typesafe/jev-latest", () => ({ bug: { probability: 0.01 } }));
const app = createIntegrationApp(f, { workflows: [], agents: [support], models });
await app.start();
export {};
