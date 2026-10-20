# @fabrial/linear

```ts
import { linear } from "@fabrial/linear";
import { createFabrial } from "fabrial";

const plugin = linear({
	apiKey: process.env.LINEAR_API_KEY!, // or accessToken: a fixed OAuth token
	webhookSecret: process.env.LINEAR_WEBHOOK_SECRET!,
	organizationId: "your-organization-uuid",
	userName: "fabrial",
});
const f = createFabrial({ plugins: [plugin] });
// Define workflows with f.defineWorkflow; wire runtime/chat with f.app({ runtime, chat, workflows }).
```

Supports single-organization API keys and fixed OAuth access tokens. Verification requires an explicit `webhookSecret` or `webhookVerifier`. Organization identity is configured, never inferred from an untrusted inbound display name. Comment mode is intentional; agent sessions, OAuth installation management and client-credentials token lifecycle are outside this plugin's v1 surface.

Configure two Linear webhook subscriptions:

- `POST /linear/events`: Comment events handled by Chat SDK, producing standard `mentioned`/`message` events. A root comment is `linear:issueId:c:commentId`; replies use the parent's comment ID.
- `POST /linear/webhook`: Issue create/update events, producing `issueCreated`/`issueUpdated`. Native HMAC signatures and the signed `webhookTimestamp` are checked (one-minute freshness); custom verifiers may supply a replacement verified body. `linear-delivery` is required for durable dedupe.

Use with `chat({ state })`. `ctx.clients.linear` is the adapter's authenticated LinearClient. Lifecycle payloads contain `issueId`, `identifier`, `title`, `description`, `url`, `teamKey`, `stateType`, `threadId`, and `updatedFrom`. Missing expanded team/state objects are resolved through their IDs. Origin/actor metadata is organization-scoped; `ctx.thread` replies with top-level comments on `linear:issueId`. Ordinary comment workflows reply under their root comment. History is bounded to 50 messages; status uses edited messages.

Helpers are named exports and properties on `linear`:

- `mentioned({ issue?, thread?, observe? })`: global < issue < thread specificity; `issue` is the issue's UUID, Chat SDK's channel.
- `issueCreated({ team?, stateType?, observe? })`, `issueUpdated(...)`: specificity counts team/state constraints.
- `identity({ organizationId, userId })`.
- `triageResponsibility({ id, team })`: team key lookup, then the API's live `triageResponsibility.currentUser`; fails closed if absent. No cached authorization or local rotation policy.

## Contract workarounds

Comment channels are Chat SDK's: the issue UUID (`linear:issueId`), stable across restarts and replicas, and one per issue, so `session: "channel"` never merges issues. Fabrial's chat payload has no field for the issue's team, so comment triggers can't filter by team; lifecycle triggers can (`teamKey`). Team-scoped comment routing needs a core `ChatCapability` hook that adds provider attributes to message payloads, rather than rewriting the SDK's channel.

Lifecycle routes use core's `EmitOptions.replyTo` for the issue thread and `requestedBy` for the creator (`creator.id`/`creatorId`, falling back to the creation actor); updates retain the update actor. A lifecycle issue-level thread and later comment-root threads are distinct SDK conversations; replying beneath a posted top-level comment does not automatically bind it back to the original issue-level interaction. A future issue-to-comment response rebind contract can support that continuation. Multiple plugin aliases/request-scoped multi-tenant clients are not supported.

One verification (signature or custom verifier, freshness, organization) runs on both ingress paths, before dispatch, including comments, reactions, agent sessions and OAuth revocations. Custom webhook-verifier body rewrites are scoped using the verified replacement body, not the original request.
