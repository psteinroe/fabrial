# @fabrial/chat

`chat({ state, userName?, waitUntil? })` connects one Chat SDK bot to all chat-capable plugins. Routes are `POST /<pluginId>/events`. Supply your deployment's `waitUntil` hook for background ingress; without it the route awaits ingress. Credentials stay in plugin options.

The port supports reconstructed threads, lazy channel roots, DMs, ephemeral messages, text/Markdown/cards, edited messages, best-effort throttled progress, attributed bounded history, and routing state under `thread.state.fabrial`. SDK state writes refresh a 30-day TTL. Card action ids/values are forwarded unchanged to core (including approval cancellation).

## Provider pattern (GitHub / Linear)

```ts
import { chatCapability, chatEvents, chatTrigger } from "@fabrial/chat";

const plugin = {
	id: "github",
	events: { ...chatEvents() /* provider-specific defineEvent entries */ },
	chat: chatCapability({
		adapter: () => createGitHubAdapter(credentials),
		installationId: configuredInstallation,
		history: { mode: "thread", limit: 50 },
		status: "message", // for platforms without meaningful typing/status
		isNewThread: (thread, message) => isRootComment(thread, message),
	}),
};
const mentioned = (filters = {}) => chatTrigger("github", "mentioned", filters);
```

Adapters must use their plugin id as the thread/channel id prefix. Installation ids are configured, trusted identity namespaces, not display names inferred from messages. Standard payloads include `channelId` (without adapter prefix), `threadId` (full SDK id), `isDM`, `isMention`, and `isNewThread`. Nonstandard root codecs should provide `isNewThread`. Additional non-chat routes/events/clients remain ordinary Fabrial plugin capabilities.

Each inbound message carries all matching categories: channel messages emit `message`, plus `mentioned` when applicable; DMs emit `dm`, plus `mentioned` when applicable (not `message`). Thus channel `message`/`newThread` observers also see mentions. Core selects one owner across the categories and dedupes each observing workflow across them. Local SDK concurrent slots serialize arrivals without queue/debounce coalescing. Webhook ingress disables SDK's pre-dispatch dedupe so failed durable ingress can be retried; core receives a stable platform message/action dedupe id.

The connection's `start`/`stop` map to SDK initialize/shutdown. Channel surfaces remain provisional until the first post; provisional refs cannot be reconstructed or store routing state. Core keeps the channel surface and rebinds from the memoized `MessageRef` on every replay. Routing state round-trips core's consumed reply IDs, requester, participants, and cancellation IDs unchanged.

Native Slack stop events forward the authenticated stopper, thread, and stable stop receipt to `host.receiveCancellation`; core owns authorization and cancellation dedupe. Application signal aborts can supply `{ actor: ExternalIdentity, dedupeId: string }` as the trusted abort reason, forwarded after ingress binds the interaction. **Bare SDK `thread.signal` aborts have no stopper identity and cannot safely authorize cancellation.** In particular Slack aborts the turn before delivering its native event, so attributing the abort to the original message author would bypass the stop event's authorization. Other platforms need an actor-bearing SDK stop event or abort reason.

## Remaining limitations

- SDK adapters with a fixed platform name cannot represent a second installation under a different plugin id without an id-rewriting adapter. Such aliases fail fast rather than silently crossing installation identity/state.
- Local ingress serialization is not a distributed routing-state transaction. Multiple webhook replicas need core/state-adapter atomic routing coordination.
- SDK native stop events do not expose a unique webhook event ID; dedupe uses thread, stopper, and stopped streaming message IDs, scoped by core to the current interaction.

Slack uses SDK assistant status/typing. The SDK Slack adapter swallows native status API errors, so absence of assistant scopes cannot be detected to automatically switch to edited-message fallback. Providers can opt into `status: "message"`; statuses are best effort. There is no attachment field in core's normalized message contract yet.
