# @fabrial/pi

```ts
import { pi, defineAgent, defineTool, defineState } from "@fabrial/pi";

const agents = pi({ models, sql }); // pass as fabrial({ agents, ... })
const investigator = defineAgent({
	name: "investigator",
	model: { provider: "anthropic", modelId: "your-model" },
	instructions: "Investigate and return an answer, without publishing it.",
});
```

Node >=22.19, ESM. `sql` is a postgres.js connection; the caller owns its lifecycle.
`start()` migrates `fabrial_pi` under a transactional advisory migration lock,
builds the registry, and rejects missing live task kinds before admitting work.

## Storage and leases

A Session is a Postgres commit log plus Pi's native `MemoryStorage` materializer.
Pi validates transactions, documents, forks, copies, ancestry and rewind reads;
Postgres atomically persists the detached, validated write batch before adoption.
The log stores JSON **text**, not jsonb: Pi permits distinct lone-surrogate string
identities which Postgres jsonb cannot represent. Reopening replays the log.
There is currently no log compaction or indexed database read path; memory and
startup cost grow with Session history. This is durable storage, not a cache.

Each acquisition atomically increments a **per-Session** epoch and assigns an
owner UUID. Leases last 30 seconds, renewed every 10 seconds using the database
clock. Acquisition never steals an unexpired lease. Every commit locks/updates
the Session row with matching owner, epoch and unexpired lease in the same SQL
transaction as the write batch. Expired/stale owners cannot write or release a
successor's lease. Renewal failures abort the bridge's Chord Context; uncertain
commit outcomes poison the owner and require reopening.

Conductor's claim UUID is **not** the Pi epoch. A counter monotonic only within
one execution is insufficient: different executions share the Session. The Pi
lease is independently authoritative and its epoch increases across all owners.
Conductor's strict Session mutex is still useful for scheduling efficiency and
avoiding paid/externally visible work during stale-worker overlap; fencing is
not an exactly-once guarantee for external effects.

Keys are `${replyTo.provider}:${replyTo.threadId}` or `interactionId` without
chat. Here `provider` is the installation-specific plugin id; Chat SDK's full
thread id is retained verbatim. Core's `ThreadRef` does not separately expose an
installation id. For a channel surface, first call `ctx.thread.post` in the workflow so core rebinds metadata from its durable receipt before invoking an agent or using Pi state. Unposted channel locators are rejected, never promoted to Session keys.
Conversation continuation is by agent name inside that Session.

## Bridge

`AgentPort.run` invokes the internal `fabrial.pi.run` RuntimeWorkflow with the
Session mutex. Submission ids derive from caller execution + explicit operation
id. Invocation identity is a request-keyed Pi document committed immediately
before submitting. Tools/sections resolve the active run's submission identity,
not a mutable conversation-wide current user. Live clients are rebuilt by the
host. Forks have no Fabrial binding and cannot use managed outbound operations.

`WorkflowTool` starts children using Pi task id + operation id dedupe keys.
When only those children are running, the harness closes, releasing the lease,
and the bridge uses `waitForAny` **execution branches**. These are the port's
race-safe native settlement primitive (rather than filtering the public
`fabrial.execution.settled` event). Results go into Pi documents before reopening;
interrupted safe tools reconnect to the same child. Permissions are checked
before starting, and unauthorized workflow tools are removed from model discovery.
Zod's `toJSONSchema()` is used when available; other Standard Schemas receive a
permissive object tool schema and are still validated before workflow invocation.

Cancellation during a run aborts the conversation. Suspended cancellation uses
`RuntimeWorkflow.onSettled`, persisted run/child references, and `runtime.cancel`;
`ctx.agent(id, agent, { input, detached: true })` keeps the agent-run child alive when its caller is cancelled; `asTool({ detached: true })` independently keeps workflow-tool children alive when the agent is cancelled. Progress is best-effort,
throttled and limited to thinking/tool descriptions, never reasoning or raw
stream output. Final answers are returned, not posted automatically. Buffered
replies submit with stable ids and `whenBusy: "steer"`.

## Structured output and configuration

For `output`, Pi is instructed to return a single JSON value without fences.
The caller parses and validates it with the original Standard Schema in a durable
step. Neither schema nor a closure crosses the child boundary. Invalid JSON/schema
fails explicitly; there is no silent coercion or model repair loop.

Call-time `configure` is rejected. Move it to `defineAgent({ configure })`, a
process-local callback run on each opening after native `configure()` fields.
It must be idempotent and must not produce unmanaged external effects.

## Native adapters and state

`defineTool` and `section` return native Pi objects. Tool handlers receive the
native API plus actor, requestedBy, clients, thread, state, invoke, start,
evaluate and interaction. Chord Context is bound automatically to native methods;
explicit native Context arguments remain supported. Sections receive native
PromptInput plus these fields; managed writes/invocations/evaluation are prohibited
in sections. Reserved-name collision assertions are compiled and tested.

Managed thread posts, updates and history use Conductor steps keyed by Pi task
and operation id; repeated operation ids are suffixed. Model evaluations use Pi
memos. `ctx.start` is independent; `ctx.invoke` is reconnectable workflow work.
Arbitrary native clients are not automatically durable: use managed operations
or workflow tools for side effects.

State uses native Pi documents: interaction family keyed by interaction id,
thread family, and rewindable/forkable agent conversation documents. Workflow
reads/updates are memoized steps acquiring a short Session lease. Agent-scoped
workflow access is rejected because StatePort carries no conversation selector.
Tool changes are staged, schema validated and merged on the serialized commit
line with a **native task result receipt**. Pi appends its transcript result in
its subsequent settlement commit. On safe replay the receipt returns the saved
result without reapplying state. This receipt workaround is necessary because
Pi 1.0 has no tool-result transaction callback; it is not an atomic transcript
append. Updates must be pure: they may run again against newer committed state.
Adapters default to `replay: "safe"`; explicitly unsafe tools retain Pi's unsafe
interruption semantics. State renders only on agents listing it.

## Registry and orphan recovery

Only the internal extension is selected by host defaults. Agents explicitly
select their extensions; plugin hooks are in the internal extension globally.
Installation includes plugin extensions and extensions/tools/state sections of
agents defined before start. **Current discovery is module-level defineAgent
registration**, not exact workflow reachability: JavaScript closures and core's
AnyWorkflow contract do not expose captured agents. This can over-install unused
extensions and is unsuitable for isolated applications with conflicting agent
names in one process. Exact reachability needs `WorkflowDefinition.agents` or a
host-provided agent registry. Agents must be defined before start.

`checkOrphans(sql, installedKinds)` checks latest task records across Sessions
without taking their leases. Missing live kinds fail startup with recovery advice.
`abortOrphans(sql, kinds)` acquires Sessions and uses native Pi abort/cascade
semantics; it throws if another owner holds a Session. Run it only deliberately,
with workers quiesced. There is no CLI yet.

## Evaluation

Uses `models.classify()`, mapping boolean to bool, choices directly, and ordered
score rubrics to Pi score criteria. Pi score is interpreted as a zero-based rubric
index; out-of-range/nonintegral scores are errors. Choice distributions/confidence,
bool probability and usage are retained. Pi does not expose score distributions,
so the public score `probabilities` is `{}` rather than fabricated confidence.
Provider errors/aborts become `stopReason: "error"`.

## Remaining contract needs

- Explicit reachable agents on workflows or FabrialHost for precise installation.
- Installation id in ThreadRef if literal `slack:<installation>:<thread>` keys are
  required independently of installation-specific plugin ids.
- Conversation/agent selector for workflow agent-scoped StatePort operations.
- JSON Schema conversion capability on workflow input schemas for all providers.
- Atomic compare-and-set/update on ChatPort routing state. Read-modify-write
  steering/progress cannot prevent concurrent ingress writes being lost.
- Pi tool settlement callback for truly atomic state + native transcript result.

A long-lived execution per Session would not remove takeover/fencing requirements,
and would complicate closing the harness during approval waits. Concurrent agents
are possible but usually sequential in v1; reopen-after-approval is a normal flow.
Unnecessary token/tool/post work is not an acceptable general concurrency policy;
retain strict Conductor claiming plus the independent Pi lease.
