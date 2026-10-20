# Fabrial

> A TypeScript framework for durable workflows, AI agents, and human approvals.

Fabrial connects [PG Conductor](https://github.com/psteinroe/postgres-conductor) (durable workflows), [Pi Durable](https://earendil.com/posts/pi-durable/) (durable agents), and [Chat SDK](https://chat-sdk.dev) (Slack, GitHub, Linear) so that events start workflows, workflows call agents, agents call workflows, and people approve consequential actions in chat.

Status: early development. See [PLAN.md](./PLAN.md) for the design and [PGCONDUCTOR.md](./PGCONDUCTOR.md) for the Conductor changes it relies on.

## API

Install `fabrial` and the plugins you need. Create the instance with `createFabrial({ database, models, plugins, identity })` in a module without workflows: `database` is a Postgres connection string, and `models` holds credentials per model provider. Define workflows, agents, tools, toolkits, and state on it with `f.defineWorkflow`, `f.defineAgent`, `f.defineTool`, `f.defineToolkit`, and `f.defineState`; agents name models as `"provider/model"` strings and list tools and workflows in `tools`. Build the app with `f.app({ workflows })`. Plugins carry their values at definition, and factories are side-effect free: credentials are validated and connections opened by `app.start()`. `identity` contains users only; define groups with `f.defineGroup` in a separate module and reference them directly as approvers. Tests use `fabrial/testing`: `createTestApp` in memory, or `createIntegrationApp` with `scriptedModels()` against a real database.

See the [fabrial package](./packages/fabrial/README.md) and the [Acme example](./examples/acme/README.md).

## Development

Requires Node ≥ 22.19, pnpm, [just](https://github.com/casey/just), and Docker (integration tests start Postgres with testcontainers).

```sh
git submodule update --init   # pgconductor-js, pinned until a version is published
pnpm install
just test                     # builds pgconductor-js first (pnpm build:conductor)
just ready                    # lint, typecheck, format
```
