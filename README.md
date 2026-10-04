# Fabrial

> A TypeScript framework for durable workflows, AI agents, and human approvals.

Fabrial connects [PG Conductor](https://github.com/psteinroe/postgres-conductor) (durable workflows), [Pi Durable](https://earendil.com/posts/pi-durable/) (durable agents), and [Chat SDK](https://chat-sdk.dev) (Slack, GitHub, Linear) so that events start workflows, workflows call agents, agents call workflows, and people approve consequential actions in chat.

Status: early development. See [PLAN.md](./PLAN.md) for the design and [PGCONDUCTOR.md](./PGCONDUCTOR.md) for the Conductor changes it relies on.

## API

Create an explicit typed catalog with `createFabrial({ plugins, identity })` in a module without workflows. Define workflows with `f.defineWorkflow` and bind Pi helpers with `withPi(f)`. Wire runtime integrations with `f.app({ runtime, chat, agents, workflows })`; plugins carry their values at definition, and tests spy on `app.host.clients()` for fake clients. `identity` contains users only; define groups with `f.defineGroup` in a separate module and reference them directly in access/approval options. Factories are side-effect free; credentials are validated and connections started by `app.start()`. No global module augmentation is needed.

See [core](./packages/fabrial/README.md), [Pi](./packages/pi/README.md), and the [Acme example](./examples/acme/README.md).

## Development

Requires Node ≥ 22.19, pnpm, [just](https://github.com/casey/just), and Docker (integration tests start Postgres with testcontainers).

```sh
git submodule update --init   # vendored pgconductor-js, until it ships a Node build
pnpm install
just test
just ready                    # lint, typecheck, format
```
