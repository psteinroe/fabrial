_default:
    just --list -u

alias f := format
alias l := lint
alias lf := lint-fix
alias r := ready
alias t := test

format:
    pnpm format

lint:
    pnpm lint
    pnpm typecheck

lint-fix:
    pnpm lint --fix

test:
    pnpm test

test-unit:
    pnpm test:unit

test-integration:
    pnpm test:integration

ready:
    just lint
    just format
