<h1 align="center">
  <img src="docs/assets/bigname-lockup-capheight.svg" alt="bigname" width="100%">
</h1>

A replayable, auditable indexing and read API for ENS, ENSv2, and Basenames.

bigname turns onchain state from Ethereum and Base into a versioned REST API. Its `v2`
routes cover the supported portions of [exact-name profiles](docs/glossary.md), name and
address collections, resolver records and overviews, primary names, history, permissions,
and verified record reads; see the [consumer capability matrix](docs/consumer-capabilities.md)
for the exact boundaries. Partial and unsupported results are reported explicitly.
[Raw facts](docs/glossary.md) are immutable; [projections](docs/glossary.md) are
rebuildable; v2 verified reads use the schema-v2 lookup engine without writing reusable
outcomes or durable [execution traces](docs/glossary.md).

## What's here

- `apps/api` — the read API (`/v1/...`, `/healthz`)
- `apps/phase-runner` — the ingest, interpret, project, verify, and live phase
  supervisor
- `crates/` — domain types, storage, manifests, schema-v2 adapters, ingest,
  interpret, lookup, and projection behavior
- `manifests/` — checked-in profile roots such as `mainnet` and `sepolia`, split by chain combo
- `crates/storage/schema/` — the fresh database baseline and schema regression fixtures
- `migrations/` — append-only SQLx history and upgrades for existing databases
- `scripts/` — development and CI checks, including `scripts/check-schema`
- `tests/e2e/` — contract-backed scenarios and the retained restore exercise
- `ops/` — operational SQL and monitoring configuration
- `docs/` — current contracts and runbooks; historical plans live in
  [`docs/internal/archive/`](docs/internal/archive/README.md)
- `site/` — the landing page and API reference, a static site hosted apart
  from the API, with a mainnet/Sepolia network switcher

## Local development

For a fresh, disposable local database:

```sh
cp .env.example .env
# Edit local settings before loading them.
set -a
. ./.env
set +a
docker compose up -d --wait
cargo phase init-schema
./scripts/dev-up
```

The sample configuration starts only the API, on `127.0.0.1:3000`. Use `/v1`
routes for REST and `/healthz` for readiness. To run indexing too, complete the
[phase-runner configuration](docs/development.md#bootstrap), including its
separate SELECT-only verification login, before starting `dev-up`.

`init-schema` refuses a nonempty schema. For a retained database, follow the
[upgrade procedure](docs/runbooks/production-docker.md) instead of initializing
it again.

Useful one-shots, with the environment loaded:

- `cargo api serve`
- `cargo phase init-schema`
- `cargo phase redo --help`

Set `BIGNAME_API_CHAIN_RPC_URLS` for schema-v2 verified ENS resolution and
ENS/60 primary-name lookup. The phase runner owns ingest, interpret, project,
verify, and continuous live follow. See [`docs/development.md`](docs/development.md).

## Container

Published as `ghcr.io/ensdomains/bigname`. The image entrypoint takes a service
name (`api`, `phases`, or `phases-migrate`). The one-time `phases-migrate`
command installs schema-v2 into an empty `bigname_phase` namespace in that
same database.

For server deployment:

```sh
cp .env.server.example .env.server         # configure credentials, image and capacity inputs
# Set a positive disk floor, pre-create the dedicated probe directory, and
# choose a memory ceiling per service.
# Complete docs/runbooks/production-docker.md#capacity-preflight before starting.
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file leaves `api` and `phase-runner` as long-running services.
Apply reviewed versioned migrations at the deployment boundary before starting
the new image; the deleted worker migration command is no longer available.

See [`docs/deployment.md`](docs/deployment.md) and [`docs/production.md`](docs/production.md) for the public-edge stack.

## Reading the docs

Start with [`docs/architecture.md`](docs/architecture.md) for the model — with [`docs/glossary.md`](docs/glossary.md) beside it for any project-specific term — then dive into the area you care about:

- [`docs/api-v1.md`](docs/api-v1.md) — the read contract; per-route reference in [`docs/api-v1-routes.md`](docs/api-v1-routes.md)
- [`docs/storage.md`](docs/storage.md) — schema and write ownership
- [`docs/manifests.md`](docs/manifests.md) — source manifests and discovery
- [`docs/chain-intake.md`](docs/chain-intake.md) — block intake, lineage, reorgs, backfill
- [`docs/projections.md`](docs/projections.md) — current-state read models
- [`docs/execution.md`](docs/execution.md) — verified resolution and primary names
- [`docs/consumer-capabilities.md`](docs/consumer-capabilities.md) — what each capability covers
- [`docs/development.md`](docs/development.md), [`docs/deployment.md`](docs/deployment.md), [`docs/production.md`](docs/production.md), [`docs/runbooks/`](docs/runbooks/) — running it
- [`docs/upstream.md`](docs/upstream.md) — pinned upstream refs and intentional divergences
- [`docs/adrs/`](docs/adrs/) — architecture decisions

Internal planning notes (implementation sequencing, parallel workstreams) live under [`docs/internal/`](docs/internal/) and are not required reading to use or deploy bigname.

## Guardrails

- schema-v2 `interpret` writes identity rows, discovery edges, and normalized
  events; adapters provide interpretation behavior, not projection writes
- the API reads projections and request-scoped schema-v2 lookup output, not raw facts
- raw facts are immutable, projections are rebuildable, and provider lookup responses are not retained
- update the relevant doc before changing public semantics, shared IDs, manifest schema, or coverage meaning
