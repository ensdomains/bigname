<h1 align="center">
  <img src="docs/assets/bigname-lockup-capheight.svg" alt="bigname" width="100%">
</h1>

A replayable, auditable indexing and read API for ENS, ENSv2, and Basenames.

bigname turns onchain state from Ethereum and Base into a versioned REST API. Its `v1` routes cover the supported portions of exact-name profiles, name and address collections, resolver records and overviews, primary names, history, roles, and verified record reads; see the [consumer capability matrix](docs/consumer-capabilities.md) for the exact boundaries. Partial and unsupported coverage is reported explicitly. Raw facts are immutable; [projections](docs/glossary.md) are rebuildable; supported on-demand verified reads are persisted with durable execution traces.

## What's here

- `apps/api` — the read API (`/v1/...`, `/healthz`, `/docs`)
- `apps/indexer` — chain intake, manifest sync, backfill, head-following
- `apps/worker` — projections, replay, verified execution, inspection commands
- `crates/` — domain types, storage, manifests, adapters (ENSv1, ENSv2, Basenames), execution
- `manifests/` — checked-in profile roots such as `mainnet` and `sepolia`, split by chain combo
- `migrations/` — Postgres schema
- `docs/` — how it works
- `tests/conformance/` — Rust Cargo conformance project (`bigname-supported-read-conformance`), run by CI with `cargo test`

## Local development

```sh
cp .env.example .env                       # optional, for custom ports/creds
docker compose up -d                       # PostgreSQL
./scripts/migrate                          # apply migrations
./scripts/dev-up                           # boot api + indexer + worker
```

The API binds to `127.0.0.1:3000` by default. Hit `http://127.0.0.1:3000/docs` for OpenAPI, `/healthz` for readiness.

Useful one-shots:

- `cargo api -- serve`
- `cargo indexer -- run`
- `cargo worker -- run`
- `cargo worker -- migrate`

To enable live ingestion, live verified ENS resolution, and the ENS/60 primary-name on-demand reverse/forward RPC fallback, set `BIGNAME_INDEXER_CHAIN_RPC_URLS` and `BIGNAME_API_CHAIN_RPC_URLS`. See [`docs/development.md`](docs/development.md).

## Container

Published as `ghcr.io/tateb/bigname`. The image entrypoint takes a service name (`api`, `indexer`, `worker`, or `migrate`).

For server deployment:

```sh
cp .env.server.example .env.server         # set passwords + image tag
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file runs `migrate` once, then leaves `api`, `indexer`, and `worker` as long-running services. One-shot invocations (`migrate`, `bigname-api print-openapi`, `bigname-worker inspect ...`) can be run with `docker run --rm ghcr.io/tateb/bigname:latest <command>`.

See [`docs/deployment.md`](docs/deployment.md) and [`docs/production.md`](docs/production.md) for the public-edge stack.

## Reading the docs

Start with [`docs/architecture.md`](docs/architecture.md) for the model — with [`docs/glossary.md`](docs/glossary.md) beside it for any project-specific term — then dive into the area you care about:

- [`docs/api-v1.md`](docs/api-v1.md) — the public read contract; per-route reference in [`docs/api-v1-routes.md`](docs/api-v1-routes.md)
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

- adapters write identity rows and normalized events, not projection rows
- the API reads projections and execution output, not raw facts
- raw facts are immutable; projections are rebuildable; verified answers are durable
- update the relevant doc before changing public semantics, shared IDs, manifest schema, or coverage meaning
