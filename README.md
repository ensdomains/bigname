# bigname

A replayable, auditable indexing and read API for ENS (v1 and v2) and Basenames.

bigname turns onchain state from Ethereum and Base into a versioned `v1` REST contract that answers point-in-time, provenance-tagged questions about names, addresses, resolvers, primary names, and verified resolution.

## Layout

| Path | What it is |
| --- | --- |
| `apps/api` | the read API (`/v1/...`, `/healthz`, `/docs`) |
| `apps/indexer` | chain intake, manifest sync, backfill, head following |
| `apps/worker` | projections, replay, verified execution, inspection commands |
| `crates/` | domain types, storage, manifests, adapters (ENSv1, ENSv2, Basenames), execution |
| `manifests/` | checked-in mainnet source manifests |
| `manifests-sepolia-dev/` | alternate ENSv2 dev profile (selected at runtime) |
| `migrations/` | Postgres schema |
| `docs/` | how it works |
| `tests/conformance/` | TypeScript conformance harness |

## Local development

```sh
cp .env.example .env                  # optional, for custom ports/creds
docker compose up -d                  # Postgres + MinIO
./scripts/migrate                     # apply migrations
./scripts/dev-up                      # boot api + indexer + worker
```

The API binds to `127.0.0.1:3000` by default. Hit `http://127.0.0.1:3000/docs` for OpenAPI, `/healthz` for readiness.

Useful one-shots:

```sh
cargo api -- serve
cargo indexer -- run
cargo worker -- run
cargo worker -- migrate
```

To enable live ingestion and live verified ENS resolution, set `BIGNAME_INDEXER_CHAIN_RPC_URLS` and `BIGNAME_API_CHAIN_RPC_URLS`. See [`docs/development.md`](docs/development.md).

## Container

Published as `ghcr.io/tateb/bigname`. The image entrypoint takes a service name (`api`, `indexer`, `worker`, `migrate`).

For server deployment:

```sh
cp .env.server.example .env.server
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file runs `migrate` once, then leaves `api`, `indexer`, and `worker` as long-running services. One-shots run with `docker run --rm ghcr.io/tateb/bigname:latest <command>`.

See [`docs/deployment.md`](docs/deployment.md) and [`docs/production.md`](docs/production.md) for the public-edge stack.

## Reading the docs

Start with [`docs/architecture.md`](docs/architecture.md) for the model, then dive in:

- [`docs/api-v1.md`](docs/api-v1.md) — the public read contract (per-route detail in [`docs/api-v1-routes.md`](docs/api-v1-routes.md))
- [`docs/consumer-capabilities.md`](docs/consumer-capabilities.md) — what each capability covers
- [`docs/storage.md`](docs/storage.md) — schema and write ownership
- [`docs/manifests.md`](docs/manifests.md) — source manifests and discovery
- [`docs/chain-intake.md`](docs/chain-intake.md) — block intake, lineage, reorgs, backfill
- [`docs/projections.md`](docs/projections.md) — current-state read models
- [`docs/execution.md`](docs/execution.md) — verified resolution and primary names
- [`docs/development.md`](docs/development.md), [`docs/deployment.md`](docs/deployment.md), [`docs/production.md`](docs/production.md) — running it
- [`docs/upstream.md`](docs/upstream.md) — pinned upstream refs and intentional divergences

Internal sequencing notes (phasing, parallel workstreams) live under [`docs/internal/`](docs/internal/) and aren't required reading to use or deploy bigname.

## Guardrails

- Adapters write identity rows and normalized events, not projection rows.
- The API reads projections and execution output, not raw facts.
- Raw facts are immutable; projections are rebuildable.
- Update the relevant doc before changing public semantics, shared IDs, manifest schema, or coverage meaning.
