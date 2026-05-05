# bigname

A point-in-time read API for ENS, ENSv2, and Basenames, backed by a replayable index.

bigname watches Ethereum and Base, builds projections from chain events, and serves them through a versioned `/v1` REST contract. Every answer carries the chain positions it was derived from, so the same query at the same snapshot replays to the same result.

## What's in the box

```
apps/
  api/         REST API. Reads projections and execution output. /v1, /healthz, /docs.
  indexer/     Chain intake. Manifest sync, backfill, head-following.
  worker/      Projections, replay, verified execution, inspection commands.
crates/        Domain types, storage, manifests, adapters, execution.
manifests/     Mainnet ENS + Basenames source manifests.
manifests-sepolia-dev/
               Alternate ENSv2 dev profile. Selected at runtime; never loaded together.
migrations/    Postgres schema.
tests/conformance/
               TypeScript conformance harness.
docs/          The rest of the story — see [Reading the docs](#reading-the-docs).
```

## Local development

```sh
cp .env.example .env          # optional, for custom ports/creds
docker compose up -d          # Postgres + MinIO
./scripts/migrate             # apply migrations
./scripts/dev-up              # boot api + indexer + worker
```

The API binds to `127.0.0.1:3000`. Useful endpoints:

| Path | What |
| --- | --- |
| `/v1/...` | The public read contract. |
| `/healthz` | Liveness + DB readiness. Operator-only, not part of `v1`. |
| `/docs` | OpenAPI viewer. |
| `/openapi.json` | Live OpenAPI document. |

To enable live ingestion and live verified ENS resolution, set `BIGNAME_INDEXER_CHAIN_RPC_URLS` and `BIGNAME_API_CHAIN_RPC_URLS`. See [`docs/development.md`](docs/development.md).

One-shots:

```sh
cargo api -- serve
cargo indexer -- run
cargo worker -- run
cargo worker -- migrate
```

## Container

Published as `ghcr.io/tateb/bigname`. The entrypoint takes a service name (`api`, `indexer`, `worker`, `migrate`).

For a server deployment:

```sh
cp .env.server.example .env.server         # set passwords + image tag
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file runs `migrate` once, then leaves `api`, `indexer`, and `worker` running. One-shots (`migrate`, `bigname-api print-openapi`, `bigname-worker inspect ...`) run via `docker run --rm ghcr.io/tateb/bigname:latest <command>`.

[`docs/deployment.md`](docs/deployment.md) covers the public-edge stack.

## Reading the docs

Start with [`architecture.md`](docs/architecture.md) for the model. From there:

| If you want to know… | Read |
| --- | --- |
| The shape of API responses | [`api-v1.md`](docs/api-v1.md) |
| Per-route semantics | [`api-v1-routes.md`](docs/api-v1-routes.md) |
| The Postgres schema and write ownership | [`storage.md`](docs/storage.md) |
| What contracts are watched and why | [`manifests.md`](docs/manifests.md) |
| How blocks turn into facts | [`chain-intake.md`](docs/chain-intake.md) |
| How facts turn into reads | [`projections.md`](docs/projections.md) |
| How verified resolution works | [`execution.md`](docs/execution.md) |
| What "supported" means per capability | [`consumer-capabilities.md`](docs/consumer-capabilities.md) |
| Running it locally / on a server | [`development.md`](docs/development.md), [`deployment.md`](docs/deployment.md) |
| Pinned upstream refs and divergences | [`upstream.md`](docs/upstream.md) |

Implementation sequencing notes live under [`docs/internal/`](docs/internal/) and aren't required reading.

## Ground rules

- Adapters write identity rows and normalized events; projections write read models; the API only reads.
- Public semantics, shared IDs, manifest schema, and capability meanings change in docs first.
- Raw facts don't move. Projections rebuild. Execution traces are kept.
