# Development

Local development uses Docker Compose for PostgreSQL and S3-compatible object storage.

## First-time setup

```sh
cp .env.example .env
docker compose up -d
./scripts/migrate
./scripts/dev-up
```

`docker compose up -d` starts:

| Service | Port | Notes |
| --- | --- | --- |
| PostgreSQL | `127.0.0.1:5432` | DB `bigname`, creds `bigname/bigname` |
| MinIO S3 API | `127.0.0.1:9000` | |
| MinIO console | `127.0.0.1:9001` | |
| Bucket bootstrap | one-shot | creates `bigname-dev` |

`./scripts/migrate` applies the checked-in migrations.

`./scripts/dev-up` sources `.env`, applies migrations, and runs the API, indexer, and worker as foreground processes.

The API binds to `127.0.0.1:3000` by default. `http://127.0.0.1:3000/docs` shows OpenAPI; `/healthz` is readiness.

Stop the local services with `docker compose down`. Add `-v` to also remove the data volumes.

## Useful one-shots

```sh
cargo api -- serve
cargo indexer -- run
cargo worker -- run
cargo worker -- migrate
cargo worker -- replay all-current-projections --json
cargo worker -- inspect watch-plan --json
cargo run -p bigname-api -- print-openapi
```

## Profile selection

`BIGNAME_INDEXER_MANIFESTS_ROOT` selects one runtime profile.

| Value | Profile |
| --- | --- |
| `manifests` (default) | shipped mainnet (Ethereum + Base) |
| `manifests-sepolia-dev` | ENSv2 Sepolia dev |

Don't load `manifests-sepolia-dev` beside `manifests` in the same local database — they don't mix.

## Live indexing

By default the indexer starts but stays idle on provider-backed work because no RPC is configured. To actually ingest, set:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

Comma-delimited `<chain>=<url>` for active watched chains in the selected profile. Bootstrap accepts `http://` only — use a local node or local HTTP proxy for hosted RPC providers that expose only HTTPS.

`BIGNAME_INDEXER_POLL_INTERVAL_SECS` controls the local indexer poll interval (default `5`).

Manifest sync, watch-plan rebuild, and checkpoint setup happen even without a configured provider. Provider-backed head fetch and live ingestion stay idle until you set the URL.

## Live API verified resolution

`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}` in `mode=verified|both` may execute supported ENS verified-resolution selectors on demand. Configure the API's own provider:

```sh
BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545
```

Separate from `BIGNAME_INDEXER_CHAIN_RPC_URLS`. Without this, supported live ENS verified selectors fail closed with `409 stale` and a configuration message — not a fall-back to declared cache.

The execution target is the selected exact-name snapshot. With no `at` or `chain_positions`, that's `consistency=head` at the latest stored Ethereum checkpoint, not provider latest.

## Reth DB source (optional)

For deployments with a same-host Reth database:

```sh
BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=ethereum-mainnet=/var/lib/reth
```

One source per chain. The Reth source is intake substrate, not a protocol adapter — adapters still consume bigname raw facts. Native Reth support is gated behind a Cargo feature:

```sh
cargo check -p bigname-indexer --features reth-db
```

This requires Clang/libclang for Reth's RocksDB/MDBX bindings. Default workspace checks skip it.

## Readiness endpoint

`GET /healthz` is private and not part of `/v1`. It separates process readiness from database readiness.

| State | HTTP | Top-level `status` | `database.status` | `database.error` |
| --- | --- | --- | --- | --- |
| Healthy | `200 OK` | `ready` | `reachable` | `null` |
| DB unreachable | `503` | `degraded` | `unreachable` | `database readiness query failed` |

Database reachability is checked with `SELECT 1` through the configured pool. A degraded response means the API process handled the request but the pool can't satisfy the readiness query.

## Migrations

Schema changes land through checked-in migrations under `migrations/`. See [`storage.md`](storage.md) § Migrations for the rules.

During bootstrap (no active deployments yet), migration findings that only affect historical data moving between pre-deployment schemas should be tracked as bootstrap cleanup unless a shared/staging database is explicitly declared non-rebuildable. Before the first stateful deployment, collapse the SQL history into a small baseline.

## Decision history

bigname doesn't keep a long-form ADR log. Past architectural decisions are recorded in commit history, the `docs/upstream.md` divergence list, and the relevant doc itself. When you make a decision that warrants persistent record:

- semantic decisions → update the relevant `docs/*.md` directly
- upstream divergences → add an entry to `docs/upstream.md` § Known divergences
- everything else → commit message + PR description

See `AGENTS.md` § Boundaries for the active development rules.
