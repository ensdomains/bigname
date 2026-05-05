# Development

Local dev runs the same three processes as a server (`api`, `indexer`, `worker`) against Dockerised Postgres and MinIO. This page walks through bootstrap, configuring chain providers, and the `/healthz` shape; the underlying stack is described in [`architecture.md`](architecture.md).

## Bootstrap

```sh
cp .env.example .env          # ports and creds; optional
docker compose up -d          # Postgres + MinIO + bucket bootstrap
./scripts/migrate             # apply checked-in migrations
./scripts/dev-up              # api + indexer + worker
```

What `docker compose up -d` brings up:

| Service | Address | Notes |
| --- | --- | --- |
| Postgres | `127.0.0.1:5432` | db `bigname`, user/pass `bigname`/`bigname` |
| MinIO S3 API | `127.0.0.1:9000` | object storage |
| MinIO console | `127.0.0.1:9001` | web UI |
| bucket bootstrap | one-shot | creates `bigname-dev` |

`docker compose down` stops it; add `-v` to drop the data volumes too.

`./scripts/dev-up` sources `.env`, runs migrations, and boots `bigname-api`, `bigname-indexer run`, and `bigname-worker`. On startup the indexer loads the selected manifest root, syncs manifest state into Postgres, rebuilds the stored watch plan, creates persisted chain checkpoint rows for active watched chains, and then polls configured providers.

### Migration hygiene during bootstrap

bigname has no shared production database to preserve across intermediate schemas yet, so migration findings that only affect pre-deployment data should be filed as bootstrap cleanup. Before the first stateful deployment, collapse the SQL history into a small baseline migration set. When collapsing, drop transition-only steps or re-audit them for hard preflight checks before destructive drops (e.g. the `raw_blocks` → `chain_header_audit` transition).

## Selecting a manifest profile

One runtime, one manifest root. Set `BIGNAME_INDEXER_MANIFESTS_ROOT`:

| Value | Profile |
| --- | --- |
| `manifests` (default) | shipped mainnet ENS + Basenames |
| `manifests-sepolia-dev` | ENSv2 dev profile |

Don't load both against the same local database.

## Chain providers

Two separate provider settings — they're not interchangeable.

| Variable | Process | Purpose |
| --- | --- | --- |
| `BIGNAME_INDEXER_CHAIN_RPC_URLS` | indexer | head following, backfill, raw-fact intake |
| `BIGNAME_API_CHAIN_RPC_URLS` | api | live verified ENS execution on cache miss |
| `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` | indexer | optional same-host Reth datadir source |
| `BIGNAME_INDEXER_POLL_INTERVAL_SECS` | indexer | poll interval, default `5` |

Format is comma-delimited `<chain>=<url>`:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

If neither indexer setting is configured, `./scripts/dev-up` still boots and the indexer still syncs manifest/watch state — provider-backed head fetch and live ingestion just stay idle. Bootstrap RPC support accepts `http://` only; for HTTPS-only hosted providers, use a local HTTP proxy or local node.

### Why the API needs its own provider

`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}` in `mode=verified|both` may execute supported ENS verified-resolution selectors on demand when matching persisted execution output is absent. Live execution targets the selected exact-name snapshot — with no `at` or `chain_positions`, that's `consistency=head` at the latest stored Ethereum checkpoint, not provider latest.

Without `BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`, supported live ENS verified selectors fail closed with `409 stale` and a configuration message. There is no silent fallback to declared record cache.

### Reth database source (optional)

`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=<chain>=<reth-datadir>` (one source per chain) feeds the same raw-fact intake contract as JSON-RPC; Reth-local table references don't replace bigname raw-fact refs or Postgres replay facts. The Reth source compiles only with the `reth-db` Cargo feature:

```sh
cargo check -p bigname-indexer --features reth-db
```

That opt-in build needs Clang/libclang headers for Reth's RocksDB/MDBX bindings. Default workspace checks skip it.

## Private readiness endpoint

`GET /healthz` lives on the API bind address (default `http://127.0.0.1:3000/healthz`). It's an operator endpoint, not part of `/v1`, and shouldn't be treated as a consumer surface.

It splits process readiness from database readiness. DB reachability is checked via `SELECT 1` through the configured pool.

| Condition | HTTP | `status` | `process.status` | `database.status` | `database.reachable` | `database.check` | `database.error` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DB reachable | `200` | `ready` | `running` | `reachable` | `true` | `select_1` | `null` |
| DB pool fails | `503` | `degraded` | `running` | `unreachable` | `false` | `select_1` | `database readiness query failed` |

A `degraded` response means the API process handled the request but the configured pool couldn't satisfy the readiness query.
