# Deployment

The published image at `ghcr.io/tateb/bigname` contains all three runnable binaries:

```sh
docker run --rm ghcr.io/tateb/bigname:latest api
docker run --rm ghcr.io/tateb/bigname:latest indexer
docker run --rm ghcr.io/tateb/bigname:latest worker
docker run --rm ghcr.io/tateb/bigname:latest migrate
```

`api` is the default. Raw binary invocations work too:

```sh
docker run --rm ghcr.io/tateb/bigname:latest bigname-api print-openapi
docker run --rm ghcr.io/tateb/bigname:latest bigname-worker inspect watch-plan --json
```

For the public-edge stack, see [`production.md`](production.md).

## Server compose

```sh
cp .env.server.example .env.server          # set passwords + image tag
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file starts PostgreSQL, MinIO, a one-shot migration service, the API, the indexer, and the worker. The API listens on the host port from `BIGNAME_API_PORT` and answers `/healthz`. Set `BIGNAME_API_HOST=127.0.0.1` for production deployments behind Caddy (see [`production.md`](production.md)).

The indexer loads exactly one manifest root. Use `/app/manifests` for mainnet or `/app/manifests-sepolia-dev` for the ENSv2 Sepolia dev profile. Don't point one runtime at both.

## Provider configuration

### Indexer

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://…,base-mainnet=http://…
```

Comma-delimited `<chain>=<url>`, HTTP only. Without this, manifest sync, watch-plan rebuild, and checkpoint setup still happen, but provider-backed live ingestion stays idle.

Per-profile, per-chain availability:

- An Ethereum-only run may omit Base entirely.
- A profile that includes Base but has no Base RPC leaves Base provider-backed intake, automatic bootstrap, backfill catch-up, and live head following idle with `no_provider`. Startup for configured chains doesn't fail because Base is missing.
- A provider for a chain not in the selected manifest root is invalid.

### API

The API needs its own Ethereum provider for live ENS verified resolution:

```sh
BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=http://…
```

`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}` in `mode=verified|both` first use matching persisted output. When supported ENS Universal Resolver selectors are missing, the API executes them against the selected exact-name snapshot, persists the trace/outcome, and returns the result.

With no `at` or `chain_positions`, the target is `consistency=head` at the latest stored Ethereum checkpoint — not provider latest. Missing API provider config or a provider that can't serve the selected block fails closed with `409 stale` plus a configuration message — not declared cache fallback. The indexer RPC and Reth DB settings don't satisfy this.

### Reth DB override (optional)

Layer `docker-compose.reth-db.yml` on top of the server compose for a same-host Reth database:

```sh
BIGNAME_INDEXER_RETH_DATADIR_HOST=/var/lib/reth \
BIGNAME_INDEXER_RETH_DATADIR_CONTAINER=/reth-data \
BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=ethereum-mainnet=/reth-data \
BIGNAME_INDEXER_RETH_DB_USER=0:0 \
BIGNAME_INDEXER_RETH_DB_NOFILE_SOFT=1048576 \
BIGNAME_INDEXER_RETH_DB_NOFILE_HARD=1048576 \
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d indexer
```

Notes:

- The override clears `BIGNAME_INDEXER_CHAIN_RPC_URLS` for the indexer so each chain has only one provider source.
- Reth DB sources are operational intake; they don't replace bigname raw facts or normalized-event `raw_fact_ref` identities.
- The repository Dockerfile builds `bigname-indexer` with the `bigname-indexer/reth-db` Cargo feature so this override keeps the Reth provider path available. Custom images that omit that feature fail fast on `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` instead of silently falling back.
- The indexer opens Reth through its read-only provider API, but the container mount is writable because MDBX cooperative read-only opens still need writable lock files.
- The override defaults to `BIGNAME_INDEXER_RETH_DB_USER=0:0` because container-managed Reth datadirs are commonly `root:root`. A less-privileged UID/GID needs write access to MDBX lock files in the datadir.
- It uses host PID/IPC namespaces and bypasses `tini` so the indexer process owns PID 1; Reth's live MDBX read-only open can fail from the default `tini` child process.
- `nofile` is raised because Reth's read-only RocksDB provider can keep thousands of SST files open.

## Bootstrap and catch-up

Startup creates finite backfill jobs from each eligible target's manifest/discovery admitted start through the provider head observed at job creation. It doesn't cap work to a recent window. Completing bootstrap alone is operational intake readiness — not consumer-replacement or route-coverage evidence without the relevant projection, route, conformance, and rollout gates.

| Variable | Purpose |
| --- | --- |
| `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_WORKERS` | `0` for auto (capped at 4); positive value to pin the count. |
| `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_RANGE_BLOCKS` | child range size, default `50000` blocks. |
| `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS` | per-chunk batch, default `1024` blocks. |
| `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_PUSH` | caps dense log spans inside a chunk. |
| `BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK` | caps automatic normalized-event replay catch-up chunks. |
| `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC` | `auto` (default), `raw-only`, `inline`. |

`auto` mode keeps the manifest-declared/raw catch-up scope while catching up, runs live polling for new blocks, and runs automatic bounded raw-fact normalized-event replay from the indexer's `normalized_replay_*` cursor. `raw-only` defers live normalized sync; `inline` replays each chunk immediately for small ranges.

The worker pool is inside one normal `bigname-indexer run` process — no need for extra indexer containers.

For dense chunks, run the indexer with `RUST_LOG=info,sqlx::query=error` to avoid SQLx slow-query warnings printing huge generated INSERT statements.

Operational catch-up to finalized head runs as bounded idempotent backfill chunks. Each chunk checks current Postgres size, writable free disk, and any configured object-cache budget before starting. Capacity shortage pauses or fails the chunk explicitly instead of silently retaining less data.

## GHCR image

Published to `ghcr.io/tateb/bigname`.

The GitHub Actions workflow publishes `latest` on the default branch and a short commit SHA tag on every push to `main`. Tags pushed to the repo are also published.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  -t ghcr.io/tateb/bigname:latest \
  -t ghcr.io/tateb/bigname:$(git rev-parse --short HEAD) \
  --push .
```
