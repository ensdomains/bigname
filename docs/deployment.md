# Deployment

The production container image contains the three runnable bigname binaries:

- `bigname-api`
- `bigname-indexer`
- `bigname-worker`

The image entrypoint accepts one service selector:

```sh
docker run --rm ghcr.io/tateb/bigname:latest api
docker run --rm ghcr.io/tateb/bigname:latest indexer
docker run --rm ghcr.io/tateb/bigname:latest worker
docker run --rm ghcr.io/tateb/bigname:latest migrate
```

The default command is `api`. Raw binary invocations are also supported:

```sh
docker run --rm ghcr.io/tateb/bigname:latest bigname-api print-openapi
docker run --rm ghcr.io/tateb/bigname:latest bigname-worker inspect watch-plan --json
```

## Fresh Server Compose

1. Install Docker and Docker Compose.
2. Copy `.env.server.example` to `.env.server` and change the placeholder passwords.
3. Set `BIGNAME_IMAGE` to the image tag to run.
4. Start the stack:

```sh
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The server compose file starts PostgreSQL, MinIO, a one-shot migration service,
the API, the indexer, and the worker. The API listens on the host port from
`BIGNAME_API_PORT` and answers readiness at `/healthz`.

The indexer loads exactly one manifest root. Use `/app/manifests` for the
mainnet profile or `/app/manifests-sepolia-dev` for the ENSv2 Sepolia dev
profile. Do not point one runtime at both manifest roots.

If `BIGNAME_INDEXER_CHAIN_RPC_URLS` is unset, the indexer still syncs
manifest/watch state, but provider-backed live ingestion remains idle. Current
bootstrap RPC support accepts `http://` endpoints.

RPC requirements are per selected profile and active watched chain. An
Ethereum-only run may omit Base entirely. If the selected profile includes Base
but no Base RPC is configured, Base provider-backed intake, automatic bootstrap,
backfill catch-up, and live head following stay idle with an explicit
`no_provider` / unavailable operational state; startup for configured chains
must not fail solely because Base is missing. A provider entry for a chain that
is not part of the selected manifest root is invalid.

Startup bootstrap creates finite backfill jobs from each eligible target's
manifest/discovery admitted start through the provider head observed at job
creation time. It does not cap work to a recent window. This is still
operational intake work: completing bootstrap alone is not consumer-replacement
or route-coverage evidence without the relevant projection, route, conformance,
and rollout gates.

Hash-pinned backfill execution batches each reserved range into
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS`-sized chunks. The default
server profile uses `1024` blocks. Larger chunks reduce checkpoint churn and RPC
round trips during long historical bootstrap, while also increasing the amount
of range work retried after a failed chunk.

Operational catch-up to finalized head should be run as bounded idempotent
backfill chunks. Before every chunk starts range work, check current Postgres
size, writable free disk, and any configured object-cache budget. Capacity
shortage should pause or fail the chunk explicitly instead of silently retaining
less selected replay data or retaining full payload bundles for empty historical
blocks.

## GHCR Image

The repository publishes the image to:

```text
ghcr.io/tateb/bigname
```

The GitHub Actions workflow publishes `latest` on the default branch and a short
commit SHA tag on every push to `main`. Tags pushed to the repository are also
published with the same tag name.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  -t ghcr.io/tateb/bigname:latest \
  -t ghcr.io/tateb/bigname:$(git rev-parse --short HEAD) \
  --push .
```
