# Development

Local development uses Docker Compose for PostgreSQL plus host-side Rust
binaries. The old indexer has been deleted. `./scripts/dev-up` starts the
surviving API and worker. When `BIGNAME_PHASE_RUNNER_CHAINS` is configured, it
also starts the Stage B phase runner against the `bigname_phase` namespace in
the same PostgreSQL database.

## Bootstrap

1. Copy `.env.example` to `.env` if local overrides are needed.
2. Run `docker compose up -d`.
3. Apply migrations with `./scripts/migrate`.
4. Before the first configured phase-runner start, run
   `cargo phase -- init-schema` once.
5. Provision the separate SELECT-only verification login described in
   [`deployment.md`](deployment.md#phase-runner-configuration).
6. Run `./scripts/dev-up`.

The default compose stack provides the retained API/worker database on
`127.0.0.1:5432`. The retained migrations live in `public`; the phase runner
puts the fresh baseline in `bigname_phase` and connects with
`search_path=bigname_phase`. Stop the local database with
`docker compose down`; add `-v` only when the local data volume should also be
removed.

Without phase-runner configuration, `dev-up` warns and runs only API and
worker. This is useful for read-model and execution development against an
existing database, but it is not live indexing.

The two PostgreSQL schemas are an explicit Stage B boundary. `./scripts/migrate`
and `bigname-worker migrate` prepare only retained `public` objects.
`phase-runner init-schema` installs only into an empty `bigname_phase` schema;
it refuses every nonempty target until a reviewed schema-v2 upgrade or rebuild
path exists. API and worker do not read the phase projections yet. The one
intentional cross-schema operation is head publication: orphaning phase lineage
and evicting affected retained execution-cache outcomes commit atomically.

## Database-backed tests

Run DB-backed Rust tests through the isolated database harness:

```sh
./scripts/test-db
```

Do not run DB-backed tests directly unless `BIGNAME_DATABASE_URL` or
`DATABASE_URL` already points at a reachable local PostgreSQL server. The
harness starts or reuses `postgres:16-alpine` on `127.0.0.1:55432`, exports both
variables, and does not source `.env`.

Pass a focused command after `--`:

```sh
./scripts/test-db -- cargo nextest run -p bigname-worker
```

`BIGNAME_TEST_DATABASE_URL` may instead name an existing PostgreSQL server
whose configured user can create and drop temporary test databases. The
phase-runner verification integration tests also create one shared, unprivileged
test login role; that server user therefore needs `CREATEROLE` for the full
phase-runner suite.

## Bootstrap migration hygiene

During bootstrap, bigname has no active deployments or shared production
databases that must preserve data across every intermediate schema. Before the
first stateful deployment, collapse checked-in history into a small baseline
migration set and re-audit any destructive transition. Until that explicit
operation, checked-in migrations are immutable history even when their Rust
writer has been removed.

## Stage B phase runner

The current phase runner implements `ingest`, `interpret`, `project`, read-only
`verify`, and continuous `live` follow. Verification compares only finalized
history: Base dRPC records `cross_checked` through the Coinbase-to-dRPC ingest
seam, while Ethereum local reth records `node_checked` through its finalized
marker. The API still reads the legacy public-schema
projections until Stage C.

Set the [deployment profile](glossary.md#deployment-profile) root and
chain/source descriptors, for example:

```sh
export BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT=manifests/mainnet
export BIGNAME_PHASE_RUNNER_CHAINS=ethereum-mainnet
export BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL=postgresql://bigname_verify:<secret>@127.0.0.1:5432/bigname
export RETH_DATA_DIR=/var/lib/reth/mainnet
export BIGNAME_PHASE_RUNNER_SOURCES=ethereum-mainnet:reth:reth_db:ethereum_head:0=RETH_DATA_DIR
export BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545
```

Then `./scripts/dev-up` launches the phase runner alongside API and worker. The
writer phases use `BIGNAME_DATABASE_URL`; verification uses the separately
credentialed URL and refuses a role that can write application relations. Both
select `bigname_phase` before `public`. The runner executes the initial spine
and then follows the live head until cancellation.
Initialize the phase schema once before the first bounded or supervised
invocation:

```sh
cargo phase -- init-schema
```

For bounded phase work, invoke an implemented phase explicitly with
`BIGNAME_DATABASE_URL` pointing at the shared PostgreSQL database:

```sh
cargo phase -- redo \
  --chain ethereum-mainnet \
  --phase ingest \
  --from-block 0 \
  --to-block 100 \
  --source ethereum-mainnet:reth:reth_db:ethereum_head:0=RETH_DATA_DIR
```

Use `--phase verify` with the same `reth_db` descriptor to recheck a finalized
Ethereum range; it also requires
`BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL` (or
`--verification-database-url`). Base verify redo uses its `drpc` descriptor and
can record only `cross_checked`; the descriptor must retain the fixed
`48,428,000` source start, and a range above that seam is rejected before the
redo marker is written. A partial redo retains the level for the
whole recorded extent, while a full-extent redo can change it.
The reader URL must authenticate the dedicated role directly and resolve to the
same PostgreSQL system/database identity as `BIGNAME_DATABASE_URL`.

Interpret and project redo use the same range without requiring an ingest
provider source. Project redo, including the automatic project cascade after
interpret redo, performs the same canonical-head hydration as supervised
project; configure `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` or pass
`--hydration-rpc CHAIN=HTTP_URL` when eligible Ethereum rows exist. The
`rewind` command selects an exact stored ancestor and delegates its
orphaning and downstream repair stamps to normal head publication. See
[`chain-intake.md`](chain-intake.md) for the phase boundary.

## Live API execution configuration

Supported verified-resolution product routes may execute an admitted selector
when matching persisted execution output is absent. Configure
`BIGNAME_API_CHAIN_RPC_URLS` for every chain expected by status and for any
route that needs provider execution:

```sh
BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545
```

Missing provider configuration fails closed according to each route contract;
it does not fall back silently to an unrelated cached answer. The worker's
projection-owned hydration can separately use `BIGNAME_WORKER_CHAIN_RPC_URLS`
for its documented block-pinned calls.

## Readiness endpoint

The API serves `GET /healthz` on its normal bind address, defaulting locally to
`http://127.0.0.1:3000/healthz`. It is an unversioned operator endpoint, not a
public versioned API route.

`api_status` reflects the API's own database reachability. Aggregate status
also evaluates retained worker and old-indexer heartbeat evidence. Because the
old indexer writer has been removed and the API readiness port has not yet
landed, that aggregate field may remain degraded in a Stage B development
database. This PR deliberately preserves that API behavior. The server compose
healthcheck tests `api_status`, so it does not claim that the missing indexing
pipeline is healthy.

The worker continues to register and update its process and named rebuild-phase
heartbeats. Its container healthcheck validates its configured instance row.
Old indexer heartbeat rows and per-chain readiness reads remain only because
the surviving API and worker still consume them; their removal is deferred to
the worker/API port.
