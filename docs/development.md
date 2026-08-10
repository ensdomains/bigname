# Development

Local development uses Docker Compose for PostgreSQL plus host-side Rust
binaries. `./scripts/dev-up` starts the API and, when
`BIGNAME_PHASE_RUNNER_CHAINS` is configured, the phase runner against the
`bigname_phase` namespace in the same PostgreSQL database.

## Bootstrap

1. Copy `.env.example` to `.env` if local overrides are needed.
2. Run `docker compose up -d`.
3. Before the first configured phase-runner start, run
   `cargo phase -- init-schema` once.
4. Provision the separate SELECT-only verification login described in
   [`deployment.md`](deployment.md#phase-runner-configuration).
5. Run `./scripts/dev-up`.

The default compose stack provides PostgreSQL on `127.0.0.1:5432`. The phase
runner puts its baseline in `bigname_phase` and connects with
`search_path=bigname_phase`. Stop the local database with
`docker compose down`; add `-v` only when the local data volume should also be
removed.

Without phase-runner configuration, `dev-up` warns and runs only the API. The
API requires an initialized, current `bigname_phase` schema for indexed and
verified reads; no local indexing occurs without the phase runner.

`phase-runner init-schema` installs only into an empty `bigname_phase` schema;
it refuses every nonempty target. Reviewed versioned schema-migrations can
upgrade an initialized namespace in place when their preconditions pass; other
changes require the reviewed replacement procedure. The API reads phase
projections and may invoke the guarded resolution-divergence write. The
checked-in SQLx schema-migration history remains append-only, but the deleted
worker schema-migration command is no longer a runtime entrypoint; deployment
automation applies reviewed versioned schema-migrations at the planned boundary.

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
./scripts/test-db -- cargo nextest run -p bigname-api
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
marker. V2 verified name, record, and ENS/60 primary-name reads use the phase
runner's schema-v2 lookup state. Other API reads use phase projections.

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

Then `./scripts/dev-up` launches the phase runner alongside the API. The
writer phases use `BIGNAME_DATABASE_URL`; verification uses the separately
credentialed URL and refuses a role that can write application relations. Both
use `bigname_phase`. The runner executes the initial spine
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
when the indexed inventory has no satisfying answer. Configure
`BIGNAME_API_CHAIN_RPC_URLS` for every chain expected by status and for any
route that needs provider execution:

```sh
BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545
```

Missing provider configuration fails closed according to each route contract;
it does not fall back silently to an unrelated answer. Phase-runner projection
hydration separately uses `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` for its
documented block-pinned calls.

## Readiness endpoint

The API serves `GET /healthz` on its normal bind address, defaulting locally to
`http://127.0.0.1:3000/healthz`. It is an unversioned operator endpoint, not a
public versioned API route.

`api_status` reflects the API's own database reachability. Aggregate status
also evaluates the phase runner's heartbeats, judged on the worst expected
chain rather than the single freshest row: an expected chain with no
phase-runner heartbeat, or whose newest heartbeat is older than
`BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS`, reports `stale` even while another
chain keeps writing heartbeats. A phase runner that has written no heartbeat at
all reports `not_started`. A standalone API therefore reports
`status="degraded"` with `api_status="ready"`; current heartbeats on every
expected chain make the aggregate ready. The server compose healthcheck tests
`api_status` so API liveness does not depend on an indexing loop restart.

Expected chains are the same set `/v2/status` reports: every chain with a
stored head or any phase state. Three consequences worth knowing before paging
on this endpoint:

- The runner writes heartbeats between batches, so a batch or an inter-phase
  transition longer than the configured max age reports `stale` on that chain
  even though it is working. Raise
  `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` above the slowest expected batch
  rather than reading the endpoint as a liveness proof for every chain.
- Nothing removes a chain from the expected set on its own. A decommissioned
  chain keeps `stale` latched until its `chain_heads` and `chain_phase_state`
  rows are deleted, which is the same removal `/v2/status` needs.
- The runner records a chain's phase state when it initializes the chain, which
  is before that chain's first heartbeat. A staged startup therefore reports
  `degraded` until every expected chain has written a heartbeat, which is the
  missing-heartbeat branch of the rule above rather than an age comparison.

When a chain is missing a heartbeat entirely, the reported `phase`,
`started_at`, `heartbeat_at`, and `heartbeat_age_seconds` are null: there is no
row to describe, and the freshest other chain's row would misreport the fault.
