# Development

Local development uses Docker Compose for PostgreSQL plus host-side Rust
binaries. `./scripts/dev-up` starts the API and, when
`BIGNAME_PHASE_RUNNER_CHAINS` is configured, the phase runner against the
`bigname_phase` namespace in the same PostgreSQL database.

## Prerequisites

Install these tools first. Where a file pins a version, use that version.
Homebrew does not link `rustup` or `libpq` onto `PATH`, so add their `bin`
directories yourself.

Run the `cargo install` commands from inside the repository, where
`rust-toolchain.toml` selects the toolchain. Outside it they fail when `rustup`
has no default toolchain.

For the local stack:

- `rustup`, for every Rust build. `rust-toolchain.toml` pins the toolchain,
  clippy and rustfmt, and the first `cargo` command in the repository installs
  them. Install it with `brew install rustup` or `apt-get install rustup`.
- Docker with the Compose plugin, for the local database and
  `./scripts/test-db`. Install Docker Desktop, or Docker Engine with the
  `docker-compose-plugin` package.
- `python3`, for the Python scripts in `scripts/`, including `scripts/sync-refs`.
  `scripts/check-upstream-rotation` and `scripts/benchmark-gate` need 3.11 or
  later. Install it with `brew install python` or `apt-get install python3`.

For tests and checks:

- `cargo-nextest`, for the `cargo nextest` commands under
  [Database-backed tests](#database-backed-tests). Install it with
  `cargo install cargo-nextest --locked --version <version>`, using the version
  `.github/workflows/ci.yml` pins.
- ripgrep, for `scripts/check-query-ownership`. Install it with
  `brew install ripgrep` or `apt-get install ripgrep`.
- Foundry, both `anvil` and `forge`, for the end-to-end tests
  ([`tests/e2e/README.md`](../tests/e2e/README.md#prerequisites)). Install it
  with its `foundryup` installer: `foundryup --install <version>`, using
  `FOUNDRY_VERSION` from `.github/workflows/ci.yml`.
- `psql`, for `scripts/check-schema` against a server named by
  `BIGNAME_TEST_DATABASE_URL`. Without `psql` the script runs it from a Docker
  container. The default test container needs no host `psql`. Install it with
  `brew install libpq` or `apt-get install postgresql-client`.

For less common work:

- `sqlx-cli`, to apply schema-migrations to a retained database. The
  [production runbook](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
  holds the procedure. Install it with
  `cargo install sqlx-cli --no-default-features --features rustls,postgres`.
- `promtool`, for the Prometheus rule step in the `static` CI job, which pins
  the version. It ships with Prometheus, so `brew install prometheus` provides
  it.
- `node`, for `node --test scripts/tests/site.test.mjs` in the `site` CI job.
  Install it with `brew install node` or `apt-get install nodejs`.

## Bootstrap

1. Run `scripts/sync-refs` to fetch the
   [pinned upstream copies](#pinned-upstream-copies).
2. Copy `.env.example` to `.env` and apply any local overrides. On a laptop,
   lower the [PostgreSQL memory settings](#database-memory-on-a-laptop).
3. Run `docker compose up -d --wait` so PostgreSQL passes its healthcheck before
   phase initialization.
4. Export the local environment and initialize the phase schema once, before
   starting either the API or the phase runner:

   ```sh
   set -a
   . ./.env
   set +a
   cargo phase init-schema
   ```
5. For indexing, configure the phase runner and provision the separate
   SELECT-only verification login described in
   [`deployment.md`](deployment.md#phase-runner-configuration). The sample
   configuration leaves indexing disabled and starts only the API.
6. Run `./scripts/dev-up`.

This bootstrap assumes a disposable fresh local database volume. For a retained
local volume, follow the production schema-migration order before phase-schema
initialization instead of treating the current baseline as an in-place upgrade.

To keep an existing local database untouched, set `COMPOSE_PROJECT_NAME` in
`.env` to a new name, such as `bigname-dev`. Docker Compose then creates a fresh
volume under that name and leaves the old volume as it is. Both projects publish
the same host port, so stop the other project's container first, or change
`POSTGRES_PORT` and both database URLs in `.env`.

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
projections and verifies provider answers without database writes. The
checked-in SQLx schema-migration history remains append-only, but the deleted
worker schema-migration command is no longer a runtime entrypoint; deployment
automation applies reviewed versioned schema-migrations at the planned boundary.

### Pinned upstream copies

`scripts/sync-refs` clones the upstream repositories pinned in
`.refs/MANIFEST.toml` into `.refs/`. It needs network access to GitHub.
`.refs/` is gitignored, so each clone and each git worktree needs its own sync.
[`.refs/README.md`](../.refs/README.md) describes the other modes.

The API and the phase runner run without the copies. These need them:

- Unit tests that compare checked-in data with upstream sources and deployment
  artifacts. Examples are `role_tables_cover_each_pinned_upstream_constant` in
  `crates/adapters/src/schema_v2/protocol/permissions.rs` and the tests in
  `crates/manifests/src/lib/official_mainnet_tests.rs`. The default
  `./scripts/test-db` run includes them.
- The end-to-end tests in [`tests/e2e`](../tests/e2e/README.md#prerequisites).

In CI, `test (core)`, `build (e2e phase-runner)`, the `test (e2e shard)` jobs,
`test (API container shutdown)` and `artifacts-and-smoke` sync the copies. The
`static` job runs without them.

### Database memory on a laptop

The PostgreSQL memory defaults in `docker-compose.yml` are sized for indexing on
a large host. To lower them on a laptop, uncomment these lines in `.env` and
adjust the values:

```sh
POSTGRES_SHM_SIZE=1gb
POSTGRES_SHARED_BUFFERS=1GB
POSTGRES_EFFECTIVE_CACHE_SIZE=4GB
```

- `POSTGRES_SHM_SIZE` sets the size of the container's `/dev/shm`.
- `POSTGRES_SHARED_BUFFERS` sets PostgreSQL's shared buffer cache. Keep it well
  under the memory Docker can use.
- `POSTGRES_EFFECTIVE_CACHE_SIZE` reserves no memory. It tells the query planner
  how much page cache to assume.

Keep the units as written, because PostgreSQL rejects a lowercase `gb`.

Docker Compose reads `.env` from the repository root, so the variables need no
export. Run `docker compose up -d --wait` to recreate the container with the new
values. The data volume stays.

## Checks before a pull request

The `static` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
holds the full list of static checks. These commands mirror its main steps. They
need no database and no pinned upstream copies:

```sh
cargo fmt --check
cargo fmt --manifest-path tests/e2e/Cargo.toml -- --check
cargo clippy --keep-going --workspace --all-targets \
  --features bigname-adapters/test-activation,bigname-storage/test-support \
  --exclude bigname-adapters --no-deps -- -D warnings
cargo clippy --manifest-path tests/e2e/Cargo.toml --locked --all-targets -- -D warnings
scripts/generate-openapi --check
./scripts/check-rust-file-size --base-ref origin/main
./scripts/check-migration-versions
./scripts/check-query-ownership
```

- The first clippy command leaves out `bigname-adapters`. That crate has its own
  step, "cargo clippy (adapter warning-class baseline)", with a list of allowed
  lints. Copy that command from the job.
- `--base-ref` applies the allowance check a pull request gets. Fetch
  `origin/main` first.
- `tests/e2e` is a separate workspace, so `--manifest-path` names it.
- At the repository root, plain `cargo build`, `cargo check`, `cargo clippy` and
  `cargo test` cover only the default members, `apps/api` and
  `apps/phase-runner`. Pass `--workspace` to cover every crate.

## CI after a merge

Pull requests run the complete CI suite. The repository's active `main`
ruleset requires the merge queue and has no bypass actors. It also requires
`static`, `artifacts-and-smoke`, `test`, and the named test-suite aggregates to
pass. The `test` aggregate covers core, API, schema, phase-runner, e2e
(including shutdown), and site checks; static and smoke checks are required
separately. A pull request therefore passes the full validation gate before it
can be queued. The merge group then reruns only the fast gates on the merged
result: static, smoke, schema, core and site. The API, phase-runner and e2e
jobs and their aggregates are skipped there, and a skipped check satisfies the
ruleset. Two queued pull requests that pass separately but break each other in
those suites are not caught by the queue, and nothing reruns those suites on
`main`. A later pull request's full run against the updated `main` may surface
the break, in that unrelated pull request. Conditions on the ref keep the
subsequent push to `main` from running the same tests and static checks again.

The main-branch follow-up still uploads the site and refreshes the shared Cargo
and shutdown-image caches. A cache miss can compile dependencies or build an
image, but does not execute the tests again. These caches must be saved on
`main` so later pull requests can restore them. Docker images are published
only from `v*` release tags on `main` commits whose CI passed, not from this
push workflow. Revisit this event split if mandatory queue
use or any required validation gate is removed.

`scripts/test-container-shutdown --build-only` builds the image and exports its
cache without running the shutdown scenario. CI uses this only to refresh a
missing main-branch cache after the required queue validation.

## CI without a pull request

A push to a branch whose name starts with `ci/` runs the full suite, with the
same jobs and steps as a pull request. Use it to run CI on a commit that is not
ready for review, such as a failing test pushed ahead of its fix. Its checks
report on the commit, and no pull request is needed.

- Name the branch `ci/<ticket>-<purpose>`, for example `ci/tyr442-smoke`.
- A newer push to the same branch cancels the older run.
- The run restores the caches `main` saved and saves no cache entry of its own.
  The repository's 10 GB cache limit would otherwise evict `main`'s entries.
- The Rust source size ratchet compares allowances against the commit where the
  branch left `main`, the merge base, because a push has no base branch.
- No branch ruleset covers `ci/` branches, so they can be pushed and deleted
  freely. Delete the branch once its run has been recorded.

## Database-backed tests

Run DB-backed Rust tests through the isolated database harness:

```sh
./scripts/test-db
```

Do not run DB-backed tests directly unless `BIGNAME_DATABASE_URL` or
`DATABASE_URL` already points at a reachable local PostgreSQL server. The
harness starts or reuses `postgres:16-alpine` on `127.0.0.1:55432` with JIT off
and `max_connections=300`, exports both variables, and does not source `.env`.

The harness reuses an existing container unchanged, so a container created
before the current settings lacks them:

- Without JIT off, the [Project](projections.md) suites, which build the served
  projections, spend most of their time compiling statements.
- With the stock 100-connection ceiling, the end-to-end suite's parallel stacks
  saturate PostgreSQL, which surfaces as pool-acquire timeouts in unrelated
  tests.

To see the settings a container was created with, run
`docker inspect --format '{{.Config.Cmd}}' bigname-test-postgres`. A current
container prints `[-c max_connections=300 -c jit=off]`. To get a fresh one,
remove it with `docker rm -f bigname-test-postgres`, or the name set in
`BIGNAME_TEST_POSTGRES_CONTAINER`. The next `./scripts/test-db` run creates it
with the current settings.

Pass a focused command after `--`:

```sh
./scripts/test-db -- cargo nextest run -p bigname-api
```

`BIGNAME_TEST_DATABASE_URL` may instead name an existing PostgreSQL server
whose configured user can create and drop temporary test databases. The
phase-runner verification integration tests also create one shared, unprivileged
test login role; that server user therefore needs `CREATEROLE` for the full
phase-runner suite.

A test returns every connection and transaction it took from
`TestDatabase::pool()` before it calls `TestDatabase::cleanup`. Cleanup closes
the pool before it drops the database, and the close can wait for connections
that are still checked out. When the close has not finished after 10 seconds,
cleanup drops the database anyway and fails the test with a message that says
pooled connections were still checked out. A held connection does not always
delay the close, so a passing cleanup does not show that none was held.

Tests have time limits:

- `cargo nextest` kills a test that runs for 12 minutes and reports it as
  `TIMEOUT`, which fails the run. The limit is the default profile's
  `slow-timeout` in `.config/nextest.toml`.
- `cargo test` has no per-test limit. CI gives the core suite's run step 20
  minutes. In the log of a run cut off there, a test that never finished has a
  "has been running for over 60 seconds" line and no result line.

On CI the slowest test takes under 4 minutes.

The CI jobs for the core, API and phase-runner suites record where a stalled
test stopped, before a limit ends it:

- `scripts/ci-pg-hang-watch` runs in the background. For each database with a
  client session older than 6 minutes it writes, once, the database's
  `pg_stat_activity` rows and the `pg_locks` rows that are not granted. The
  rows give each session's state, wait event, blocking sessions, start times
  and last statement. A statement that mentions a password or a URL is not
  shown. The job's "Report stalled test databases" step prints the report.
- The PostgreSQL service runs with `log_lock_waits=on`. Its log, which GitHub
  prints when the job ends, names every lock wait longer than one second with
  the sessions that hold the lock.

## Schema-migration history

bigname's public deployments are [live](production.md), and each database
records every applied schema-migration and its checksum in `_sqlx_migrations`.
Do not squash the history in `migrations/` into a smaller baseline.

Checked-in schema-migrations are append-only, immutable history, even when their
Rust writer has been removed. Changing an applied file makes `sqlx migrate run`
[refuse the deploy](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary),
so add a new versioned file instead. The
[schema-migration rules](storage.md#schema-migration-rules) apply to every file.

A destructive transition runs against the retained data of those deployments,
so audit it before it ships.

## Stage B phase runner

Normal builds and CI use RPC intake without the optional direct Reth database
reader. Explicit local builds may enable `phase-runner/reth-db`; see
[Direct Reth reader](reth-db-reader.md). Avoid `--all-features` when exercising
the normal build, since it opts back into that reader.

The phase runner implements `ingest`, `interpret`, `project`, read-only
`verify`, and continuous `live` follow. Verification operates only on finalized
history. A distinct [verification-only](glossary.md#source-role) Base dRPC records `cross_checked` through the Coinbase-to-dRPC ingest seam, an explicit verification-only Ethereum Mainnet reth records `node_checked` in an opt-in reader build, and Ethereum Sepolia records `cross_checked` with a distinct verification-only
dRPC or `quick_synced` without one.
V2 verified name, record, and ENS/60 primary-name reads use the phase runner's
schema-v2 lookup state. Other API reads use phase projections.

Set the [deployment profile](glossary.md#deployment-profile) root and
chain/source descriptors, for example:

```sh
export BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT=manifests/mainnet
export BIGNAME_PHASE_RUNNER_CHAINS=ethereum-mainnet
export BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL=postgresql://bigname_verify:<secret>@127.0.0.1:5432/bigname
export ETHEREUM_INTAKE_RPC_URL=http://127.0.0.1:8545
export BIGNAME_PHASE_RUNNER_SOURCES=ethereum-mainnet:rpc:drpc:ethereum_head:0=ETHEREUM_INTAKE_RPC_URL
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
cargo phase init-schema
```

For bounded phase work, invoke an implemented phase explicitly with
`BIGNAME_DATABASE_URL` pointing at the shared PostgreSQL database:

```sh
cargo phase redo \
  --chain ethereum-mainnet \
  --phase ingest \
  --from-block 0 \
  --to-block 100 \
  --source ethereum-mainnet:rpc:drpc:ethereum_head:0=ETHEREUM_INTAKE_RPC_URL
```

After normal Mainnet Verify has completed through block 100, the same current
five-field descriptor can recheck that retained extent:

```sh
cargo phase redo \
  --chain ethereum-mainnet \
  --phase verify \
  --from-block 0 \
  --to-block 100 \
  --source ethereum-mainnet:rpc:drpc:ethereum_head:0=ETHEREUM_INTAKE_RPC_URL
```

Verify redo cannot establish a chain's first verified extent; use the normal
phase pipeline for initial verification.

Use `--phase verify` with the intake
descriptors plus a distinct verification-only `reth_db` descriptor to earn
`node_checked` for a finalized Ethereum range; without the independent
descriptor it records `quick_synced`. Verify also requires
`BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL` (or
`--verification-database-url`). Base Verify uses the required target-covering
intake dRPC for `quick_synced`, or an optional distinct dRPC reference for
`cross_checked` through the fixed `48,428,000` seam. Comparison redo above that
seam is rejected before writing the redo marker. A Base
`reth_db` reference is explicitly unsupported and tracked by
[issue #433](https://github.com/ensdomains/bigname/issues/433); the operator
rationale and pinned reth evidence are in [deployment](deployment.md). A partial
redo keeps the weaker of the retained full-extent level and the level available
from the current source roles; a full-extent redo can establish the current plan's level.
Sepolia Verify redo records `cross_checked` with a distinct verification-only dRPC and otherwise records `quick_synced` without selecting intake as a reference.
The reader URL must authenticate the dedicated role directly and resolve to the
same PostgreSQL system/database identity as `BIGNAME_DATABASE_URL`.

Standalone Interpret and Project redo perform no ingest-provider I/O, but they require the complete intake-capable source descriptor set so the persisted cursor identities can prove the redo range. In an `all` redo, Interpret receives only intake-capable descriptors after Ingest validates their exact cursor-key set.
Project redo, including the automatic project cascade after
interpret redo, undoes and replays without hydration RPC and runs without
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`; the supervised `run`, which needs
it for `ethereum-mainnet`, refreshes the hydrated values the replay leaves
empty. The
`rewind` command selects an exact stored ancestor and delegates its
orphaning and downstream repair stamps to normal head publication. See
[`chain-intake.md`](chain-intake.md) for the phase boundary.

### Adaptive Interpret batches

An Interpret redo, alone or within `--phase all`, can opt into
`--interpret-adaptive`. Each batch then holds whole canonical blocks until the
next block would pass a target, instead of a fixed 500 blocks. Dense history gets
shorter batches and sparse history longer ones.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--interpret-target-logs` | 200000 | Raw logs per batch |
| `--interpret-target-input-mib` | 128 | Estimated raw input MiB per batch |
| `--interpret-adaptive-max-blocks` | 131072 | Most canonical blocks one batch spans |

Other redo phases and the supervised `run` refuse the flag. The options require
`--interpret-adaptive`, and each must be positive. An explicit
`--interpret-blocks-per-batch`, or `BIGNAME_INTERPRET_BLOCKS_PER_BATCH`, caps
the block span. The implicit 500-block default does not. A block is never
split: one block over a target runs alone and is logged as oversized. Normal
Interpret batches that the redo runs after a discovery repair keep the fixed
length.

The planner reads a raw log count and an input estimate for up to the maximum
span of blocks at once, and reuses them for the following batches. The end of
that span is itself a batch boundary, so a batch that starts partway into it ends
there even when it is under its targets. Each batch
still reads its blocks again inside its own database snapshot. A chain
reorganization between planning and loading therefore fails that batch with a
retryable error before it writes anything, and the retry plans from fresh
lineage. A restarted redo plans again from its last committed block, so its
batch boundaries may differ from an uninterrupted run while its stored rows do
not.

The estimate covers raw input only. It does not bound adapter state, the prior
history the loader restores, or PostgreSQL memory.

The runner refreshes its database phase heartbeat only between batches. A long
adaptive batch can therefore make [`GET /v1/status`](api-v1-routes.md#get-v1status)
report the chain `stale` and can fire `BignamePhaseRunnerHeartbeatStale`. Before
an adaptive redo, raise `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` on the API
and `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` on each scraped phase
runner above the longest expected batch, as
[phase-runner configuration](deployment.md#phase-runner-configuration)
describes, or expect the stale status and the page until the redo ends.

To compare with fixed batches,
run the same redo on the same restored database with and without the flag, and
compare the Interpret phase time, peak memory and the
`planned adaptive whole-block batch` log lines. An Interpret redo also performs
the Project work it requires, so time the completed Interpret boundary rather
than the whole process.

## Live API execution configuration

Supported verified-resolution product routes may execute an admitted selector
when the indexed inventory has no satisfying answer. Configure
`BIGNAME_API_CHAIN_RPC_URLS` for every chain expected by status and for any
route that needs provider execution:

```sh
BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545
```

Against a `manifests/sepolia` projection, verified ENS reads execute on Sepolia
and need `ethereum-sepolia=<url>` instead; `GET /v1/namespaces/ens` shows which
chains' verified capabilities the running configuration enables.

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
When the readiness query succeeds, `database.identity` is a one-way token scoped
to the currently running PostgreSQL postmaster, database OID, and server
listener used by the connection. It changes after a PostgreSQL restart or when
the connection reaches a different listener. The raw credentials, database
name, and network address are not exposed. Alternate access paths to the same
postmaster, such as a Unix socket and TCP or different listen addresses, can
produce different tokens. It is `null` when the database cannot be reached or
when bounded probes of the serving and readiness pools produce different
tokens. These audit failures do not change `api_status`; the benchmark gate
requires a populated identity and fails closed.

Expected chains are the same set `/v1/status` reports: every chain with a
stored head or any phase state. Three consequences worth knowing before paging
on this endpoint:

- The runner writes heartbeats between batches, so a batch or an inter-phase
  transition longer than the configured max age reports `stale` on that chain
  even though it is working. Raise
  `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` above the slowest expected batch
  rather than reading the endpoint as a liveness proof for every chain.
- Nothing removes a chain from the expected set on its own. A decommissioned
  chain keeps `stale` latched until its `chain_heads` and `chain_phase_state`
  rows are deleted, which is the same removal `/v1/status` needs.
- The runner records a chain's phase state when it initializes the chain, which
  is before that chain's first heartbeat. A staged startup therefore reports
  `degraded` until every expected chain has written a heartbeat, which is the
  missing-heartbeat branch of the rule above rather than an age comparison.

When a chain is missing a heartbeat entirely, the reported `phase`,
`started_at`, `heartbeat_at`, and `heartbeat_age_seconds` are null: there is no
row to describe, and the freshest other chain's row would misreport the fault.
