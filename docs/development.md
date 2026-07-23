# Development

Local development uses Docker Compose for PostgreSQL plus host-side Rust binaries for the API, indexer, and worker.

## Bootstrap

1. Copy `.env.example` to `.env`.
2. Run `docker compose up -d`.
3. Apply the checked-in migration with `./scripts/migrate`.
4. Boot the API, indexer, and worker together with `./scripts/dev-up`.

The compose stack starts:

- PostgreSQL on `127.0.0.1:5432` with database `bigname` and credentials `bigname` / `bigname`

Stop the local services with `docker compose down`. Add `-v` if you also want to remove the local data volumes.

## Database-backed tests

Run DB-backed Rust tests through the local test harness:

```sh
./scripts/test-db
```

Do not run DB-backed tests directly with `cargo test` unless
`BIGNAME_DATABASE_URL` or `DATABASE_URL` already points at a reachable local
PostgreSQL server. Direct runs otherwise sit on the default development URL and
usually fail with an admin-pool timeout; rerun the same command through
`./scripts/test-db -- ...`.

The harness starts or reuses an isolated `postgres:16-alpine` container named
`bigname-test-postgres` on `127.0.0.1:55432`, exports both
`BIGNAME_DATABASE_URL` and `DATABASE_URL`, and then runs the requested command.
It intentionally does not source `.env`, so server-oriented values such as
`postgres:5432` do not leak into host-side cargo test runs.

Pass a focused command after `--`:

```sh
./scripts/test-db -- cargo test -p bigname-worker projection_apply -- --nocapture
```

Set `BIGNAME_TEST_DATABASE_URL` to point at an already-running PostgreSQL server
instead of using Docker. That server must allow the configured user to create
and drop temporary test databases.

## Bootstrap Migration Hygiene

During bootstrap, bigname has no active deployments or shared production
databases that must preserve data across every intermediate schema. Migration
findings that only affect historical data moving between pre-deployment schemas
should be tracked as bootstrap cleanup unless a shared/staging database is
explicitly declared non-rebuildable.

Before the first stateful deployment, collapse the checked-in SQL history into a
small baseline migration set. When collapsing, remove obsolete transition-only
steps or re-audit them for hard preflight checks before destructive drops, such
as the pre-deployment `raw_blocks` to `chain_header_audit` transition.

## Live Indexing Configuration

`./scripts/dev-up` sources `.env`, applies migrations, starts the API, starts
`bigname-indexer run`, and starts the worker. On startup the indexer loads the
selected manifest root, syncs manifest state into PostgreSQL, rebuilds the
stored watch plan, creates persisted chain checkpoint rows for active watched
chains, and then polls configured provider sources.

Set `BIGNAME_INDEXER_MANIFESTS_ROOT` to select one runtime profile. The default
is `manifests/mainnet` for the shipped mainnet profile. Use `manifests/sepolia`
only when running the ENSv2 Sepolia profile; do not load it beside
`manifests/mainnet` in the same local database.

Set `BIGNAME_INDEXER_CHAIN_RPC_URLS` to a comma-delimited list of
`<chain>=<url>` entries matching active watched chains in the selected profile:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

If both provider source settings are unset, `./scripts/dev-up` still boots the
processes and the indexer still syncs manifest/watch state, but provider-backed
head fetch and live ingestion stay idle. Current bootstrap RPC support accepts
`http://` and `https://` endpoints.

## Live API Execution Configuration

`GET /v1/profiles/names/{name}` in `mode=verified|both`,
`GET /v1/names/{namespace}/{name}/records` when it needs verified values,
`GET /v2/names/{name}?source=verified`, and `GET /v2/names/{name}/records` in
`source=verified|auto` may execute supported ENS verified-resolution selectors
on demand when matching persisted execution output is absent. That live
execution uses the selected exact-name snapshot: no `at` and no
`chain_positions` means `consistency=head` and the latest stored Ethereum
checkpoint, and the API call targets that selected block rather than provider
latest.

Configure `BIGNAME_API_CHAIN_RPC_URLS` for every chain expected by the status
chain set before relying on `/v1/status` or `/v2/status`; startup warns with
the exact missing chain names and their readiness stays fail-closed. Include
`ethereum-mainnet=<http-url>` before relying on live ENS verified resolution or
the ENS/60 primary-name on-demand reverse/forward RPC fallback. This is separate from
`BIGNAME_INDEXER_CHAIN_RPC_URLS`, which feeds indexer intake and checkpoint
state only. If the API Ethereum provider is not configured, supported live ENS
verified selectors fail closed instead of falling back to declared record cache:
v1 returns `409 stale` with a configuration message, while v2 product routes use
their documented in-band `status=stale`/`failure_reason` envelope. For
`GET /v1/primary-names/{address}` defaulting to `namespace=ens&coin_type=60`,
the API pins the reverse lookup to the stored checkpoint used in response
metadata. Missing provider configuration or reverse-provider failure returns
`claimed_primary_name.status=execution_failed`; successful fallback misses still
return `claimed_primary_name.status=not_found` with ENS reverse-RPC partial
coverage. When `mode=verified|both` and the reverse claim passes the
normalization gate, the API uses the same provider and block hash for `addr:60`
verification through the ENS Universal Resolver. It persists the verified-mode
trace and outcome before responding so an identical request at the same
checkpoint can use durable readback.

Deployments with a local Reth database can also set
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` to a comma-delimited list of
`<chain>=<reth-datadir>` entries. Configure at most one source per chain. The
Reth source is optional and operational: it must feed the same raw-fact intake
contract as JSON-RPC, and Reth-local table references do not replace bigname raw
fact refs or Postgres replay facts.
Native Reth database support is compiled only when the indexer is built with
the `reth-db` feature, for example
`cargo check -p bigname-indexer --features reth-db`. That opt-in build requires
Clang/libclang development headers for Reth's RocksDB/MDBX bindings. Default
workspace checks do not build those native dependencies.

`BIGNAME_INDEXER_POLL_INTERVAL_SECS` controls the local indexer poll interval
and defaults to `5`.

## Readiness Endpoint

The API process exposes `GET /healthz` on the same bind address as
`cargo api -- serve` and `./scripts/dev-up`. The default local address is
`http://127.0.0.1:3000/healthz`.

`/healthz` is an unversioned operator contract endpoint. The production compose probe
connects to `127.0.0.1` inside the API container even though the process listens
on its configured bind address (`0.0.0.0:3000` by default in compose); the
public Caddy edge does not expose it. It is not part of the versioned `/v1` read
API and should not be treated as a consumer compatibility surface. Its frozen
response may grow additively.

The response's `identity` object describes the binary's compatibility inputs:
`version` is the Cargo package version; `build_sha` is the compile-time
`BIGNAME_BUILD_SHA` value (the published image supplies its Git commit, while
other builds fall back to `unknown`); `schema_migration_version` is the latest
checked-in database migration; `projection_replay_version` is the expected
current-state [replay](glossary.md) version; and
`projection_publication_versions.permissions_current` is the reader
compatibility version for the published permissions read model. These are
binary identity values, not a report of the database's applied migration or
replay progress.

The endpoint separates API-process readiness, database readiness, and
indexer/worker main-loop liveness:

- Healthy database and recent indexer and worker loop heartbeats: `200 OK`,
  top-level `api_status` and aggregate `status` are `ready`,
  `process.status` is `running`, `database.status` is `reachable`,
  `database.reachable` is `true`, `database.check` is `select_1`, and
  `database.error` is `null`. `loops.indexer.status` and
  `loops.worker.status` are `running` and each includes `started_at`,
  `heartbeat_at`, `heartbeat_age_seconds`, and `max_age_seconds`.
- Unreachable database or pool: `503 Service Unavailable`, top-level `status`
  and `api_status` are `degraded`, `process.status` remains `running`, `database.status` is
  `unreachable`, `database.reachable` is `false`, `database.check` remains
  `select_1`, `database.error` is `database readiness query failed`, and both
  loop statuses are `unavailable`.
- Reachable database with a missing or old loop heartbeat: `200 OK` with
  `api_status=ready` and aggregate `status=degraded`. This keeps API container
  and public-edge readiness local to the serving process and database while
  retaining the indexer/worker failure in the payload. A missing row is
  `not_started`; a row older than
  `BIGNAME_API_HEARTBEAT_MAX_AGE_SECS` is `stale`. The default maximum age is
  20 seconds, four times the default five-second indexer and worker loop
  intervals.

Database reachability is checked with `SELECT 1` through the configured
PostgreSQL pool. `api_status` is API-local readiness: because the handler is
serving by definition, it is `ready` exactly when that query succeeds, and the
HTTP status follows this field. Aggregate `status` additionally requires both
service loops and may therefore be `degraded` in an HTTP 200 response.
The API prefers the retained service instance with a currently healthy normal
heartbeat or named phase, using the newest stale evidence only when none is
healthy. Indexer phases use the ordinary indexer heartbeat maximum; worker
rebuild phases use their separate long-operation maximum. A deployment runs
one active writer for each service; process
rows retained across a restart make that selection robust during the handoff
without authorizing concurrent workers. The indexer and worker `healthcheck`
subcommands instead validate their own `BIGNAME_HEARTBEAT_INSTANCE_ID`,
including any active named phase. Local binaries fall
back to `HOSTNAME`; the server compose file pins stable `indexer` and `worker`
identities so a recreated container refreshes the same process row. The checks
fail when the row is absent or older than the service-specific
`BIGNAME_INDEXER_HEARTBEAT_MAX_AGE_SECS` or
`BIGNAME_WORKER_HEARTBEAT_MAX_AGE_SECS` limit. This distinguishes a loop that
never registered from one that registered and then stopped advancing. Worker
bootstrap replay and projection apply refresh the process row only at actual
progress boundaries; the indexer registers before startup bootstrap and
refreshes after completed hash-pinned progress units of at most 32 blocks
inside the configured checkpoint chunk, then after completed startup adapter
checkpoint stream pages and bounded discovery, identity, binding, and
normalized-event finalization batches. Live manifest and discovery refresh
adapter passes reuse those checkpoint-page callbacks and family-boundary beats.
A free-running heartbeat task does not
keep a stuck operation healthy. Worker rebuilds refresh at their
existing projection batch boundaries. A monolithic worker SQL or hydration
operation instead sets `loops.worker.phase` and uses
`BIGNAME_API_WORKER_REBUILD_PHASE_MAX_AGE_SECS` (default 43,200) in the API and
`BIGNAME_WORKER_REBUILD_PHASE_MAX_AGE_SECS` in the worker container check. The
named phase is removed when normal bounded progress resumes; graceful shutdown
deregisters the instance, fencing further phase writes and removing all of its
heartbeat rows before exit. Registering a new single-writer worker retires a
dead predecessor's phase. If the process exits without either cleanup path and
no replacement registers, the retained phase becomes `stale` at that separate
limit.
Failure to persist the phase start aborts that rebuild attempt before its
monolithic operation begins; ordinary bounded-progress beat failures still
warn and retry at the next progress boundary.
[Full-closure replay](glossary.md) ownership waits set `loops.indexer.phase`
and use the ordinary indexer heartbeat maximum. Lock poll ticks and
finite-deadline retries do not refresh that phase; acquiring ownership removes
it.
