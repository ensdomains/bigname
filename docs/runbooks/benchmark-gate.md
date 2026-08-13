# Production-scale benchmark release gate

Run this gate before restoring traffic to a new serving generation and at every
planned [re-derivation boundary](../glossary.md#re-derivation-boundary). A green
small test database is only a harness check. Release approval requires the
production budget set against production-shaped data.

The checked-in source of every limit is
[`benchmarks/release-gate.toml`](../../benchmarks/release-gate.toml). Do not
override a failed limit. Production commands always load that exact path and
record it in the JSON report; they accept no alternate budget file. A budget
change is a reviewed release-policy change and moves in the same PR as this
runbook and the harness. Production runs require a clean worktree, including no
untracked files, relative to the reported `HEAD`. Write reports outside the
checkout as shown below. The wrapper checks before and after its release-profile
build, passes the captured commit into the binary, and the binary rechecks it
before and after measurement. The binary derives the reported Cargo build
profile from its compiled assertion mode, while production commands also
require the wrapper to attest that it selected the release Cargo build profile.
The wrapper requires `jq` and resolves both executables from Cargo's JSON
artifact output, so `CARGO_TARGET_DIR` and Cargo `build.target-dir` settings are
honored for the harness and the smoke API.

## What the gate measures

The indexing half measures the existing Interpret and
[Project](../glossary.md#projection) phase entrypoints. It requires a disposable
database copy because all three operations write derived state:

- one incremental Project tick at the selected head must finish within 1
  second, the live poll interval, including canonical-head record hydration;
- a full Project rebuild must finish within 6 hours inside an operator-scheduled
  window of at least 8 hours, leaving at least 2 hours of headroom; the measured
  wall-clock includes canonical-head record hydration;
- an Interpret redo over a dense-era range must sustain at least 500,000 blocks
  per hour;
- the dense range must contain at least 100,000 consecutive canonical blocks;
- that range must contain at least 8,000 retained raw logs per 1,000 blocks;
- the restored copy must contain at least 3 million current name rows before
  any timed projection work begins, owned by the selected chain's Project
  output;
- the Interpret process must stay at or below 32 GiB peak RSS while using the
  configured 65,536-entry interpreter-state cache.

The density check prevents a sparse historical range from producing a false
green throughput result. The current-name floor prevents a staging-sized copy
from producing a false-green full rebuild. The harness counts current names
before and after the rebuild, requires both totals to meet the floor, and
records both totals and the floor. The 32 GiB limit is a whole-process limit. It includes
the bounded value cache introduced after the 94 GiB out-of-memory incident and
the smaller protocol-state maps that remain resident; it is not merely the
cache's estimated JSON size.

The API half sends each Tier 1 and Tier 2 REST route in
[`api-v2-routes.md`](../api-v2-routes.md) 2,000 requests per second for 60
seconds after a 10-second warmup. It loads 10,000 names and 10,000 distinct
address/name/relation combinations from the target projections, plus at least
1,000 populated subname parents, permission subjects, and successful primary
name claims. Before sampling that corpus, it counts the complete
`name_current` and `address_names_current` tables and requires at least 3
million supported rows in each. Unsupported rows are excluded because the gate
cannot use them as request seeds. These floors leave headroom below the roughly
3.5 million names in the production dataset while excluding staging-sized
databases. The JSON report records both supported-row totals and both floors. It varies names, addresses,
search text, relations, history scopes, sort order, page size, and any cursors
returned by seed requests, and uses the real resolver and namespace rows. The
lookup mix covers 1, 10, 100, 250, and 1,000 inputs per batch, with large
batches weighted toward the tail. Timings end only after the complete response
body has been read. The report contains achieved throughput, success rate, and
p50, p95, and p99 for each route. Diagnostics, GraphQL compatibility, health, and documentation
routes are outside this traffic gate; they retain their ordinary functional
checks.

`POST /v2/lookup` keeps the latency-sensitive 5/10/25 ms p50/p95/p99 limits.
Point reads use either 10/25/50 ms or 25/75/150 ms limits. List reads use
25/75/150 ms or 50/150/300 ms limits, depending on query shape. The budgets
file is authoritative for the exact route mapping.

## Prepare the targets

Record the release commit, [interpreter content hash](../glossary.md#interpreter-content-hash), host CPU and memory,
PostgreSQL version and settings, database size, selected chain and head, dense
Interpret range, and whether the API target is host-local or reached over a
network. Keep those facts with both JSON reports.

Use two targets:

1. A disposable production-shaped PostgreSQL copy for indexing. Give the copy
   a database name distinct from production, restore it
   from the same production generation and retain the full immutable raw facts,
   canonical lineage, interpreted rows, and current projections. The benchmark
   rewrites interpreted and projected state. Never point this command at the
   production database, even while traffic is drained. Record the copy's exact
   database name and the selected chain's production JSON-RPC URL; the command
   requires both so the Project measurement follows the deployed hydration
   path.
2. The new API generation while public traffic is drained. Use its real
   production-scale database. The harness forces its own PostgreSQL sessions
   into read-only mode, and a database login with only `CONNECT`, schema
   `USAGE`, and table `SELECT` remains the preferred additional control. The API
   itself continues to use its normal read-only serving paths.

Do not run the indexing and API measurements at the same time. That would mix
rebuild contention into the steady serving measurement.

## Run the indexing half

Choose a contiguous dense-era range on the selected chain. The command reports
the raw-log density and fails before claiming throughput green when it is below
the checked-in floor.

Immediately after restoring the copy, prepare it with a fresh UUID that is not
stored in production. This table is intentionally in a separate
`bigname_benchmark` schema, outside the application's exact `bigname_phase`
table inventory and schema history. It exists only on the disposable benchmark
copy. Keep the two shell variables for the indexing command that follows.

```sh
DISPOSABLE_DATABASE='bigname_benchmark_copy_20260813'
DISPOSABLE_MARKER="$(uuidgen)"
psql "postgres://benchmark-writer@copy-host/$DISPOSABLE_DATABASE" \
  --set=ON_ERROR_STOP=1 \
  --set=disposable_marker="$DISPOSABLE_MARKER" <<'SQL'
CREATE SCHEMA bigname_benchmark;
CREATE TABLE bigname_benchmark.disposable_copy_marker (
    marker uuid PRIMARY KEY,
    database_name text NOT NULL UNIQUE,
    prepared_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO bigname_benchmark.disposable_copy_marker (
    marker,
    database_name
) VALUES (:'disposable_marker'::uuid, current_database());
SQL
```

Do not create this table in production or include it in a production backup.
Restoring production alone therefore never produces the marker required for
writes.

```sh
BIGNAME_BENCHMARK_DATABASE_URL="postgres://benchmark-writer@copy-host/$DISPOSABLE_DATABASE" \
  scripts/benchmark-gate \
    --report /tmp/indexing-benchmark.json \
    index \
    --chain ethereum-mainnet \
    --head-block <copy-head> \
    --walk-from-block <dense-range-start> \
    --walk-to-block <dense-range-end> \
    --expected-database-name "$DISPOSABLE_DATABASE" \
    --disposable-marker "$DISPOSABLE_MARKER" \
    --chain-rpc-url <selected-chain-production-rpc-url> \
    --allow-disposable-copy-writes
```

The acknowledgement flag, expected database name, disposable marker UUID, and
hydration RPC URL are required. Without them, argument parsing stops before
opening PostgreSQL. After connecting, the harness compares `current_database()`
with the typed name, requires the marker table, and requires a row whose UUID
and database name both match. It refuses writes before Interpret or Project on
any mismatch. The connection carries the same interpreter content-hash setting
as the phase runner, so ENSv1→ENSv2 migration correlation writes behave
normally. The incremental tick runs first, the Interpret redo runs second, and
the full Project rebuild runs last so the rebuilt projections match the
interpreted copy. The report records the opaque database identity token before
and after those measurements; a restart, failover, or listener change during
the indexing run is red.

## Run the API half

Keep traffic drained and indexing paused, then target the new API process. Run
the release measurement from an AWS `us-east` vantage point; the lookup p95
target is a regional/server-side requirement from that vantage. A host-local
run is a useful companion diagnostic for server and database latency, but is
not sufficient release evidence for the under-10-ms lookup p95.

```sh
BIGNAME_BENCHMARK_DATABASE_URL='postgres://benchmark-reader@prod-host/bigname' \
BIGNAME_BENCHMARK_API_BASE_URL='https://drained-generation-api.example' \
  scripts/benchmark-gate \
    --report /tmp/api-benchmark.json \
    api
```

This command cannot run an indexing operation. Every connection it opens sets
`default_transaction_read_only=on` and verifies `transaction_read_only=on`
before reading the corpus. Before load begins, it checks `/healthz` and requires
the target build SHA to match the clean harness checkout's `HEAD`, and requires
the interpreter content hash to match the harness.
It also requires the API-reported opaque database identity to match the
read-only database connection used to count and sample the corpus. The report
records both sides of that identity check. Configure the API database URL and
`BIGNAME_BENCHMARK_DATABASE_URL` to reach PostgreSQL through the same TCP
listener address and port; do not mix a Unix socket with TCP or use alternate
listen addresses for the two connections. The endpoint-scoped identity makes
an ambiguous access path red rather than treating two connections as proven to
reach the same serving database. Cursor seed requests cover top-level list cursors,
per-result reverse-lookup cursors, and the resolver route's nested bound-name
cursor.

After the complete timed endpoint sequence, the harness repeats the API build,
content hash, and running-database identity checks and rechecks the corpus connection.
A build or database-identity change during the run is red even when every
individual request succeeded. This detects a PostgreSQL restart or failover
and a deployment roll that changes the build; a same-build API process restart
is not distinguished from a continuously running process.

Before timed load begins, production mode sends seed requests and refuses to
run unless every route returns at least one populated result. Every paginated
route must also return a real continuation cursor. If the initial seed prefix
does not produce the required populated result or cursor, the harness continues
through the bounded target-database corpus before declaring the route red; a resumed
cursor request becomes part of the timed workload. For the records route,
populated means at least one requested key has an `ok` answer, not merely that
the name exists. This prevents a fast empty-result workload from counting as
release evidence.

## Decide green or red

Both commands must exit zero and both JSON reports must say `"green": true`.
For indexing, every timing, density, throughput, and memory assertion must pass.
For API load, every listed route must sustain at least 1,950 completed requests
per second, return 100 percent successful HTTP responses, and meet all three
latency percentiles. Missing corpus cardinality or a missing real resolver row
is red rather than silently reducing request variety.

Attach both reports and the recorded environment facts to the release record.
Each report includes the database name and the database server address observed
by PostgreSQL; the API report also includes the drained API URL.
Keep traffic drained after any red result. Fix the measured path, restore a
fresh disposable copy if indexing was partially run, and repeat the complete
half that failed. Restore traffic only after this gate and the ordinary health,
readiness, Verify, and public-edge checks are all green.

## Check the harness at small scale

This repository lane proves wiring only against the sanctioned local test
PostgreSQL container. It creates a uniquely named database, applies the checked-in
schema baseline, admits smoke manifests, and inserts registrar, registry, and
resolver logs through event fragments admitted by the checked-in production
manifests. It runs both real phase engines, starts the real API, exercises all
Tier 1 and Tier 2 routes
including status and resolver bound-name pagination, uses the smoke budget set,
and removes the database. It intentionally omits external JSON-RPC hydration;
the production indexing command requires and measures it.

```sh
BIGNAME_BENCHMARK_CARGO_PROFILE=dev \
  scripts/test-db -- \
  scripts/benchmark-gate smoke
```

The smoke budget set is intentionally too small to support a release decision.
It exists to keep safety checks, request construction, report generation, and
the end-to-end operating path executable without production data. CI runs the
benchmark crate's unit tests but does not run the production-scale harness.
