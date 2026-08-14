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
The wrapper requires `jq`, Python 3 with its standard-library TOML parser, and
`sha256sum`, resolves both executables from Cargo's JSON artifact output, and copies them to digest-addressed snapshots
before launching the harness or smoke API. The report digests therefore name
the stable snapshot paths the wrapper actually executes, not Cargo artifact
paths that a concurrent build can replace. For a release Cargo build, it
overrides ambient `CARGO_TARGET_DIR` and Cargo `build.target-dir` with
`target/benchmark-gate/<source-commit>`. The compile-time commit pins the
benchmark-harness unit, and the commit-keyed target directory pins every linked
engine crate to the same clean source tree. Development smoke builds continue
to honor ambient Cargo target-directory settings.

The commit-keyed directories under `target/benchmark-gate/` and their
digest-addressed snapshot stores are caches. They are safe to delete entirely
between runs; the next gate pays one cold rebuild, about one minute on the
reference host. On a space-constrained host, manually prune directories for
commits other than the one being gated. The wrapper does not prune them
automatically because concurrent runs at different commits may still be using
their own cache directories.

Each report records `rustc_version`, `rustflags`,
`cargo_encoded_rustflags`, `benchmark_binary_sha256`, and
`locally_built_api_binary_sha256`. The release wrapper refuses non-empty
`RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS`; clear either variable instead of
measuring custom code generation. For a release build, the build invocation
then sets both variables present-and-empty, suppressing Rust flags from
user-level, workspace, and target-specific Cargo configuration. It also refuses
non-empty `RUSTC`, `CARGO_BUILD_RUSTC`, `RUSTC_WRAPPER`, or
`RUSTC_WORKSPACE_WRAPPER`; pins an
absolute `rustc` path; and sets both wrapper variables empty, so user-level
compiler and wrapper configuration cannot select different tools. Compiler or
compiler-wrapper keys in the workspace `.cargo/config.toml` remain a named
refusal. The wrapper also refuses every non-empty ambient `CARGO_PROFILE_*`
variable, so environment overrides cannot change the release profile selected
from the workspace manifest. The wrapper records the selected compiler's `-Vv`
output. User-level `~/.cargo/config.toml` `[profile.*]` overrides remain
uninspected because the release wrapper does not isolate `CARGO_HOME`;
[issue #465](https://github.com/ensdomains/bigname/issues/465) tracks closing
that release-infrastructure gap. Other Cargo configuration,
notably linker selection, also remains uninspected. The recorded executable
digests distinguish the resulting local binary bytes. Smoke always runs the locally built API binary
named by its digest. For a production API run, that digest is a companion build
artifact; the remote target remains bound to the clean source commit through
`/healthz`, not to the local executable digest.

## What the gate measures

The indexing half measures the existing Interpret and
[Project](../glossary.md#projection) phase entrypoints. It requires a disposable
database copy because all three operations write derived state:

- one published-head Project re-apply at the selected head must finish within
  1 second, the live poll interval, including canonical-head record hydration;
- a full Project rebuild must finish within 6 hours inside an operator-scheduled
  window of at least 8 hours, leaving at least 2 hours of headroom; the measured
  wall-clock includes canonical-head record hydration;
- an Interpret redo over a dense-era range must sustain at least 500,000 blocks
  per hour;
- the dense range must contain at least 100,000 consecutive canonical blocks;
- that range must contain at least 8,000 retained raw logs per 1,000 blocks;
- the restored copy must contain at least 3 million supported current name rows
  before any timed projection work begins, owned by the selected chain's
  Project output;
- the Interpret walk must stay at or below 32 GiB peak RSS while using the
  configured 65,536-entry interpreter-state cache.

The published-head re-apply starts from a copy whose selected head is already
published under the current
[interpreter content hash](../glossary.md#interpreter-content-hash), then
measures Project deleting and rebuilding that head's affected projection rows.
It is not a live head-minus-one to head transition. It does not observe
first-time row insertion, newly affected-row discovery, or publication advancement;
[issue #467](https://github.com/ensdomains/bigname/issues/467) tracks the missing
production rewind capability needed to measure those costs. The checked-in
budget remains attached to this narrower, reproducible quantity.

The density check prevents a sparse historical range from producing a false
green throughput result. The current-name floor prevents a staging-sized copy
from producing a false-green full rebuild. The harness counts current names
before and after the rebuild, requires both totals to meet the floor, and
records both totals and the floor. Immediately before the Interpret walk, the
harness resets the Linux kernel's process high-water RSS counter to the current
RSS through `/proc/self/clear_refs`; failure to reset it is a hard error. This
excludes the earlier published-head Project re-apply or smoke-fixture setup
while still including the process memory already resident when the walk starts.
The 32 GiB cap uses the larger of the post-walk kernel `VmHWM` value and the
existing 20 ms RSS sampler peak. The report records both inputs separately for
diagnosis. It includes the bounded value cache introduced after the 94 GiB
out-of-memory incident and the smaller protocol-state maps that remain
resident; it is not merely the cache's estimated JSON size.

The API half sends each Tier 1 and Tier 2 REST route in
[`api-v2-routes.md`](../api-v2-routes.md) 2,000 requests per second for 60
seconds after a 10-second warmup. It loads 10,000 names and 10,000 distinct
address/name/relation combinations from the target projections, plus at least
1,000 populated subname parents, permission subjects, and successful primary
name claims. The resolver corpus is instead derived from the copy's active
resolver [source-family](../glossary.md) manifests:
`ens_v1_resolver_l1`, `ens_v2_resolver_l1`, and
`basenames_base_resolver`. Before constructing requests, the gate first selects
the latest canonical `SourceManifestUpdated` event that Project could consume
for each manifest ID at the copy's current head. Only after selecting the latest
event per manifest does it scope reconciliation to those three resolver
families. It reconciles that event set in both directions with stored active
manifest rows. A family
that Project admits from its latest event but whose stored row is missing or not
active is red; a family deprecated in both places remains outside the workload.
For every stored active row, the version, normalizer version, and payload must
equal the latest Project-eligible event. Each projected resolver row must cite
that exact event through `provenance.manifest_event_id`. A reconciliation or
event-ID mismatch is red and names the manifest, chain, source family, and both
stored and event versions where available; this detects a manifest
event that disagrees with its stored manifest row without changing manifest
synchronization behavior.

ENSv1 and Basenames families use currently applicable concrete `contracts`
declarations. ENSv2 families that declare `resolver_implementations` instead use
the latest canonical `Upgraded` event for each discovered proxy and admit the
proxy only when that event names a declared implementation. This mirrors
Project's implementation-based ENSv2 resolver admission and binds the projected
row to both the manifest event and upgrade event. The row's recorded upgrade
block number and hash must match that event, and its Project target cannot
predate the upgrade. A valid empty declaration set is reportable as zero; a
non-array value or an entry without an address is also preserved as a zero-count
report row but makes the gate red as a malformed resolver-manifest payload. The
failure names whether the stored row or the latest Project event supplied that
payload. Every active resolver family must contribute at least one currently
applicable, supported, API-visible request target; one healthy family cannot
conceal zero ENSv2 coverage. The head must be a completed
publication at the latest published/readable chain head under the
API's [interpreter content hash](../glossary.md#interpreter-content-hash); a
missing, running, stale, or invalidated head
makes the gate red. A concrete declaration whose `start_block` is no later than
that head must have a
supported `resolver_current` row from that active manifest version. The row must
pass the API's canonical-lineage read filter. The gate additionally requires
the row's declared block hash and number to identify the same readable lineage
block; the publication target cannot precede the latest applicable `start_block`
for that address. A missing or unsupported row, a row from another manifest
version, a row published before `start_block`, a canonically hidden row, or an
incoherent block anchor makes the run red and names its chain and source family.
If [eligible interpreted resolver evidence](../projections.md#live-maintenance)
exists but its projection row is missing or stale, rebuild Project. If that
evidence should exist but is missing or stale, repair chain intake or Interpret
first, then rebuild Project. A raw log-emitter address alone is not resolver
evidence. If the selected chain has no eligible evidence for the declared
address, a Project rebuild cannot create the row: check the manifest declaration
against the chain instead. A
declaration that starts after that head keeps its family visible in
the report but is not yet demanded. If no declaration is currently applicable,
the resolver workload cannot be constructed and the gate is red. Request volume
supplies resolver load scale, so there is no unrelated resolver row-count floor.
The report records total expected, currently applicable, and exercised resolver
address counts for each chain and [source family](../glossary.md). The existing
`declared_addresses` field means concrete declarations for ENSv1/Basenames and
implementation-admitted proxy addresses for ENSv2; future concrete declarations
remain visible without being treated as request targets.
`exercised_addresses` stays zero during corpus loading and on an early red
report. Once the resolver workload is constructed, it counts distinct currently
applicable declared addresses for which the endpoint has built and validated a
request variant; it is construction evidence, while the resolver endpoint's
request outcomes carry the timed send evidence. Name, parent,
address/name/relation, and successful primary-name samples are divided
deterministically across every active public namespace. An active namespace
with no seed of any one of those kinds makes the run red instead of letting
another namespace consume that corpus's entire limit.
Name, address, parent, permission, primary-name, and resolver samples use the
same [read-safe](../glossary.md#readable--read-safe) canonical-block checks for
each projection and its referenced identity rows as their API reads, so hidden
rows cannot satisfy a corpus floor or enter the timed request set.
The report records the name and parent counts contributed by each namespace,
and name-mode lookup batches alternate those namespace buckets. Before sampling
that corpus, it counts API-visible rows in active public namespaces in the
`name_current` and `address_names_current` tables and requires at least 3
million supported rows in each. Unsupported rows, rows from inactive
namespaces, and rows whose projection or referenced identity rows are not
read-safe are excluded because the API cannot return them as request seeds.
These floors leave headroom below the roughly 3.5 million names in the
production dataset while excluding staging-sized databases. The JSON report
records both API-visible supported-row totals and both floors. It varies names, addresses,
search text, relations, history scopes, sort order, page size, and any cursors
returned by seed requests, and uses the real resolver and namespace rows. The
address-name rotation retains the base listing request and deterministically
mixes `include=role_summary`, `dedupe=registration`, and their combination into
the timed requests. The sampled corpus does not identify addresses known to
span repeated registrations, so registration-deduplicated variants use the
same production-distribution address subjects as the other variants. The search
workload keeps explicit-namespace requests for every seed and adds bare
requests for a deterministic half; both forms cover prefix and contains
matching, and production mode requires a populated bare-search seed response.
Bare requests are a minority of the mixed search pool, so a regression limited
to the bare path can move p95 or p99 without moving p50. The first production
run after this coverage was added may also show higher search tails: bare search
derives the public namespace set for each request and revalidates it after the
read, while the earlier workload measured explicit-namespace requests only.
The lookup mix covers 1, 10, 100, 250, and 1,000 inputs per batch, with large
batches weighted toward the tail. Each timing starts at dispatch, immediately
before the request task is spawned, so executor queue delay after spawn is
included. Time spent in the single dispatcher before that point is excluded and
is bounded separately by the achieved-throughput floor. Timing ends only after
the complete response body has been read. The report contains
achieved throughput, success rate, and p50, p95, and p99 for each route.
Diagnostics, GraphQL compatibility, health, and documentation routes are
outside this traffic gate; they retain their ordinary functional checks.

`POST /v2/lookup` keeps the latency-sensitive 5/10/25 ms p50/p95/p99 limits.
Point reads use either 10/25/50 ms or 25/75/150 ms limits. List reads use
25/75/150 ms or 50/150/300 ms limits, depending on query shape. The budgets
file is authoritative for the exact route mapping.

## Prepare the targets

Record the release commit, [interpreter content hash](../glossary.md#interpreter-content-hash), host CPU and memory,
PostgreSQL version and settings, database size, selected chain and head, dense
Interpret range, and whether the API target is host-local or reached over a
network. Keep those facts with both JSON reports. The API report records the
normalized base URL without username or password userinfo. Database and chain
RPC URLs are never included in either report.

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
writes. Prepare the marker immediately before the indexing run: it expires 12
hours after `prepared_at` for purposes of starting the gate, so a durable marker
left on an old copy cannot authorize a later run. A timestamp more than five
minutes ahead of the database clock is also refused; correct clock skew instead
of widening this window. Once that startup check passes, marker age places no
limit on how long the measurement may run.

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
with the typed name, requires the marker table, requires a row whose UUID and
database name both match, and checks marker freshness on one direct preflight
connection before creating the pool. Startup refusal therefore returns its
specific reason immediately rather than waiting for the pool deadline. It
refuses writes before Interpret or Project on any mismatch. The connection
carries the same interpreter content-hash setting as the phase runner, so
ENSv1→ENSv2 migration correlation writes behave normally.

The writable database URL hostname must resolve to one stable listener address
and port for the complete run. Connect directly or through a session-affine
proxy to one backend, meaning each open client connection remains on that
PostgreSQL backend until it disconnects. Do not use transaction or statement
pooling, a session-mode pooler with multiple backend hosts, or a failover proxy
that can silently move an open client connection between backends; a
connection-setup check cannot observe that kind of switch.

Every new pooled connection repeats the expected-name, marker-table, UUID, and
database-name checks and requires its opaque database-instance token to exactly
match the direct preflight connection before it can issue a query. Losing or
retargeting a database connection therefore cannot bypass the preflight.
Freshness is not repeated because a passing production run may legitimately
exceed 12 hours. A restart, failover, or retargeted listener changes the token,
so a replacement connection is refused before it can query. The pool retries a
refused replacement until its deadline, so a mid-run instance or marker
mismatch may surface as a pool timeout rather than the startup check's specific
message. It does not claim to detect an in-place logical restore that preserves
the database instance identity. The published-head Project re-apply runs first,
the Interpret redo runs second, and
the full Project rebuild runs last so the rebuilt projections match the
interpreted copy. The report's pre/post opaque database identity fields are
recorded evidence and a defense-in-depth comparison. In the supported stable
connection setup, a restart, failover, or listener change that requires a new
connection is red at the per-connection check and may stop the run with a pool
timeout before a report is written.

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
before reading the corpus. A standalone connection captures the database
instance identity before the pool opens, and every new pooled connection must
match it. Before corpus sampling or timed load, the harness refuses an active
ENS chain, or the Basenames `base-mainnet` serving-authority chain, when its
`chain_phase_state.interpret.redo_in_progress` flag is set. Complete or roll
back the Interpret redo, take a fresh copy, and rerun the gate; otherwise
address-mode lookup can omit the affected namespace.
The same namespace/chain pairs, flag, and PostgreSQL row version (`xmin`) are
rechecked after every timed endpoint window. An Interpret redo started during
the API half therefore makes the run red at the next boundary, even if it
completed and cleared the flag inside the endpoint window, instead of silently
narrowing lookup requests. The harness also checks `/healthz` and requires
the target build SHA to match the clean harness checkout's `HEAD`, and requires
the interpreter content hash to match the harness.
It also requires the API-reported opaque database identity to match the
read-only database connection used to count and sample the corpus. The report
records both sides of that identity check. Configure the API database URL and
`BIGNAME_BENCHMARK_DATABASE_URL` to reach PostgreSQL through the same TCP
listener address and port. Each hostname must resolve to that one stable
address for the complete run; do not mix a Unix socket with TCP, use alternate
listen addresses, or put either connection behind a pooler that can choose
different backend hosts. The endpoint-scoped identity makes an ambiguous access
path red rather than treating two connections as proven to reach the same
serving database. Cursor seed requests cover top-level list cursors,
per-result reverse-lookup cursors, and the resolver route's nested bound-name
cursor.

After every timed endpoint window, the harness repeats the Interpret namespace
and chain membership, redo flag, PostgreSQL row-version, API build,
content-hash, and running-database identity checks and rechecks the corpus
connection.
A build or database-identity change during the run is red even when every
individual request succeeded. A failed boundary probe is recorded as a red,
endpoint-named report failure rather than discarding the run's evidence. The
read-only pool refuses a replacement connection to a different instance, so a
PostgreSQL restart or failover may instead stop the run with the named refusal
on stderr followed by a pool timeout. A deployment roll that changes the build
is also red; a same-build API process restart is not distinguished from a
continuously running process. A transient
same-build identity flip entirely inside one endpoint window also cannot be
distinguished; the boundary checks limit that blind window to one route's
measurement rather than the complete load sequence.

Before timed load begins, production mode sends seed requests and refuses to
run unless every route returns at least one populated result. Every paginated
route must also return a real continuation cursor. If the initial seed prefix
does not produce the required populated result or cursor, the harness continues
through the bounded target-database corpus before declaring the route red; a resumed
cursor request becomes part of the timed workload. For the records route,
populated means at least one requested key has an `ok` answer, not merely that
the name exists. For lookup, populated means at least one address-kind result
has status `ok` and a non-empty `records` list; a forward name result or an
empty reverse result is not evidence that reverse lookup was measured. This
prevents a fast empty-result workload from counting as release evidence. Timed
records responses are also classified as populated or empty. At least 1 percent
must contain an `ok` requested record, and the report records the observed
populated share and its checked-in floor.

The timed primary-name workload always sends `source=indexed`. Omitting
`source` asks that route for both its indexed answer and live verification; for
ENS coin type 60, verification can require two or three sequential mainnet RPC
calls. Sending that shape at 2,000 requests per second would benchmark the RPC
provider, issue roughly 120,000 live-verification requests and several times
that number of RPC calls in one timed window, and cannot fit the route's
checked-in 10/25/50 ms budget. It is deliberately outside the timed release
measurement. Before the timed windows, the harness instead sends an untimed
functional check for at most ten ENS/coin-60 tuples with `source` omitted. Each
check must return a well-formed `2xx` response with indexed and verified answers
in that order, or the route's documented whole-request `409 stale` response.
Any other status or error code names the tuple and makes the run red. Production
mode also requires at least one ENS coin-type-60 tuple; smoke mode may send zero.
The JSON report always records the number sent and the HTTP status/error-code
outcomes. Name and records requests also send `source=indexed`, which matches
those routes' default; only the primary-name route has a broader default.

Per-request namespace readiness cannot be fully precomputed by the corpus SQL.
Restricting address and primary-name seeds to active public namespaces, plus the
Interpret-redo preflight and endpoint-boundary checks above, closes the
confirmed state that silently narrowed address-mode lookup. The remaining
serving-side readiness asymmetry is tracked in
[issue #449](https://github.com/ensdomains/bigname/issues/449).

## Decide green or red

Both commands must exit zero and both JSON reports must say `"green": true`.
For indexing, every timing, density, throughput, and memory assertion must pass.
For API load, every listed route must sustain at least 1,950 completed requests
per second, return 100 percent successful HTTP responses, and meet all three
latency percentiles. The records route must also meet its 1-percent populated
response floor. Missing corpus cardinality or a missing active namespace is red
rather than silently reducing request variety. Resolver coverage is red when a
stored active declaration is missing, unsupported, hidden by the API's
canonical-lineage read filter, or rejected by the gate's additional block-anchor
integrity checks at the current Project head. It is also red when the active
resolver families contribute no currently applicable resolver address. The red
JSON report retains the per-chain and per-family resolver counts and the named
refusal.

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
