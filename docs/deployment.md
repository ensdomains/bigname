# Deployment

Project-specific terms used below (checkpoint promotion, coverage frontier,
watched tuple, companion checks, retention generation, admission epoch) are
defined in the [glossary](glossary.md); "promotion" in this document always
means checkpoint promotion, the chain-safety sense.

The production container image contains the three runnable bigname binaries:

- `bigname-api`
- `bigname-indexer`
- `bigname-worker`

The image entrypoint accepts one service selector:

```sh
docker run --rm ghcr.io/ensdomains/bigname:latest api
docker run --rm ghcr.io/ensdomains/bigname:latest indexer
docker run --rm ghcr.io/ensdomains/bigname:latest worker
docker run --rm ghcr.io/ensdomains/bigname:latest migrate
```

The default command is `api`. Raw binary invocations are also supported:

```sh
docker run --rm ghcr.io/ensdomains/bigname:latest bigname-api print-openapi
docker run --rm ghcr.io/ensdomains/bigname:latest bigname-worker inspect watch-plan --json
```

## Fresh Server Compose

1. Install Docker and Docker Compose.
2. Copy `.env.server.example` to `.env.server` and change the placeholder passwords.
3. Set `BIGNAME_IMAGE` to the image tag to run.
4. Set `BIGNAME_API_CHAIN_RPC_URLS` with one `<chain>=<url>` entry for every
   chain expected by an active/shadow manifest or stored checkpoint. This
   variable is load-bearing for `/v1/status` and `/v2/status` readiness. The
   API starts and serves local readiness when entries are absent, but logs a
   `WARN` naming every missing chain and keeps those status chains fail-closed
   as `degraded`.
5. Start the stack:

```sh
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The server compose file starts PostgreSQL, a one-shot migration service, the API,
the indexer, and the worker. The API listens on the host port from
`BIGNAME_API_PORT` and answers readiness at `/healthz`. Set
`BIGNAME_API_HOST` to control the host bind address; production public-edge
deployments normally set it to `127.0.0.1` and expose traffic through the Caddy
override documented in `docs/production.md`.

The indexer and worker healthcheck commands verify that applied database
migrations exactly match the migration set compiled into the running binary
and that the checked process instance's main-loop heartbeat is recent. They
fail closed for missing, failed, checksum-mismatched, or newer unknown
migrations, for a loop that never registered, and for a loop whose heartbeat
exceeds the service-specific maximum age. The default maximum is 20 seconds;
set `BIGNAME_INDEXER_HEARTBEAT_MAX_AGE_SECS` and
`BIGNAME_WORKER_HEARTBEAT_MAX_AGE_SECS` in proportion to custom poll
intervals. Worker rebuild operations with no safe inner batch boundary use a
named phase row and the independently tunable
`BIGNAME_WORKER_REBUILD_PHASE_MAX_AGE_SECS` (default 43,200); set the matching
API interpretation with `BIGNAME_API_WORKER_REBUILD_PHASE_MAX_AGE_SECS`.
`docker-compose.server.yml` maps stable per-service instance IDs from
`BIGNAME_INDEXER_HEARTBEAT_INSTANCE_ID` and
`BIGNAME_WORKER_HEARTBEAT_INSTANCE_ID`, defaulting to `indexer` and `worker`.
This lets a recreated single-writer service retire unfinished non-process
heartbeat rows from the prior container during registration.
During rolling upgrades, running `migrate` before recreating old
service containers can therefore mark those old indexer/worker containers
unhealthy until they are replaced with the matching image; treat that as
version skew, not evidence that PostgreSQL is down.

The API `/healthz` HTTP status and `api_status` field cover only the serving API
process and its `SELECT 1` database probe. Its aggregate `status` and `loops`
object still require recent indexer and worker evidence, using
`BIGNAME_API_HEARTBEAT_MAX_AGE_SECS` (default 20), so a planned indexer restart
or long worker phase stays visible without making the API container or public
edge unhealthy. The status routes use the API chain RPC mapping for an
asynchronous cached network-head probe. Tune its provider timeout, refresh
interval, cache TTL, and ingestion block/time limits with the
`BIGNAME_API_STATUS_PROVIDER_*` and `BIGNAME_API_STATUS_MAX_*` variables in
`.env.server.example`; the default maximum block lag is 30.

The primary-name hardening migration is safe in that ordering. Database triggers
make a preceding-release projection writer join the new per-tuple fence and
repeat invalidation after a changed write; an overlap that would reverse the
old writer's table/advisory lock order aborts that projection transaction with
retryable SQLSTATE `40001`. The two retention indexes are separate
no-transaction `CREATE INDEX CONCURRENTLY` migrations, as is the tuple-
invalidation lookup index, so their builds do not hold execution-table writes
behind a transactional `SHARE` lock.

The two compatibility triggers are temporary rolling-upgrade support. Once the
fleet is fully upgraded and no writer from before the #233 primary-name
hardening release can run, including from a rollback image,
`primary_names_current_tuple_fence_before_write` and
`primary_names_current_cache_invalidation_after_write` can be removed. The
follow-up release must first remove those trigger names from the full-rebuild
writer's disable/enable list. During that deployment, stop every worker that
still references the compatibility triggers, apply a checked-in migration that
drops both triggers, and then start only the upgraded workers. Do not remove the
triggers manually or add the code change or drop migration to this rollout.

### Resolver-profile replay after an upgrade or compaction

The raw-log retention migration marks every pre-existing chain as generation
one because the database cannot prove that its staged ENSv1 resolver history was
never compacted. The resolver-profile queue migration does not schedule
historical repair for those chains, and the first authority-journal capture is a
baseline rather than a change. A later resolver code-hash or manifest/discovery
authority change is still recorded as pending work, but the indexer fails that
repair closed before publishing projection invalidations or acknowledging the
queue generation.

The current release has no in-place ENSv1 resolver-profile coverage proof and
no versioned adapter-snapshot import. Running an ordinary ranged backfill does
not make a generation-one or later corpus acceptable to this repair path. The
current recovery is a full database rebootstrap from the checked-in migrations
and configured historical bootstrap into a new generation-zero corpus. Do not
delete the pending queue row or manually advance `processed_generation`; that
would discard an unperformed absence-aware repair.

### Projection replay version upgrades

A replay-version change that widens a projection's consumed input set is not a
worker-rolling-compatible upgrade. Deployment automation must drain public API
traffic, stop every old worker, and confirm no old worker process remains before
starting any worker from the new image. Start one new worker, wait until every
current projection family has a marker for the new replay version and
`projection_invalidations` is empty, and only then start or undrain the API.
The supported deployment has one active worker; do not overlap old and new
workers during this handoff. An indexer from the same release may continue
ingesting while workers are stopped and during replay; its durable changes will
be consumed after replay handoff. Replace an indexer from before this change
before admitting the new worker because its unstamped writes are rejected once
enforcement activates. Apply the migrations for the
[projection replay-version fence](glossary.md#projection-replay-version-fence)
at a staging-quiet moment: the installing migration's `ALTER TABLE` must wait
behind any in-flight projection publication transaction. Between the successful
migration and the process swap, run no repair tooling. A pre-fence repair can
hold the singleton row exclusively; for that interval, every unstamped
protected statement that reaches the trigger's non-waiting lock path fails with
SQLSTATE `55000`.

The database also enforces this version boundary with the
[projection replay-version fence](glossary.md#projection-replay-version-fence).
The singleton
`current_projection_full_replay_input_revision` row stores the minimum
projection replay version admitted to projection writes and whether enforcement
has been activated. Every new process stamps its compiled replay version on
each database connection. Applying the migration leaves enforcement inactive;
the first fence-aware replay-write transaction from the new release activates
it and raises the minimum under an exclusive row lock. This is not limited to
the automatic worker's first replay-loop pass. It also includes a standalone
one-shot rebuild clearing its family marker and an indexer-side repair advancing
the full-replay input revision, including name-surface repair and a Base
normalized-event rederive reset.

Database triggers on the invalidation queue and cursor, current projection
tables and companion tables, and durable replay state normally take the shared
side of that lock before every write. An older write that began first therefore
commits before activation and is included in the new replay. After activation,
a lower version or a connection with no stamp from a pre-fence binary receives
a loud database error before it can claim, extend a claim lease, publish,
complete queue work, or change replay state. Fence-aware workers treat this and
every other fatal fence error, including missing singleton state or an invalid
stamp, as process-fatal and exit. A binary from before the check existed cannot
be retrofitted: its protected writes remain blocked, but it may keep retrying
and may continue updating its health heartbeat until the operator or supervisor
terminates and replaces it. A current, validly stamped statement that finds
replay admission holding the singleton receives the one retryable admission
error instead of a fatal fence error.

The invalidation queue is also an indexer-to-worker handoff. Its stamped DML at
`READ COMMITTED` reads the committed activation state and version floor without
taking the singleton row lock, avoiding a lock-order cycle when ingestion
already owns a staging input journal lock. An enqueue that commits before
replay captures the journals participates in the replay drift check; an enqueue
that commits after that capture remains durable post-replay apply work. Roles,
database defaults, and explicit transactions used by stamped queue producers
must therefore retain PostgreSQL's `READ COMMITTED` isolation; the trigger
fails a queue-writing statement at any isolation level with a longer-lived
snapshot. Queue `TRUNCATE` and unstamped queue writers retain the non-waiting
singleton-lock path.

The worker also compares its compiled version with the durable minimum and
every persisted attempt, checkpoint, and marker inside claim and replay
transactions. These cooperative checks provide a typed fatal error; the
database triggers are the enforcement boundary that also catches a binary
deployed before the check existed. This turns accidental mixed-version overlap
into a database-enforced stop, but it does not remove the API drain: operators
must still wait for current markers and an empty invalidation queue before
serving reads.

#### Manual protected-table repair sessions

Before changing any protected table from `psql`, read the active floor:

```sql
SELECT
    projection_replay_version_floor,
    projection_replay_version_fence_active
FROM current_projection_full_replay_input_revision
WHERE singleton;
```

Stamp only the repair transaction at that floor or a higher version:

```sql
BEGIN;
SET LOCAL bigname.projection_replay_version = '<floor-or-higher>';
-- supervised protected-table repair
COMMIT;
```

`SET LOCAL` clears the stamp at commit or rollback, including after a repair
error. The stamp admits that transaction to the same database boundary as a
process; it does not authorize a repair or make unsafe SQL correct. Do not lower
or deactivate the fence merely to make a manual write succeed.

#### Emergency rollback after activation

A previous fence-aware release with a lower compiled replay version is rejected
before it writes, just as a pre-fence binary with no stamp is rejected. Lowering
an activated floor is destructive operator surgery, not a normal image rollback.
Stop API traffic and every indexer, worker, and repair session; take a restorable
database backup; and verify that the target binary accepts the current schema
and migration set. If it does not, restore a compatible database instead.

The cooperative version check reads the floor plus persisted versions from
exactly `current_projection_replay_status`,
`current_projection_replay_attempt`, and
`current_projection_staging_checkpoints`. Lowering the singleton alone is
therefore insufficient. In one supervised transaction, stamp the session at the
current floor or higher, lock the singleton, remove every row newer than the
rollback target from all three tables, and only then lower the floor while
leaving enforcement active:

```sql
\set rollback_version 9
\set operator_version 10

SELECT projection, replay_version, stage_tables
FROM current_projection_staging_checkpoints
WHERE replay_version > :rollback_version;

BEGIN;
SET LOCAL bigname.projection_replay_version = :'operator_version';

SELECT projection_replay_version_floor
FROM current_projection_full_replay_input_revision
WHERE singleton
FOR UPDATE;

DELETE FROM current_projection_replay_status
WHERE replay_version > :rollback_version;

DELETE FROM current_projection_replay_attempt
WHERE replay_version > :rollback_version;

DELETE FROM current_projection_staging_checkpoints
WHERE replay_version > :rollback_version;

UPDATE current_projection_full_replay_input_revision
SET
    projection_replay_version_floor = :rollback_version,
    projection_replay_version_fence_active = true
WHERE singleton;

COMMIT;
```

Record the checkpoint `stage_tables` values before deletion and remove those
exact logged staging tables while all processes remain stopped; deleting the
checkpoint rows does not drop them. If any target-or-older progress cannot be
proven compatible with the rollback release, discard that progress too rather
than letting it satisfy replay. Start only the target fence-aware worker, force
all current projection families through that release's replay, drain
`projection_invalidations`, and validate reads before undraining the API.
This surgery discards replay progress and can temporarily leave projection rows
written with newer semantics. It does not reverse schema migrations, repair raw
or normalized inputs, or prove that the older release is data-compatible.

Replay version 9 is such an upgrade: it forces the full permission cutover that
discovers canonical zero-event resources and seeds
`permissions_current_publication` version 2 plus its read-consistency revision.
A version-9 all-current replay also republishes `name_current` so every name whose
current binding is an ENSv1 wrapper resource stores explicit unsupported control
instead of stale pre-wrap control facets. Exact-name reads have no equivalent of
the permission publication compatibility gate: before that `name_current` replay
finishes, a row last written before the wrapper-control mask was introduced can
still expose its pre-wrap control summary.
A version-8 completion marker cannot satisfy this upgrade because an upgraded
database may otherwise retain its apply cursor, skip full replay, and leave the
new publication artifact empty. Version-8 and version-9 workers must never
overlap, and the API must remain drained until pending invalidations are empty
and every version-9 marker, including `name_current`, is current. Replay version
8 was the preceding upgrade that backfilled
`permissions_current_resource_summary` and introduced the current-wrapper
unsupported control summary; version 9 retains those behaviors and the version-7
ENSv2 exact-name-profile evidence. The replay-version marker prevents a new
worker from trusting old bootstrap completion. The version-8/version-9 handoff
predated the database fence and is the motivating historical example of the
former documentation-only protection. Once the fence migration is installed,
the first replay from the new release rejects protected writes from any
still-running pre-fence worker or rollback image because its database
connections have no replay-version stamp. Future replay-version handoffs
therefore have structural writer enforcement in addition to the documented
rollout procedure. Container healthchecks still report version skew, but the
database—not the healthcheck—prevents a stale writer from committing. The stop
and drain steps remain required rollout and read-freshness gates.

The version-9 full permission cutover writes publication version 2 and advances
its monotonic `data_revision` atomically with holder rows and per-resource
summaries. Compatible keyed permission rebuilds advance that revision in their
own row-and-summary transaction without creating or upgrading a missing or old
compatibility version. Permission-backed API reads capture the revision before
reading and verify it again before returning; a change fails closed with `409
stale`. The revision is request-coherence evidence only, not freshness. The API
does not read `current_projection_replay_status`, and operators still wait for
all replay markers and pending invalidations before undraining traffic.

### Stored-lineage coverage frontier upgrade

Migration `20260716122000_stored_lineage_coverage_frontiers.sql` creates the
durable stored-lineage [coverage frontier](glossary.md) header and normalized
requirement tables. It is
schema-only: it does not seed proof from a chain checkpoint, stored lineage,
projection state, prior process memory, backfill job identity, or migration-time
scan. Each upgraded chain therefore starts cold and publishes proof format
`stored_lineage_coverage_v1` only after the new indexer verifies the current
candidate against durable `backfill_coverage_facts`.

This indexer change is not compatible with a mixed old/new rolling deployment.
Stop every old indexer before applying the migration and do not restart one after
a new indexer has begun publishing frontier revisions. An old process can still
promote from its process-local memo and does not participate in the durable
compare-and-swap fence. API and worker processes do not read these tables, so
this upgrade alone does not require a projection rebuild or API drain; stored-
lineage promotion may pause while the first cold proof runs. Migration
healthchecks expose version skew but, as elsewhere, do not stop an already
running old process.

On the first large-gap promotion for a chain, expect a complete, chunked coverage
verification from the earliest required watched interval at least through the
current promotion target and, when covered, through the chosen stored anchor.
Missing or stale facts, epoch drift, and a frontier publication conflict leave
the checkpoint unchanged. The indexer reloads and replans on a compare-and-swap
conflict, but never promotes from the losing unpublished candidate. Once a proof
publishes, restarts and additional new-version indexer processes reuse it;
unchanged history is not verified again.

The indexer loads exactly one manifest profile root. Use `/app/manifests/mainnet`
for the mainnet profile or `/app/manifests/sepolia` for the ENSv2 Sepolia
profile. Do not point one runtime at both manifest roots.

If `BIGNAME_INDEXER_CHAIN_RPC_URLS` is unset, the indexer still syncs
manifest/watch state, but provider-backed live ingestion remains idle. Current
bootstrap RPC support accepts `http://` and `https://` endpoints.

The API service uses its own JSON-RPC mapping both for indexing-status
network-head readiness and for live execution. Every chain expected by the
status chain set needs an entry; startup logs a loud warning with the missing
chain names, and their status remains fail-closed. Ethereum mainnet is also
required for live ENS verified resolution and the ENS/60 primary-name
on-demand reverse/forward RPC fallback, configured as
`BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. `GET /v1/profiles/names/{name}`
in `mode=verified|both`, `GET /v1/names/{namespace}/{name}/records` when it
needs verified values, `GET /v2/names/{name}?source=verified`, and
`GET /v2/names/{name}/records` in `source=verified|auto` first use matching
persisted execution output; when supported ENS verified-resolution selectors are
missing from execution storage, the API executes them against the selected
exact-name snapshot, persists the trace/outcome, and then returns the result.
With no `at` or `chain_positions` selector, that target is `consistency=head` at
the latest stored Ethereum checkpoint, not provider latest. Missing API provider
configuration or a JSON-RPC response recognized as unable to serve the selected
block must fail closed; v1 returns `409 stale`, while v2 product routes return
their documented in-band `status=stale`/`failure_reason` envelope. For on-demand
verified record resolution, expiration of an API-configured provider connect or
total-response deadline instead returns and persists the affected selector's
in-band execution-failure class. Other record-resolution transport failures,
including DNS, TLS, and connection-reset errors, abort without persisting an
outcome so the next read retries. Neither generation may fall back to declared
record cache for verified values. The indexer RPC setting and Reth DB source
settings do not satisfy this API live-execution provider requirement by
themselves.

When `GET /v1/primary-names/{address}` defaults to
`namespace=ens&coin_type=60` and the persisted tuple is missing, a configured
API provider lets the route read the Ethereum Mainnet reverse resolver at the
latest stored checkpoint and, in verified modes, validate the claimed name's
`addr:60` value through the ENS Universal Resolver at that same block hash. A
zero resolver, empty name, wrong namespace, unnormalizable reverse name, or
empty forward `addr` is a supported fallback miss. Missing provider
configuration, a completed reverse-provider JSON-RPC failure, a malformed
response, or a configured provider timeout returns the route's in-band
execution-failure class instead of being reported as `not_found`. In verified modes, the API
persists the reverse result, normalization gate, optional forward call, and
outcome before responding; an identical request at the same selected checkpoint
can reuse that trace without another provider call. Forward-verification
completed JSON-RPC failure, malformed response, or configured timeout after a
reverse claim likewise returns `verified_primary_name.status=execution_failed`.
A configured RPC deadline that
expires remains a persisted in-band execution failure; DNS, TLS, connection
reset, and other non-timeout transport failures abort before persistence so the
next request retries. On a freshly migrated database with no stored Ethereum
checkpoint, an eligible tuple-miss request returns `409 stale` until indexing
publishes its first checkpoint.

The worker bounds route-local primary-name execution storage with
`BIGNAME_WORKER_PRIMARY_NAME_ROUTE_CACHE_RETENTION_CHECKPOINTS` (default
`50000`) and
`BIGNAME_WORKER_PRIMARY_NAME_ROUTE_CACHE_PRUNE_BATCH_SIZE` (default `5000`).
See [`storage.md`](storage.md#execution-storage) for the exact cleanup scope.

### API bounds for public undrain

Every API route is covered by the request deadline. `/healthz` bypasses the
shared HTTP load-shed ceiling and uses a separate health ceiling. Its `SELECT 1`
runs on a persistent one-connection readiness pool instead of the request pool,
and the entire database check is limited to two seconds. Consequently both
HTTP-concurrency saturation and exhaustion of the request database pool are
answered within the compose probe's five-second window: a healthy but busy
process still returns `200` with `status="ready"`, while a failed or timed-out
readiness connection returns `503` with `status="degraded"`. The database
round-trip is retained deliberately so a process that cannot reach PostgreSQL
does not report ready. The separate health ceiling prevents unbounded probe
work. The status routes retain global admission because their aggregate
database query can be expensive under backlog. Requests whose route and
source/mode can initiate verified execution also pass through a separate
concurrency ceiling and, when enabled, a client token bucket keyed by an IPv4
address or IPv6 `/64` before handler work starts. Configure the API service with
these values before public undrain; binary defaults remain generous for
development and test workloads. The readiness connection is additional to the
primary-pool limit set by `BIGNAME_DATABASE_MAX_CONNECTIONS`.

| Environment variable | Binary default | Undrain starting value |
| --- | ---: | ---: |
| `BIGNAME_API_REQUEST_TIMEOUT_MS` | `30000` | `30000` |
| `BIGNAME_API_DB_STATEMENT_TIMEOUT_MS` | `25000` | `25000` |
| `BIGNAME_API_MAX_IN_FLIGHT` | `1024` | `256` |
| `BIGNAME_API_HEALTH_MAX_IN_FLIGHT` | `4` | `4` |
| `BIGNAME_API_VERIFIED_EXECUTION_MAX_IN_FLIGHT` | `128` | `16` |
| `BIGNAME_API_RPC_CONNECT_TIMEOUT_MS` | `2000` | `2000` |
| `BIGNAME_API_RPC_TIMEOUT_MS` | `8000` | `8000` |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_PER_SECOND` | `0` (off) | `1` |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_BURST` | `10` | `5` |
| `BIGNAME_API_VERIFIED_RATE_LIMIT_MAX_CLIENTS` | `65536` | `65536` |
| `BIGNAME_API_TRUST_X_FORWARDED_FOR` | `false` | `true` |

The binary leaves IP rate limiting off because there is no authenticated stable
client identifier and shared addresses are common. The public-undrain values
enable it as an operational starting policy. Forwarded client addresses are
ignored unless `BIGNAME_API_TRUST_X_FORWARDED_FOR=true`; the undrain example may
enable that trust because it binds the host-published API port to `127.0.0.1`
and sends public traffic through Caddy. See
[`production.md`](production.md#api-request-bounds) for response codes,
fallback identity behavior, and tuning guidance.

The worker may use the same provider shape for projection-owned ENSv1 text
hydration, configured as
`BIGNAME_WORKER_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. During automatic
all-current projection replay, the first worker handoff to continuous apply, and
continuous `record_inventory_current` invalidation apply, the worker hydrates
legacy `text:<key>` values only after the normalized-event row has been rebuilt
and only at the stored chain checkpoint selected for the current projection.
Missing worker RPC configuration leaves those projection cache entries explicit
`unsupported`; it must not make the worker query provider `latest` or mutate
normalized events.

The same worker RPC setting enables projection-owned legacy ENSv1 reverse
resolver hydration for `primary_names_current`. The built-in configured legacy
reverse resolver set can be extended with
`BIGNAME_INDEXER_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES` on the indexer and
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_RESOLVER_ADDRESSES` on the worker;
deployment-specific additions must use the same comma-delimited address list in
both places so live direct-call observation and worker hydration stay aligned.
Operators may tune the Multicall3 target and batch size with
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_MULTICALL3_ADDRESS` and
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_BATCH_SIZE`. The pass runs
after replay/bootstrap and during continuous apply, evaluates reverse-claim,
resolver, claim-name, and retained direct-call inputs at or behind the stored
Ethereum Mainnet checkpoint, and writes only `primary_names_current`.

Deployments with a same-host Reth database can layer
`docker-compose.reth-db.yml` on top of the server compose file. Set
`BIGNAME_INDEXER_RETH_DATADIR_HOST` to the host Reth datadir,
`BIGNAME_INDEXER_RETH_DATADIR_CONTAINER` to the in-container mount path, and
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` to comma-delimited `<chain>=<path>`
entries that use that in-container path. `BIGNAME_INDEXER_CHAIN_RPC_URLS` may
still provide RPC sources for other active watched chains, for example
Base Mainnet while Ethereum Mainnet uses same-host Reth. Do not configure the
same chain in both settings; duplicate provider sources fail at startup. Reth
DB sources remain operational intake sources; they do not replace bigname raw
facts or normalized-event `raw_fact_ref` identities.
The repository Dockerfile builds `bigname-indexer` with the
`bigname-indexer/reth-db` Cargo feature so this override keeps the Reth provider
path available. Custom images that omit that feature fail fast when
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` is set, with a rebuild instruction
instead of silently falling back to JSON-RPC or dropping the provider.
The indexer opens the Reth database through Reth's read-only provider API, but
the container mount is writable because MDBX cooperative read-only opens still
need writable lock/coordination files in the datadir.
The override defaults the indexer to `BIGNAME_INDEXER_RETH_DB_USER=0:0` because
container-managed Reth datadirs are commonly `root:root`; operators may set a
less-privileged UID/GID after granting that identity write access to
the Reth datadir's MDBX lock files. The override also raises `nofile` because
Reth's read-only RocksDB provider can keep thousands of SST files open.
It uses the host PID/IPC namespaces and bypasses the image's `tini` entrypoint
so the indexer process owns PID 1; Reth's live MDBX read-only open can fail
from the default `tini` child process.

The Reth DB reader has ignored live verification tests for code-hash decode
checks against the local Reth datadir, JSON-RPC `eth_getCode`, and stored
`raw_code_hashes`. They are not CI tests because they require the live Ethereum
Mainnet archive node and bigname storage. These tests only read provider and
storage data. The green windowed test defaults to the 688-row known-correct
island sample and is overrideable with comma-separated block numbers in
`BIGNAME_INDEXER_TEST_RETH_CODE_HASH_COMPARE_BLOCKS`. The latest-row-per-watched
address check is the post-remediation acceptance gate for the supervised padded
`raw_code_hashes` correction run; before that run, failures reflect the known
padded corpus rather than a new reader regression. The full-table audit is
outside this harness and must also be green after remediation. Local observation,
not an upstream Reth guarantee: on this host, fresh read-only verifier opens have
lagged the node persistence horizon by roughly 1-1.5k blocks, so near-head
compare blocks can fail spuriously.

The raw code-hash correction's mandatory per-run RPC oracle is `eth_getCode` by
block hash, which the tool uses to compare both bytecode hash and byte length
against the Reth DB re-derived value. Keep this host's archive Reth historical
proof window widened for the optional `eth_getProof` spot-check when the node
can serve it. The corpus reaches roughly 470,000 blocks behind head, so set
`--rpc.eth-proof-window` to at least 500,000; pinned Reth exposes that argument
and caps it at `MAX_ETH_PROOF_WINDOW` 1,209,600 blocks
`(upstream: .refs/reth/crates/node/core/src/args/rpc_server.rs:L601 @ reth@88505c7)`
`(upstream: .refs/reth/crates/node/core/src/args/rpc_server.rs:L603 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-server-types/src/constants.rs:L69 @ reth@88505c7)`.
Edit `/home/ubuntu/eth-archive-node/docker-compose.yml` under
`services.reth.command`:

```yaml
      - --http.api=eth,net,web3,txpool,debug,trace
      - --rpc.eth-proof-window=500000
      - --ws
```

Then restart the Reth service:

```sh
cd /home/ubuntu/eth-archive-node
docker compose up -d reth
```

Local observation, not an upstream Reth guarantee: even with
`--rpc.eth-proof-window=500000`, deep `eth_getProof` calls on this node have
taken roughly 120-240 seconds or more per call while Reth computes historical
state roots, with long-lived MDBX reads and `historical_sp` warnings in the
node logs. A 1% proof sample is therefore infeasible here. The correction tool
records whether the small proof spot-check verified, timed out, or hit a
provider-serving error; timeout or provider-serving failure is non-fatal after
the mandatory `eth_getCode` sample succeeds.

Run the ignored cargo live-verification tests from a one-off Rust container
attached to the `bigname_default` Docker network so `postgres` resolves to the
PostgreSQL service and `host.docker.internal` reaches the host-published Reth
RPC port. The live Reth datadir on this host is
`/home/ubuntu/eth-archive-node/data/reth`; it is mounted at `/reth-data` because
the Reth reader may need writable MDBX/RocksDB coordination files even for
read-only verification.

```sh
docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  -v /home/ubuntu/bigname-worktrees/ws-code-hash-fix:/workspace:ro \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -w /workspace \
  rust:1.93.1-bookworm \
  bash -lc '
    apt-get update &&
    apt-get install -y --no-install-recommends clang libclang-dev &&
    export CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/tmp/bigname-target &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_COMPARE_BLOCKS=25287255,25287268 &&
    export BIGNAME_INDEXER_TEST_RETH_DB_DATADIR=/reth-data &&
    export BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL=http://host.docker.internal:8545 &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_DATABASE_URL=postgres://bigname:bigname@postgres:5432/bigname &&
    cargo test -p bigname-indexer --features reth-db \
      reth_db_provider_matches_rpc_and_stored_for_known_correct_code_hash_window \
      -- --ignored --nocapture
  '
```

The post-remediation latest-row check uses the same three live inputs and must
pass after the padded-row remediation:

```sh
docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  -v /home/ubuntu/bigname-worktrees/ws-code-hash-fix:/workspace:ro \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -w /workspace \
  rust:1.93.1-bookworm \
  bash -lc '
    apt-get update &&
    apt-get install -y --no-install-recommends clang libclang-dev &&
    export CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/tmp/bigname-target &&
    export BIGNAME_INDEXER_TEST_RETH_DB_DATADIR=/reth-data &&
    export BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL=http://host.docker.internal:8545 &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_DATABASE_URL=postgres://bigname:bigname@postgres:5432/bigname &&
    cargo test -p bigname-indexer --features reth-db \
      reth_db_provider_latest_rows_match_consensus \
      -- --ignored --nocapture
  '
```

The supervised correction run itself is a two-step operator action and must run
with the indexer stopped; otherwise live intake can race the audited window.
Expect roughly 1 GB of additional RAM and hours-scale wall time on the current
corpus. Use an `--observed-before` value at least two hours behind now because
the Reth DB reader can lag the node persistence horizon; after the main pass,
run a later tail pass from that first upper bound to a fresh two-hours-behind
upper bound.

```sh
cd /home/ubuntu/bigname
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  stop indexer

export OBSERVED_BEFORE="$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ)"
export BIGNAME_IMAGE="${BIGNAME_IMAGE:-ghcr.io/ensdomains/bigname:latest}"

docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  --user 0:0 \
  --pid host \
  --ipc host \
  --ulimit nofile=1048576:1048576 \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -e BIGNAME_INDEXER_RAW_CODE_HASH_CORRECTION_RETH_DB_SOURCE=ethereum-mainnet=/reth-data \
  -e BIGNAME_INDEXER_RAW_CODE_HASH_CORRECTION_RPC_URL=ethereum-mainnet=http://host.docker.internal:8545 \
  "$BIGNAME_IMAGE" \
  bigname-indexer repair raw-code-hashes \
  --database-url postgres://bigname:bigname@postgres:5432/bigname \
  --chain ethereum-mainnet \
  --observed-before "$OBSERVED_BEFORE" \
  --dry-run
```

After the dry-run census matches the ratified correction scope, rerun the same
`docker run` command without `--dry-run`. The command logs the census and
per-address breakdown before the RPC-verification and out-of-family gates,
verifies its mandatory `eth_getCode` RPC sample before writing, records the
best-effort `eth_getProof` spot-check status, skips rows whose block hash is
orphaned or absent from retained lineage, and rewrites only
`raw_code_hashes.code_hash` and `raw_code_hashes.code_byte_length` in guarded
batches. The dry-run census after the write must show zero remaining
correctable non-orphan rows and report the expected `orphaned_skipped` bucket.
This repository change ships the tool and record only; it does not execute the
supervised correction.

For the tail pass, wait until the previous `OBSERVED_BEFORE` is safely behind
the Reth reader horizon, then repeat the dry-run/write sequence with:

```sh
export OBSERVED_FROM="$OBSERVED_BEFORE"
export OBSERVED_BEFORE="$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ)"
# add these arguments to the same docker run command:
#   --observed-from "$OBSERVED_FROM" --observed-before "$OBSERVED_BEFORE"
```

Before the supervised Basenames Base registry-only derivation repair, apply
checked-in migrations and wait for the concurrent `resources` provenance-index
builds to finish. Do not run the registrar-family EXPLAIN gate or repair until
both `resources_basenames_registry_authority_key_idx` and
`resources_basenames_registry_logical_labelhash_idx` are present, ready, and
valid:

```sql
WITH expected(index_name) AS (
    VALUES
        ('resources_basenames_registry_authority_key_idx'),
        ('resources_basenames_registry_logical_labelhash_idx')
)
SELECT
    expected.index_name,
    pg_index.indisready,
    pg_index.indisvalid
FROM expected
LEFT JOIN pg_class
  ON pg_class.oid = to_regclass(expected.index_name)
LEFT JOIN pg_index
  ON pg_index.indexrelid = pg_class.oid;
```

The archived registrar-family `EXPLAIN (ANALYZE, BUFFERS)` must then show index
scans on those two indexes for the stale before-key resource proof and the
current registry-only counterpart lookup. A plan with repeated `resources`
sequential scans is still a hard stop. For 10k-row preflight batches, run the
EXPLAIN in a rolled-back transaction; a bounded `SET LOCAL work_mem = '256MB'`
may be used to keep the materialized proof CTEs in memory, but it is not a
substitute for the required index scans.

High-volume bootstrap defaults to
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC=auto`. In `auto` mode,
hash-pinned backfill chunks use the manifest-declared/raw catch-up scope while
the indexer is catching up, live polling keeps new block-derived events current,
and the indexer also runs automatic bounded raw-fact normalized-event replay
from its `normalized_replay_*` cursor until historical normalized events reach
the persisted raw-log head. Broad manifest-observation and discovery-emitter
adapter sync stay outside the live tailer. Operators may set `raw-only` to defer
live normalized sync manually, or `inline` to replay each chunk immediately for
small ranges and enable broad runtime refreshes.

`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC` scopes bootstrap backfill,
but do not read it as bootstrap-only: on the live path it also controls
adapter-owned normalized sync. `raw-only` runs the tailer with no live adapter
sync (a manual deferral); `auto` runs a focused post-bootstrap pass for only the
ENSv1/Basenames subregistry-discovery and ENSv2 registry families before it
widens the live plan. It does not synchronously derive the other five adapter
families. With normalized replay catch-up enabled, bounded asynchronous replay
then owns the remaining historical normalized events, live adapter sync stays
deferred until the cursors reach the raw-log head, and admission-epoch refreshes
add replay discoveries to the live plan as replay advances. Live discovery
writes remain deferred during that catch-up window, so a newly discovered
emitter is not watched until replay materializes its edge; and
only `inline` runs adapter sync inline per block and additionally re-derives
discovery edges from the whole stored raw-log corpus on each refresh tick. The
non-`inline` modes reload the live plan from edges already in storage rather than
re-deriving them.

What the mode does not change is the live watch set — which targets the tailer
watches. Bootstrap turns each selected target into an address-filtered range
scan, so a manifest-declared bootstrap scope bounds provider cost on chains with
a large discovered-target set. Live intake instead fetches every log in a block
by block hash and filters client-side, so watching discovered targets costs no
additional log fetches — though the
[raw-code baseline](glossary.md#raw-code-baseline) still issues one
`eth_getCode` per watched address that lacks a baseline observation, a cost that
scales with the watched set. That baseline runs as a capped cursor sweep
(`BIGNAME_INDEXER_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK` addresses per chain
per poll tick, default 2048, fetched in batched provider rounds and upserted per round),
so a large discovered-target set is baselined across ticks instead of inside
one. The live tailer therefore always watches the active watched chain —
manifest-declared and discovery-admitted targets alike — in every adapter-sync
mode, and always refreshes that plan from stored discovery edges so targets
admitted after startup enter the watch set without a restart. The plan reload
itself is gated on the per-chain `discovery_admission_epochs` sentinel, so a
quiet watched surface costs one tiny read per tick rather than a full plan
scan.

Widening the live watch plan writes more `raw_logs` rows per block, because a
block is retained whenever any watched target emits in it and same-transaction
sibling logs are retained with it. It does not widen public API routes,
route-level coverage, manifest capability flags, ENSv2 resolver profile support, or
consumer-replacement meaning.

```sh
BIGNAME_INDEXER_RETH_DATADIR_HOST=/home/ubuntu/eth-archive-node/data/reth \
BIGNAME_INDEXER_RETH_DATADIR_CONTAINER=/reth-data \
BIGNAME_INDEXER_CHAIN_RPC_URLS=base-mainnet=http://host.docker.internal:9545 \
BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=ethereum-mainnet=/reth-data \
BIGNAME_INDEXER_RETH_DB_USER=0:0 \
BIGNAME_INDEXER_RETH_DB_NOFILE_SOFT=1048576 \
BIGNAME_INDEXER_RETH_DB_NOFILE_HARD=1048576 \
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d indexer
```

RPC requirements are per selected profile and active watched chain. An
Ethereum-only run may omit Base entirely. If the selected profile includes Base
but no Base RPC is configured, Base provider-backed intake, automatic bootstrap,
backfill catch-up, and live head following stay idle with an explicit
`no_provider` / unavailable operational state; startup for configured chains
must not fail solely because Base is missing. A provider entry for a chain that
is not part of the selected manifest root is invalid.

Startup bootstrap creates finite backfill jobs from the planning snapshot of
eligible manifest-declared targets and already-materialized finite-known-start
ENSv2 root, registry, and resolver discovery targets. Their ranges end at one
provider finalized head latched for that chain's startup run. A configured
provider that omits that finalized head, or reports it above the canonical head,
fails automatic bootstrap for the chain. Bootstrap does not fall back to the
canonical tip: the unfinalized tail remains live-intake work and cannot produce
number-keyed bootstrap coverage. Automatic ENSv2 startup may repeat this plan as
newly admitted ENSv2 targets expand the authoritative set, but every pass keeps
the same finalized upper bound; it does not generically enumerate discovery
targets for other families. ENSv1 generic resolver and Basenames recursive
registry history use their separate scan mechanisms. Bootstrap does not cap
work to a recent window. This is still operational intake work: completing
bootstrap alone is not consumer-replacement or route-coverage evidence without
the relevant projection, route, conformance, and rollout gates.

Bootstrap backfill identity is tied to the selected deployment profile, chain,
finite range, and source identity, not the manifest root path used by a given
host. Moving an unchanged manifest corpus between directories must not make the
indexer reread historical ranges. Large whole-active source identities store a
compact selected-target digest instead of every target in `source_identity`.
Rollback to a binary that predates compact/full source-identity matching can no
longer prove those compact jobs are the same historical job; finish or fail the
compact jobs, or roll forward again, before relying on resumable job reuse.

Automatic bootstrap partitions large job segments into child range leases for
internal workers. `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_WORKERS=0` selects an
automatic worker count capped at 4; set a positive value to pin the count.
`BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_RANGE_BLOCKS` controls the child range size
and defaults to `50000` blocks. The worker pool is inside one normal
`bigname-indexer run` process; operators do not need to launch extra indexer
containers for parallel bootstrap. Parallel bootstrap applies to the effective
raw-only startup path used by `auto` / `raw-only`; explicit `inline` adapter sync
keeps startup bootstrap sequential so normalized-event writes remain ordered.

Hash-pinned backfill execution batches each reserved range into
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS`-sized chunks. The default
server profile uses `1024` blocks. Larger chunks reduce checkpoint churn and RPC
round trips during long historical bootstrap, while also increasing the amount
of range work retried after a failed chunk. During automatic startup only, each
configured chunk is executed as progress units of at most 32 blocks and the
indexer heartbeat advances after each completed unit; manual backfills retain
the configured chunk as their execution unit. The startup adapter pass then
advances that same heartbeat after checkpoint stream pages and bounded
discovery, identity, binding, and normalized-event finalization batches, so a
large materialization stays live without a free-running timer masking a stuck
operation. Live manifest and discovery refresh adapter passes use the same
checkpoint-page callbacks and family-boundary beats. Raw-only sparse backfill
also caps each materialized push with
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_PUSH` so dense log spans are
split before transaction and receipt fetch/persist work. The older
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_RANGE` name is still accepted
as a fallback.

Live checkpoint promotion can advance over a gap larger than the live fill limit
only when a previous bounded backfill has already stored enough evidence. The
indexer selects a stored anchor with two strategies. Primary (works for
arbitrarily deep gaps): it loads the highest stored canonical/safe/finalized
`chain_lineage` row, fetches the provider block at exactly that height by
number, and anchors directly on the stored frontier when the hashes match and
the height is at or below the provider safe/finalized head — no parent
walking and no provider-head proximity requirement. Fallback (near-tip): it
walks the provider safe/finalized ancestry down to the highest stored
canonical/safe/finalized `chain_lineage` block within the bounded
stored-anchor parent-fetch depth of `4096` blocks
(`MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS * 4`), for the case where the stored
frontier is close to or above the safe candidates. Provider RPC failures in
either strategy surface as provider errors, never as a missing-anchor
refusal. Promotion then advances at most one configured chunk per poll
through the stored canonical child path and validates each promoted step. A
non-anchor promotion target must either be parent-linked to the provider-
verified stored anchor or match the provider's block hash at that exact height;
canonical markings and same-height uniqueness alone are insufficient because
independently stored fork segments need not share ancestry. When the fallback
provider check is needed, provider failure surfaces as an error, while an
unavailable or mismatching target refuses promotion before the normal
checkpoint-advance path. The live canonical `latest` head is not required to
be stored, and normally is not stored during an over-limit catch-up.

For every promoted block, fetch evidence must come from durable
`backfill_coverage_facts` rows written when backfill jobs complete (or
re-derived by `repair derive-backfill-coverage-facts` from legacy verbatim
full-payload identities). Promotion never recomputes selector plans from
persisted job identities — plan recomputation is invalidated by the very
discovery that runs during long backfills. A watched log-producing
`(source_family, address)` tuple is covered for an evaluated slice when its
required interval (active window ∩ slice) is fully contained in the gap-free
union of address-scoped fact rows for that exact tuple and family-scope fact
rows (`address IS NULL`) for its family. Family-scope rows record
topics-complete scans (ENSv1 generic resolver scans, Base Basenames registry
Coinbase SQL scan-all) that cover every address of the family over each fact
interval. Overlapping or adjacent facts from independently completed jobs may
form the union, so the default sequence of 32-block `ops-catchup` jobs can
prove a 1,024-block promotion step. A one-block gap still refuses, and one source family's facts never
credit another family at the same address.
Manifest source families that have no active ABI event topics, such as
execution-only transport entrypoints, do not impose historical selected-log
coverage for checkpoint promotion.

Coverage is verified by an indexed per-tuple probe that merges only matching
exact-address and family fact intervals. Declaration-only requirements come
from active manifest versions; deprecated, shadow, and draft declarations do
not create open-ended coverage obligations. Closed discovery intervals remain
historical requirements for the blocks where they were admitted.

The saved coverage frontier has one chain header with a revision, proof format,
discovery-admission epoch, inclusive verified bounds, and the exact active event
topic set for each log-producing source family. It also stores the exact child
row count and constant-state 128-bit integrity fingerprint. Its companion
snapshot has one row per `(source_family, address)` with coalesced inclusive
required intervals.
Postgres materializes the complete current candidate and computes the interval
difference against that snapshot in a transaction-local table. The indexer
receives only proof work: added intervals for unchanged-topic families and every
current interval for a family whose topic set changed. Removed and shortened
requirements need no historical read, but publication atomically replaces the
old snapshot so removed coverage cannot survive a later readmission.

Cold or missing proof, malformed current-format proof (including a child count
or fingerprint mismatch), and proof made range-stale by a deep checkpoint
regression are never partially reused. An unsupported proof
format is a hard refusal and is not overwritten. The cold candidate starts at
the earlier of the promotion path and the earliest explicit watched
`active_from_block` through the attempted stored anchor. This includes closed
historical discovery intervals, so a tuple admitted retroactively cannot inherit
an already-advanced checkpoint. An unknown start remains unknown rather than
becoming block zero. A checkpoint regression whose next path begins below the
saved lower bound invalidates the whole frontier and requires the same cold
proof. Verification runs in large block chunks (`131072` blocks per fact query).
If look-ahead through the stored anchor finds a gap above the current promotion
target, the indexer retries exactly through the target and publishes only that
shorter verified bound; an unverified suffix is never saved.

After proof, the indexer takes the discovery-admission epoch fence and publishes
the whole candidate with a compare-and-swap on the saved revision. Epoch drift
or a revision conflict publishes nothing and leaves checkpoint promotion
fail-closed until a reloaded candidate succeeds. A successful revision is
durable across process restarts, so later polls verify only added or topic-
changed intervals and bound extensions. Verification still fails closed on
topic-set drift: fact coverage is complete only relative to the family's
manifest ABI event set at fetch time, so promotion validates the immutable topic
plan of each topic-filtered (Coinbase SQL) fact that could supply a required
watched tuple interval. A stale or missing persisted topic set refuses promotion
only when its evidence is still needed; a gap-free union of current-topic or
topic-unfiltered facts over the same required interval replaces it.
Address-enumerated hash-pinned fetches are topic-unfiltered and immune to drift.

The saved frontier is not a lineage or fork proof. Every promotion still checks
the provider anchor and target hash, stored parent path, same-height non-orphan
forks, selected-log companion rows, and the discovery-admission epoch again in
the checkpoint-write transaction.

Promotion reconstructs family-selected companion obligations the same way the
backfill write side does: a stored log is selected only when its emitter is
watched under the source family, its block is inside that watched entry's active
window, and its topic0 is in the family's current active manifest ABI.
Same-transaction sibling logs retained for replay context do not independently
require a code observation for the sibling emitter; the transaction and receipt
that contain a selected log remain required.

Fact-based coverage means Reth-db backfills can promote after completion even
though they do not write `raw_payload_cache_metadata` rows; retained payload
metadata alone is not historic promotion evidence. Fact coverage
distinguishes "selected no logs in this block" from "this block was never
fetched" only when the stored path is not ambiguous with another non-orphan
same-height lineage row; orphaned repair residue does not make a block
ambiguous. Incomplete or crashed backfills write no facts (facts are written
in the same transaction as the job completion flip), so their lineage-only
residue is refused.
Event-silent reverse-resolver indexing is a live-tip concern: ordinary live
reconciliation retains direct-call transaction and receipt facts from full-block
payloads for the built-in Ethereum Mainnet event-silent reverse resolver set and
any configured extra addresses. Those durable observations trigger later
projection-owned hydration of the resolver's current state. Historic
stored-lineage checkpoint promotion does not require retained full-block
payloads for that latest-only resolver state and does not reconstruct per-block
event-silent reverse-resolver data. Once the checkpoint reaches live
reconciliation again, current-tip payload fetches resume and event-silent
reverse-resolver observations are recorded from live block payloads.

Actionable refusal classes:

- Missing stored anchor: the stored frontier's hash did not match the
  provider's block at that height (stale fork tip) and no stored
  canonical/safe/finalized ancestor was reachable within the bounded
  `4096`-block parent walk. Run hash-pinned backfill so the stored canonical
  frontier reflects the provider's canonical chain, then retry. The provider
  safe/finalized head itself does not need to be in `chain_lineage`.
- Incomplete lineage path or duplicate canonical children: rerun hash-pinned
  backfill for the missing range; if duplicate canonical rows remain at one
  height, repair/orphan the losing lineage before retrying.
- Watched tuples without gap-free fact coverage: the refusal names the
  violating `(source_family, address, block-range)` tuples. Run hash-pinned
  or Coinbase SQL backfill for those tuples so completion writes facts, or
  run `repair derive-backfill-coverage-facts` for already-completed legacy
  jobs whose identity carries the fetched targets verbatim, then retry.
- Manifest ABI topic0 set changed for a topic-filtered fact still needed by a
  watched tuple (or its job persisted no topic set): re-run the affected range
  on the current manifest so one fresh current-topic or topic-unfiltered fact
  completely replaces the stale required interval.
- Same-height non-orphan fork ambiguity: repair/orphan the losing lineage row,
  refetch the range on the winning branch under a FRESH idempotency key (the
  original job is completed and immutable — it will not refetch), then retry.
  Numeric completed-range coverage is accepted only when the stored promoted
  path has no competing non-orphan row at the same height; orphan-repairing
  without refetching would let facts fetched on the losing branch credit the
  winning branch's numbers.

Threat-model boundary for number-keyed coverage: coverage facts and completed
ranges are keyed by block number, not hash. The design is sound only while the
blocks a job fetches are final relative to the validation provider at fetch
time — a fact recorded from a fetch of a block that is later reorged would
credit the replacement block's number. Automatic bootstrap enforces this
boundary: it latches the provider finalized head, clips every manifest and
discovery target, retry pass, fetched range, and coverage fact to that inclusive
height, and fails closed when the provider cannot supply a coherent finalized
head. Blocks above that height through the canonical tip are not fetched by
bootstrap and remain live-intake work. The same-height non-orphan fork probe is
an additional refusal for already-stored ambiguity, not a substitute for the
finalized bootstrap bound. Manual ranged number-keyed backfills still have no
per-fact finality watermark, so operators must not give them an upper bound
above the validation provider's finalized head.
- Selected-log companion rows missing: rerun the selected hash-pinned backfill so
  raw code, transaction, and receipt rows are persisted with the selected logs.
  The demand is scoped the way backfill writes companions: a stored log demands
  them only when its emitter is watched under a source family, its block is
  inside that watched entry's active window, and its topic0 is in the family's
  current manifest ABI topic0 set. Sibling-retained foreign-topic logs from
  watched addresses never demand companions.
- Missing event-silent current resolver state after catch-up: let ordinary live
  reconciliation process the current tip so direct-call observations are
  retained for the built-in Ethereum Mainnet event-silent reverse resolver set.
  Configure `BIGNAME_INDEXER_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES` only for
  deployment-specific extra resolver addresses. Do not rerun historic promotion
  ranges solely to retain full-block payloads for event-silent reverse-resolver
  data; that data is latest-only by design.

Manual Base historical backfills can select Coinbase CDP SQL with
`BIGNAME_INDEXER_BACKFILL_SOURCE=coinbase-sql` or allow Base-only automatic
selection with `BIGNAME_INDEXER_BACKFILL_SOURCE=auto` plus
`BIGNAME_INDEXER_COINBASE_SQL_URLS=base-mainnet=default`. The `default` URL is
the CDP SQL `/run` endpoint; custom URLs must use `https://` because the runner
sends generated bearer JWTs. Configure `COINBASE_CDP_SQL_API_KEY_ID` and
`COINBASE_CDP_SQL_API_KEY_SECRET`, or override the env var names with
`BIGNAME_INDEXER_COINBASE_SQL_API_KEY_ID_ENV` and
`BIGNAME_INDEXER_COINBASE_SQL_API_KEY_SECRET_ENV`. The runner keeps the Secret
API Key material in env and generates a fresh CDP REST bearer JWT for each SQL
request, so operators should not paste a short-lived JWT into the server
configuration. The generated JWT is passed to the HTTPS client through a
0600-permission temporary config file and removed after the request; operators
should still treat process temp directories as sensitive because a crash can
leave a short-lived JWT behind until normal temp cleanup. This source is
backfill-only and finite-range-only: it is
unavailable to `run` live following, `ops-catchup`, repair, chain-head
promotion, and checkpoint promotion. Operators must still configure
`BIGNAME_INDEXER_CHAIN_RPC_URLS` or `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` for
the same Base chain so the validation provider owns block hashes, headers,
canonicality evidence, selected-log-emitter code observations, and
transaction/receipt fills. The Coinbase SQL runner respects
`BIGNAME_INDEXER_COINBASE_SQL_PAGE_LIMIT`,
`BIGNAME_INDEXER_COINBASE_SQL_QUERY_CHAR_LIMIT`,
`BIGNAME_INDEXER_COINBASE_SQL_QUERY_TIMEOUT_SECS`, and
`BIGNAME_INDEXER_COINBASE_SQL_RATE_LIMIT_QPS`; query length and timeout defaults
track the stricter currently published CDP SQL limits, while the row default
uses bigname's conservative 10,000-row effective cap because Coinbase's
[SQL FAQ](https://docs.cdp.coinbase.com/data/sql-api/faq) and
[REST reference](https://docs.cdp.coinbase.com/api-reference/v2/rest-api/onchain-data/run-sql-query)
currently publish different row/query-length ceilings. If the configured page
limit is above the effective result cap, pagination and window tuning use the
cap. The QPS default is a conservative per-process guardrail and remains
operator-configurable if product limits change. The default validation mode is
`full`, so the validation provider fetches the same address/topic log span and
fails the range if Coinbase SQL omitted or added a selected log identity.
Coinbase SQL reads decoded event rows and undecoded encoded-log rows; undecoded
rows require validation-provider payload fill because Coinbase SQL supplies only
the log identity and topics for them. Empty Coinbase SQL windows do not force
code observations for every selected address. `sample` uses Coinbase SQL
identities for selected logs, resolves and hash-checks only the returned log
blocks with the validation provider, fills exact logs from those block bundles,
and then uses the same validation provider for canonicality evidence, selected
transaction/receipt fills, and selected-log-emitter code observations.

Coinbase SQL backfills can split a finite range into concurrent range workers
with `BIGNAME_INDEXER_COINBASE_SQL_WORKERS`; the default `1` keeps one worker.
`BIGNAME_INDEXER_COINBASE_SQL_RANGE_BLOCKS` chooses the fixed range size used by
that concurrent mode. The default `0` keeps the normal adaptive resumable range
planner. Basenames authority Coinbase SQL backfills remain raw-only in both the
single-worker and concurrent paths so ordered normalized-event replay owns
authority closure materialization.

JSON-RPC validation providers can tune batch shape with
`BIGNAME_INDEXER_JSON_RPC_BATCH_ITEM_LIMIT` and
`BIGNAME_INDEXER_JSON_RPC_BATCH_CONCURRENCY`. The defaults favor conservative
provider compatibility; higher values reduce round trips but can trip provider
payload or rate limits. Receipt-heavy backfills can configure
`BIGNAME_INDEXER_CHAIN_RPC_RECEIPT_FALLBACK_URLS` as comma-separated
`<chain>=<url>` entries. The provider uses those URLs only for transaction
receipt fallback work; block hashes, headers, canonicality, and primary payload
validation still come from the chain's configured primary validation provider.
Historical code reads can similarly configure
`BIGNAME_INDEXER_CHAIN_RPC_CODE_FALLBACK_URLS` as comma-separated
`<chain>=<url>` entries; every entry must name a chain with one configured
primary provider source. The provider uses those URLs only when
[hash-pinned](glossary.md#hash-pinned) `eth_getCode` fails because the primary
source has pruned that block's state; bulk headers, logs, transactions, and
receipts stay on the local source, so the remote-provider budget is spent only
on unavailable historical code.

Startup ENSv1 registry discovery and
[unwrapped-authority](glossary.md#derivation-kind) sync target
`BIGNAME_INDEXER_STARTUP_DISCOVERY_PAGE_LOGS` raw logs per physical page. The
setting must be positive and below PostgreSQL's `BIGINT` maximum because paging
reserves one value for lookahead. It defaults to `100000`, replacing registry
discovery's former internal `10000`-log startup page. At roughly 2 KiB per log,
the default target is about 200 MB of page-resident data. Unwrapped-authority
paging preserves whole-block boundaries, so one unusually dense block can
exceed the target; treat this setting as a page-size target, not a strict memory
ceiling.

Automatic normalized-event replay catch-up keeps its block cursor, but also caps
each replay chunk with `BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK`
so sparse eras can move in large block jumps while dense spans are bounded by
the number of persisted canonical raw-log event candidates considered. For adapters classified
`stateless_raw_fact`, the cap bounds each cursor chunk. For implemented
`stateful_closure_required` and `contextual_dependency_required` full-closure
replay, the same cap bounds physical adapter pages while preserving whole-block
boundaries, and adapter routing may then filter those pages down to the watched
or generic source events consumed by the closure pass. If one block exceeds the
cap, that whole block is still replayed as one page. Adapter implementations may also use a larger scan guard to bound one
database range probe, but that guard is not a fixed replay window and should not
force sparse eras through 512-block pages. The automatic cursor is one all-source
chain cursor over persisted canonical raw facts, and chunk/log caps are only IO
hints; they are not semantic adapter-state or dependency-closure snapshots. The
ENSv1 unwrapped-authority closure implementation keeps its database scan guard
at the catch-up chunk-block scale so sparse eras normally page by the configured
candidate-log cap instead of a small fixed block window. Because those pages can
carry many more derived events than the old small-window pages, the implementation
flushes and checkpoints the adapter-private closure snapshot after each physical
page. The
runner fails closed for
stateful/contextual adapters that do not have a documented closure/dependency
replay session, rather than advancing the cursor over possibly divergent
transition rows. Source-scoped replay is reserved for explicit repair/backfill
runs and is not deterministic stateful/contextual regeneration unless the
selection is closure-complete. After automatic replay catch-up completes, the
post-replay live-adapter backlog cursor pages already-persisted raw-log blocks
with `BIGNAME_INDEXER_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE`. The default `1`
keeps the handoff conservative; values above the internal cap are clamped.
Use `RUST_LOG=info,sqlx::query=error` for these runs; otherwise SQLx slow-query
warnings can print huge generated INSERT statements for dense chunks and waste
time on logging instead of ingest.

### Targeted stateless normalized-event repair

Use `replay normalized-events --stateless-only` when a retained canonical raw
fact re-derives a stateless normalized event differently from a stale stored
row and the ordinary family sync correctly fails closed on that stable event
identity. This is a write-authoritative repair, not a dry run: stop the indexer
so live adapter or startup family sync cannot race the selected rows. Projection
workers may remain running because the repair publishes the ordinary durable
normalized-event change journal they consume.

For an exact block, run:

```sh
bigname-indexer replay normalized-events \
  --deployment-profile <deployment-profile> \
  --chain <chain-id> \
  --stateless-only \
  --block-hash <canonical-block-hash>
```

Repeat `--block-hash` to select more than one exact block. A contiguous repair
window uses the same authority without invoking full closure:

```sh
bigname-indexer replay normalized-events \
  --deployment-profile <deployment-profile> \
  --chain <chain-id> \
  --stateless-only \
  --from-block <first-block> \
  --to-block <last-block>
```

`BIGNAME_INDEXER_REPLAY_NORMALIZED_EVENTS_STATELESS_ONLY=true` is the
environment equivalent of the flag. The mode selects only producers classified
`stateless_raw_fact` by the central normalized-event replay contract: the
complete block-derived and ENSv1 reverse-claim producers. Selection is derived
directly from the ordinary replay dependency model; there is no separate
per-adapter stateless-only flag. ENSv1 subregistry discovery is excluded because
its event derivation reads manifest contract instances, discovery-derived
emitter addresses, migration state, the current registry emitter, and the
reconciled edge at the event block/hash. It never runs ENSv1
unwrapped-authority or any other closure/stateful lane. Leaving off the flag
preserves the existing refusal when a block-hash or source-scoped selection
includes a closure/context-dependent adapter.

For every derived identity, the storage log message
`stateless-only normalized-event replay identity examined` carries
`event_identity`, `derivation_kind`,
`identity_outcome=inserted|unchanged|superseded|skipped_non_canonical_source`,
and `differing_fields`. An `observed` or `orphaned` input receives the skip
outcome and cannot overwrite a canonical row. The
storage message `stateless-only normalized-event replay authority completed`
reports those counts for one persistence transaction and can appear more than
once for a chunked range. Use the command-wide `raw-fact normalized-event replay
completed` message for aggregate `identities_examined`, `identities_inserted`,
`identities_unchanged`, `identities_superseded`, and
`identities_skipped_non_canonical_source`. Treat an unexpected identity or
differing field as a hard stop before widening the selection. A superseded row
keeps its `normalized_event_id`, receives the current derivation, and the
normalized-event storage trigger appends a `content_update` record so the
worker invalidates and re-derives dependent projections. If the same update
also changes canonicality, the trigger additionally appends the ordinary
`canonicality_update`. An unchanged rerun appends no additional change record.
Replay fails closed with `would change downstream projection identity` if old
and current content would address different projection keys; the retained-row
journal cannot reconstruct the old key, so that case needs a separately
reviewed key-aware repair rather than this flag.

The 2026-07-23 Ethereum Mainnet repair is the reference scenario. Four
`ens_v1_registry_resolver_changed` rows retained pre-#208 attribution in
`after_state` (the old registry instance and resolver-address-keyed observation
key), so startup family sync rejected the first mismatch. For each implicated
canonical block, capture the family-sync error and stored row for review. This
kind is owned by the `ens_v1_registry_l1` source family and the contextual
`ens_v1_subregistry_discovery` adapter, so `--stateless-only` intentionally does
not repair it. Run ordinary normalized-event replay from the proven retained
closure boundary through the last implicated block instead:

```sh
bigname-indexer replay normalized-events \
  --deployment-profile <deployment-profile> \
  --chain ethereum-mainnet \
  --from-block <retained-closure-start> \
  --to-block <last-implicated-block>
```

The guarded repair accepts only the stale `observation_key` and
`from_contract_instance_id` reattribution and verifies the replayed values
against the reconciled resolver edge at the exact block/hash. Expect a
`content_update` for each repaired row. Restart the indexer after the full
closure run succeeds, and let projection derive/apply drain the resulting
journal work. Do not hand-update `after_state`, delete the normalized row, or
remove its existing journal records.

### Single-phase to two-phase normalized replay upgrade

The `raw_fact_normalized_events` cursor has no phase-version field. Its state at
the image change determines the upgrade procedure:

- A cursor that completed under a pre-two-phase image has
  `next_block_number > target_block_number`. The upgraded catch-up loop reports
  `Idle`; it does not run phase 1 retroactively, so any stateless
  [label-preimage](glossary.md) omission persists across that cursor's completed
  span. For each affected
  chain, use the manual stateless replay stopgap over the cursor's exact saved
  range (with the normal database environment configured):

  ```sh
  bigname-indexer replay normalized-events \
    --deployment-profile <deployment-profile> \
    --chain <chain-id> \
    --from-block <range_start_block_number> \
    --to-block <target_block_number>
  ```

  This manual range replay runs the stateless producers before its full-closure
  pass; the completed automatic cursor itself is not reset or treated as proof
  that the older image ran phase 1.

  Cursor completion may already have allowed raw-log staging compaction. Before
  the manual replay, inspect the chain's retention generation:

  ```sql
  SELECT
      retention_generation,
      retained_history_complete,
      incomplete_since,
      proven_retention_generation,
      proven_discovery_admission_epoch,
      proven_through_block
  FROM raw_log_staging_input_revisions
  WHERE chain_id = '<chain-id>';
  ```

  A missing row is a hard stop. Generation zero means the staging corpus has
  never been destructively rotated. A later generation means compaction has
  occurred, and cursor completion is not retained-history authority. The replay
  command performs the authoritative source-family coverage/proof check before
  phase 1; if it reports incomplete or stale retained history, stop and restore
  raw facts before retrying.

  When all required targets remain selectable, restore the saved range with a
  provider-backed, raw-only hash-pinned backfill in the current retention
  generation. Use a fresh idempotency key containing that generation so an old
  completed job cannot be reused:

  ```sh
  bigname-indexer backfill \
    --deployment-profile <deployment-profile> \
    --chain <chain-id> \
    --from-block <range_start_block_number> \
    --to-block <target_block_number> \
    --idempotency-key two-phase-upgrade-<chain-id>-g<retention_generation> \
    --hash-pinned-adapter-sync raw-only
  ```

  Run it with the normal manifest root, database, and validation-provider
  configuration, then retry manual normalized-event replay. If retention
  validation still names a closed historical discovery interval that the
  standalone current watch selector cannot refetch, the supported recovery is
  a clean database rebootstrap: apply the checked-in migrations to a new
  database, configure the same manifests and historical provider, and let
  generation-zero historical bootstrap plus two-phase normalized replay finish
  before cutover. Never weaken the retention check or mark the old cursor
  pending without restoring its raw facts.
- A cursor still in progress at upgrade has
  `next_block_number <= target_block_number`. The two-phase image runs one full
  phase-1 pass over the saved range and latched target, then resumes phase 2 from
  the existing closure checkpoints. Those checkpoints carry over because both
  images use the same deployment profile, chain, cursor kind, range, and target
  as their replay checkpoint context.

Where feasible, deploy the two-phase image before in-flight cursors complete.
This converts the omission into the expected one-time full phase-1 cost and
avoids a later manual replay of already completed spans.

### Streamed full-closure discovery finalize

The ENSv1/Basenames registry full-closure replay finalizes its discovery-edge
reconciliation as a streamed temp-table set-diff instead of an in-memory
reconcile, so memory stays bounded by pages rather than the staged observation
count (#168). Two operational consequences:

- **Two-level deactivation guard.** The finalize must be a near-no-op after a
  verified rederive. A coarse cap (`max(100_000, 10%)` of the source's active
  edges) aborts before the deactivation diff is even materialized, and a
  precise fail-closed threshold (`max(10_000, 1%)`) applies to the actual
  post-chronology deactivation set right before mutating. A mass deactivation
  indicates spec drift; after confirming an intended large diff, raise both
  bounds with `BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS`
  (the value is the permitted deactivation count). An aborted finalize rolls
  back cleanly and keeps the replay checkpoint resumable.
- **Connection minimum.** The checkpointed replay concurrently holds the
  raw-log staging guard, the streamed reconcile transaction, and a third
  pooled connection for staged assignment page reads. The replay entry point
  refuses pools with `max_connections < 3` up front instead of deadlocking.

The 2026-07-03 ratified Base normalized-event corpus correction is supervised
and off by default. It exists only for the comprehensive Basenames Base
drop-and-full-closure-rederive window documented in [`storage.md`](storage.md).
Do not run it against a live indexer or worker process, and do not treat it as a
projection rebuild. The execute step deletes Base current-projection rows because
those rows have foreign keys into the identity rows being dropped; the API must
not serve during the destructive and replay/rebuild window. The delete is
batched and resumable under a session advisory lock, with durable progress in
`base_normalized_rederive_runs` and `base_normalized_rederive_run_batches`.
The passed deployment profile must already own a
`base-mainnet/raw_fact_normalized_events` replay cursor because the delete scope
is global to Base while replay reset is profile-scoped. Updated indexer and
worker runtimes and write-capable one-shot commands hold a shared advisory lock
while running; the execute path takes the exclusive form of that lock and also
refuses visible `bigname-indexer`/`bigname-worker` sessions before it writes.
Those guarded writers also refuse to start while any Base rederive run is still
incomplete, even if a crashed execute container released its session advisory
lock. Apply this release's checked-in migrations before starting the correction;
the worker migration command remains a guarded writer because checked-in
migrations can include replay-adjacent data repairs. If a crash occurs before
the abort-status migration is installed, resume/complete the run or restore the
database to a consistent pre-run snapshot before running migrations or writers.
Guarded writer processes require at least two database pool connections so the
held advisory lock connection cannot starve ordinary writer work. The indexer
requires at least four so its permanent runtime writer guard, nested bounded
work guards, and progress-heartbeat writer cannot exhaust the pool. Each path
rejects a smaller pool before starting that work.

1. Stop the indexer and worker services, leaving PostgreSQL and the API online
   for dry-run review if desired.
2. Run the dry-run census and capture stdout for maintainer review:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps indexer \
       bigname-indexer drop-and-rederive-base-normalized-events \
       --deployment-profile mainnet \
       --run-id base-normalized-rederive-2026-07-03 \
       --batch-size 100000
   ```

3. Review the printed derivation-kind/source-family partition, including the
   re-derivable delete count and explicitly kept nonreplay pairs such as
   `raw_log_preimage_observation` and non-closure source families;
   identity/projection/change-log delete counts; deferred raw-fact safety line,
   including that its canonical Base raw-log head is the reviewed replay target;
   the `ratified_dropped_orphan_emitters` line, which must report
   exactly 3,939,502 legacy Basenames `ReverseRegistrar`
   `0x79ea96012eea67a83431f1701b3dff7e37f9e282` rows under
   the 2026-07-05 ratified deliberate drop (recorded as `2026-07-05 option A`; see
   storage.md § corrections) for `ens_v1_reverse_claim` /
   `basenames_base_primary` with `ReverseChanged` / `BaseReverseClaimed`,
   coin type `60`, and blocks `17575714..46903158`
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f);
   active replay target snapshot row count and digest;
   active manifest snapshot row count and digest; both replay cursor counts;
   affected current-projection replay marker count; run id, batch size, batch
   count/order; max affected block; replay target floor; and the replay reset target
   `mainnet/base-mainnet/raw_fact_normalized_events: 17571485..=<validated replay target>`.
   The `delete_census` fields are exact and execute-gated by the matching
   `--expected-*` arguments. The derivation-kind/source-family partition,
   ratified dropped-emitter section, cursor-census breakdown, estimated batch
   counts, and deferred raw-fact safety line are informational review aids.
   `resolver_current` and `primary_names_current` are not per-row projection
   delete counts for this correction; they are covered by the exact
   `current_projection_replay_status` reset count because the later
   all-current-projections replay rebuilds those families.
   That ratified dropped-emitter class remains part of the normalized-event
   delete count and is deliberately not re-derived; any other orphaned emitter
   still indicates an unsafe dry run and must hard-stop before execute.
4. Execute only after review, passing the dry-run counts back as exact
   `--expected-*` arguments and a reviewed `--replay-target-block` so the tool
   refuses drift between review and write. Use the dry-run's reported head, or
   another reviewed value that is at least the printed replay target floor and
   not above the current canonical raw-log head. On a rerun after the drop, the
   floor includes any still-pending prior reset raw replay cursor target, so the
   target cannot be shrunk while replay is still pending. Immediately before
   this step, drain or stop the `api` service and keep it unavailable until the
   replay, projection rebuild, and verification steps complete. This is total
   API impact for the stack, including Ethereum name reads, not only Basenames
   reads:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     stop api

   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps indexer \
       bigname-indexer drop-and-rederive-base-normalized-events \
       --deployment-profile mainnet \
       --run-id base-normalized-rederive-2026-07-03 \
       --batch-size 100000 \
       --replay-target-block <dry-run-target-block> \
       --execute \
       --confirm-ratified-2026-07-03 \
       --expected-normalized-events <dry-run-value> \
       --expected-resources <dry-run-value> \
       --expected-token-lineages <dry-run-value> \
       --expected-name-surfaces <dry-run-value> \
       --expected-surface-bindings <dry-run-value> \
       --expected-name-current <dry-run-value> \
       --expected-address-names-current <dry-run-value> \
       --expected-children-current <dry-run-value> \
       --expected-permissions-current <dry-run-value> \
       --expected-record-inventory-current <dry-run-value> \
       --expected-projection-normalized-event-changes <dry-run-value> \
       --expected-current-projection-replay-status <dry-run-value> \
       --expected-replay-cursor-rows <dry-run-value> \
       --expected-adapter-checkpoint-rows <dry-run-value> \
       --expected-adapter-checkpoint-item-rows <dry-run-value> \
       --expected-active-replay-target-snapshot-digest <dry-run-value> \
       --expected-active-manifest-snapshot-digest <dry-run-value>
   ```

5. Monitor batch progress while execute is running from another SQL session:

   ```sql
   SELECT run_id, status, current_step, deleted_counts, updated_at
   FROM base_normalized_rederive_runs
   WHERE run_id = 'base-normalized-rederive-2026-07-03';

   SELECT step, count(*) AS batches, sum(row_count) AS rows
   FROM base_normalized_rederive_run_batches
   WHERE run_id = 'base-normalized-rederive-2026-07-03'
   GROUP BY step
   ORDER BY min(batch_sequence);
   ```

   The default `--batch-size 100000` bounds WAL and row locks per commit.
   Current-step delete candidates are materialized once per execution session
   from temporary scope tables into step-local temporary candidate tables, so
   each logged batch should delete from the candidate table rather than rescan
   the full projection. Reverse-identity sidecar triggers are disabled only
   inside the affected projection and identity-anchor delete transactions. The
   sidecars are intentionally stale while the API is drained and the run is
   incomplete; the final reset transaction rebuilds
   `address_names_current_identity_counts` and
   `address_names_current_identity_feed` from the remaining current projections
   before setting the run to `completed`. Lower the batch size only if
   per-commit WAL or lock duration needs more headroom.
6. If the execute container dies before completion, keep the API drained and run
   the same execute command again with the same `--run-id`, `--batch-size`,
   `--replay-target-block`, and expected counts. The command resumes only when
   recorded deleted counts plus the remaining live census still equal the
   reviewed dry-run census, the current active replay target/range snapshot
   and active manifest snapshot still hash to the reviewed digests stored in
   the run row, and retained raw facts remain complete for the stored target.
   In-progress resume does not repeat the full retained raw-log byte-checksum
   proof captured at run creation; the advisory lock and guarded-writer
   exclusion make raw facts immutable during the correction, while the resume
   census and digest checks remain live.
   Do not run replay until the run row is `status='completed'`; before that
   final state, replay cursors and projection markers are intentionally
   untouched. Completion also advances the full-projection input revision and
   invalidates any automatic replay attempt or durable projection stage built
   from the pre-delete corpus. Normal indexer, worker, and guarded one-shot
   writers also refuse to start while the run remains incomplete. If the
   operator decides not to resume, restore the database to a consistent pre-run
   snapshot first, then explicitly mark the run aborted so guarded writers may
   start:

   ```sql
   UPDATE base_normalized_rederive_runs
   SET status = 'aborted',
       current_step = 'aborted',
       updated_at = now()
   WHERE run_id = 'base-normalized-rederive-2026-07-03'
     AND status <> 'completed';
   ```

   Do not mark a half-deleted database aborted merely to unblock writers; either
   complete the rederive run or restore the database before aborting.
7. Run only the catch-up indexer with normalized replay catch-up enabled and
   `--hash-pinned-adapter-sync auto` so the reset cursor runs full-closure
   replay from block `17571485` through the reviewed target block. Keep the API
   drained. Run this from the same reviewed manifest image/root used for dry-run
   and execute. The catch-up path rebuilds the current active Base replay
   target/range snapshot and active manifest snapshot, then compares their
   digests with the reviewed digests stored in the completed run row before
   replaying. If either digest differs, catch-up bails before re-emitting rows.
   While this completed correction reset cursor is still pending replay, the
   indexer skips repository manifest sync, so normal repository sync cannot
   rotate the active manifest tables during full-closure re-derivation. The
   skipped repository refresh remains marked for retry, so the same
   long-running indexer syncs normally once the pending reset replay cursor
   completes. The final reset already
   validated that the retained canonical Base raw-log floor equals block
   `17571485`; catch-up repeats that check while the reset cursor is pending, so
   normal cursor refresh cannot widen this correction replay below the delete
   boundary on the reviewed deployment. The command also clears any stale
   `post_replay_live_adapter_backlog` cursor for the same Base deployment. The
   legacy Basenames reverse-registrar rows dropped under the 2026-07-05
ratification ("option A") are not
   expected to return during this replay; after the following projection rebuild,
   `primary_names_current` should reflect only the ENS Base `L2ReverseRegistrar`
   declared primary-name authority for Base
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f).
   This mode can re-enable live
   adapter sync after replay catches up, so do not let it
   overlap the projection rebuild:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --name bigname-base-normalized-replay indexer \
       bigname-indexer run \
       --hash-pinned-adapter-sync auto \
       --normalized-replay-catchup-enabled \
       --normalized-replay-defer-projection-indexes
   ```

8. After the normalized replay cursor reaches the reviewed target, stop the
   catch-up indexer before rebuilding projections. If the command above is still
   running because live sync resumed after catch-up, stop that container first:

   ```sh
   docker stop bigname-base-normalized-replay
   ```

9. Rebuild all current projections:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps worker \
       bigname-worker replay all-current-projections
   ```

10. Verify the conflict block, the `linkerman` and `harsh007` one-timeline checks,
   and the identity-10k sample before restoring the API, worker, and normal
   indexer service.

Operational catch-up to finalized head should be run as bounded idempotent
backfill chunks. Before every chunk starts range work, check current Postgres
size, writable free disk, and any configured object-cache budget. Capacity
shortage should pause or fail the chunk explicitly instead of silently retaining
less selected replay data or retaining full payload bundles for empty historical
blocks.

## GHCR Image

The repository publishes the image to:

```text
ghcr.io/ensdomains/bigname
```

The GitHub Actions workflow publishes only after the full CI workflow succeeds
for a push to `main`. Successful main pushes publish `latest` and the short
commit SHA tag. Release-tag image publishing is deferred and is not automatic.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  --build-arg BIGNAME_BUILD_SHA=$(git rev-parse HEAD) \
  -t ghcr.io/ensdomains/bigname:latest \
  -t ghcr.io/ensdomains/bigname:$(git rev-parse --short HEAD) \
  --push .
```
