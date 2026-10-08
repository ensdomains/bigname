# Deployment

The deployable runtime consists of the API and the phase runner. The phase
runner implements `ingest`, `interpret`, `project`, read-only `verify`, and
continuous `live` follow. The deleted indexer, worker, legacy execution crate,
and their operational commands are not present in the image.

## Container contents

The image contains these runnable binaries:

- `bigname-api`
- `phase-runner`

The entrypoint selectors are:

```sh
docker run --rm ghcr.io/ensdomains/bigname:latest api
docker run --rm ghcr.io/ensdomains/bigname:latest phases-migrate
docker run --rm ghcr.io/ensdomains/bigname:latest phases
```

The one-time `phases-migrate` command invokes `phase-runner init-schema` and
requires an empty `bigname_phase` schema. It refuses every nonempty phase schema
because initialized-schema upgrades are applied separately through reviewed
versioned schema-migrations, or through the replacement procedure when an
in-place change cannot preserve durable state. `phases` then invokes
`phase-runner run` with `bigname_phase` as its search path. It can
persist ingest-through-project output and continuously follow provider heads,
including reorg-driven downstream redo and canonical-head hydration. Its
read-only verification phase can compare Base's Coinbase-loaded range with dRPC
through the `48,428,000` ingest seam and, in an opt-in reader build, Ethereum Mainnet with local reth. Only a distinct [verification-only](glossary.md#source-role) reference earns an independent level, and the target-covering intake cursor records
`quick_synced` without one. V2 and operational paths consume its
phase projections and lookup output. Apply append-only SQLx schema-migrations
through deployment automation; there is no application schema-migration command
in the image. A release may also carry explicitly reviewed additive baseline
indexes whose exact `CREATE INDEX CONCURRENTLY` statements and validity checks
are listed in the release runbook. Those exceptional indexes are applied and
recorded as a separate pre-deploy step rather than entered in `_sqlx_migrations`.

Standard builds and the published image currently omit direct Reth database
support and the `reth-db-smoke` executable. Mainnet intake uses the configured
`ETHEREUM_INTAKE_RPC_URL` via `drpc`; without an independent reference, Verify
records `quick_synced`. The optional reader remains available for explicit local
builds described in [Direct Reth reader](reth-db-reader.md).

## Server Compose

`docker-compose.server.yml` starts PostgreSQL, the API, and the phase runner.

```sh
cp .env.server.example .env.server
# Set credentials, the image, a positive disk floor and an absolute probe path.
# Complete the capacity preflight linked below before starting services.
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

Server Compose requires nonempty `BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES`
and `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`; missing or empty values fail rendering.
It also requires a container memory ceiling per service —
`POSTGRES_MEMORY_LIMIT`, `BIGNAME_API_MEMORY_LIMIT`,
`BIGNAME_PHASE_RUNNER_MEMORY_LIMIT`, and `BIGNAME_PUBLIC_PROXY_MEMORY_LIMIT`
with the public overlay — rendered as `deploy.resources.limits.memory`, so a
data-dependent spike is contained to the container that produced it (it is
OOM-killed and restarted under `restart: unless-stopped`) rather than left to
the host OOM killer to resolve among the runner, the API, PostgreSQL and a
co-resident archive node. There are no defaults: size them in the
[capacity preflight](runbooks/production-docker.md#capacity-preflight), where
PostgreSQL's ceiling includes the page cache it reads through (the kernel
charges it to the container), and validate the rendered model with
`scripts/check-compose-memory-limits`, since Compose accepts `0` and Docker
reads it as no limit. Every
service logs through the `json-file` driver with rotation
(`BIGNAME_LOG_MAX_SIZE`, default `100m`, times `BIGNAME_LOG_MAX_FILE`, default
`5`), so container logs are bounded on the volume PostgreSQL writes to.
Choose a positive reserve for the actual deployment, and pre-create a dedicated
writable sibling on PostgreSQL's filesystem. The same absolute path is used on
the Docker daemon host and inside the runner. Do not expose database files or
change the existing PostgreSQL volume. Complete the [capacity preflight](runbooks/production-docker.md#capacity-preflight)
for every active overlay before recreating the runner. A successful render alone
does not prove filesystem identity, permissions or protection.

Before the full `up`, apply reviewed versioned schema-migrations with the
schema-migration runner — `sqlx migrate run --source migrations` from the
deployed commit, named
step by step in
[`runbooks/production-docker.md`](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
— then initialize `bigname_phase` only for a fresh database. Retain an existing
namespace when its in-place schema-migrations pass; replace it as described
below only when a reviewed schema-migration cannot preserve its durable state.
Then provision the non-owner `bigname_api` login. Set
`BIGNAME_API_DATABASE_URL` to that login; Compose deliberately does not fall
back to `BIGNAME_DATABASE_URL` for the API. The phase runner and
schema-migration automation use the writer URL.

Preflight every release with `sqlx migrate info --source migrations` against
the writer URL and confirm no version is pending. Also complete any explicitly
listed manual concurrent baseline-index step and verify each named index before
starting the new artifact. Neither the API nor the phase runner reports the
applied schema version. Missing lookup DDL checked by API startup produces the
diagnostic described under [Surviving services](#surviving-services); other
forgotten schema-migrations or release-specific index steps surface only as
runtime query failures or unacceptable query plans.

For the historical discovery lookup index, prebuild concurrently on a large live
database following [the index runbook](../ops/discovery-history-index/README.md)
before applying its matching schema-migration. Verify index validity and record
the before/after query plans and completed-batch throughput.

The exact discovery observation reopen lookup has a separate unrestricted index
because replay must also find orphaned and closed observations. Follow its
[online index runbook](../ops/discovery-reopen-index/README.md) before applying
the matching schema-migration on a large initialized database.

Project's history lookups for changed names and primary names use eight indexes
on `normalized_events`. Prebuild them concurrently on a large initialized
database following [their index runbook](../ops/project-scoped-history/README.md)
before applying the matching schema-migrations. The script fails unless all eight
are valid, ready, and have the reviewed definition, and the later validity-check
schema-migration refuses the same shapes.

Project's scoped node history and progressive mirror dependency traversal read
`normalized_events` and `name_surfaces` through five indexes. Prebuild them
concurrently on a large initialized database following
[their index runbook](../ops/project-progressive/README.md) before applying the
matching schema-migrations, and run its `validate.sql` before recording the
release. A deployment that already recorded
`20260922010100_project_mirror_scope_indexes.sql` (Sepolia) must first update
that version's recorded checksum, as the runbook's
[checksum section](../ops/project-progressive/README.md#recorded-checksum-of-20260922010100)
describes, because the file no longer builds two obsolete label indexes.

Interpret's per-batch [lookahead loader](glossary.md#lookahead-loader) reads
`normalized_events` through eight partial indexes: two expression indexes for
the ENSv1 families, two for the Basenames Base families and four for the ENSv2
families. Follow their
[online index runbook](../ops/v1-lookahead-indexes/README.md) before applying
the matching schema-migrations on a large initialized database, and before
starting a release whose loader covers the chain.

The address history read looks up an address's past names and resources
through three partial expression indexes on `normalized_events`, and its
registry root role changes through a fourth. Follow their
[online index runbook](../ops/address-history-indexes/README.md) before applying
the matching schema-migrations on a large initialized database.

History and event pages read `normalized_events` in chain-position order through
`normalized_events_chain_block_number_desc_idx`. Follow its
[online index runbook](../ops/events-order-index/README.md) before applying the
matching schema-migration on a large initialized database; without the prebuild
that schema-migration blocks writes to `normalized_events` while it builds.

Project's [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
dependency expansion and evidence staging, and the history reader's mirror
lookup, find ENSv1 registry resolver pointers by the node each event addresses
through `normalized_events_project_v1_pointer_addressed_node_idx`. Follow its
[online index runbook](../ops/mirror-pointer-index/README.md) before applying the
matching schema-migration on a large initialized database, and before starting a
release that reads it; without the prebuild that schema-migration blocks writes to
`normalized_events` while it builds.

The permission family readers use the indexes in
`20260928190000_project_families_permission_read_indexes.sql` and
`20260928223000_project_permission_candidate_indexes.sql`. On a large initialized
database, prebuild these concurrently before applying those schema-migrations;
without a prebuild their ordinary index creation blocks writes to the indexed
tables. Run each statement outside a transaction:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_grant_subject_idx
    ON bigname_phase.project_grant (subject);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_grant_scope_idx
    ON bigname_phase.project_grant (chain_id, scope);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_account_approval_subject_idx
    ON bigname_phase.project_account_approval (subject, authority_kind);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_registry_binding_observation_resource_idx
    ON bigname_phase.project_registry_binding_observation (chain_id, resource_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_registry_binding_observation_owner_idx
    ON bigname_phase.project_registry_binding_observation
        (chain_id, registry_contract, registry_owner);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_grant_subject_resource_idx
    ON bigname_phase.project_grant (subject COLLATE "C", resource_id, scope COLLATE "C");
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_grant_resource_subject_idx
    ON bigname_phase.project_grant (resource_id, subject COLLATE "C", scope COLLATE "C");
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_registry_binding_observation_owner_target_idx
    ON bigname_phase.project_registry_binding_observation
        (chain_id, registry_contract, registry_owner, target_resource_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_registry_binding_observation_owner_resource_idx
    ON bigname_phase.project_registry_binding_observation
        (chain_id, registry_contract, registry_owner, resource_id);
```

Confirm all nine indexes are `indisvalid` and `indisready` in `pg_index`, and
compare `pg_get_indexdef` with the statements above before applying the
schema-migrations. `IF NOT EXISTS` does not validate an existing definition or
repair an invalid concurrent build. Record account-page query plans and latency
on representative data before serving the release; bounded candidate batches
can still examine many resources when most grants are masked or observations
no longer match the current registry binding.

`20260928130000_project_families_name_history.sql` adds `project_name_history`
to the [owned key families](glossary.md#owned-key-family) and, on a database
whose families were built without it, resets every family table and the
[family marker](glossary.md#family-marker), so the next family run rebuilds
them. Every fenced route answers `409 stale` until that rebuild finishes. It
does not coordinate with a
running family publisher: a family run in flight when it applies fails once and
the next run rebuilds.

`20260928160000_project_families_name_summary.sql` adds `project_name_summary`,
the [name summary](glossary.md#name-summary), the same way, with the indexes its
writer reads. Apply it before starting a release that writes it: that
release's family step writes the table on every block, so it fails until the
table exists. It opens the same `409 stale` window until the rebuild finishes,
and, as above, a family run in flight when it applies fails once and the next
run rebuilds.

`20261005170000_project_address_history_catalogue.sql` installs the compact Project
address-history tables, replaces four source-event indexes with the full public history
order, and adds two complementary noncanonical name/resource indexes for conservative work
discovery. On a populated database, first follow the
[concurrent prebuild and adoption runbook](../ops/address-history-catalogue-indexes/README.md).
It retains the four old indexes while building their replacements under temporary names,
and builds the two new indexes concurrently. The migration validates all six before any
old-index drop and adopts the replacements by a short transactional rename; missing or
invalid prebuilt candidates fail before old-index loss. Empty databases can build directly.
Record the preceding migration versions before prebuilding, and budget the temporary index,
WAL and sort-file space described in the runbook. Installing it alone publishes no catalogue. This producer changes the interpreter
content hash: use matching runner/API binaries and complete the full-history Interpret redo
and the Project redo it installs before serving with the new binary. Existing interpreted
inputs are replayed through the normal lifecycle; no request backfill, manual marker edit or
extra Ingest fetch is part of this change. The per-chain completeness stamp must match the
family marker's content hash, version, sequence and block/hash; absent or mismatched state
remains stale. When another release change already requires replay, one replay under the final
combined binary covers both changes. A replay under an earlier hash does not cover this one.
After applying this schema-migration, grant an existing API role SELECT on all four
[address-history catalogue](glossary.md#address-history-catalogue) tables using the
[upgrade grants below](#address-history-catalogue-role-upgrade) before starting the new API.

`20260929160000_remove_served_projections.sql` drops the tables the API and
Project used before the [owned key families](glossary.md#owned-key-family)
became the only serving path: `name_current`, `children_current`,
`permissions_current`, `account_permission_state_current`,
`permissions_current_resource_summary`, `record_inventory_current`,
`resolver_current`, `address_names_current`, `address_records_current`,
`primary_names_current`, the append-only `project_generation_failures` audit,
and the three Project redo handoff tables `project_redo_resolver_evidence`,
`project_redo_expiry_roots` and `project_redo_child_registration_history`. It
also drops the trigger function that retired divergence observations when the
old exact-name table published a null exact resolver, and replaces the two guarded
lookup functions so they compare captured family publications only, keeping
their signatures and grants. Family tables, the [family
marker](glossary.md#family-marker), normalized events and their history
indexes, and `child_registration_events` are unchanged; the migration itself
does not reset or rebuild them.
Stop every older phase runner, one-shot redo and API process before applying
it: those binaries still read the dropped tables and fail once they are gone.
Rows in the dropped tables, including historical generation-failure audit rows,
are not preserved; export them first if an incident record needs them.
`BIGNAME_SERVE_FROM_FAMILIES` no longer exists: Compose does not forward it, and
neither binary reads it, so a value left in `.env.server` has no effect.

`20260929180000_project_resource_pointer_root_node_index.sql` adds the partial
index `project_resource_pointer_root_node_idx` that the composed name reader's
resource pointer lookup can probe for ENSv2 root registry pointers. It builds
with a plain `CREATE INDEX` inside the schema-migration transaction, which takes a
SHARE lock on the whole of `project_resource_pointer`, every chain and every
source family, until the schema-migration commits. That blocks every write to the
table and `VACUUM` and `ANALYZE` on it; reads continue. Before the build starts,
the schema-migration also waits for any transaction already writing the table. Apply
it in the same planned window as the 7c removal above, with the phase runner,
redo processes and API stopped, so nothing waits on it and it waits on nothing.
It needs no concurrent prebuild in that window.

The build reads the table once and stores only the root registry rows. On a
500,000-row copy held in cache it took about 50 ms. That is an observation of
that copy, not a bound on staging or production, whose size, cache state and
dead rows differ; watch the actual lock wait and duration. To bound them, apply
the versions before it as usual, then apply this one on its own with a lock and
statement budget, for example:

```sh
PGOPTIONS='-c lock_timeout=10s -c statement_timeout=10min' \
  sqlx migrate run --source migrations --database-url "$BIGNAME_DATABASE_URL" \
  --target-version 20260929180000
```

If it fails on either timeout, the schema-migration's transaction rolls back and
`_sqlx_migrations` does not record it, so it stays pending. On a lock timeout,
find the session holding a lock on `bigname_phase.project_resource_pointer` in
`pg_locks` and `pg_stat_activity`, stop the process that owns it, and run the
same command again. On a statement timeout, check the table's size and rerun
with a larger budget that still fits the window. Afterwards, confirm that
`project_resource_pointer_root_node_idx` is `indisvalid` and `indisready` in
`pg_index` and that `pg_get_indexdef` shows `(chain_id, namespace, namehash)`
with `WHERE (source_family = 'ens_v2_root_l1'::text)`: `IF NOT EXISTS` skips an
existing index of the same name without checking its definition.

`20260929190000_project_family_name_lookup_indexes.sql` adds
`project_lifecycle_key_state_name_idx` (partial, `WHERE logical_name_id IS NOT NULL`) and
`project_name_state_name_idx`, both on `(chain_id, logical_name_id)`, which the name summary
composition probes by name id. Each is a plain `CREATE INDEX` inside the
schema-migration transaction, so it takes a SHARE lock on its whole table until the
schema-migration commits, blocking every write to the table and `VACUUM` and `ANALYZE` on
it, and waits first for any transaction already writing it. Apply it in the same planned
window as the two above, with the phase runner, redo processes and API stopped. The same
`lock_timeout`, `statement_timeout` and retry procedure apply, with
`--target-version 20260929190000`. Afterwards, confirm both indexes are `indisvalid` and
`indisready` in `pg_index` and that `pg_get_indexdef` shows `(chain_id, logical_name_id)`,
with `WHERE (logical_name_id IS NOT NULL)` on the key state index only.

`20261001110000_project_child_registration_registry_index.sql` adds
`project_child_registration_state_registry_idx` on `(chain_id,
registry_contract_instance_id)`, and
`20261001110100_project_child_edge_candidate_child_index.sql` adds
`project_child_edge_candidate_child_idx` on `(chain_id, namespace, child_node)`; the child
pages and counts, the registry labels read among them, probe both. Each is a plain
`CREATE INDEX` with the same SHARE lock on its table until the schema-migration commits.
Apply them with the phase runner, redo processes and API stopped, with the same
`lock_timeout`, `statement_timeout` and retry procedure, `--target-version
20261001110000` and then `20261001110100`. Afterwards, confirm both indexes are
`indisvalid` and `indisready` and that `pg_get_indexdef` shows those columns.
The same build narrows the child reads in `crates/storage/src/families`, so it also
rotates the [interpreter content hash](glossary.md#interpreter-content-hash) for every
chain even though no stored row changes. After the migrations, an existing deployment finishes the
full-range Interpret redo and then the stamped Project redo it installs before the
matching API serves, as the [handoff](#phase-runner-configuration) describes; until
then its snapshot-selected reads answer `409 stale`. When this build ships together with the
[resolver set while registering a wrapped name](#resolver-set-while-registering-a-wrapped-name)
change, which rotates the hash too, that change's single redo pair discharges both rotations.

`20261001120000_name_surfaces_name_order_index.sql` adds `name_surfaces_name_order_idx` on
`name_surfaces (raw_name, namespace, namehash, logical_name_id)`, partial on active surfaces in
the three readable canonicality states whose name is at most 2000 bytes. The search candidates
and a resolver's bound names can read it in page order instead of sorting every match ([storage](storage.md#table-ownership)). It is a plain `CREATE INDEX`
with the same SHARE lock on `name_surfaces` until the schema-migration commits, which blocks
Interpret's writes to it. Apply it with the phase runner, redo processes and API stopped, with
the same `lock_timeout`, `statement_timeout` and retry procedure and `--target-version
20261001120000`. On a large initialized database, prebuild it concurrently first, outside a
transaction, so the schema-migration finds it and skips the build:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_name_order_idx
    ON bigname_phase.name_surfaces (raw_name, namespace, namehash, logical_name_id)
    WHERE visibility_state = 'active'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND octet_length(raw_name) <= 2000;
```

Either way, confirm the index is `indisvalid` and `indisready` in `pg_index` and that
`pg_get_indexdef` shows those four columns and the three predicate terms before serving. `IF NOT EXISTS` matches the name
only: an interrupted concurrent build leaves an invalid index that must be dropped with `DROP
INDEX CONCURRENTLY` before the build is run again. The same build adds the matching length bound
to those two readers in `crates/storage/src/families`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every chain although no
stored row changes. An existing deployment finishes the full-range Interpret redo and then the
stamped Project redo it installs before the matching API serves, as the
[handoff](#phase-runner-configuration) describes; a release batch that rotates the hash for
another change discharges both with one redo pair. Names longer than 2000 bytes are no longer
listed by search or a resolver's `bound_names` ([routes](api-v1-routes.md#get-v1search));
reverse lookup still lists them. Cursors issued before the change continue.

The record inventory reads behind `GET /v1/names/{name}`, `GET /v1/names/{name}/records`,
`POST /v1/lookup`, verified lookup and `GET /v1/diagnostics/names/{name}/records` no longer evaluate the history record attribution, which
none of them serves or checks ([storage](storage.md#table-ownership)); a name on a resolver with
many writes no longer holds a database connection for seconds on each read. Responses do not
change. The edit is in `crates/storage/src/families`, so it rotates the [interpreter content
hash](glossary.md#interpreter-content-hash) for every chain although no stored row changes; a
release batch that rotates the hash for another change discharges both with one redo pair.

The API binds to the configured `BIGNAME_API_HOST` and
`BIGNAME_API_PORT`; `/healthz` remains its local readiness endpoint. Current
runtime configuration is documented in
[`production.md`](production.md) and [`development.md`](development.md).
A directly launched API can configure its metrics listener with
`BIGNAME_API_METRICS_BIND_ADDR`. The server Compose file instead fixes that
container listener at `0.0.0.0:9464`; `BIGNAME_API_METRICS_HOST` and
`BIGNAME_API_METRICS_PORT` change only its host port mapping.

## Phase-runner configuration

The implemented phases use:

- `BIGNAME_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT`
- `BIGNAME_PHASE_RUNNER_CHAINS`
- `BIGNAME_PHASE_RUNNER_SOURCES`
- `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`
- `BIGNAME_PHASE_RUNNER_RPC_CHAIN_CHECK` — `full` (default) or `chain-id-only`; see [RPC chain check](#rpc-chain-check)
- `BIGNAME_PHASE_RUNNER_INSTANCE_ID`
- `BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES`
- `BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES` — required server-Compose floor
- `BIGNAME_PHASE_RUNNER_WRITABLE_PATH` — required server-Compose probe directory
- `BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES` — optional logical database ceiling
- `BIGNAME_PHASE_RUNNER_MEMORY_LIMIT` — required server-Compose container memory ceiling
- `BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR`
- `BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR`
- `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS`

An unset optional ceiling configures no limit; Docker inspection may show its bare
variable name without `=`. An explicitly empty `KEY=` value is invalid.
A configured ceiling must parse as an unsigned 64-bit integer; zero is a limit,
not a way to disable it. The floor must also parse as an unsigned 64-bit integer.
Operational admission rejects floor zero: Compose only enforces presence, and
the unchanged CLI accepts zero. Direct CLI defaults remain floor zero, path `.`
and no ceiling; the existing capacity poll interval remains five seconds.
These settings are read at startup. Shell values override `--env-file`, so inspect
both interpolation inputs and the effective service/container configuration.

`BIGNAME_DATABASE_URL` is the writer credential. Supervised `run` and a
`verify` redo also require
`BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL`, pointing at the same
database with a different login. The verifier rejects that login unless it has
USAGE on `bigname_phase`, SELECT on every relation there, no write privilege on
an application relation, no database/schema creation authority, no elevated
role attributes, and no role memberships. The URL must authenticate that login
directly: startup rejects a writer session that assumes the reader role. A
reader is accepted only when its PostgreSQL system identifier, database OID,
and database name match the writer connection. A non-verification redo does
not require the reader URL.

`BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES` bounds the number of
persisted per-key interpreter values held by each active [interpreter
session](glossary.md#interpreter-session). It defaults to 65,536 entries. Lower
values reduce process memory and cause more indexed reads from
`normalized_events`; zero is valid and forces every required pre-batch value
through that read path. The setting does not change stored output or the
[interpreter content hash](glossary.md#interpreter-content-hash).

Interpret chooses how it restores prior adapter state for each chain and each
batch; there is nothing to enable. When every active or deprecated manifest of
the chain belongs to a source family the
[lookahead loader](glossary.md#lookahead-loader) covers (the five `ens_v1_*`
families, the four `basenames_base_*` families, plus `basenames_l1_compat` and
the `*_execution` families, whose only log, a Universal Resolver proxy's
`Upgraded`, reads no prior state, and the five `ens_v2_*` families), and the
chain retains no `normalized_events` history of an uncovered family whose
manifest has moved to `draft` or `shadow`, Interpret uses the lookahead loader:
it reads the names and resources the batch's logs mention plus the
registrations falling due in the batch, restores only their history, and keeps
no [interpreter session](glossary.md#interpreter-session) between batches. On a
chain with an ENSv2 manifest it also restores the ENSv2 events filed under the
[ENSv2 state keys](glossary.md#ensv2-state-key) those logs and events link to,
such as the registry, token and resolver a log names and the registries above
it, plus the ENSv2 tokens whose expiry falls in the batch, and then the ENSv1
history of every name those events mention. Interpretation can reach a name or
key no log or event mentions, such as the ENSv1 predecessor of a name being
migrated; when it does, Interpret adds it, reads its history and interprets the
batch again inside the same read snapshot, until an attempt reads only loaded
names and keys. Only that attempt's output is published.
Otherwise it uses the full-state loader, which restores all retained history
once and then carries the session. Ethereum mainnet, Ethereum Sepolia and Base
all use the lookahead loader. On a lookahead chain a reorg costs one redo batch read for
the names it touches and a runner restart costs nothing, where the full-state
loader restores the chain's history again after each. Both loaders must produce identical stored output and share one
[interpreter content hash](glossary.md#interpreter-content-hash), so a change
of loader needs no redo. The choice can change only when a release changes the
chain's manifest set, including moving to `draft` or `shadow` a manifest whose
family wrote history that is still retained, or when a redo removes the last
retained history of such a family; a change to the full-state loader costs one
cold restore of the chain's history, with the memory that implies.

The runner logs the choice at info level when a chain's loader is first chosen
and whenever it changes (`interpret chose its prior-state loader`,
`interpret changed its prior-state loader`), with the source family, and the
rollout status of its manifest, that required the full-state loader.
Each full-state cold restore logs at info level when it starts (`interpret is
restoring prior adapter state from stored events`, with the chain, the first
block of the batch and the reason) and when it finishes (`interpret restored
prior adapter state`, with the number of events read and the elapsed
milliseconds). After a reorganization the chain restores once, for the redo of
the orphaned blocks: the session the completed redo ends with carries into the
next normal batch, unless the lineage is orphaned again before that batch.
`BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true`
(`--interpret-force-full-state-loader`) is the one operator override: it makes
every chain use the full-state loader. It defaults to false. The lookahead
loader depends on all eight: the two `normalized_events_v1_*_probe_idx`, the
two `normalized_events_basenames_*_probe_idx` and the four
`normalized_events_v2_*_probe_idx` indexes. Every chain runs the same name
probe queries, each with an ENSv1, a Basenames and an ENSv2 arm, so a missing
name index slows every lookahead chain, not only the chain whose events it
holds; only a chain with an ENSv2 manifest reads the other three ENSv2 indexes.
Build all eight on an
initialized database as described in
[`ops/v1-lookahead-indexes/README.md`](../ops/v1-lookahead-indexes/README.md)
before starting a release that contains the loader. If interpretation reads a
name the loader did not restore, that attempt is discarded; Interpret never
publishes output from partial state.

Ingest throughput can be tuned at process startup for both `run` and `redo`:

| Environment variable | CLI option | Default | Allowed values |
| --- | --- | --- | --- |
| `BIGNAME_INGEST_BLOCKS_PER_BATCH` | `--ingest-blocks-per-batch` | 256 | 1–4096 |
| `BIGNAME_INGEST_RPC_BATCH_SIZE` | `--ingest-rpc-batch-size` | 32 | 1–256 |
| `BIGNAME_INGEST_RPC_MAX_IN_FLIGHT` | `--ingest-rpc-max-in-flight` | 8 | 1–32 |

The block limit applies to normal RPC Ingest and the shared window used by
Ingest redo. Normal Coinbase SQL keeps its 1024-block window; normal direct-Reth
and Live keep their existing block limits. RPC request settings apply to the
shared Ingest/Live engine, including Coinbase's companion RPC. They do not tune
the separate verification provider or API requests. CLI options override the
environment; invalid limits fail configuration before database startup.

RPC batch size counts JSON-RPC calls within one HTTP request; a value of 1 sends
standalone calls. The in-flight limit is shared across HTTP requests to each
provider instance, including range-log queries, retries and batch fallbacks.
It is not a process-wide limit across different chains/providers. All header,
source-boundary and reorg checks still run, regardless of window size.

Some hosted providers answer `null` for a receipt or transaction they hold when
many batches run at once. Ingest asks for each selected receipt or transaction
that came back `null` again, without the rest of the window, up to three times
with a 250 ms, 500 ms and 1 s pause, before it fails the window as a transient
error. A result still `null` after that is treated as the transaction leaving
the chain, as before. `phase_runner_ingest_provider_null_results_total` counts
every `null` answer Ingest accepted, including re-requests ([monitoring
runbook](runbooks/pipeline-monitoring.md#ingest-rpc-traffic)); if it keeps
climbing, lower the batch size or the in-flight limit.

For a first historical-ingest comparison, set these in the server Compose env
file, or export them when launching `phase-runner` directly:

```dotenv
BIGNAME_INGEST_BLOCKS_PER_BATCH=1024
BIGNAME_INGEST_RPC_BATCH_SIZE=128
BIGNAME_INGEST_RPC_MAX_IN_FLIGHT=8
```

Confirm the RPC endpoint accepts that batch width, then compare throughput,
request failures, memory and batch duration against the defaults. Larger windows
reduce per-window work and commits, but still fetch and verify every block.
They retain more data, extend transactions and increase work retried after a
failure. The settings do not impose a log-count or memory-byte cap, and the
provider's per-response log limit is not an aggregate memory budget. Normal
finalized-history log prefetch remains 10000 blocks; redo does not use it.
Changes require a runner restart, but no schema migration or redo of completed
history. Reset to 256 / 32 / 8 to restore the default tuning.

`BIGNAME_INTERPRET_BLOCKS_PER_BATCH` (`--interpret-blocks-per-batch`) sets how
many canonical blocks one Interpret [batch](glossary.md#batch-grid) reads,
interprets and publishes in one transaction. It defaults to 500 and must be at
least 1. It is the operator's control over Interpret memory per batch: the
lookahead loader has no row or byte limit of its own, so a batch in which very
many registrations fall due or very many names change loads all of their
history, and a smaller batch holds fewer of them at once. Names that fall due at
one block timestamp cannot be split across batches. The setting must not change
stored output or the interpreter content hash, so it can be changed between runs
without a redo.

`BIGNAME_INTERPRET_LOOKAHEAD_STATEMENT_TIMEOUT_SECS`
(`--interpret-lookahead-statement-timeout-secs`) sets a PostgreSQL
`statement_timeout`, in seconds, on the lookahead loader's read transaction. It
defaults to 0, which sets no timeout, so a legitimately large batch is never
killed by default. With a value set, a read that exceeds it fails the batch with
a database error and the runner retries the same batch; use it only to surface a
bad query plan, and prefer a smaller batch when a batch is simply large.

`BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR` configures the Prometheus listener for
a directly launched runner and defaults to `127.0.0.1:9465`. The server Compose
file fixes the container listener at `0.0.0.0:9465` and publishes it on host
loopback by default; `BIGNAME_PHASE_RUNNER_METRICS_HOST` and
`BIGNAME_PHASE_RUNNER_METRICS_PORT` change only that Compose port mapping. The
redo command deliberately ignores that runner variable: its separate
`BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR` defaults to the ephemeral
`127.0.0.1:0`. Its info-level startup event records the selected port; set
`RUST_LOG=info` to display it. Set the redo variable or pass
`--metrics-bind-addr` when a stable, unique repair target will be scraped. Each
listener serves `GET /metrics`. Every five seconds it reads phase progress,
heartbeats, verification, unfinished repair work, and the published chain head
from the runner-owned tables. It also reports an in-process heartbeat for the
runner loop of each supervised chain or the active one-shot repair chain, plus
the process-start timestamp used to detect repeated restarts.
It does not write metric state to PostgreSQL. Missing block positions and phase
heartbeats are exported as `-1`, rather than being silently omitted. See the
[pipeline monitoring runbook](runbooks/pipeline-monitoring.md) for the checked-in
Prometheus rules and Grafana dashboard.

`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` is exported for the checked-in
phase and runner-loop heartbeat alerts. It defaults to 900 seconds. Set it
above the slowest healthy batch or inter-phase transition observed in the
deployment: heartbeats record completed work opportunities between batches,
not proof that a long batch is still executing. Rebuild batches during a
planned [re-derivation boundary](glossary.md#re-derivation-boundary) have
historically exceeded eight minutes, so calibrate this threshold before the
full source re-walk rather than after its first false page. The alerts then
require the configured age to remain exceeded for another two minutes before
paging.

Point both database URLs at the writer primary. Never point the verification
URL at a replica, standby, physical basebackup clone, or a pooler that can route
it to one. Physical copies retain the system identifier, database OID, and
database name, so a lagging copy can pass the identity check and then cause a
spurious fatal mismatch because recent stored rows are absent. A logical
restore has a new identity: repoint both URLs to its primary together so both
connections observe that new identity. A mixed old/restored pair fails the
startup check, as intended.

Provision the login after `phase-runner init-schema` (substitute the database,
role, and secret through the normal secret-management path):

```sql
CREATE ROLE bigname_verify
    LOGIN PASSWORD '<secret>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
REVOKE CREATE ON DATABASE bigname FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE bigname TO bigname_verify;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO bigname_verify;
GRANT USAGE ON SCHEMA bigname_phase TO bigname_verify;
GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase TO bigname_verify;
```

The role provisioning is an operational database grant, not schema-v2
schema-migration authority. Reapply and revalidate the SELECT grant after every
approved phase-schema rebuild or additive schema-migration that creates a
table. In particular, after applying the attestation-audit schema-migration for
the [manifest-authority marker](glossary.md#manifest-authority-marker), run the
`GRANT SELECT ON ALL TABLES` statement again before
starting the runner. Stop every old phase-runner and one-shot redo process
before applying that schema-migration, and keep them stopped until the new
binary is ready. An old binary recognizes the marker prefix but does not bind
its boolean attestation to the new generation token or write the durable audit
row. PostgreSQL does not extend an earlier
all-tables grant to tables created later. Do not reuse the writer credential in
the verification URL:
setting a writer session's default transaction to read-only does not remove
that role's write authority, and startup rejects it.

The phase runner accepts each
`BIGNAME_PHASE_RUNNER_SOURCES` entry in the form
`CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV`; omission of `ROLE` defaults to `both`. Role tokens are exact: use `verification-only`, not `verification_only`; source-kind normalization does not apply to roles. The named
environment variable contains the provider URL. Under the ratified contract,
before Ingest can make its first provider write, the runner persists each
[intake-capable source](glossary.md#source-role)'s cursor row with its kind, seed basis,
and start block and with empty progress fields. On every chain, changing a
source's normalized kind after that row exists is a data-integrity error checked
before Ingest runs. Case-only changes, surrounding whitespace, and
hyphen/underscore spelling changes are equivalent. Before a runnable Ingest
phase contacts a provider, each row's seed basis and start block must also match
the runtime source. A restart that skips an already-completed Ingest phase still
requires every configured intake-capable source's persisted key, kind, seed basis, and start
block to match, and the configured intake-capable source-key set must exactly match the persisted cursor keys. Standalone Interpret and Project redo require the complete intake-capable descriptor set and perform that exact-key check without contacting ingest providers; in an `all` redo, Ingest performs the check before Interpret replays. Any change to a persisted identity field—source key,
normalized kind, seed basis, or start block—requires an explicitly reviewed
reset that removes the cursor and every durable Ingest output that may have come
from the source, followed by a [full source
re-walk](glossary.md#re-derivation-boundary); never relabel the row in place.
Changing only the provider endpoint is allowed because endpoints are not part
of persisted source identity and will not trigger the runtime reset guard.

### RPC chain check

Before `run` opens its database, before `redo` writes anything, and before
`source-transport` connects, every RPC endpoint the runner is given must show
that it serves the chain it is configured for. That covers each `BIGNAME_PHASE_RUNNER_SOURCES`
entry with an RPC kind, whatever its role, and each
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` entry; hydration URLs are checked and
reported under the source key `hydration`. A configured source may also use
that key; only its own endpoint's observation is recorded on its ingest cursor.
Only a source whose endpoint is a well-formed URL with a host for another
scheme, such as a fixture placeholder, is skipped; a malformed endpoint, or a
bare `host:port` without `http://`, refuses the start. Replay and rebuild never
hydrate, so `redo` checks no hydration URL, and a Project redo reads no source provider, so it checks
nothing. An Interpret redo checks its sources, because its discovery repair can
run Ingest. The endpoint must answer `eth_chainId` with the chain's EIP-155 id
and, in the default `full` mode, return block 0; a wrong chain id is refused
before block 0 is asked for. On `ethereum-mainnet` and `ethereum-sepolia` block 0's hash
must be that chain's genesis hash (`crates/domain/src/chain_identity.rs`);
`base-mainnet` has no pinned genesis hash, so it is checked by chain id plus a
readable block 0. A chain slug with no known chain id is refused. A mismatch
exits with code 1 and one error log naming the chain, source key, expected
and observed chain id, the expected genesis hash where one is pinned, and the
observed genesis hash when block 0 was read. An endpoint that cannot be read,
including one that still does not answer after the provider's usual retries,
also exits with code 1, and its error log names the chain, source key and the
error only. Neither log, nor the retry warnings before it, carries the URL's
path, key or query; an HTTP error is logged by its status only and a JSON-RPC
error by its code and that code's standard meaning, such as `-32601 (method not
found; provider message omitted)`, without the provider's response body or error
message, which can echo them.

`BIGNAME_PHASE_RUNNER_RPC_CHAIN_CHECK=chain-id-only` skips the block 0 read,
for a local node that runs a production chain id on its own genesis, such as
the end-to-end suite's Anvil chains. There is no mode that skips the check.

Ingest, Live, Verify and `source-transport` readers repeat the check before their
first request, again once five minutes have passed, and before any request on
an HTTP client rebuilt after a timeout, including a retry. On `base-mainnet`,
which has no pinned genesis hash, Ingest and Live hold each recheck to the
genesis hash the startup check observed and the cursor recorded, so an endpoint
that moves to another network with the same chain id is refused rather than
accepted on any readable block 0. A Verify reference there has no cursor, so
its rechecks are held to the hash it reported at this start: a reference that
moves mid-run is refused, but one repointed between starts is checked only for
the chain id and a readable block 0. A mismatch then stops
that chain with a configuration error, which is not retried. On an Ingest or
Live source it also sets `phase_runner_rpc_chain_mismatch` (see the
[monitoring runbook](runbooks/pipeline-monitoring.md#alerts)). That gauge pages
only if Prometheus scrapes it before the process exits, so it covers runners
that keep serving other chains; a single-chain runner exits on the mismatch and
is caught by `BignamePhaseRunnerDown` and the refusal log. A Verify reference
that fails pages through `BignamePhaseFailed`. Hydration URLs are
checked at startup only.

Each intake cursor records the chain id its endpoint reported and, once checked
in `full` mode, its genesis hash (`ingest_cursors.verified_chain_id` and
`verified_genesis_hash`). A later start whose endpoint reports another chain
id, or another genesis hash when both are known, is refused as a
data-integrity error before any phase runs, or, when another start created the
cursor after this start's check, when Ingest, Live or Verify next starts, before
the phase runs or reads from its endpoints (a Verify redo may already have built
its reference provider, which reads nothing until then). These columns are not source
identity: a cursor created before them fills them in on its next start, and
moving a source to another node on the same chain stays allowed.

Each chain must have exactly one block-provider intake source that Live follows; the Coinbase SQL historical source is not a block provider.
Adding a second such source is not failover configuration: before configuring
it, define how Live selects one source, because after the Ingest handoff
Interpret fails closed rather than choosing between sources.

**Endpoint-rotation gate:** never reuse an endpoint that served intake during
the retained walk as `verification-only`, even under a different key. The
stronger level covers only facts retained since the last full source re-walk
under the current endpoint-and-role configuration. A former intake endpoint
requires the reviewed affected-chain reset and full source re-walk before it can
serve as the independent reference; current endpoint inequality is not a
substitute. Record and review endpoint-rotation history outside the database,
because phase-runner does not persist it.

Retained raw facts, chain lineage, or header-audit rows block initialization of
any missing configured source row. Lineage and header rows can remain after a
range with no watched transactions, receipts, or logs, and none of this output
identifies its provider. The runner therefore cannot distinguish a safe source
addition from replacement of the source that supplied it. The
[verification-mismatch repair](#verification-mismatch-repair) section describes
the state that a reviewed repair must cover, but it is not an executable reset
authorization. For the Issue #411 source-role transition, only the
[owner-ratified rollout gate](#owner-ratified-sepolia-source-role-rollout), its
applicable reviewed reset and preservation procedure, and the owner-approved
rollback and restoration plan authorize the reset. An ordinary redo is not that
reset.
Capacity, retry, and polling controls use the
`BIGNAME_PHASE_RUNNER_*` names exposed by `phase-runner --help`.
For that rollout, [source roles](glossary.md#source-role) are `intake`, `verification-only`, and `both`; omission defaults to `both`. Only intake-capable keys receive cursors or Ingest/Live requests, and only verification-only sources earn `cross_checked` or `node_checked`; `both` falls back to `quick_synced`. The runner rejects dRPC endpoints with the same parsed URL identity and reth paths that share the configured datadir or any provider-opened storage root (`db`, `static_files`, or `rocksdb`) by filesystem device and inode, without exposing either value. This catches symlink and bind-mount aliases; a missing or inaccessible root falls back individually to canonical or lexical spelling identity. Intake-membership changes require reset. Stronger levels are downgraded after provider-trusted revalidation, while `quick_synced` is not auto-upgraded.
Sepolia's from-zero sources for the Issue #411 rollout are `ethereum-sepolia:sepolia-intake:drpc:ethereum_head:0:intake=SEPOLIA_INTAKE_RPC_URL` and `ethereum-sepolia:sepolia-verify:drpc:ethereum_head:0:verification-only=SEPOLIA_VERIFY_RPC_URL`.
The server Compose file forwards `ETHEREUM_INTAKE_RPC_URL`, the optional
`RETH_DATA_DIR` source and the
hydration URL map. Its reth overlay (`docker-compose.reth-db.yml`) builds the
[direct reader's mount contract](reth-db-reader.md#mount-contract) at the same
container path: a separate writable host directory (`RETH_READER_DIR`) as the
wrapper at `RETH_DATA_DIR`, the node's `db`, `static_files` and `rocksdb`
directories read-only inside it, and the node's existing `db/mdbx.lck` file
writable. A single read-only bind of the datadir does not work: the reader must
update the MDBX lock file, and Reth creates a temporary RocksDB directory beside
`rocksdb`. The overlay also requires `RETH_READER_USER` (the numeric user that
owns the node's lock file) and `RETH_NODE_PID_NAMESPACE` (the node's PID
namespace), and accepts `RETH_NETWORK_NAME` for a node whose Docker network is
not `eth-archive-node_default`. Add any differently named provider environment variable
to the phase-runner service explicitly; `docker compose --env-file` supplies
interpolation values but does not expose arbitrary variables to a container.
Base intake requires Coinbase history
plus the target-covering dRPC at block `48,428,000`. An optional distinct
verification-only dRPC records `cross_checked` through that seam; without one,
the intake dRPC records `quick_synced`. A moved verification source start or
comparison redo above the seam is rejected before redo state is created. Base
with `reth_db` is also rejected during configuration validation:
the pinned reader uses reth's Ethereum node type and Ethereum transaction and
receipt primitives (upstream: .refs/reth/crates/ethereum/node/src/node.rs:L128 @ reth@189c0df3)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L27 @ reth@189c0df3)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L51 @ reth@189c0df3). Bigname does not
implement a separate OP Stack transaction and receipt reader.
Base-aware local database verification is tracked by
[issue #433](https://github.com/ensdomains/bigname/issues/433).
In an opt-in reader build, an explicit verification-only Ethereum Mainnet
`reth_db` records `node_checked`; intake-capable reth alone records
`quick_synced`. `ethereum-sepolia` requires exactly one `drpc` or `reth_db`
intake source at block zero. A distinct verification-only dRPC records `cross_checked`;
otherwise Verify records `quick_synced` when the intake cursor matches its
configuration and covers the finalized target. That binding and coverage are
checked when verification completes, and the returned final block-number/hash
marker must equal the frozen target before completion is recorded or Live can
run. A later reorg may orphan the retained cursor tip above that target, but
the stored parent chain must still reach the exact frozen target hash; a fork
at or below the target is rejected. The runner validates this exact Sepolia
source shape before Ingest creates the source cursor or contacts the provider.
The runner always completes a provider-trusted Verify plan before starting Live, including reference-less Base, Ethereum Mainnet, and Sepolia. A Compared Base plan remains paired unless Base is listed in `verify-before-live`; Ethereum-head intake keeps Mainnet and Sepolia serial for either plan shape. For a provider-trusted completed row, Verify
checks the current configuration and target-covering intake cursor against the
completion-time target without changing the recorded extent as Live finality
moves. A generic RPC
kind is not accepted as Base verification authority
because it does not identify the ratified independent provider.
Each completed dRPC comparison batch logs its actual request count, including
transport retries, range-splitting attempts, and target-marker checks. The count
is log-only: `chain_phase_state` does not persist it. At sweep time, copy every
structured `INFO` event with
`message="stored history verification batch matched its reference"` and fields
`chain_id`, `source_key`, `reference_kind`,
`reference_verification_level`, `reported_verification_level`, `from_block`,
`to_block`, and `reference_rpc_request_count` into the durable operational
record alongside the provider's billed volume. If those events are lost, phase
state cannot reconstruct the count. The measured dRPC cost remains a required
D3 cutover input; D1/D7 tooling must close this durable-accounting gap before
automating the evidence capture.
For every configured chain on which canonical-head hydration runs (currently
`ethereum-mainnet`), the supervised `phase-runner run` needs a `CHAIN=HTTP_URL`
entry in `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`. A missing entry is a fatal
project-phase configuration error. The check runs before any Project batch
publishes, a rebuild after a fingerprint change included, so previously
hydrated values remain intact while the chain is stopped for configuration
repair. The one-shot `phase-runner redo` does not need the entry: its Project
undo and replay read no hydration RPC, and the supervised runner refreshes the
values the replay leaves empty.

Hydration reads run only on a head block, the highest readable block Project
holds, never while it catches up, replays or rebuilds
([follow-only hydration](projections.md#follow-only-hydration)). The endpoint
must therefore answer `eth_call` by block hash at the newest ingested blocks;
it needs no deep historical state for hydration. The runner gives each
hydration request a 5-second connect and 10-second total timeout, and Project
limits the time one block waits for its reads to 30 seconds. An endpoint that fails does not stop
publication. When it does not serve the block, no stored value changes. When
it answers some calls and fails others, a call that fails inside an answered
aggregate clears its own overlay, and a batch that fails as a whole removes no
reverse name and no text value that was still served: the only overlay it can
clear is a text overlay that no longer matched its selector and so was already
not served. See [follow-only hydration](projections.md#follow-only-hydration)
for the outcomes and
[Project family work](runbooks/pipeline-monitoring.md#project-family-work) for
the counters and log lines.

The retained ENS chain set is the union of chains in ENS [name
surfaces](glossary.md#surface-name-surface) and active ENS manifests. Later
`run` and `redo` synchronization allows an empty retained set only when the
incoming [deployment profile](glossary.md#deployment-profile) has zero or one
ENS chain. When retained state exists, exactly one retained ENS chain must equal
the single incoming ENS chain. Every other combination is refused before
manifest versions, contract instances, discovery rules, or normalized
`SourceManifestUpdated` events change. Multi-chain ENS deployment profiles need
an explicit contract change; this guard does not allow them.

Validation and the complete manifest-mutation transaction run on the same
PostgreSQL session that holds the startup advisory lock, so losing that session
also aborts the transaction. Concurrent runners wait and validate against the
manifests installed by the preceding synchronization. Use a separate
database/schema for the other ENS deployment.

Ordinary redo and `recompute-flags` operations do not authorize an in-place ENS
chain switch. Only the explicitly reviewed [full phase-schema replacement
procedure](#replacing-an-initialized-phase-schema) can do so when its documented
preconditions apply.

An empty retained set means that neither ENS name surfaces nor active ENS
manifests supply persisted-chain evidence to this guard. Deprecated ENS
manifests and raw facts without an active ENS manifest or ENS name surface are
outside this startup predicate.

One-shot finite phase work is available through `phase-runner redo` for
`ingest`, `interpret`, `project`, `verify`, and `recompute-flags`.
`--phase all` runs ingest through verify for each selected chain, and
`--all-chains` discovers active manifest chains before dispatching the same
per-chain path. A chain failure stops its remaining phases but does not prevent
later selected chains from running; the command still exits nonzero with the
collected failures. Interpret's effective replay range is handed to Project
through the downstream redo stamp. `--phase all` refuses a chain with any
already-pending redo rather than absorbing that work. If one of its phases
fails, the error lists every pending phase-specific recovery command in
dependency order, including a required Verify redo created by Ingest. The
operator must complete those durable markers before rerunning `--phase all`.
A completed redo restores the phase's pre-redo cursors and lifecycle, except
that a normal phase the redo found running or paused becomes `failed` and must
be resumed. When the restored status is `completed`,
`chain_phase_state.finished_at` is stamped with the redo's completion time, so
it reads as when the phase last completed; a restored `failed` status keeps its
failure time and error.
Verify redo checks its source
and SELECT-only database configuration before phase initialization, locking,
or redo-state publication.
It rechecks only a range inside the recorded verification extent: the range
end cannot exceed the current verify cursor. Each batch is additionally
constrained to finalized lineage. Blocks above the verify cursor are covered
by normal verification resume, never by redo.
Completion restores the pre-redo normal extent. A partial redo keeps the weaker of the retained full-extent level
and the level available from the current source roles, while a redo covering
the full retained extent can establish the current plan's level. An interrupted attempt keeps the normal resumable
redo marker and must be rerun with the same range.
Historical `live` redo is rejected because live follows only the current head.
Live does not advance the finite per-source ingest cursors. Interpret redo
checks each source only through its persisted finite target and separately
requires readable lineage at every height through the effective redo end. That
cursor coverage and lineage prove only the facts selected by the [watch
plan](glossary.md#watch-plan--watched-tuple) active when each block was loaded,
not facts required by a later watch plan.

The [manifest-authority marker](glossary.md#manifest-authority-marker) records
the active authority set's fingerprint.
The interpreter content hash and the manifest-authority fingerprint are independent deploy gates.
The interpreter hash covers inputs that can change
Interpret or Project output, including manifest `[[abi.events]]` declarations;
when it changes, complete the full-history Interpret redo and the stamped
Project redo before deploying the matching API. A new Project-owned table is
such a change: see [child registration events](#child-registration-events-in-name-history).
`read_features` can change the manifest-authority fingerprint while the interpreter content hash remains byte-identical.
On an initialized chain, that authority change still blocks
ordinary derived work until the exact token-attested full-range Interpret redo
and downstream stamped Project redo complete; if it widened the watch plan,
complete the stamped Ingest redo first.
When the active Ethereum Mainnet `basenames_execution` authority changes,
Ethereum Mainnet follows the rule above and, in addition, the Base Project phase
is invalidated on its own ([cross-chain
exception](manifests.md#discovery-admission)): complete an explicit full-range
Project redo on `base-mainnet` (the runner prints the required range); it needs
no stamp, no attestation token, and no Base Interpret redo, and it does not
appear in the pending-redo listing.
An unchanged interpreter hash therefore does not waive authority-transition re-derivation.

Manifest synchronization records a [manifest-authority
marker](glossary.md#manifest-authority-marker) when its authority changes. Every
Interpret redo that would discharge that marker uses this operator flow:

1. If the change widened the watch plan, complete the [mandatory historical
   fetch for the affected
   range](manifests.md#mandatory-historical-fetch-after-watch-plan-widening).
   Otherwise, confirm that the change widened nothing.
2. Copy the invalidation token printed by the fence error and re-run the redo
   with `--attest-watch-set-coverage <token>`. For a multi-chain redo, repeat
   `--attest-watch-set-coverage <chain>=<token>` for each affected chain.

Without the flag, the redo fails closed. With it, the runner logs an error-level
structured event from an immutable audit row containing the chain, phase, redo
range, authority fingerprint, invalidation token, runner instance ID, and
attestation time. The audit row is committed in the same transaction that
begins the marker-discharging redo and is unique for that chain, phase, and
invalidation generation. A restart re-emits it only after the locked begin
matches and commits the same active redo; rerunning the same token-valued
command is valid only for that exact active, audited redo. If a binary upgrade
changes the [interpreter content hash](glossary.md#interpreter-content-hash)
while that redo is interrupted, use the same token and exact audited range. The
locked begin keeps the audit association but discards progress written under
the prior hash, so Interpret restarts the range from its beginning under the
new hash. Later interruptions under the new hash resume normally. The locked
begin rejects a stale token, including one from an earlier transition to the
same authority. Manifest synchronization detects manifest-authored watch-plan
widening over retained Ingest coverage and stamps the exact required Ingest
range. Normal phase execution stops and prints the `ingest` redo chain, phase,
and range command prefix plus an instruction to append configured sources; it
never performs the potentially expensive historical fetch automatically.
Complete that command before the attested Interpret redo.
Until it completes, the runner also refuses the start of any explicit
Interpret, Project, or recompute-flags redo and repeats the required Ingest
command prefix and source instruction. Supplying
`--attest-watch-set-coverage` does not override this refusal: the attestation
describes retained-fact coverage, while the durable Ingest stamp records an
uncompleted historical-fetch obligation.
Narrowing, a same-set sync, and a newly admitted chain with no Ingest coverage
do not stamp Ingest. The attestation remains the operator's responsibility for
the whole authority transition. Do not edit cursors. An interpreter content hash
rotation with neither a current manifest-authority marker nor an active audited
redo remains flagless. When a full-history Interpret redo for an interpreter
content hash rotation starts at the finite ingest bounds after Live has
advanced, the runner extends Interpret
through its recorded head and stamps Project with the range its hash
adoption requires: from the first ingested block to the Ingest handoff or
Project's own recorded head, whichever is higher. That is the Interpret range
unless a crash between the two phases' live-cycle advances left Project one
block behind, in which case it ends at Project's head; when Project stood below
the handoff it still reaches the handoff. While upstream discovery repair has
installed required Ingest work, completed Ingest bounds are unavailable and the
stamp falls back to Project's head; the runner refuses Project until the repair
and the Interpret replay after it complete, and that replay widens the stamp.
Run or resume the stamped Project range exactly as recorded. Project hash adoption uses its
recorded head rather than narrowing the stamp to the older ingest handoff, and
an interrupted attempt keeps the live-extended range. When that interruption
belongs to an attested Interpret redo from the prior interpreter content hash,
restart the same audited range with its token; the range restarts from its
beginning rather than resuming the cursor written under the prior interpreter
content hash. A Project redo the prior binary started and left unfinished is
likewise invalid under the new hash and would otherwise block the new Interpret redo:
the Interpret redo that starts the new hash supersedes it in its own start
transaction, restoring the Project row as a finished redo would, provided no
runner holds the Project lock, and its completion stamps the Project redo
again. A Project redo whose row records the running binary's hash still
blocks a new Interpret redo.

The first manifest sync under the binary that adds `_bigname_compiled_watch`
rewrites every stored active payload. For every chain with existing derived
output, that rewrite mints a manifest-authority marker. Schedule the resulting
mandatory full attested Interpret redo across the already-derived fleet at the
planned walk-from-zero re-derivation boundary. A fresh or partially initialized
chain with no derived output receives no marker and derives normally; do not
supply an attestation for it. This rollout does not cause a spurious Ingest
refetch: when the prior payload lacks the compiled field, watch comparison
compiles that side from the same TOML under this binary. Once that snapshot
exists, a later binary-policy widening is detectable.

The binary that adds the manifest namespace to stored family-emitter entries
likewise enriches legacy `_bigname_compiled_watch` payloads from their enclosing
manifest. On chains with derived output, that payload rewrite mints a
manifest-authority marker and requires the same full attested Interpret redo
and downstream Project redo even when the TOML is unchanged. It stamps no
Ingest redo when namespace enrichment reveals no actual watch-plan widening.

`recompute-flags` recalculates label and name-surface normalization metadata
under the current normalizer through Interpret. It holds the Project lock while
finalizing metadata and recording any required replay, but does not run Project
or refresh primary names. Names that remain active or remain shadow complete
without replay. Names that cross between active and shadow are reported and
merged atomically into the ordinary Interpret and Project redo markers; only
that replay path may create or retract their bindings. Complete the stamped
redo before treating the changed visibility as published family state. On
completion the command writes one JSON object to standard output with the
same-class and transition counts plus every stamped phase range; this report
does not depend on `RUST_LOG`.

After a normalizer-version bump (a change to the `ENS_NORMALIZER_VERSION`
constant), run `recompute-flags` per chain over the chain's full retained range
(`--from-block`/`--to-block` are required and a bounded range skips labels whose
only selection arm is range-scoped), then a full-range Project redo per chain.
This is the same full-range redo the
[rainbow-table import](storage.md#rainbow-table-preimage-import) requires:
label verdicts gate what Project composes into served names, and a verdict flip
on a label with no name surface stamps no redo of its own. Only surface
visibility-class transitions stamp replay, so the full-range redo also carries
surface-less verdict changes into served names.

An interrupted recompute resumes its durable Interpret marker over the same
range. An unrelated ordinary Project redo remains pending and unchanged unless
a visibility transition expands its demanded range. There is no preliminary
Project refresh or separate handoff marker. Recompute and bounded Project
redo/rebuild require no hydration RPC; current values are refreshed by later
Project Follow work, whose RPC configuration is
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` (or
`--hydration-rpc CHAIN=HTTP_URL`). `phase-runner rewind` moves the
published latest marker to an exact stored readable ancestor and uses normal
head publication to orphan the suffix, clear affected divergence observations,
and stamp downstream redo. If the rewind makes the end of an uncompleted
required Ingest redo unreadable, the next supervised run first uses Live intake
to publish the winning suffix and then repeats the pending command prefix and
source instruction. When
finite Ingest was interrupted before it recorded a handoff, that recovery-only
Live pass anchors at the published readable ancestor.

`phase-runner inspect block-canonicality`, `stored-lineage`, and `raw-events`
provide the three read-only bounded schema-v2 operator windows. They do not
expose API routes. No drift, cache, execution-trace, or watch-plan inspection
surface is ported to the phase runner.

Before these schema-v2 operator commands are first used, run
`phase-runner init-schema` once. The phase runner owns the `bigname_phase`
namespace in that database. Head publication atomically marks phase lineage
orphaned, clears affected resolution-divergence observations, and stamps
downstream redo within that namespace.

## Verification mismatch repair

A [stored-history verification](glossary.md#stored-history-verification)
mismatch stops only the affected chain. Against an independent RPC reference
the runner fetches the same batch once more before stopping, so a single
`verification reference mismatch; fetching the same batch once more` warning
followed by normal progress needs no action; the chain stops only when the
second comparison also mismatches, and that stop is not retried. Against a
local Reth reference the first mismatch stops the chain.
`chain_phase_state.last_error` on the `verify` row records the block number,
field, stored value, and reference value. If verification was paired with live
follow, the `live` row records the same stop reason. The other configured chain
continues.

Treat the mismatch as a data-integrity incident. Preserve the recorded context
for diagnosis. Then wipe the affected chain's schema-v2 data, including its
`chain_phase_state` and `ingest_cursors` rows, ingest it again from the
configured sources, rebuild interpretation and projections, and rerun
verification from an empty verify cursor. Do not edit immutable raw rows in
place and do not mark the phase complete manually. A raw-data-only wipe is
unsafe: normal verification resumes at one block above its last successful
cursor and does not re-verify the re-ingested prefix below that cursor. If an
approved repair procedure intentionally preserves phase state, run verify redo
from the durable ingest start through the retained verified extent (the current
verify cursor). That range satisfies the full-extent condition and records the
current plan's level again. Normal verification resume then covers
the re-ingested blocks above the cursor. A mismatch in the first-ever verify
batch leaves no recorded verification extent, so no verify redo range is
expressible and a full phase-state reset is the only repair. Under the
state-preserving alternative, a failed verify redo retains its marker and is
resumed by rerunning the same redo command after repair. After a full
phase-state reset, rerun the normal pipeline instead.

## Surviving services

The API uses one `bigname_phase` request pool plus a reserved readiness
connection. `/v1/status`, snapshot selection,
[verified lookup](glossary.md#verified-lookup), and all projection reads use
phase relations. The `/v1/status` phase-runner heartbeat
threshold uses `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` (60 seconds by
default). `BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS` (one block by default,
negative values refused at startup) sets how many blocks the family publication
may trail the stored head and still be served, beside the status thresholds
`BIGNAME_API_STATUS_MAX_BLOCK_LAG` and `BIGNAME_API_STATUS_MAX_LAG_SECS`. It is
one count for every chain, so the same value covers six times as much time on a
12-second chain as on a 2-second one. Set above the status thresholds, it lets
`/v1/status` report `stale` while the API still serves. Same-chain verified
record and primary-name calls run at the publication's block, and Basenames
calls at the Ethereum position the projected name carries, so verified answers
keep serving through the same lag. Every API path is read-only, including verified records, automatic
live fallback, primary names with an omitted `source`, and diagnostics.
`BIGNAME_API_DATABASE_URL` may point at a primary or a physical streaming hot
standby with the reviewed schema installed. Logical replicas are not supported:
lookup authority checks compare PostgreSQL manifest row versions, which physical
replication preserves. This does not change the phase runner's separate
verification-database requirement: that URL must still point at the writer's
same database, as described above.

The API starts a fresh repeatable-read, read-only transaction after provider
calls and revalidates the captured family publication, canonical positions and
manifest authority without row or advisory locks. Changed state keeps the
existing stale rejection; a standby replay conflict also refuses the answer.
API requests do not create, refresh or clear the diagnostic
[resolution divergence ledger](glossary.md#resolution-divergence-ledger), even
when connected to a writable primary. Existing observations remain available to
operators and may still be retired by Project or reorg handling. No serving
path reads them.

The API role needs `USAGE` on `bigname_phase`, `SELECT` on the serving relations
below, and `EXECUTE` only on the fixed read-only snapshot guard below. This
fixed-`search_path`, security-definer function is owned by the schema owner; its
installer revokes default `PUBLIC` execution. Do not grant the API role
`CREATE` on `bigname_phase` or `public`, writes on any application relation, or
execution of the retained diagnostic ledger writer. The nine-argument boolean
core is private to the schema owner and must not be granted to the API role.

API startup tolerates a wholly absent phase schema so `/v1/status` can return
its empty, `degraded` response. Once the phase schema exists, startup checks
every phase-schema relation, function, and type its serving paths read:
relations by name and `SELECT` privilege, the snapshot guard by exact signature
and `EXECUTE` privilege, and the `canonicality_state` type. If an object or its
required privilege is unavailable, the API refuses to start and its diagnostic
names every unavailable identity.

After the phase schema exists, the schema owner provisions the dedicated login
with these privileges (substitute
database, role, and secret through the normal secret-management path):

```sql
CREATE ROLE bigname_api
    LOGIN PASSWORD '<secret>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE bigname TO bigname_api;
GRANT USAGE ON SCHEMA bigname_phase TO bigname_api;
GRANT SELECT ON TABLE
    bigname_phase.chain_heads,
    bigname_phase.chain_header_audit,
    bigname_phase.chain_lineage,
    bigname_phase.chain_phase_state,
    bigname_phase.project_family_marker,
    bigname_phase.service_heartbeats,
    bigname_phase.normalized_events,
    bigname_phase.migration_event_associations,
    bigname_phase.child_registration_events,
    bigname_phase.label_preimages,
    bigname_phase.discovery_edges,
    bigname_phase.migration_discovery_associations,
    bigname_phase.name_surfaces,
    bigname_phase.name_search_documents,
    bigname_phase.name_search_postings,
    bigname_phase.resources,
    bigname_phase.surface_bindings,
    bigname_phase.token_lineages,
    bigname_phase.manifest_versions,
    bigname_phase.manifest_contract_instances,
    bigname_phase.contract_instance_addresses,
    bigname_phase.project_family_undo,
    bigname_phase.project_repair_record,
    bigname_phase.project_name_state,
    bigname_phase.project_binding_candidate,
    bigname_phase.project_lifecycle_key_state,
    bigname_phase.project_lifecycle_triple_summary,
    bigname_phase.project_lifecycle_association,
    bigname_phase.project_lifecycle_event,
    bigname_phase.project_child_registration_state,
    bigname_phase.project_wrapper_state,
    bigname_phase.project_registry_node_state,
    bigname_phase.project_registry_owner_event,
    bigname_phase.project_registry_binding_observation,
    bigname_phase.project_resolver_classification,
    bigname_phase.project_registry_pointer,
    bigname_phase.project_resource_pointer,
    bigname_phase.project_named_resource_pointer,
    bigname_phase.project_universal_resolver_proxy,
    bigname_phase.project_node_record_partition,
    bigname_phase.project_node_record_value,
    bigname_phase.project_record_id_value,
    bigname_phase.project_resolver_link,
    bigname_phase.project_grant,
    bigname_phase.project_resource_admin_aggregate,
    bigname_phase.project_account_approval,
    bigname_phase.project_ens_v2_entry_owner,
    bigname_phase.project_ens_v2_registry_parent,
    bigname_phase.project_child_edge_candidate,
    bigname_phase.project_parent_subregistry,
    bigname_phase.project_reverse_tuple,
    bigname_phase.project_reverse_node_claim,
    bigname_phase.project_claim_normalization,
    bigname_phase.project_address_name_fold,
    bigname_phase.project_address_controller_candidate,
    bigname_phase.project_address_name_index,
    bigname_phase.project_address_history_anchor,
    bigname_phase.project_history_source,
    bigname_phase.project_history_source_edge,
    bigname_phase.project_history_catalogue_marker,
    bigname_phase.project_address_record_node_index,
    bigname_phase.project_address_record_id_index,
    bigname_phase.project_name_history,
    bigname_phase.project_name_summary,
    bigname_phase.project_lookup_name,
    bigname_phase.project_lookup_relation,
    bigname_phase.project_lookup_inventory,
    bigname_phase.project_lookup_record
TO bigname_api;
GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state_read_only(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) TO bigname_api;
```

<a id="address-history-catalogue-role-upgrade"></a>
For an existing API role, after applying
`20261005170000_project_address_history_catalogue.sql`, the schema owner must apply these
additional SELECT grants before serving the new API binary. On a serving standby, wait for
the schema-migration and grants applied on the primary to replay there. The API startup check
refuses to start if any of these relations is absent or unreadable.

```sql
GRANT SELECT ON TABLE
    bigname_phase.project_address_history_anchor,
    bigname_phase.project_history_source,
    bigname_phase.project_history_source_edge,
    bigname_phase.project_history_catalogue_marker
TO bigname_api;
```

For an existing deployment, apply
`20260930100000_read_only_lookup_guard.sql` on the primary, wait for it to replay
on any serving standby, and grant the fixed read-only function above before
restarting the API. Existing eight-argument and writer-function grants remain
on upgrade for non-API callers; remove them specifically from the API role once
all API instances use the read-only build:

```sql
REVOKE EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) FROM bigname_api;
REVOKE EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text, boolean
) FROM bigname_api;
REVOKE EXECUTE ON FUNCTION bigname_phase.write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) FROM bigname_api;
```

The existing non-API writer remains compatible. An older API build needs those
old grants restored and a writable primary if rolled back; it cannot serve
verified lookups on a standby. This schema-migration and API change do not alter
the interpreter content hash or require a projection rebuild.

This role cannot read raw facts, the divergence table, or unrelated operational
tables directly. Its discovery-state reads are
`contract_instance_addresses`, `discovery_edges` and
`migration_discovery_associations`: the registry overview and labels routes use
declared address intervals to recognize registry contracts at the selected
block, and the family child reader (`crates/storage/src/families/topology/children.rs`)
checks a migrated parent's migration registry against its readable
`registry_announcement` edge and `migration_registry_creation` association. The
same reader takes child labels from `label_preimages`. The grant is SELECT-only
and does not admit discovery writes.
Reapply these explicit relation and function grants after a reviewed
phase-schema replacement; do not use ownership
or schema-wide write grants as a shortcut.

The `project_*` tables are the [owned key families](glossary.md#owned-key-family)
the API composes names, records, permissions, resolver collections and primary
claims from, together with their [family marker](glossary.md#family-marker),
undo journal and repair record. Snapshot selection, the verified lookup and its
guard, and `/v1/status` read the marker. `discovery_edges`, `label_preimages`
and `migration_discovery_associations` are on the list because the family
children reader joins them. Startup (`crates/storage/src/api_preflight.rs`)
refuses a role that cannot read any of them.

`migration_event_associations` is on the list because
`GET /v1/diagnostics/events` selects the ENSv1→ENSv2 migration correlation rows
for its candidate payload; the public `GET /v1/events` path shares the same
loader but does not select from that table. The row set is Interpret
coordination state rather than a projection, so the grant is deliberately
read-only and does not widen the API's write boundary. A database provisioned
without it serves every other route and fails only that one, with a permission
error rather than an empty payload.

### Replacing an initialized phase schema

The current installer cannot upgrade a nonempty `bigname_phase` schema. When a
reviewed versioned schema-migration cannot preserve an existing initialized
database, the cutover requires an offline replacement and full pipeline walk:
This procedure is not used for
`20260814130000_surface_binding_authority_arm.sql`; that shared boundary must
preserve sequence-assigned manifest IDs and instead uses the targeted binding
reset in the production runbook.

1. Build `phase-runner` and `bigname-api` from the same commit. Stop the
   phase runner and every API process that can open the phase schema, and retain
   a database backup.
2. As the phase-schema owner, move the old namespace aside and create the empty
   target expected by the installer:

   ```sql
   BEGIN;
   ALTER SCHEMA bigname_phase RENAME TO bigname_phase_pre_c2;
   CREATE SCHEMA bigname_phase AUTHORIZATION <phase_owner>;
   COMMIT;
   ```

3. Run `sqlx migrate run --source migrations --database-url
   "$BIGNAME_DATABASE_URL"` from the deployed commit while the replacement
   namespace is empty, then run the new binary's `phase-runner init-schema`
   with the same database URL. This order lets an append-numbered schema-migration
   record its version when its phase table is absent before the fresh baseline
   creates the current table shape. Reapply the verification-role `USAGE`/`SELECT`
   grants and the exact API-role relation/function grant block above; schema
   rename and replacement do not carry those grants to the new namespace.
4. Run the configured `phase-runner run` from each admitted source's historical
   start through the current head. Wait for ingest, interpretation, projection,
   and stored-history verification to complete and for live follow to catch up.
   Do not copy phase tables from `bigname_phase_pre_c2` into the new schema.
5. Validate the rebuilt projections and grants, deploy the same-commit API, and
   only then retire the archived schema under the normal backup-retention
   policy.

The expected cost is one complete historical ingest-through-verification walk,
the associated provider traffic and projection work, and temporary storage for
both schemas. The v2 lookup writer is not admitted before this cutover, so the
old [resolution divergence ledger](glossary.md#resolution-divergence-ledger) is
expected to contain no rows and nothing from it is copied. After cutover,
ledger rows are not reconstructable from raw facts: once any row exists, a
future schema upgrade must use a separately reviewed schema-migration or lossless
export/import mechanism rather than this replacement procedure.
Schema-migration `20260831120000_retire_direct_divergences_for_null_resolver.sql` is
such an additive upgrade: it preserves the populated ledger and marks
already-active observations stale where the current ENS Mainnet exact resolver
is null. The trigger it installed went with the old exact-name table; Project now
retires those observations in the family publication transaction
([storage](storage.md#verified-lookup-storage)).

The project-at-head guard also binds the API's compiled interpreter content
hash. `bigname-api` and `phase-runner` must therefore come from the same commit.
After any interpreter content hash rotation, deploy the new phase runner and
finish its required re-walk before deploying the matching API; deploying the
API first makes all v2 snapshot-selected reads return `409 stale` until the new
project generation is published. This includes indexed reads because snapshot
selection itself requires the matching project publication before any
projection row is admitted.

The [complete-group](glossary.md#complete-group) ENSv1→ENSv2 activation is such a walk gate. Its manifest
profiles and generated watch plans do not change, so no historical fetch or
manifest-authority attestation is introduced. Deploy the new phase runner,
complete the retained-range Interpret redo under the new interpreter content
hash, and run the stamped Project range. Only after that Project range publishes
may the matching API be deployed. A Sepolia name with facts on both ENSv1 and ENSv2
and no proof follows the chain per name
([ADR 0007](adrs/0007-follow-the-chain-ens-authority.md)) and never blocks
publication. The connected
wrapped and locked publication prerequisite is recorded in
[PR #852](https://github.com/ensdomains/bigname/pull/852).
An interrupted walk resumes only from its existing exact phase
[redo-marker scope](glossary.md#redo-marker-scope). Interpret separately
validates the normalized arm-wide replay preimage, keeps its named replacement
binding closed, and reopens only the other matching bindings in that authority
arm. An activated boundary reopens only the ENSv1 bindings it closed at its
recorded predecessor position.
Activation does not create, infer, widen, or relax the phase marker or that
replay evidence.

Configure
`BIGNAME_API_CHAIN_RPC_URLS` for status and verified lookup as described in the
API docs. Verified ENS reads (`source=verified` and `source=auto` on
`/v1/names/{name}/records`, `/v1/lookup`, and ENS/60 verification on
`/v1/addresses/{address}/primary-name`) execute against the Ethereum L1 of the
deployment profile the API serves, so an API in front of a `manifests/sepolia`
projection needs an `ethereum-sepolia=<https url>` entry (an API in front of
`manifests/mainnet` needs `ethereum-mainnet=`). Without that entry the verified
routes fail closed with `409 stale` and `GET /v1/namespaces/ens` reports
`verified_records` and `verified_primary_name` as `unsupported` with
`unsupported_reason=execution_provider_not_configured` for chain `11155111`;
with it, both report `full`. The Sepolia entrypoint is the checked-in active
`manifests/sepolia/ethereum/ens/ens_execution/v2.toml`, which the normal
manifest sync installs. The request pool uses `BIGNAME_DATABASE_MAX_CONNECTIONS`; together
with the reserved readiness connection, one API process can open at most
`BIGNAME_DATABASE_MAX_CONNECTIONS + 1` PostgreSQL connections. A current-state
collection request holds one request-pool connection while its read snapshot is
open.

### Database connection budget

`BIGNAME_DATABASE_MAX_CONNECTIONS` is an API-only setting. The phase runner does
not read it; it derives its own pools from the number of configured chains, so
the two services must be budgeted separately.

For `C` configured chains, one phase-runner process opens at most:

| Pool | Size | Where |
| --- | --- | --- |
| Phase pool | `max(2C, 4)` | `apps/phase-runner/src/main.rs`, `RunnerDatabase::connect` in the `Run` arm |
| Verification pool | `max(C, 1)` | `apps/phase-runner/src/main.rs`, `VerificationDatabase::connect` in the `Run` arm |
| Advisory phase locks, peak | `3C` | see below |

Each lock is a dedicated connection outside both pools, because it holds a
session-scoped `pg_try_advisory_lock` (`PhaseLock::acquire`, `apps/phase-runner/src/phase_lock.rs`).
How many are held at once depends on where the chain is in its cycle, and the
budget has to cover the peak, not the common case:

| Situation | Locks per chain | Where |
| --- | --- | --- |
| Serial path: Verify runs before Live (`verify_before_live`) | `1` | `PhaseRunner::run_chain`, `apps/phase-runner/src/runner_chain.rs` |
| Combined path: Verify and Live polled concurrently, each holding its own lock | `2` | `apps/phase-runner/src/runner_live_follow.rs` |
| Post-Live discovery repair: a Verify fence, then an Ingest fence inside it, then one phase lock inside that | `3` | `runner_live_follow.rs:70`, `:112`, `:143` |
| `rewind` (separate operator process): the four writer-phase locks, no Verify lock | `4`, plus its own pool | `rewind::acquire_writer_locks`, `apps/phase-runner/src/rewind.rs`; `RunnerDatabase::connect` in the `Rewind` arm |

A fence is an ordinary phase lock on that phase's name, so it excludes the
phase itself rather than adding to it — the post-Live Verify fence waits for the
paired Verify to release before it is granted. Catch-up and the spine phases run
one after another and never hold two of their own locks at once.

So budget `max(2C, 4) + max(C, 1) + 3C` for the running service: a one-chain
deployment peaks at `4 + 1 + 3 = 8` connections and settles at `6` or `7`
depending on the path; three chains peak at `6 + 3 + 9 = 18`. A start that
finds phases recorded against chains no longer configured takes one lock at a
time to close them out (`settle_unconfigured_phases`, `apps/phase-runner/src/runner_chain.rs`) and does
not raise the peak.

`phase-runner rewind` is not part of that figure: it is a separate process
with its own pool of up to `2` connections (`RunnerDatabase::connect` in the `Rewind` arm)
that takes the Ingest, Interpret, Project, and Live locks for one chain and
never the Verify lock (`rewind::acquire_writer_locks`, `apps/phase-runner/src/rewind.rs`). It therefore
succeeds while the supervised runner is alive whenever that chain is not in a
writer phase — during its serial Verify phase, for instance — so the two
processes can hold connections at the same time. Either stop the supervised
runner before a rewind, or budget `6` more connections for the duration:
`14` for one chain, `24` for three. A rewind against a chain whose writer
phase is running fails on the held lock rather than waiting.

`phase-runner redo` is likewise a separate, and potentially long-running,
process: a writer pool of up to `4` (`RunnerDatabase::connect` in the `Redo` arm), a
verifier pool of `1` opened at start whenever the requested redo includes
Verify (`VerificationDatabase::connect` under `phase.requires_verify()`), and up to two locks at once — the Project
lock is held while the Interpret phase runs beneath it
(`run_recompute_interpret_with_project_lock`, `apps/phase-runner/src/runner_operator_redo.rs`), and every phase run
takes its own lock (`PhaseRunner::run_phase`, `apps/phase-runner/src/runner.rs`). The advisory locks
let it run beside a supervised runner that holds a non-conflicting phase such as
Live. Either stop the supervised runner before an explicit redo, or budget `7`
more for its duration.

The advisory locks do **not** serialize explicit processes against each other:
they only prevent the same phase from running twice on the same chain. A
Verify-only redo holds the Verify lock alone
(`redo_phase_only`, `apps/phase-runner/src/runner_operator_redo.rs`), rewind never takes
Verify, and lock keys are per chain, so a redo and a rewind — or two redos on
different chains — can run at the same time and each brings its own pools and
locks: up to `7` for a redo, `6` for a rewind. Run one explicit process at a
time, which is what the ceiling below assumes, or add each additional
overlapping process to the budget in full.

**The superuser reservation.** The writer login created from `POSTGRES_USER` is
a superuser; `bigname_api` and `bigname_verify` are created `NOSUPERUSER`
(the grant blocks above). PostgreSQL 16 keeps
`superuser_reserved_connections` (default `3`, set explicitly in the compose
files as `POSTGRES_SUPERUSER_RESERVED_CONNECTIONS`) usable by superusers only,
so a non-superuser connection is refused once `max_connections` minus that
reservation is in use — even though the superuser writer pool, its advisory
locks, and a redo or rewind can still connect. A ceiling set exactly to the
service sum therefore starves the API and the verifier first. Count the
reservation in the ceiling rather than relying on it as headroom.

Set the server's own ceiling explicitly with `POSTGRES_MAX_CONNECTIONS` rather
than inheriting the PostgreSQL default, and size it as the sum of:

| Term | Value |
| --- | --- |
| Supervised runner peak | `max(2C, 4) + max(C, 1) + 3C` |
| Explicit maintenance, one process at a time | `7` (a redo; a rewind needs `6`) — add `7` per additional process you intend to overlap |
| Superuser reservation | `superuser_reserved_connections`, `3` by default |
| Administrative headroom | `2` for `psql` and `sqlx migrate` |
| Each API process | `BIGNAME_DATABASE_MAX_CONNECTIONS + 1` |

One chain with one API process at the default pool of `10` and one explicit
process at a time: `8 + 7 + 3 + 2 + 11 = 31`. Three chains: `18 + 7 + 3 + 2 +
11 = 41`. A redo (`7`) and a rewind (`6`) overlapping would need `37` rather than
`31`. The shipped default of `100` clears all of these; the arithmetic
matters when the ceiling is lowered to fit `work_mem`. Then budget it against `work_mem`: a single
backend can hold several `work_mem` allocations at once, so the worst case a
server commits to is roughly `max_connections x work_mem x concurrent sort or
hash nodes`, on top of `shared_buffers`.

## PostgreSQL JIT

Both compose files start PostgreSQL with `jit=off` (`POSTGRES_JIT`, default
`off`). PostgreSQL's just-in-time compiler turns a statement's expressions into
native code before running it when the planner's cost estimate crosses
`jit_above_cost`. That pays off for one long statement over millions of rows.
Bigname's statements are the opposite shape: many short statements, prepared
and re-planned per batch, whose costs the planner overestimates because JSONB
filters, partial expression indexes and temporary tables carry poor statistics.
The estimate crosses the threshold, the statement compiles for tens of
milliseconds to seconds, then touches a few dozen rows. Measured on Sepolia:
one resolver statement took 11.4 s with JIT (about 2,000 compiled functions)
and 19 ms without; the Project test suites went from more than 30 minutes to
their normal length when the test databases turned JIT off (#922).

The setting is server-wide and applies to every chain and every role. It is a
Compose command argument, so changing it means recreating the `postgres`
container with the server Compose definition and environment
(`docker compose --env-file .env.server -f docker-compose.server.yml up -d postgres`),
which restarts every session; stop the phase runner and the API first, as for
any PostgreSQL restart. To
use JIT for one deliberately heavy statement, run `SET LOCAL jit = on` inside
that transaction rather than turning it on globally.

CI keeps JIT on for the API test job on purpose: the API plan tests assert
that a page or count plan stays below the JIT threshold, which is a bound on
the plan's cost, and they can only observe it with JIT enabled. That guard is
independent of the production setting.

## Owner-ratified Sepolia source-role rollout

Do not begin this destructive rollout until the Issue #411 part-2 release
artifact, two distinct endpoint secrets, and an owner-approved rollback and
restoration procedure are available; a binary-only rollback is insufficient.
No narrower per-chain reset procedure is checked in. The only checked-in reset
broad enough to remove complete intake and source-identity state,
[Replacing an initialized phase schema](#replacing-an-initialized-phase-schema),
replaces the entire `bigname_phase` namespace and rebuilds every configured
chain, not Sepolia alone. Its authorization is limited to a reviewed
schema-migration that cannot preserve an initialized namespace; a source-role
transition does not meet that condition. It therefore does not authorize this
rollout, and using it would incorrectly give a nominally single-chain Sepolia
transition whole-schema downtime, all-chain rebuild scope, and public-identity
and audit-preservation obligations. The rollout must stop until part 3 supplies
a reviewed per-chain reset and lossless preservation procedure. Never improvise
a reset, data transfer, or rollback. Once that procedure exists, the
owner-ratified from-zero Sepolia source-role rollout is: stop old runners and
redo processes; deploy the part-2 binary and distinct secrets; configure and
validate `sepolia-intake` as intake and `sepolia-verify` as verification-only;
perform the reviewed per-chain reset; run Ingest through Verify before Live.
Confirm only the intake cursor exists, Verify reaches its frozen target with
`cross_checked`, match logs name `sepolia-verify`, and provider/operator request
accounting shows zero Ingest/Live requests for that key. Do not substitute an
ordinary redo. The required per-chain reset and preservation procedure is
part-3 work.

## Removed operational surfaces

This source tree has no command for the deleted indexer or worker planes,
including:

- old-indexer startup, live polling, or head-following
- persisted `backfill_*` job creation, leasing, advancement, or repair
- normalized-event catch-up, adapter startup synchronization, supersession, or
  coverage recovery
- the Base drop-and-rederive correction
- resolver-profile reconciliation or authority-journal draining
- old raw-code and name-normalization indexer repair commands
- legacy projection replay, hydration, migration, or inspection commands
- persisted legacy execution-cache or trace inspection

The corresponding SQL migrations remain immutable history, followed by the
append-only migration that drops their `public`-schema tables. Existing rows
are not current readiness or replay authority during the planned transition.

## API shutdown

Unix API processes accept Ctrl-C and SIGTERM through the existing Axum graceful
shutdown path. Non-Unix builds retain Ctrl-C only. Accepted signals stop fresh
connections while accepted requests finish within their configured deadlines
(defaults: 30-second request and 25-second SQL timeouts). Signal listener failures are errors, not
accepted shutdown signals. The metrics listener has no new drain guarantee.

Compose uses `BIGNAME_API_STOP_GRACE_MS` for both its stop grace and API startup
validation, defaulting to 45000 (45 seconds). Startup rejects a request timeout
that leaves less than 5000 ms of grace, before database connections or listeners.
For a 60000 ms request timeout, use at least 65000 ms of grace. Direct launches
without this optional budget retain their existing request-timeout behavior.
External stop-timeout overrides must honor the same budget. The API slice is
Part of #641. The phase runner handles SIGTERM as well as SIGINT
(`apps/phase-runner/src/shutdown.rs`), for the supervised run and explicit redo
only, observing the request at the next batch boundary so the batch in flight
commits before the loop exits; Compose sets an explicit `stop_grace_period` on
`phase-runner` (default 120s, `BIGNAME_PHASE_RUNNER_STOP_GRACE_PERIOD`). See the
runbook's [Pause and resume indexing](runbooks/production-docker.md#pause-and-resume-indexing)
for what an expired grace leaves behind. Runner heartbeat, restart, and redo
behaviour under SIGTERM are not separately verified by the container shutdown
job, which drains the API only. Neither slice authorizes production rollout or
completes the issue.

### Switching Sepolia from local RPC to direct Reth reads

This procedure requires a custom image with `phase-runner/reth-db` enabled.
The standard image currently omits the direct reader and its smoke executable.

The direct reader is compiled against Reth v2.5.0 (`crates/ingest/Cargo.toml`) and selects Reth's built-in Sepolia chain specification from the chain id; there is no chain specification to supply and no other Reth version to build against. It refuses to open a datadir whose stored genesis block hash is not Sepolia's, naming both hashes. Run it only against a Reth v2.5.0 node, and test a [bounded read-only sample](reth-db-reader.md#bounded-sample) before pausing ingestion. Supply the [direct-reader mounts](reth-db-reader.md#mount-contract) and one `reth_db` intake descriptor; keep historical state RPC separate. Pause any host automation that would restart or recreate the runner during the change (for example an image auto-update job), gracefully stop the runner, retain the cursor and phase-state evidence, and run the [same-node transport command](chain-intake.md#same-node-sepolia-transport-change), which performs the [source transport](glossary.md#source-transport) change. The command refuses, and changes nothing, when the node has pruned history that Ingest plans to read when it resumes: a direct reader whose retention floor is above block zero is refused while Ingest has catch-up work or an unstarted redo below that floor, because resumed Ingest applies the same [source-floor admission](chain-intake.md#download-range-planning). Once Ingest has handed off to live follow (including a completed extent that is awaiting completed-phase revalidation), the floor is judged where live follow resumes, the block after the highest published block the node still holds, so a node that pruned history below the handoff but retains what live follow needs is admitted. Save its receipt before resuming the existing replay range with the new intake descriptor. Do not reset the database or restart from block zero. The command may be reversed against the same node for rollback; keep the matching runtime/configuration until progress is verified.

The direct-reader container, including one-off smoke and source-transport
commands, must share the Reth node's PID namespace. The reth overlay sets the
phase runner's `pid:` from the required `RETH_NODE_PID_NAMESPACE`; for the
Sepolia deployment that is `container:bigname-sepolia-reth` (the corresponding
Docker option is `--pid=container:bigname-sepolia-reth`), and `host` for a node
that runs directly on the host. MDBX uses
`getpid()` and takes a byte-range lock at that numeric PID in the shared data
file; a writer's exclusive byte lock conflicts with a reader using the same
number. Separate container PID namespaces can therefore make unrelated
processes collide and fail opening with `Resource temporarily unavailable (11)`
(upstream: .refs/reth/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c:L1505 @ reth@189c0df3)
(upstream: .refs/reth/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c:L24855 @ reth@189c0df3)
(upstream: .refs/reth/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c:L24872 @ reth@189c0df3).
Validate the final image entrypoint and PID configuration: overriding the
entrypoint for a successful smoke test can change the process's PID and hide
this collision. Keep the existing read-only data mounts and writable MDBX lock
file; do not disable MDBX locking to work around it.

This build rotates the [interpreter content hash](glossary.md#interpreter-content-hash), for every chain and whether or not direct reads are used: the Reth v2.5.0 update moves the seven Alloy crates the hash fingerprints from 1.5.7 to 1.7.3 in `Cargo.lock` (`crates/content-hash/src/lockfile.rs`), and the Rust 1.98 update edits `crates/interpret/src/recompute.rs`, a hashed source file (`crates/content-hash/src/compute.rs`). An existing deployment must therefore finish the full-history Interpret redo and the Project redo it installs, as [interpretation replay](storage.md#interpretation-replay) requires for any rotation, before the matching API serves; follow the runbook's [planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary). A full Interpret replay that is already required for another reason discharges this obligation when it runs under the new binary; it must not be bypassed. The source transport change itself neither requires nor performs that redo.

### Child registration events in name history

The build that adds name history's
[`include=child_registrations`](api-v1-routes.md#direct-child-registrations-includechild_registrations)
adds the Project-owned table
[`child_registration_events`](projections.md#child-registration-events) and
changes `crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. Schema-migration `20260923150000_child_registration_events.sql` creates
the empty table on an existing phase schema, and `init-schema` installs it on a
fresh one. The table fills only when Project rebuilds, so an existing
deployment applies the schema-migration, reapplies the API role's SELECT grant
above, and finishes the full-history Interpret redo and the Project redo it
installs before the matching API serves, as for any rotation. Until then the
API refuses to serve the new build's snapshots, as described above, so no
request sees an empty table as a complete answer.

### Child registration history ordered by transaction index

The build that orders history rows within a block by transaction index changes
the ordering key of
[`child_registration_events`](projections.md#child-registration-events) from
the event's transaction hash to its transaction index, or -1 when the event has
none. It changes `crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. Schema-migration
`20260926120000_child_registration_events_transaction_index_key.sql` converts
an existing table: it drops `child_registration_events_parent_history_idx`,
rewrites every row's `transaction_order_key` from its event in
`normalized_events` by `event_identity` (a row whose event is gone gets -1 and
is never served), changes the column to `bigint` with a `>= -1` check, and
rebuilds the index on the same columns. The column change rewrites the whole
table and, with the index rebuild, holds an exclusive lock on it until the
migration commits, so name history with `include=child_registrations` waits
for it. Apply it in the deployment's planned migration window; its running
time on the Sepolia table has not been measured yet. A fresh schema gets the
new column from `init-schema`, and a rerun on a table that already has the
`bigint` key only resets the column and index comments. The full Project
rebuild the rotation requires rewrites every row again, so the backfill only
keeps the table consistent with the new readers until that rebuild finishes.

### ENSv1 mirror ancestor gate

The build that stops deriving an
[ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
name through an ancestor's resolver, and reads ENSv1 registry pointers by the
node each event addresses, changes `crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no manifest change, no watch-plan widening and no historical
ingest fetch. An existing deployment prebuilds
`normalized_events_project_v1_pointer_addressed_node_idx` with
[`ops/mirror-pointer-index/install.sql`](../ops/mirror-pointer-index/README.md)
while the old runner is still processing, applies
`20260924120000_normalized_events_project_v1_pointer_addressed_node_idx.sql`,
and finishes the full-history Interpret redo and the Project redo it installs
before the matching API serves, as for any rotation. Names bound to the mirror
whose nearest ENSv1 resolver is an ancestor become unsupported with
`mirrored_resolver_not_projected` when that Project redo publishes; before the
release is recorded, recount the mirror rows by support status and
`provenance.mirror.mirrored_unsupported_reason` at the published Project target,
separating inventory resources from the resources names currently serve, and
check the address-record reads for the withdrawn rows.

### ENSv2 support without a registrar event

The build that serves a selected ENSv2 registration without a registrar event
([architecture](architecture.md#ensv1ensv2-current-authority)) edits
the Project name-authority SQL, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration, no watch-plan widening and no historical
ingest fetch. It also rewords the notes of the `exact_name_profile` flag in the
Sepolia `ens_v2_registrar_l1` manifest without changing its status. That changes
the Sepolia manifest payload, so manifest synchronization records a
[manifest-authority marker](glossary.md#manifest-authority-marker) for Sepolia and its full-history Interpret redo runs with
`--attest-watch-set-coverage`, attesting that no watch-plan range widened. An
existing deployment finishes that Interpret redo and the Project redo it
installs before the matching API serves, as for any rotation. When the Project
redo publishes on Sepolia, `eth` and `reverse`, the only rows that carried
`ensv2_exact_name_profile_shadow` on 2026-09-24, become supported with no
unsupported reason; their selected authority and projected values do not
change. Before the release is recorded, confirm that no published name still
carries that reason at the published Project target.

### Name-list contains match, created_at sort and authority sets

The build that adds `match=contains` to the `q` of
[`GET /v1/addresses/{address}/names`](api-v1-routes.md#get-v1addressesaddressnames)
and [`GET /v1/names/{name}/subnames`](api-v1-routes.md#get-v1namesnamesubnames),
`sort=created_at` and `authority` sets to address names, changes only API read
paths, but some of those reads live in hashed storage sources:
`crates/storage/src/address_names/query.rs` and the family readers under
`crates/storage/src/families` (`crates/content-hash/src/compute.rs`). It
therefore rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration, no manifest change and no historical
ingest fetch. An existing deployment runs the new binary for both the phase
runner and the API, and finishes the full-history Interpret redo and the
Project redo it installs before the matching API serves, as for any rotation;
an API upgraded alone refuses the old build's family publication with
`409 stale`. It ships inside the TYR-61 batch, whose single Interpret redo and
Project rebuild discharge this obligation. Once the matching publication is
readable, [current-state collection cursors](api-v1.md#current-state-list-cursors)
continue after their saved positions across the rebuild. While the publication
is unavailable, clients retry with the same cursor. Known legacy cursor layouts
follow the compatibility rules in that contract; the generation change itself
requires no pagination restart.

### Registry label owner filters

The build that adds `owner` and `exclude_owner` to
[`GET /v1/registries/{chain_id}/{address}/labels`](api-v1-routes.md#get-v1registrieschain_idaddresslabels)
stores the owner each name serves in the [name summary](glossary.md#name-summary),
so it needs `20260929170000_project_name_summary_owner.sql`, which adds
`project_name_summary.owner`. On a database without the column it also resets
every owned key family, including `child_registration_events`, with the
[family marker](glossary.md#family-marker), undo journal and repair records, as
the name-summary schema-migration above does, so the next family run rebuilds
them and writes every name's owner; fenced routes answer `409 stale` until that
rebuild finishes. It takes the marker table in `EXCLUSIVE` mode first and holds
it to commit, so a family run cannot create or lock a marker, for a chain with
or without one, between the reset and the new column. A run that starts
meanwhile waits; afterwards it either rebuilds with the column or, if it had
planned against a marker the reset removed, fails its generation check once and
the next run rebuilds. A run already holding its marker delays the
schema-migration until that run's transaction ends. While it runs, API requests
that read the name summary (the subname and label lists) or lock the marker
(verified lookups) wait for it rather than answer `409 stale`, up to the API's
statement and request timeouts, so apply it at a quiet moment. Apply it before starting the release: the family writer
inserts summary rows by column name, so without the column that release's
summaries silently lose their owner and the filtered label reads fail. The
composition that fills it lives in hashed storage sources
(`crates/storage/src/families`), so the build rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) and its first
family run rebuilds the families anyway; the reset adds no second rebuild when
the schema-migration is applied first. It ships inside the TYR-61 batch, whose single Interpret
redo and Project rebuild discharge this; collection cursors continue as
described above.

### Resolver set while registering a wrapped name

The build that keeps a registry resolver write that follows `NameWrapped` in a
wrapped registration transaction on the wrapper resource, instead of moving it
to the registrar resource the NameWrapper holds
([projections](projections.md#records-shared-through-resolver-links)), changes
`crates/adapters/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration, no manifest change and no historical
ingest fetch. An existing deployment finishes the full-history Interpret redo
and the Project redo it installs before the matching API serves, as for any
rotation. Only that redo corrects names already registered this way: when the
Project redo publishes, a wrapped name whose registration transaction set a
resolver serves its latest resolver state rather than none, including any later
change or clear. Before the release is recorded, confirm that both redos
adopted the new hash, then compare Sepolia `taytems.eth` (registered at block
4052977) with `source=verified` at the same published block: the resolver and
the record values must agree.

### Retired resolver alias path

The build that stops interpreting the 2026-06-29 ENSv2 resolver `AliasChanged`
event ([upstream](upstream.md)) removes it from the ENSv2 resolver adapter and
the Project topology family, so it changes `crates/adapters/src` and hashed
Project and storage sources and rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs `20261001100000_retire_resolver_alias_families.sql`, which drops
`project_name_alias` and `project_resolver_alias`. No admitted manifest declares
`AliasChanged`, so both are expected to be empty, but a database whose families
were built from retained June-generation events can hold rows; the
schema-migration refuses to drop a table that still has rows, so census any
such rows and decide whether to retire them before retrying. It needs no
manifest change and no historical ingest fetch.

This is a coordinated release with the old API and phase-runner supervisor
stopped, not a rolling migration: the old Project and API still read and write
both tables, and the old API preflight requires them. Follow the
[planned migration procedure](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary),
steps 4 to 8: apply the schema-migration, then run the new build's
full-history Interpret redo and the matching full-history Project redo on every
configured chain whose hash rotates, then check that each chain's family
publication carries the new hash and that `/v1/status` reports ready before
serving. When it ships with the
[resolver set while registering a wrapped name](#resolver-set-while-registering-a-wrapped-name)
release, that release's single Interpret redo and Project redo discharge this
rotation too. An API upgraded alone refuses the old build's family publication
with `409 stale`. After the drop, rolling back only the binary fails, because
the old API preflight requires both tables: roll the schema back together with
it, or run forward.

No served value changes except the removed fields: the resolver overview no
longer reports an `aliases` section, `/aliases` answers like any unknown route,
lookup topology has no `alias` field, and the `set_alias` and
`admin_set_alias` powers are no longer reported.

### ENSv1 lease date on name rows

The build that serves the `ens_v1` object on name-shaped rows
([naming dictionary](api-v1.md#naming-dictionary)) keeps the BaseRegistrar
lease expiry as `registration.ens_v1_expiry` in the composed name row. Nothing
stores that value, and the build adds no schema-migration, manifest change or
historical ingest fetch, but the code that composes it lives in hashed storage
sources (`crates/storage/src/families/control/lifecycle`), so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. An existing deployment finishes the full-history Interpret redo and the
Project redo it installs before the matching API serves, as for any rotation;
until then the fenced name routes answer `409 stale`. It ships batched with the
TYR-116 release (the wrapped-registration resolver fix), whose single Interpret
redo and Project rebuild discharge both.

The build also moves `wrapper_state` and `wrapper_fuses` off the top level of
name-shaped rows into `ens_v1`, a breaking response change that the app
integration takes in the same release. Once the redo publishes, check on Sepolia
that `GET /v1/names/nick.eth` serves `ens_v1.expires_at` as the BaseRegistrar
lease date (`"1798608633"` at the time of writing) beside the top-level ENSv2
`expires_at` (`"1803965433"`), with `ens_v1.wrapper_state` `"emancipated"`.

### Registration time kept through the ENSv1→ENSv2 migration

The build that keeps a migrated name's `registered_at` at its ENSv1 lease's
registration time instead of the migration's block time
([naming dictionary](api-v1.md#naming-dictionary)) reads the stored migration
position the name state already holds. It adds no schema-migration, manifest
change or historical ingest fetch, but the code lives in hashed storage sources
(`crates/storage/src/families/control/lifecycle`), so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. An existing deployment finishes the full-history Interpret redo and the
Project redo it installs before the matching API serves, as for any rotation;
until then the fenced name routes answer `409 stale`. It ships batched with the
TYR-116 release (the wrapped-registration resolver fix), whose single Interpret
redo and Project rebuild discharge it. Once the redo publishes, check on Sepolia
that `GET /v1/names/cosmic-heron.eth` serves `registered_at` `"1779115572"`,
its May 2026 ENSv1 registration (block 10874493, tx
0x61ab3b00cf6863c2aeeea3b584d4395fed2ebd1a65420d21f59d28bee536f988), equal to
its `created_at`, while `migrated_at` stays `"1790687028"`, its
`unlocked_wrapped` migration (block 11807758, tx
0x4ed8fa96e344fc2bb29c61f6f0cce4e9f3e31a7acf5cc033917dd7ff7c39c554). Both are
read from `GET /v1/names/cosmic-heron.eth/history?include=data`: the ENSv1 time
is the `RegistrationGranted` row emitted by the ENSv1 BaseRegistrar
(`0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85`), and the migration time is the
`MigrationApplied` row (`type=migration`), which shares its block, transaction
and log with the ENSv2 `RegistrationGranted` it accompanies. Each row carries
its `block_number`, `transaction_hash` and block `timestamp`.

### Resolver set when a wrapped name is registered again

The build that lets a later registry resolver write, by block, transaction and
log position, replace a resolver pointer that an earlier write left on the
registry read resource
([projections](projections.md#records-shared-through-resolver-links)) changes
`crates/adapters/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration, no manifest change and no historical
ingest fetch. It ships in the same full-history Interpret redo and Project redo
as [Resolver set while registering a wrapped name](#resolver-set-while-registering-a-wrapped-name),
and an existing deployment finishes both redos before the matching API serves, as for any rotation. Only
that redo corrects names already affected: when the Project redo publishes, a
wrapped `.eth` name whose earlier registration set its resolver through
`NameWrapper.setResolver`, and which was registered again with a resolver after
expiry and grace, serves the resolver from the new registration rather than the
earlier one, including any later change or clear. The same holds when the
registry owner left by an earlier unwrap set a resolver earlier in the block of
the new registration.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666-L671 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1019 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)

### Authority of registry children with no name surface

The build that serves `authority` on an ENSv1 registry child with no name
surface (a `setSubnodeOwner` or `setSubnodeRecord` node with no registrar
lease: both write the child's registry owner, emit `NewOwner` and create no
registrar lease
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L58 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)),
from the registry generation that owns its node: `ens_v0` while the deployed
registry still answers for the node from the 2017 registry, which it does
until it holds a record of its own
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f),
and `ens_v1` after; on
`GET /v1/addresses/{address}/names` and `GET /v1/names/{name}/subnames`, and
lets the address-names `authority` filter match it
([api-v1-routes](api-v1-routes.md#get-v1addressesaddressnames)), changes
`crates/storage/src/families`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration, no manifest change and no historical
ingest fetch, and it changes no projected row: the value is read from
`project_registry_node_state` at request time. It ships in the same
full-history Interpret redo and Project redo as
[Resolver set while registering a wrapped name](#resolver-set-while-registering-a-wrapped-name),
and an existing deployment finishes both redos before the matching API serves,
as for any rotation. Such a child also carries the `ens_v1` object, with a
null `expires_at` because it holds no lease. Subname rows also gain the
optional `authority` field, taken from the child's name row when it has one.
Before the release is recorded, confirm that both redos adopted the new hash.

### Capability flags without shadow

The build that removes `shadow` as a capability-flag status
([manifests](manifests.md#capability_flags)) and flips the checked-in `shadow`
flags to `supported` changes `crates/manifests/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain, although no interpreted or projected row changes. It needs no
schema-migration and no historical ingest fetch. It changes the payloads of the
active Sepolia `ens_execution`, `ens_v1_registrar_l1` and `ens_v2_registrar_l1`
manifests and the active Mainnet `ens_v1_registrar_l1` manifest, so manifest
synchronization records a
[manifest-authority marker](glossary.md#manifest-authority-marker) for
`ethereum-sepolia` (and `ethereum-mainnet` under the Mainnet profile), and the
Interpret redo that discharges it runs with `--attest-watch-set-coverage`: a
capability flag widens no watch-plan range. The Mainnet `ens_execution` and
`basenames_execution` v1 manifests are `shadow` rollouts, so their changed
payloads record no marker and do not invalidate the Base Project phase.

Ship it with the
[resolver set while registering a wrapped name](#resolver-set-while-registering-a-wrapped-name)
release: that release's full-history Interpret redo and Project redo on every
chain discharge this rotation and the marker too, and the matching API serves
only after both redos publish, as for any rotation. Before the release is
recorded, confirm that `GET /v1/namespaces/ens` reports `name_profile` and
`name_history` as `full` and that the Sepolia `verified_records` and
`verified_primary_name` are unchanged.

### Default reverse names

The build that admits the ENSIP-19 `default.reverse` registrar and serves its
name as the coin type `60` fallback
([primary-name route](api-v1-routes.md#get-v1addressesaddressprimary-name))
adds a `default_reverse_registrar` contract and its `NameForAddrChanged` event
to the `ens_v1_reverse_l1` manifests: Sepolia
`0x4F382928805ba0e23B30cFB75fC9E848e82DFD47` from block `8579966`
(upstream: .refs/ens_v1/deployments/sepolia/DefaultReverseRegistrar.json:L2 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/deployments/sepolia/DefaultReverseRegistrar.json:L270 @ ens_v1@91c966f)
and Mainnet `0x283F227c4Bd38ecE252C4Ae7ECE650B0e913f1f9` from block `22764819`
(upstream: .refs/ens_v1/deployments/mainnet/DefaultReverseRegistrar.json:L2 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/deployments/mainnet/DefaultReverseRegistrar.json:L270 @ ens_v1@91c966f)
([manifests](manifests.md#ens-mainnet)). It also changes
`crates/adapters/src` and the family readers under
`crates/storage/src/families`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration. The new address widens the watch plan:
manifest synchronization records a
[manifest-authority marker](glossary.md#manifest-authority-marker) and stamps a
required Ingest redo from the declared start block through the published head.
Complete that Ingest redo, then the full-history Interpret redo with
`--attest-watch-set-coverage`, then the Project redo it installs, before the
matching API serves. It may ship with the "Resolver set while registering a
wrapped name" release, whose Interpret and Project redos then run once for
both. Before the release is recorded, run the Sepolia check in the route
contract: `0x4f06fd857f8d4c6172aaa3f6a96a645b6940aacc` must answer
`evers.eth` and `0x1d84ad46f1ec91b4bb3208f645ad2fa7abec19f8` must answer
`artitest.eth` on the indexed source and `not_found` on the verified source,
and
`GET /v1/events?contract_address=0x4F382928805ba0e23B30cFB75fC9E848e82DFD47`
must list `primary_name` rows with `coin_type` `2147483648`.

### Missing sort keys sort as the smallest value

The build that makes a missing `expires_at`, `registered_at` or `created_at`
the smallest value in list sorts (first ascending, last descending) on
[`GET /v1/addresses/{address}/names`](api-v1-routes.md#get-v1addressesaddressnames),
[`GET /v1/names/{name}/subnames`](api-v1-routes.md#get-v1namesnamesubnames) and
[`GET /v1/names`](api-v1-routes.md#get-v1names) changes only API read paths,
but it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain: `crates/storage/src/address_names/query.rs` is on the content hash's
file list because the stored name summary takes its timestamps from it, and the
subname and former-registrant readers it edits sit under the hashed
`crates/storage/src/families` root. It needs no schema-migration, no manifest change and no historical
ingest fetch. An existing deployment runs the new binary for both the phase
runner and the API, and finishes the full-history Interpret redo and the
Project redo it installs before the matching API serves, as for any rotation;
an API upgraded alone refuses the old build's family publication with
`409 stale`. A cursor issued before the change on one of these sorts still
decodes and resumes after its saved row, but in the new order, which moved the
rows without the key to the other end of the list:

- Ascending (undated rows were last and are now first): a cursor saved on a
  dated row skips every undated row, and a cursor saved on an undated row
  returns every dated row a second time.
- Descending (undated rows were first and are now last): a cursor saved on an
  undated row skips every dated row, so rows with a real timestamp are lost,
  and a cursor saved on a dated row returns the undated rows already seen a
  second time at the end.

A client walking one of these sorts across the change restarts from the first
page.

### Current-state collection pages read one database snapshot

The build that reads each current-state collection page, the resolver and
registry overviews and name detail's `include=counts` on one read-only
`REPEATABLE READ` database snapshot (TYR-144) changes only API read paths, but
it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain: the readers it changes take the request's snapshot connection, and some
of them sit under the hashed `crates/storage/src/families` root. Stored rows do
not change. It needs no schema-migration, no manifest change and no historical
ingest fetch. An existing deployment runs the new binary for both the phase
runner and the API, and finishes the full-history Interpret redo and the
Project redo it installs before the matching API serves, as for any rotation;
an API upgraded alone refuses the old build's family publication with
`409 stale`.

Each such request holds one request-pool connection from its first read until
its snapshot commits, and takes no other pool connection meanwhile; admission
before it and the namespace manifest recheck after it each take a connection
briefly. `BIGNAME_DATABASE_MAX_CONNECTIONS` therefore bounds the collection
reads one API process runs at once. Responses keep their shapes, status codes and cursors; a
publication that lands while a page is read no longer turns it into
`409 stale`.

### Configurable publication lag tolerance

The build that adds `BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS` (see
[Surviving services](#surviving-services)) keeps today's behaviour at its
default of one block. It changes only API, lookup and status read paths and the
verified lookup's database guard, none of them hashed sources, so the
[interpreter content hash](glossary.md#interpreter-content-hash) does not
rotate and no redo is needed. It needs
`20261001150000_lookup_guard_configured_publication_lag.sql`, which replaces
`bigname_phase.revalidate_resolution_lookup_state` so the guard no longer fixes
its own one-block bound: it still requires the head the lookup pinned and the
exact publication the lookup captured, and the lookup applied the configured
tolerance when it captured them. The schema-migration changes no rows. Apply it
on the primary and wait for it to replay on any serving standby before raising
the tolerance; with the old guard, a verified lookup against a publication more
than one block behind is refused at revalidation. Applying it also applies any
earlier pending schema-migration, including those that build the lookahead
indexes, so on a large initialized database finish the lookahead index steps
below first.

### Verified lookups at the publication block

The build that runs verified record lookups at the captured family publication's
block, rather than the stored head, changes only lookup read paths, none of them
hashed sources, so the
[interpreter content hash](glossary.md#interpreter-content-hash) does not
rotate and no redo is needed. It adds no schema-migration: the lookup guard
still requires the head the lookup pinned and the exact publication it
captured. The lookup RPC provider must serve `eth_call` by block hash for blocks
up to the publication lag tolerance behind its newest block; a lookup whose
provider cannot serve that block is refused as `stale`, as before.

### Primary-name verification at the publication block

The build that runs ENS primary-name verification at the captured family
publication's block, rather than the stored head, changes only lookup read
paths, none of them hashed sources, so the
[interpreter content hash](glossary.md#interpreter-content-hash) does not
rotate and no redo is needed. It adds no schema-migration. While the
publication trails the head within the
[publication lag tolerance](glossary.md#publication-lag-tolerance),
`GET /v1/addresses/{address}/primary-name` with `source` omitted now answers
at the publication instead of `409 stale`, and its verified-only `meta.as_of`
reports the publication's block rather than the head. The lookup RPC provider
needs the same `eth_call`-by-hash depth as verified record lookups.

### Lookahead loader on Base

The build that extends the [lookahead loader](glossary.md#lookahead-loader) to
the four Basenames Base families makes `base-mainnet` choose it: Interpret no
longer restores Base's retained history when the runner starts or after a
reorg, and keeps no Base [interpreter session](glossary.md#interpreter-session)
in memory. Ethereum Sepolia keeps the full-state loader until the build in
[Lookahead loader on Sepolia](#lookahead-loader-on-sepolia). Both loaders produce
identical stored output, so no interpreted or projected row changes. The build
changes `crates/adapters/src`, so it still rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain: finish the full-history Interpret redo and the Project redo it installs
before the matching API serves, as for any rotation, or ship it in a release
whose redos already run. It needs no manifest change and no historical ingest
fetch.

Schema-migration
`20261001120100_normalized_events_basenames_lookahead_indexes.sql` adds
`normalized_events_basenames_direct_node_probe_idx` and
`normalized_events_basenames_due_probe_idx`. On a large initialized database,
rerun [`ops/v1-lookahead-indexes/install.sql`](../ops/v1-lookahead-indexes/README.md)
before applying the schema-migrations and starting the release: it accepts the
existing ENSv1 pair and builds the Basenames pair concurrently. Then run
`ANALYZE bigname_phase.normalized_events` and apply the schema-migrations with
`--target-version 20261001120100`, which then only adopts and checks the prebuilt pair. The Basenames pair is needed on a
database that holds only Ethereum mainnet too: the Ethereum batches run the same
queries, whose Basenames arms would otherwise scan `normalized_events` once per
requested name. `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true`
keeps the full-state loader on every chain while they build. Before the release
is recorded, confirm the runner logged `interpret chose its prior-state loader`
with `lookahead` for `base-mainnet`.

### Lookahead loader on Sepolia

The build that extends the [lookahead loader](glossary.md#lookahead-loader) to
the five ENSv2 families makes `ethereum-sepolia` choose it: Interpret no longer
restores Sepolia's retained history when the runner starts or after a reorg,
and keeps no Sepolia [interpreter session](glossary.md#interpreter-session) in
memory. Each Sepolia batch instead reads the ENSv2 history of the names and
[ENSv2 state keys](glossary.md#ensv2-state-key) it touches, then the ENSv1
history of the names those events mention, so its per-batch read grows with
what the batch touches rather than with the chain's history. Both loaders produce identical stored output, so no interpreted or
projected row changes. The build changes `crates/adapters/src`, so it rotates
the [interpreter content hash](glossary.md#interpreter-content-hash) for every
chain: finish the full-history Interpret redo and the Project redo it installs
before the matching API serves, as for any rotation, or ship it in a release
whose redos already run. It needs no manifest change and no historical ingest
fetch.

Schema-migration `20261001130000_normalized_events_v2_lookahead_indexes.sql`
adds the four `normalized_events_v2_*_probe_idx` indexes. On a large
initialized database, rerun
[`ops/v1-lookahead-indexes/install.sql`](../ops/v1-lookahead-indexes/README.md)
before applying the schema-migrations and starting the release: it accepts the
existing four indexes and builds the ENSv2 ones concurrently. Then run
`ANALYZE bigname_phase.normalized_events` and apply the schema-migrations with
`--target-version 20261001130000`, which then only adopts and checks the
prebuilt indexes. Ethereum mainnet and Base read only
the ENSv2 name index, but the schema-migration builds all four without
`CONCURRENTLY` by scanning `normalized_events`, so prebuild them on every large
database. `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true` keeps the full-state loader on
every chain while it builds. Before the release is recorded, confirm the runner
logged `interpret chose its prior-state loader` with `lookahead` for
`ethereum-sepolia`.

### RPC chain check at startup

The build that adds the [RPC chain check](#rpc-chain-check) edits no file the
[interpreter content hash](glossary.md#interpreter-content-hash) covers, so it
needs no redo and no historical ingest fetch. Schema-migration
`20261001153000_ingest_cursor_verified_chain.sql` adds the nullable
`verified_chain_id` and `verified_genesis_hash` columns to `ingest_cursors` on
an existing phase schema, and `init-schema` installs them on a fresh one. An
existing deployment applies the schema-migration, then starts the new runner,
which fills both columns on every cursor whose endpoint passes; the runner
refuses to start until it is applied. Applying it also applies any earlier
pending schema-migration, including `20261001120100` and `20261001130000`: on a
large initialized database, finish the lookahead loader index steps for
[Base](#lookahead-loader-on-base) and [Sepolia](#lookahead-loader-on-sepolia)
first. Before
upgrading, confirm each configured RPC endpoint answers `eth_chainId` and
returns block 0: a provider that cannot serve block 0 refuses the start in the
default `full` mode. The API gains `BIGNAME_API_RPC_CHAIN_CHECK` with the same
two values ([production environment](production.md#api-request-bounds)). Load
the new `BignamePhaseRunnerRpcChainMismatch` rule with the runner.

### Null receipt re-requests and Ingest RPC counters

The build that re-requests `null` receipts and transactions in place (see
[the RPC settings](#phase-runner-configuration)) edits no file the
[interpreter content hash](glossary.md#interpreter-content-hash) covers and adds
no schema-migration, so it needs no redo and no historical ingest fetch. A
phase that fails with a retryable error now waits the initial restart delay
again once a later batch settles, including an idle Live poll, instead of
keeping the longer delay an earlier run of failures reached. An HTTP 400 that
says the provider cannot route the request to a node that serves it is retried
with backoff instead of stopping the chain as a data integrity fault. The runner exports three new
counters for Ingest and Live RPC traffic, described in the
[monitoring runbook](runbooks/pipeline-monitoring.md#ingest-rpc-traffic).

### Read-only family queries outside the content hash

The build that hashes only the composition part of `crates/storage/src/families`
(TYR-149, see [interpretation replay](storage.md#interpretation-replay)) changes
which files the [interpreter content hash](glossary.md#interpreter-content-hash)
reads, so it rotates the hash once for every chain although no code that runs
changes and no stored row changes. An existing deployment finishes the
full-history Interpret redo and the Project redo it installs before the matching
API serves, as for any rotation; a release batch that rotates the hash for
another change discharges both with one redo pair. It needs no schema-migration,
no manifest change and no historical ingest fetch. After it, a change to a
read-only family query (search, bound names, record, reverse, permission,
children or topology readers) no longer rotates the hash or forces a redo.

### Verified primary names without a forward resolver

The build that answers a verified ENS primary name `not_found` when its forward
check reverts with `ResolverNotFound` for the claimed name (see
[the route contract](api-v1-routes.md#get-v1addressesaddressprimary-name))
changes only the lookup read path, none of it a hashed source, so the
[interpreter content hash](glossary.md#interpreter-content-hash) does not
rotate. It adds no schema-migration and needs no redo or historical ingest
fetch. Verified answers that were `failed` with `resolver_call_reverted` for
such names become `not_found`; indexed answers do not change. After it serves,
run the Sepolia check in [Default reverse names](#default-reverse-names).

### Universal Resolver cutover gauges and alert

The build that reports the
[Universal Resolver cutover](glossary.md#universal-resolver-cutover) per chain
edits no file the [interpreter content hash](glossary.md#interpreter-content-hash)
covers and adds no schema-migration, so it needs no redo and no historical
ingest fetch. The runner exports `phase_runner_universal_resolver_cut_over` and
`phase_runner_universal_resolver_unadmitted` and logs a warning when a chain's
client-facing Universal Resolver comes to end at an implementation the
`ens_execution` manifest does not admit
([monitoring runbook](runbooks/pipeline-monitoring.md#universal-resolver-cutover)).
Load the new `BignameUniversalResolverUnadmitted` rule with the runner. On a
deployment whose manifests do not admit the implementation a chain's Universal
Resolver currently points at, the rule pages once the publication is current;
that page is the re-admission to do, not a fault of the build.

### Resolver implementation start blocks

The build that lets a `resolver_implementations` entry carry an optional
`start_block` (TYR-193, see [Resolver admission by implementation
announcement](manifests.md#resolver-admission-by-implementation-announcement))
changes `crates/manifests/src` and the Sepolia `ens_v2_resolver_l1` manifest, so
it rotates the [interpreter content hash](glossary.md#interpreter-content-hash)
for every chain. It also changes the Sepolia manifest payload, so manifest
synchronization records a [manifest-authority
marker](glossary.md#manifest-authority-marker) for Sepolia and its
full-history Interpret redo runs with `--attest-watch-set-coverage`, attesting
that no watch-plan range widened. A release batch that rotates the hash for
another change, such as [read-only family queries outside the content
hash](#read-only-family-queries-outside-the-content-hash) (TYR-149),
discharges both with that one Interpret and Project redo pair; v0.3.0 runs it
for TYR-149, TYR-183 and TYR-191, so this adds no redo of its own. It sets
the start of the admitted implementation
`0x14f09fd05d4585759e54844dc9b00147131cf243` to its creation block `11709070`.
The compiled watch plan already covers that implementation from block zero, so
the later start stamps no historical ingest fetch. It adds no
schema-migration. A later implementation declared with a `start_block` stamps
its required Ingest redo from that block, clamped to the chain's ingest start,
instead of from block zero. The build also changes `crates/adapters/src`:
Interpret no longer admits a resolver from an `Upgraded` announcement before
its implementation's `start_block`, so a database that never fetched such a log
writes the same interpreted rows as one that did (the fetched log can add only
an operator diagnostic in `interpret_decode_skips`). On Sepolia this changes interpreted output only
if a log before block `11709070` names `0x14f09fd0…`; the shared full-history
Interpret redo applies the rule either way. The same release replaces that
implementation with the 2026-10-01 redeploy's `0x115eb53f…` from block
`11820406` ([Sepolia ENSv2 redeploy of 2026-10-01](#sepolia-ensv2-redeploy-of-2026-10-01)).

### Sepolia ENSv2 redeploy of 2026-10-01

The build that admits the 2026-10-01 Sepolia ENSv2 redeploy (TYR-183, see the
[deployment inventory](sepolia-deployment.md)) replaces the six Sepolia ENSv2
families (root, registry, registrar, resolver, migration and `ens_execution`)
with version 2 under `deployment_epoch = "ens_v2_sepolia_20261001"` and deletes
version 1. The 2026-09-15 deployment's contracts are dropped, not kept as
retired history. Both Universal Resolver proxies keep their addresses and start blocks;
`ens_execution` lists only the redeploy's UniversalResolverV2 `0x24e1d8e0…`,
which the managed proxy moved to at block `11821680`, so that block is the
Sepolia [Universal Resolver cutover](glossary.md#universal-resolver-cutover).
The redeploy ships as a new version file because synchronization upserts a
manifest on its namespace, family, chain, epoch and version while the stored
file path must stay unique, so a new epoch written into the same `v1.toml`
would stop the runner at startup.

The manifest change rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It needs no schema-migration. On first start, manifest synchronization
deprecates the six version-1 rows, retires the dropped addresses at the
published head, records a
[manifest-authority marker](glossary.md#manifest-authority-marker) on Sepolia's
Interpret and Project rows and stamps a required Ingest redo from the earliest
retained `RegistryCreated` log (block `10893181` on Sepolia), clamped to the
chain's first ingest cursor, to the published head, not from the redeploy's
blocks: the new ETHRegistry's announcement rule reaches back to that log. The
redeploy's `PermissionedResolverImpl` watch starts at its creation block
`11820406` ([resolver implementation start
blocks](#resolver-implementation-start-blocks)), so it widens nothing earlier. Synchronization refuses
to retire an address before its start, so start the build only once the
recorded Sepolia head is at or past `11709095`, the latest start among the
dropped declarations (the 2026-09-15 `MigrationHelper`,
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/MigrationHelper.json:L558 @ ens_v2_sepolia_20260916@366de741)),
and keep the previous release until then. Size the refetch from the stamped
range before the release. Complete that Ingest redo, then the
full-history Interpret redo with `--attest-watch-set-coverage`, then the
Project redo it installs, before the matching API serves. A release that also
rotates the hash for another change, such as "Read-only family queries outside
the content hash", runs that Interpret and Project pair once for both.

After the redo, Sepolia's ENSv2 history starts with the redeploy. The replayed
proxy history classifies every block before `11821680` as not cut over,
including `11710193` to `11821679`, when the dropped deployment answered
resolution, so Project derives that range with ENSv1 expiry, grace and
resolvers for every `.eth` name. Name reads still serve only the current
family publication: an `at=` below it answers `stale`, as before. The dropped registries
announced themselves with `RegistryCreated`, so like any self-announced registry
they stay on the registry routes, but nothing admitted reaches them and their
entries name no `.eth` name: check that no normalized event from the dropped
ETHRegistry `0x657ea849…` carries a logical name. An upgraded database ignores
a dropped registry's writes after the synchronization head, because the retired
declaration caps its re-announced admission there; a fresh corpus derives them
unnamed. Nothing is named on either path (TYR-195 tracks converging the two). The redeploy re-ran premigration: `nick.eth` is
reserved on the new ETHRegistry at block `11821474` with expiry `1803965433`,
so the "ENSv1 lease date on name rows" check stands as written. Also check that
an ENSv1-only `.eth` name with no entry on the new ETHRegistry serves
`unresolvable_reason` `no_live_ens_v2_entry`.

### Authority and filters on the names-by-expiry listing

The build that adds `authority` to [`GET /v1/names`](api-v1-routes.md#get-v1names)
and [`GET /v1/search`](api-v1-routes.md#get-v1search) rows and the `authority`
and `parent` filters to `GET /v1/names` changes only API and family read paths.
The storage files it edits, `crates/storage/src/families/name/list.rs`,
`crates/storage/src/name_current/expiring.rs` and
`crates/storage/src/name_current/public_authority.rs`, are read-only queries
outside the [interpreter content hash](glossary.md#interpreter-content-hash),
so the hash does not rotate. It adds no schema-migration and needs no redo or historical
ingest fetch. Rows gain one field; cursors issued before it continue unchanged,
and a cursor issued with `authority` or `parent` must be continued with the
same filters.

### Owner as the token holder

The build that serves `owner` as the token holder (TYR-191, see the
[naming dictionary](api-v1.md#naming-dictionary) and [Manager](api-v1.md#manager))
changes the composition in `crates/storage/src/families` and the address-name
index in `crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every chain.
Finish the full-history Interpret redo and the Project redo it installs before
the matching API serves. In v0.3.0 it shares the release's one Interpret and
Project redo pair with the other hash-rotating changes in the batch
([read-only family queries outside the content hash](#read-only-family-queries-outside-the-content-hash),
TYR-149, the [Sepolia ENSv2 redeploy of 2026-10-01](#sepolia-ensv2-redeploy-of-2026-10-01),
TYR-183, and
[resolver implementation start blocks](#resolver-implementation-start-blocks),
TYR-193): run one pair under a binary that holds all of them. It needs no schema-migration, no manifest or
environment change and no historical ingest fetch. The stored
`project_name_summary.owner`, which the registry-label `owner` and
`exclude_owner` filters read, now holds the token holder of wrapped names and
unwrapped `.eth` second-level names, and the address index drops its
`registrant` rows; the Project redo rebuilds both. It also recomposes every
released name: none keeps an owner or manager (except a surface-less released `.eth`
child without a label preimage, which still lists its registry owner as both, TYR-196, until
the build of [released registrar children](#released-registrar-children)), and
an ENSv1 lease that lapsed with
its registry record left in place now carries `lapsed_registration`, so it lists
under `relation=former_owner`. The API change is breaking
for clients of `owner`, `registrant`, `relation=registrant`,
`relation=former_registrant` and `lapsed_registration.registrant`.

### Ingest redo after a killed supervisor

The build that lets a required Ingest redo settle phases a killed supervisor
left `running` (TYR-106, see
[chain intake](chain-intake.md#implemented-phase-boundary)) edits no file
the [interpreter content hash](glossary.md#interpreter-content-hash) covers and
adds no schema-migration, so it needs no redo and no historical ingest fetch.
Before it, a supervisor killed before a deploy that widens the watch plan left
its Project or Interpret row `running`, and the
[runbook's](runbooks/production-docker.md) one-shot Ingest redo refused with
`cannot start phase ingest ... while phase project is running`. On an older
build, instead of editing `chain_phase_state` by hand, start the supervisor
once from the same image that ran the refused redo, so that its start-up
manifest synchronization installs no new required work or authority marker.
Its start-up recovery settles the row, and the chain then stops with the
required Ingest error (`manifest watch plan widened over already-ingested
blocks ...`). Compose restarts an exited supervisor and other configured chains
run their unattended work, so once that error is logged, stop the
`phase-runner` service with the runbook's `docker compose --env-file
.env.server -f docker-compose.server.yml stop phase-runner` within its grace
period, then rerun the same Ingest redo.

### History record attribution indexes

The build that keys history's record attribution (TYR-168, see
[table ownership](storage.md#table-ownership)) changes `crates/storage/src/history`, the
normalized-events baseline and one schema-migration, all outside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash does not rotate
and it needs no redo, no manifest or environment change and no historical ingest fetch. It
speeds up `GET /v1/names/{name}/history` with `scope=registration` or `scope=both` and
`GET /v1/events?registration_id=` on every chain that holds writes with
[storage model](glossary.md#storage-model) `resolver_record_id` or ENSv2 resolver pointers;
today only the Sepolia manifests admit ENSv2 sources. Responses do not change.

`20261003120000_normalized_events_record_id_attribution_indexes.sql` adds
`normalized_events_record_id_write_idx` and `normalized_events_record_id_link_idx`, partial on
`RecordChanged` and `ResolverRecordLinked` rows with storage model `resolver_record_id`. Each is
a plain `CREATE INDEX` that scans all of `normalized_events` while holding a SHARE lock on it
until the schema-migration commits, which blocks Interpret's writes. On a large initialized
database, prebuild both concurrently first, outside a transaction; the phase runner and API can
keep running while they build:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_write_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        (after_state ->> 'resolver_record_id')
    )
    WHERE event_kind = 'RecordChanged'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_id_link_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        lower(after_state ->> 'node')
    )
    WHERE event_kind = 'ResolverRecordLinked'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

ANALYZE bigname_phase.normalized_events;
```

These statements are the historical definition required by that target version. The later
`20261005170000_project_address_history_catalogue.sql` replaces the write index with the full
history order. Apply migrations in order; do not prebuild the later definition before this
historical migration has been recorded. The current walk-index installer uses the later
catalogue definition and belongs after that upgrade. The dedicated
[catalogue prebuild](../ops/address-history-catalogue-indexes/README.md) instead builds
replacement candidates after this historical version is recorded and before the catalogue
migration; its temporary names preserve the historical indexes until adoption.

Then apply the schema-migrations with `--target-version 20261003120000` and the same
`lock_timeout`, `statement_timeout` and retry procedure; it finds both indexes and skips the
build. `CREATE INDEX IF NOT EXISTS` matches the name only, so the schema-migration then checks
that each name is an index on `normalized_events` that is `indisvalid` and `indisready` with the
reviewed `pg_get_indexdef`, and fails without recording itself otherwise. To recover, drop the
named relation (an interrupted concurrent build leaves an invalid index: confirm in
`pg_stat_progress_create_index` that no build is still running, then `DROP INDEX CONCURRENTLY`
it), rebuild it with the statement above and apply the schema-migrations again. Without the
prebuild, apply the schema-migration with the phase runner and redo processes stopped. API
standbys receive the indexes through replication.

### Resolution protocol on the namespace route

The build that adds `resolution` to each network on
[`GET /v1/namespaces/{namespace}`](api-v1-routes.md#get-v1namespacesnamespace)
(TYR-184) changes only the API and `crates/storage/src/resolution_state.rs`, a
read-only query outside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash
does not rotate. It needs no schema-migration, no manifest or environment
change, no redo and no historical ingest fetch. The field is additive. The
phase-runner's Universal Resolver warning shares the read, so its `block` now
names the latest `Upgraded` on the client-facing proxy's path rather than the
block of the row the path ends at. After deploy, with the
[Sepolia ENSv2 redeploy of 2026-10-01](#sepolia-ensv2-redeploy-of-2026-10-01)
in place, `GET /v1/namespaces/ens` on Sepolia serves `resolution` with
`protocol` `ens_v2` and `since_block` equal to the Sepolia
[Universal Resolver cutover](glossary.md#universal-resolver-cutover) block,
when the managed proxy moved to the listed UniversalResolverV2
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05).
On Mainnet the `ens_execution` manifest declares no `Upgraded` event, so no
proxy row exists and the network serves `{"protocol": "ens_v1", "since_block": null}`.

### Parent filter on names by address

The build that adds `parent` to
[`GET /v1/addresses/{address}/names`](api-v1-routes.md#get-v1addressesaddressnames)
changes only API and read paths. The storage files it edits,
`crates/storage/src/address_names/{source,page,read,resolves_to_page,resolves_to_evm}.rs`,
`crates/storage/src/name_current.rs`,
`crates/storage/src/families/name/list.rs` (a comment only) and
`crates/storage/src/families/records/{address_names,resolves_to_serving,former_owners}.rs`,
are read-only queries outside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash
does not rotate. It adds no schema-migration and needs no redo or historical
ingest fetch. Cursors issued before it continue unchanged, and a cursor issued
with `parent` must be continued with the same `parent`.

### Released registrar children

The build that stops serving an owner or manager for a released registry child with no name
row (TYR-196, see [subnames](api-v1-routes.md#get-v1namesnamesubnames) and
[names by address](api-v1-routes.md#get-v1addressesaddressnames)) changes only readers in
`crates/storage/src/families`, the API, the projections baseline and one schema-migration, all
outside the [interpreter content hash](glossary.md#interpreter-content-hash), so the hash does
not rotate and it needs no redo, no manifest or environment change and no historical ingest
fetch. Such a child's registrar lease is already projected without a name row; once it has been
released, `GET /v1/addresses/{address}/names` stops listing an ENSv1 `.eth` child for its
surviving registry owner (that route lists no Basenames child without a name row), and the
parent's subnames page serves an ENSv1 or Basenames child as `released` with no `owner` or
`manager`, omitted under `include_expired=false`.

`20261003130000_project_lifecycle_event_namehash_index.sql` adds
`project_lifecycle_event_namehash_idx` on `project_lifecycle_event (chain_id, namehash)`, which
the child reads probe for each child with no name row. It is a plain `CREATE INDEX` that holds a
SHARE lock on `project_lifecycle_event` until the schema-migration commits, blocking Project's
writes and `VACUUM` and `ANALYZE` on it. On a large initialized database, prebuild it
concurrently first, outside a transaction; the phase runner and API can keep running:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS project_lifecycle_event_namehash_idx
    ON bigname_phase.project_lifecycle_event (chain_id, namehash);
```

Then apply the schema-migrations with `--target-version 20261003130000` and the same
`lock_timeout`, `statement_timeout` and retry procedure; it finds the index and skips the build.
Without the prebuild, apply it with the phase runner and redo processes stopped.
`CREATE INDEX IF NOT EXISTS` matches the name only, so the schema-migration then checks that the
name is an index on `project_lifecycle_event` that is `indisvalid` and `indisready` with the
reviewed `pg_get_indexdef`, `(chain_id, namehash)` with no predicate, and fails without
recording itself otherwise. To recover, drop the named relation (an interrupted concurrent
build leaves an invalid index: confirm in `pg_stat_progress_create_index` that no build is still
running, then `DROP INDEX CONCURRENTLY` it), rebuild it with the statement above and apply the
schema-migrations again. An API started before the index exists serves the same rows, only
slower. API standbys receive the index through replication.

### NameWrapper authority ends when the registry record leaves NameWrapper

The build that ends a wrapped name's NameWrapper authority once a registry
write moves its record away from NameWrapper (TYR-147 and TYR-100, see
[Manager](api-v1.md#manager)) changes the ENSv1 registry adapter in
`crates/adapters/src` and the registrant composition in
`crates/storage/src/families`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every chain.
Normalized events change for every wrapped name whose registry record was
written to an owner other than NameWrapper without a following unwrap, such as
a parent owner's `setSubnodeOwner` over a wrapped child or the Graveyard
clearing a wrapped subname. Finish the full-history Interpret redo and the
Project redo it installs before the matching API serves. Stamp no Ingest redo:
the build changes no manifest, watch set, start block, table or
schema-migration, and the adapter reads only the logs of the batch it
interprets, so no historical ingest fetch is needed. In v0.4.0 it shares the
release's one Interpret and Project redo pair with the other hash-rotating
changes in the bundle: run one pair under a binary that holds all of them.
After the redo such a name serves its registry owner as `owner` and `manager`,
or no owner when the parent set it to zero, carries no `ens_v1.wrapper_state`, and is listed under its registry owner
instead of the old token holder; a Graveyard sent a cleared subname's surviving
token is no longer listed as its manager. The Project redo also recomposes a
subname that was unwrapped and whose registry record was later given to
another owner: it now serves that registry owner as `owner`, not the holder it
was unwrapped to.

### Token holder of a lease with no name surface in the address index

The build that indexes the token holder of a `.eth` or Basenames lease with no
[name surface](glossary.md#surface-name-surface) (TYR-201) changes
`crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every chain.
A registrar `Transfer` of such a lease now writes its row in
`project_address_name_fold` (the per-name summary of the addresses that hold or
control a name) under the lease's `<namespace>:<namehash>` id, so the recipient, who holds the
token while the previous holder keeps the registry record until `reclaim`
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L319-L330 @ basenames@1809bbc),
reaches the address index beside the registry owner. Normalized events do not
change. Finish the full-history Interpret redo and the Project redo it installs
before the matching API serves; in v0.4.0 it shares the release's one Interpret
and Project redo pair with the other hash-rotating changes in the batch. Stamp no
Ingest redo: it changes no manifest, watch set, start block, table or
schema-migration, and Project reads only retained normalized events. It needs no
environment change and no historical ingest fetch. It changes no API response
until the build of
[the lease holder of a registry child with no name surface](#lease-holder-of-a-registry-child-with-no-name-surface),
which serves the token holder these rows add.

### ENSv2 registries read whole only when their suffix moves

The build that stops the lookahead loader from reading a whole ENSv2 registry
for a batch that leaves the registry's
[name suffix](glossary.md#ensv2-name-suffix-walk) unchanged (TYR-202, see
[Interpret process memory](storage.md#interpret-process-memory)) changes the
ENSv2 name refresh in `crates/adapters/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. Names do not change: a token whose registry's suffix walk is unchanged
keeps its name unless the batch touches it for its own reasons, such as its
expiry, release or replacement, which still refresh it, and both loaders apply
the same rule. The build also
fixes which resource is current for a name that both an ENSv1 registration and
an ENSv2 token hold, such as a name moved to ENSv2 whose ENSv1 registration is
renewed afterwards. Before, an ENSv1 event could make the ENSv1 resource current
and the ENSv2 token's resource came back only if a name refresh happened to
reach that token, which depended on batch boundaries and on unrelated registry
changes. Now the ENSv2 token's resource stays current immediately, as a full
refresh of every name elects, so the result no longer depends on how the history
was batched. No stored event reads this choice today; it keeps the state
consistent for any that will. It adds no schema-migration, table, index,
manifest or setting, so stamp no Ingest redo. In v0.4.0 it shares the release's
one Interpret and Project redo pair with the other hash-rotating changes in the
bundle. A batch that touches an ENSv2 registry without moving its name suffix
now reads only the history of the names and tokens it touches instead of the
whole registry's, so a lookahead redo no longer reads a busy registry whole on
nearly every batch. A batch whose registry suffix does move, or the
first batch with ENSv2 events on a chain, still reads the registry whole and
logs a warning; see [Verify health](runbooks/production-docker.md#verify-health).

### ENSv2 role changes filed under their token

The build that files a role change on one ENSv2 registry token under that
token's [ENSv2 state key](glossary.md#ensv2-state-key) instead of the
registry-level key (TYR-213) changes `crates/adapters/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. A registry builds a token's access-control resource id from the same
labelhash as its token id, replacing only the low 32 bits
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L678-L694 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L15-L17 @ ens_v2_sepolia_20261001@07e55a05),
so the role change now lands on the token's own key. Before, a token's role
changes sat under the registry-level key, so a lookahead batch touching one
token of a registry could read other tokens' role changes and then those
tokens' histories, and through their subregistries the tokens beneath them. Now such a batch reads the tokens it touches and the registry's
own rows: its creation, upgrades, parent claim and role changes on its root
resource. Stored events change only on ENSv2 registry and root registry
`PermissionChanged` rows, whose `raw_fact_ref.state_scope` and
`raw_fact_ref.interpreter_state_key` change; `/v1/diagnostics/events` shows
those two fields and no product row changes (see
[Cursors And Pagination](api-v1.md#cursors-and-pagination)). It adds no
schema-migration, table, index, manifest or setting, so stamp no Ingest redo. In v0.4.0 it shares the release's
one Interpret and Project redo pair with the other hash-rotating changes in the
bundle. The whole-registry warning now counts a token once under all of its
ids, so its token count no longer includes resource ids.

### Lease holder of a registry child with no name surface

The build that serves the holder of a `.eth` or Basenames lease as the `owner` of a
registry child with no [name surface](glossary.md#surface-name-surface) (TYR-201, see
[subnames](api-v1-routes.md#get-v1namesnamesubnames) and
[names by address](api-v1-routes.md#get-v1addressesaddressnames)) changes only readers in
`crates/storage/src/families`, `crates/storage/src/children` and
`crates/storage/src/address_names`, and the API, all outside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash does not rotate.
It needs no schema-migration, no redo, no manifest or environment change and no historical
ingest fetch: it reads the address index rows of
[the token holder of a lease with no name surface](#token-holder-of-a-lease-with-no-name-surface-in-the-address-index)
and the registrar lease rows Project already keeps. While the registrar retains such a
child's lease, its `owner` is the lease's holder, the recipient of its latest token
`Transfer` or else its registrant, and its `manager` stays its registry owner: after a
token transfer without `reclaim`
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f),
`GET /v1/addresses/{address}/names`, which lists ENSv1 `.eth` children only, lists the child for
the buyer under `owner` and for the seller under `manager` only, and
`relation=owner&parent=eth&dedupe=registration` counts it for the buyer and not for the
seller. The parent's subnames page serves the buyer as `owner` and the seller as `manager`,
with the same rows and counts. The change is breaking for clients that relied on the seller
counting under `owner`. A released lease, and a child the NameWrapper that named it holds,
are still served for no one.

### ENSv2 registry operator approvals and registry entries

The build that captures ENSv2 registry `ApprovalForAll` and keeps each registry
entry's current token owner (TYR-236, see
[ENSv2 registry operator approvals](manifests.md#ensv2-registry-operator-approvals)
and [ENSv2 registry entries](projections.md#ensv2-registry-entries)) changes
`crates/manifests/src`, `crates/adapters/src`, `crates/project/src` and the
Sepolia `ens_v2_registry_l1` and `ens_v2_root_l1` manifests, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. Permissions exposure is unchanged: `GET /v1/permissions` still names
`ens_v2_registry_operators` as an unlisted surface. The shared product-history
reader excludes the marked tokenless expiry described below from listing and
counting before pagination; that reader change does not by itself rotate the
interpreter content hash. Besides the approval rows,
stored events gain one kind of row: an `ExpiryChanged` with no name, no resource
and `token_state_absent = true` for an `ExpiryUpdated` whose token the adapter
holds no state for, such as a renewal that revives an unregistered entry or a
renewal of an entry registered before the registry's retained history. It
updates the registry entry row and creates no named lifecycle or ownership
state. `/v1/diagnostics/events` shows it. `GET /v1/events` and the other
product history reads neither list nor count it: the shared history query
omits it before pagination (see
[`GET /v1/events`](api-v1-routes.md#get-v1events)). That reader change is
outside the interpreter content hash. The Mainnet and Base
manifests are unchanged, so those chains get the hash rotation and its
Interpret and Project redo pair and no Ingest redo.

On Sepolia the two manifest payloads change and the
[compiled watch plan](glossary.md#compiled-watch-plan) widens by the registry
family's `ApprovalForAll` entry, from block `10893181`, and by the same event at
the declared ETHRegistry and RootRegistry from their own start blocks. Roll it
out in this order:

1. Apply schema-migration `20261005140000_project_ens_v2_registry_entries.sql`.
   It adds the empty tables `project_ens_v2_entry_owner` and
   `project_ens_v2_registry_parent` with three indexes and corrects one column
   comment. It rewrites no existing row and needs no prebuild.
2. Start the build. Manifest synchronization records a
   [manifest-authority marker](glossary.md#manifest-authority-marker) on
   Sepolia's Interpret and Project rows and stamps a required Ingest redo from
   block `10893181`, clamped to the chain's first ingest cursor, to the
   published head. That is the same lower bound the
   [Sepolia ENSv2 redeploy](#sepolia-ensv2-redeploy-of-2026-10-01) stamped. The
   redo fetches the one added topic at the admitted registries; size it from
   the range `chain_phase_state` records.
3. Complete that Ingest redo, then the full-history Interpret redo with
   `--attest-watch-set-coverage`, then the Project redo it installs, before the
   matching API serves. This build targets the next hash-rotating release; when
   it ships with other rotating changes, one Interpret and Project redo pair
   discharges them all.

The floor is one value for the family, the earliest start of any admitted
registry on a database that retains Sepolia's history; the manifests document
says how to re-derive it. A deployment whose admitted registries start earlier
must lower the manifest value before it starts the build, or approvals below
the floor stay unfetched. An approval a registry emitted before its own
admission start is not fetched either way; on Sepolia every admitted registry's
first retained log is at its admission start. After the redo, check that
`project_account_approval` holds `authority_kind = 'ens_v2_registry'` rows with
an empty `effective_powers`, that `project_ens_v2_entry_owner` has a row for a
known ETHRegistry name with its current owner, and that no
`AccountPermissionChanged` row names a resolver as its authority contract.

### ENSv2 registry operators and root holders on permission reads

The build that serves ENSv2 registry operators and lists registry root holders
on a registration's permission read changes readers and the API only. It adds
no schema-migration and does not rotate the
[interpreter content hash](glossary.md#interpreter-content-hash), so it needs
no redo of its own. It reads the approval rows and the registry entry rows of
[the build above](#ensv2-registry-operator-approvals-and-registry-entries), so
it must not serve before that build's schema-migration and redo sequence has
completed on the database: until the families are rebuilt, the entry table is
empty and no ENSv2 operator is listed.

The API startup check now requires `bigname_phase.project_ens_v2_entry_owner`.
Grant the API role `SELECT` on it, as in the
[API role grant list](#surviving-services), before starting this API build; a role without
it fails startup.

What changes on `GET /v1/permissions` and `include=role_summary`
([route contract](api-v1-routes.md#get-v1permissions)):

- An ENSv2 registration gains one `grant_relation=operator` row per account,
  other than the owner itself, that its current token owner approved on the
  registry, while the owner has a served grant on the token and the entry has
  not expired: the registry adds the current owner's token roles to each
  operator that owner approved, and reports no owner once the entry's expiry
  has passed. The row has `authority_kind` `ens_v2_registry`, a value the
  account scope did not use before.
  (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L622-L636 @ ens_v2_sepolia_20261001@07e55a05)
  (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L350-L352 @ ens_v2_sepolia_20261001@07e55a05)
- A `name` or `registration_id` read of an ENSv2 registration gains the
  registry's root holders as `root` rows of that registration, because a
  role check on a token reads the caller's root roles together with its token
  roles. Its row count and pages change. `role_summary` does not repeat them.
  (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L454-L465 @ ens_v2_sepolia_20261001@07e55a05)
- A registration of a manifest-declared ENSv2 registry stops reporting
  `ens_v2_registry_operators` and reports `["resolver_approvals"]`. A
  registration of a discovered registry, a discovered registry's root and an
  address-only read keep the code.

### v0.4.0 rollout

v0.4.0 carries four hash-rotating builds: the end of NameWrapper authority
when the registry record leaves NameWrapper (TYR-147 and TYR-100), the
token-holder index for leases with no name surface (TYR-201), the ENSv2
suffix-walk reads (TYR-202) and
[ENSv2 role changes filed under their token](#ensv2-role-changes-filed-under-their-token)
(TYR-213). The last changes stored events only in two `raw_fact_ref` fields of
ENSv2 registry and root registry `PermissionChanged` rows, which
`/v1/diagnostics/events` shows; no product row changes. It also carries the builds from
[Ingest redo after a killed supervisor](#ingest-redo-after-a-killed-supervisor)
through [released registrar children](#released-registrar-children), the
lookahead loader's read-path changes and the
[lease holder of a registry child with no name surface](#lease-holder-of-a-registry-child-with-no-name-surface),
and the optional [walk index set](#walk-index-set) operator scripts (TYR-209),
none of which rotates the hash. Deploy
it with the
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).
From v0.3.0:

- The [interpreter content hash](glossary.md#interpreter-content-hash) rotates
  once for every chain. Run one full-history Interpret redo and the Project redo
  it installs under the v0.4.0 binary before the API serves, and record the new
  hash in the release record. The [walk index set](#walk-index-set) scripts are an
  optional step of that redo: `drop.sql` once the Interpret redo has started,
  `install.sql` before Interpret completes, as the runbook's planned boundary
  describes.
- Stamp no Ingest redo. No build since v0.3.0 changes a manifest, watch set or
  start block, so manifest synchronization records no
  [manifest-authority marker](glossary.md#manifest-authority-marker) and no
  historical ingest fetch is needed.
- Apply two schema-migrations in step 4,
  `20261003120000_normalized_events_record_id_attribution_indexes.sql` and
  `20261003130000_project_lifecycle_event_namehash_index.sql`. On a large
  initialized database, prebuild their indexes concurrently first, as
  [history record attribution indexes](#history-record-attribution-indexes) and
  [released registrar children](#released-registrar-children) describe; without
  the prebuild, the plain builds block Interpret's and Project's writes until
  they commit. The four hash-rotating builds add no schema-migration.
- From a build before v0.3.0, the deploy also carries v0.3.0's requirements
  and every earlier section's since that build: their schema-migrations and
  index prebuilds, including the
  [retired resolver alias path](#retired-resolver-alias-path)'s refusal to drop
  a table that still has rows; the manifest-authority markers that the
  [Sepolia ENSv2 redeploy of 2026-10-01](#sepolia-ensv2-redeploy-of-2026-10-01),
  [resolver implementation start blocks](#resolver-implementation-start-blocks)
  and [default reverse names](#default-reverse-names) record; and the required
  Ingest redos the redeploy and default reverse names stamp. Complete every
  stamped Ingest redo first, sized from the ranges `chain_phase_state` records
  after the first start. One full-history Interpret redo with
  `--attest-watch-set-coverage` under the v0.4.0 binary, and the Project redo it
  installs, then discharge every rotation and marker in between.

### Walk index set

The build that adds [`ops/walk-index-set`](../ops/walk-index-set/README.md) (TYR-209, see
[walk index set](storage.md#walk-index-set)) adds two operator scripts, documentation and
tests. It changes no file the [interpreter content hash](glossary.md#interpreter-content-hash)
covers, so the hash does not rotate, and it adds no schema-migration, environment setting or
runner behavior, so it needs no redo and no historical ingest fetch. Deploying it changes
nothing until an operator runs the scripts.

The scripts are an optional step for a from-zero walk or a full-history Interpret redo:
`drop.sql` drops the 37 `normalized_events` indexes Interpret does not read, so Interpret
maintains 17 indexes on the table instead of 54, and `install.sql` rebuilds them concurrently with their
reviewed definitions and analyzes the table before Project runs. `drop.sql` refuses while any
chain on the database may be served. Rebuilding takes a pass over the table per index; on a
large database, schedule it before Project starts, as the
[production runbook](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
describes. Without the rebuild no row changes, but Project reads more slowly and an API read
that needs a dropped index may exceed `BIGNAME_API_DB_STATEMENT_TIMEOUT_MS`.

### Registry pointing a label at itself

The build that treats a discovery pointer at its own emitter, such as a
registry pointing one of its own labels at itself as that label's subregistry,
as a pointer with no target (TYR-222, see
[discovery admission](manifests.md#discovery-admission)) changes `crates/adapters/src`, so it
rotates the [interpreter content hash](glossary.md#interpreter-content-hash)
for every chain. Before, such a `SubregistryUpdated` from a manifest-declared
registry stopped Interpret with
`SubregistryUpdated produced a non-announcement self-edge of kind subregistry`.
The 2026-10-01 Sepolia `ETHRegistry` emitted one at block 11840453 (transaction
`0xea03502e4a0eaa4a65c2021bb5d9f77bfb531c4568805e09054454c34607454e`, log 122,
pinned in the
[interpreter fixture](../crates/adapters/tests/fixtures/interpreters/v2-registry-self-subregistry.json))
and again at block 11840461 (transaction
`0xae61dbac6716e749f0d6a2f3560adc2aaa7b7a81d3ba5288809c3737b4a3793e`, log 90,
pinned in the same fixture), so every
build that admits that deployment, including v0.4.0, stops Sepolia there. Now
the pointer closes the label's previous `subregistry` edge and opens none, for
a manifest-declared or a discovery-admitted registry alike, and Interpret
continues. A name below that label walks back into the parent registry and
reads the parent's own entries (`x.label.eth` reads `x`'s entry), so its
subnames alias the parent's children rather than living under a registry of
their own; the self-pointer gives the label no canonical registry and no new
canonical suffix, and bigname models no alias subtree through it. A
discovery-admitted registry's self-pointer used to add an
operator diagnostic row in `interpret_decode_skips`; this build writes none. A
`resolver` or `proxy_implementation` pointer at its own emitter, which also
stopped Interpret for a manifest-declared emitter, now closes the previous edge
and opens none in the same way, with a logged warning and no diagnostic row.
Rows that earlier builds wrote stay, because the table is append-only and keyed
by the content hash. Normalized events do not change: the `SubregistryChanged`
event is written as before. It adds
no schema-migration, table, index, manifest or setting, so stamp no Ingest
redo.

### v0.4.1 rollout

v0.4.1 is v0.4.0 plus the
[registry pointing a label at itself](#registry-pointing-a-label-at-itself)
build. Deploy it with the
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).
From v0.4.0:

- The [interpreter content hash](glossary.md#interpreter-content-hash) rotates
  once for every chain. Run one full-history Interpret redo and the Project redo
  it installs under the v0.4.1 binary before the API serves, and record the new
  hash in the release record. A bounded redo range cannot adopt a new hash. The
  [walk index set](#walk-index-set) scripts are an optional step of that redo,
  as in v0.4.0.
- Stamp no Ingest redo and apply no schema-migration.
- A Sepolia database that stopped at block 11840453 under v0.4.0 continues
  past it under v0.4.1, after that redo.
- From a build before v0.4.0, the deploy also carries the
  [v0.4.0 rollout](#v040-rollout)'s requirements. The one full-history
  Interpret redo and its Project redo under v0.4.1 discharge both rotations.

### Manifest sync index

The build that indexes the manifest sync's startup read (TYR-220, see
[walk index set](storage.md#walk-index-set)) changes the normalized-events baseline, one
schema-migration, the walk index set lists and their checks, all outside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash does not rotate
and it needs no redo, no manifest or environment change and no historical ingest fetch. At every
start the phase runner's manifest sync reads the latest `SourceManifestUpdated` event of each
manifest. Without an index on `(source_manifest_id, event_kind)`, PostgreSQL walks the primary
key backward from the newest event, and because those events were written near the start of the
walk, each probe reads most of `normalized_events`. On a mainnet-size table (about 109 million
rows) a runner start spent over an hour in that read before doing any work; with the index it
finishes in seconds. Sepolia-size tables pay seconds without it.

`20261004120000_normalized_events_manifest_idx.sql` adds `normalized_events_manifest_idx` on
`normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)`, partial on
`source_manifest_id IS NOT NULL`, the name and definition the retired public-schema baseline
gave it. It joins the [walk index set](glossary.md#walk-index-set), so `ops/walk-index-set/drop.sql`
keeps it. The schema-migration is a plain `CREATE INDEX` that scans all of `normalized_events`
while holding a SHARE lock on it until the schema-migration commits, which blocks Interpret's
writes. On a mainnet-size database the prebuild is required: build it concurrently first,
outside a transaction, while the phase runner and API keep running (on the mainnet table above
the concurrent build took about four minutes and the index is about 6 GB):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_manifest_idx
    ON bigname_phase.normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
    WHERE source_manifest_id IS NOT NULL;
```

Then apply the schema-migrations with `--target-version 20261004120000` and the same
`lock_timeout`, `statement_timeout` and retry procedure; it finds the index and skips the build.
A database that already carries this index under this name and definition, for example one
prebuilt before this build, converges the same way. On a small database, such as a Sepolia one,
the plain build may instead run with the phase runner and redo processes stopped.
`CREATE INDEX IF NOT EXISTS` matches the name only, so the schema-migration then checks that the
name is an index on `normalized_events` that is `indisvalid` and `indisready` with the reviewed
`pg_get_indexdef`, and fails without recording itself otherwise. To recover, drop the named
relation (an interrupted concurrent build leaves an invalid index: confirm in
`pg_stat_progress_create_index` that no build is still running, then `DROP INDEX CONCURRENTLY`
it), rebuild it with the statement above and apply the schema-migrations again. A runner started
before the index exists behaves the same, only slower at start. API standbys receive the index
through replication.

### Registry root role changes in history

The build that serves ENSv2 registry root role changes (`RootPermissionChanged`) as `permission`
history rows (see [permission change values](api-v1-routes.md#permission-change-values))
changes the API, `crates/storage/src/history`, the normalized-events baseline, one
schema-migration, the address-history and walk index set scripts and their checks, all outside
the [interpreter content hash](glossary.md#interpreter-content-hash), so the hash does not rotate
and it needs no redo, no manifest or environment change and no historical ingest fetch. Stored
rows do not change; the API starts serving rows it already had. `GET /v1/events`, a registry's
`contract_address` history and its overview's `counts.events` gain the registry's root role
changes, and address history in `both` or `registration` scope with the `role_holder` relation
gains the address's own. Only the Sepolia manifests admit ENSv2 sources today, so responses
change only there.

`20261005120000_normalized_events_address_root_permission_idx.sql` adds
`normalized_events_address_root_permission_idx`, keyed by the lowercased subject, then the block
and log position, and partial on activated, readable `RootPermissionChanged` rows. Address
history needs it: without it every address history page and count that includes root role
changes (`both` or `registration` scope with the `role_holder` relation) scans
`normalized_events`. It
joins the walk index set's drop list. The index holds one entry per retained root role change and
none on a chain without ENSv2 sources, so its size, and the disk the build needs, follow the
number of root role changes rather than the size of `normalized_events`; check that count first
(`SELECT count(*) FROM bigname_phase.normalized_events WHERE event_kind = 'RootPermissionChanged'`,
which itself scans the table). The plain build in the schema-migration costs one scan of
`normalized_events` under a SHARE lock, which blocks Interpret's writes for that scan. The
concurrent build costs two scans plus waits for transactions open at each phase, without
blocking writes, so on a large
initialized database rerun [`ops/address-history-indexes/install.sql`](../ops/address-history-indexes/README.md)
first, outside a transaction, while the phase runner and API keep running. It finds the three
existing address-history indexes and builds only this one, concurrently, then checks all four.
Then apply the schema-migrations with `--target-version 20261005120000` and the same
`lock_timeout`, `statement_timeout` and retry procedure; it finds the index and skips the build.
On a small database the plain build may instead run with the phase runner and redo processes
stopped. The schema-migration checks that the name is an index on `normalized_events` that is
`indisvalid` and `indisready` with the reviewed `pg_get_indexdef`, and fails without recording
itself otherwise; the runbook's recovery applies. Start the new API only after the
schema-migration has applied. API standbys receive the index through replication.

### Name surfaces without raw label bytes

The build that lets a [name surface](glossary.md#surface-name-surface) exist before the raw
bytes of its labels are known changes the identity baseline, one schema-migration, the adapter
surface model and Interpret's surface writer, redo re-anchoring and flag recompute
([storage](storage.md#name-identity-and-raw-evidence)). Those sources are inside the
[interpreter content hash](glossary.md#interpreter-content-hash), so the hash rotates for
every chain. No adapter produces a surface without raw bytes yet: a re-derivation under this
build writes the same surfaces, bindings and normalized events as before, and each surface
additionally names its [preimage witness](glossary.md#preimage-witness).

`20261005130000_name_surfaces_optional_raw_evidence.sql` drops `NOT NULL` from
`name_surfaces.raw_name`, `raw_labels` and `dns_encoded_name`, adds the nullable
`preimage_event_identity` column, replaces the label-count check with
`name_surfaces_raw_evidence_check`, and fills the new column for each existing row that has
a canonical `PreimageObserved` event on a canonical block, from the earliest one. A row with
no such event keeps a NULL witness; the full-range Interpret redo below fills it if the
replay observes the name's bytes. No existing row loses a value. The
schema-migration takes an ACCESS EXCLUSIVE lock on `name_surfaces` and holds it while the
backfill and the new check's validation of every row run. The backfill is one statement: it
reads every canonical `PreimageObserved` event with a name, joins each to its
`chain_lineage` row, keeps the earliest per chain and name, and joins that result to
`name_surfaces`. Its cost follows the amount of preimage history as well as the number of
surfaces, and the plan PostgreSQL chooses has not been measured at production size; time it
on a copy of the database first. Apply it with
the phase runner, redo processes and API stopped, with the same `lock_timeout`,
`statement_timeout` and retry procedure as the other schema-migrations and `--target-version
20261005130000`; it is not a concurrent step.
A binary from before this build can still read and write the migrated table, because it
writes all three raw columns on every row.

After the schema-migration, an existing deployment finishes the full-range Interpret redo and
the stamped Project redo the rotation installs before the matching API serves, as the
[handoff](#phase-runner-configuration) describes. This build targets the next hash-rotating
release; when it ships with other rotating changes, one redo pair discharges them all.

### Expiry selector on the name summary

`20261005150000_project_name_summary_expiry_selector.sql` adds two columns to the
[name summary](glossary.md#name-summary), `project_name_summary.expiry_listable` and
`project_name_summary.public_authority`, and two partial indexes over them,
`project_name_summary_expiry_idx` on `(namespace, expires_at, logical_name_id, chain_id)` and
`project_name_summary_authority_expiry_idx` on
`(namespace, public_authority, expires_at, logical_name_id, chain_id)`, both
`WHERE expiry_listable AND expires_at IS NOT NULL`. The family step fills the columns from the
same composition as the rest of the summary.
[`GET /v1/names`](api-v1-routes.md#get-v1names) selects each page's names by them (see
[Expiry listing reads the selector](#expiry-listing-reads-the-selector)).

The composition that fills the columns lives in hashed storage sources
(`crates/storage/src/families`), so this build rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) and needs a full re-derivation.
Stop the phase runner, apply the schema-migration, then start the new build. On a database
without the columns the schema-migration resets every owned key family with the
[family marker](glossary.md#family-marker), undo journal and repair records, exactly as
[the owner column's schema-migration](#registry-label-owner-filters) does and under the same
`EXCLUSIVE` lock on the marker table, held to commit. It is a blocking maintenance
schema-migration: in one transaction it waits for the locks it needs behind any transaction
already holding them, deletes every family row, alters the summary table and builds both
indexes (on the emptied summary), so how long it runs depends on those transactions and on the
volume of family data. Run it in a maintenance window. The next family run
rebuilds the families and writes every selector; fenced routes answer `409 stale` until it
finishes. The reset adds no second rebuild, because the rotated hash rebuilds the families
anyway. Do not run the previous build against the migrated schema: its family writer inserts
summary rows by column name and fails on the new `NOT NULL` column. The reverse order does
not fail: the new build's writer run against a schema without the columns drops the two values
it has no column for and publishes summaries with no selector, so apply the schema-migration
before the new build ever starts. API requests that read the
name summary or lock the marker wait for the schema-migration, up to their timeouts.

### Expiry listing reads the selector

The build that makes [`GET /v1/names`](api-v1-routes.md#get-v1names) select its names from the
[name summary](glossary.md#name-summary)'s expiry selector changes reader sources only
(`crates/storage/src/families/name/list.rs`, `list/expiring.rs` and
`crates/storage/src/name_current`), all outside the
[interpreter content hash](glossary.md#interpreter-content-hash): it does not rotate the hash
itself and needs no redo, manifest or environment change. It depends on the selector columns
of [the previous entry](#expiry-selector-on-the-name-summary), so it ships in the same
release, after that entry's schema-migration and the re-derivation it requires. The route's
contract does not change: the same rows, order, cursors and errors. A page composes at most
`page_size + 1` names.

`20261005160000_project_families_drop_expiry_walk_indexes.sql` drops
`project_lifecycle_event_expiry_idx`, `project_lifecycle_event_inexact_expiry_idx` and
`project_wrapper_state_expiry_idx`, which only the previous reader's event walk read. No other
statement, the [walk index set](../ops/walk-index-set/README.md) and no index installer under
`ops/` names them; the `expiry_seconds` columns stay. It also rewords the comment on
`project_name_summary.public_authority`. Each `DROP INDEX` takes a brief `ACCESS EXCLUSIVE`
lock on `project_lifecycle_event` or `project_wrapper_state` and waits behind transactions
that hold the table, so apply it with the selector schema-migration while the phase runner is
stopped, with the same `lock_timeout`, `statement_timeout` and retry procedure as the other
schema-migrations. Rollout order: stop the phase runner, apply both schema-migrations, start
the new phase runner and the new API. The new API must not start before
`20261005150000` has applied: its listing reads the selector columns and fails without them.
After the schema-migrations and until the family rebuild publishes, the listing answers
`409 stale`, as every fenced route does. An API from before this build still answers the
listing after the indexes are dropped, by scanning the two tables, so it is slower there and
nowhere else; replace it rather than leave it running.


### ENSv1 registry node identity production

The producer now establishes a name identity from an admitted ENSv1 `NewOwner`
whose complete labelhash path is proven from the root or a directly witnessed
ancestor. This changes Interpret output and Project's named inputs. Rollout
requires a full Interpret re-derivation followed by full Project re-derivation
under the new interpreter content hash; a bounded replay of recent owner changes
cannot recover all historical ancestor paths or repair all older same-block
preimage witnesses. Keep the preceding compatible publication until both phases
finish and ordinary publication admission accepts the new generation.

Apply `20261005190000_name_surfaces_byte_shadow_path.sql` with the normal schema
upgrade. It narrowly permits a shadow with empty decoded text, a nonempty full
hash path and a nonempty byte witness. It neither backfills a path nor relaxes
active raw-backed or unknown-byte bundles. Full Interpret re-derivation repairs
legacy paths from the actual bytes. Witness repair uses the earliest surviving
byte observation's plain block timestamp and orders event identities separately,
including replacement within one block.

Admitted registry histories must begin at their declared deployment bounds.
Missing ancestry is not synthesized from imports, arbitrary owner/resolver logs,
or guessed `.eth` suffixes. A deployment that omits the ancestor's actual path
must restore that intake coverage before claiming complete node production.
The shared reader policies for imports, bracketed exact inputs, search fragments,
parent filters and between-page spelling changes remain as documented in
[API v1](api-v1.md). Capacity and full rebuild measurements remain separate
release gates; the schema upgrade alone does not satisfy them.

### Hydration only at the head

The build that makes [hydration](glossary.md#hydration) run only on the head
block changes `crates/project/src`, so it rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) for every
chain. It carries one schema-migration,
`20261005180000_project_hydration_schedule.sql`, and no manifest change,
watch-plan change or environment variable; no API response shape changes.

The schema-migration adds hydration's scheduling columns: `attempt_limit` and
`attempt_failures` on `project_reverse_tuple`, `hydration_limit` and
`hydration_failures` on `project_node_record_value`, and a copy of each
failure count on the two hydration work indexes. They are nullable and added
without a default, so no row is rewritten and no family is reset; on an empty
schema-migration database it is a no-op and `phase-runner init-schema`
installs the same columns. It takes the family marker table in `EXCLUSIVE`
mode until it commits. Apply it with the runner stopped and before the new
binary starts: hydration selection now reads these columns directly and fails
if they are absent. The generic family writer also cannot persist scheduling
fields missing from the schema. A previous binary on the new schema leaves
the columns null, which reads as no limit.

Behavior changes on `ethereum-mainnet`, the only hydrated chain
([follow-only hydration](projections.md#follow-only-hydration)):

- Project no longer calls the hydration endpoint for blocks it applies while
  catching up. Before this build every such block read up to 250 text
  selectors and up to 250 reverse tuples of the rolling refresh, plus the
  tuples it changed, at its own block hash; against an endpoint without that
  block's state each read failed, cleared the reverse name it was refreshing
  and recorded a failed attempt on the text selector.
- An RPC batch that fails as a whole no longer clears hydrated reverse names.
  A served primary name can therefore be the last one successfully observed
  rather than absent while the endpoint fails. Against an endpoint that does
  not serve the block nothing is written at all. Against one that answers
  other calls at the block, the failed batch is split, within a call and time
  limit per block, and what is still unread records only where to resume. A
  call that fails inside an answered aggregate still clears its value.
- Old waiting work receives 63 of the 250 text selection slots and a rounded-up
  quarter of each kind's call budget, before new arrivals can use that time.
  Unused shares stay available. A failed child response waits 7,200 blocks
  before retry; fresh selector evidence clears obsolete scheduling state.
  Outer failures keep the separate non-observation and split policy above.
- Seven hydration metrics and two `warn` log lines are new
  ([Project family work](runbooks/pipeline-monitoring.md#project-family-work)).
  The hydration HTTP client now has a 5-second connect and 10-second total
  timeout.

Adoption and restart plan for an existing deployment, following the
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary):

1. Stop the supervised phase runner. Do not run a binary of the previous hash
   against the database again once the next step has started, and do not
   alternate the two: a rebuild left part-way by one hash is not resumed by the
   other.
2. Apply the release's schema-migrations, `20261005180000` among them.
3. Under the new binary, run the full-history Interpret redo and then the
   Project redo it installs, to completion, as for any rotation. A bounded
   range cannot adopt a new hash. Neither redo needs
   `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`: replay and rebuild make no
   hydration call.
4. Start the supervised phase runner with the hydration URL configured, and
   the matching API once the family publication is live.

The rebuild starts every hydrated value from the event-derived baseline: text
overlays are empty and event-silent reverse names are absent, including on the
block the rebuild ends on, which is not hydrated. Values return as new blocks
arrive after the restart, 250 text selectors per head block at most, with the
reverse tuples in rotation beside them. How long that takes depends on the
head blocks that arrive and on the endpoint, so it is not a fixed time; watch
`phase_runner_project_hydration_selectors_total`. A release that rotates the
hash for another change discharges this rotation with the same redo pair.

### WrapperRegistry permission reader upgrade

The reader additionally needs `SELECT` on
`bigname_phase.project_ens_v2_registry_parent`. Before restarting an existing
API role, apply the grant below on the primary and allow it to replay on any
serving standby. Fresh role setup above already includes it. Startup preflight
refuses an API role without the grant.

```sql
GRANT SELECT ON bigname_phase.project_ens_v2_registry_parent TO bigname_api;
```

Schema-migration `20261005200000_registry_permission_history_indexes.sql`
installs four read-only normalized-event indexes for registry origin, ordinary
announcement and upgrade-disqualifier probes, plus the parent root-grant lookup index on
`project_grant`. Each populated target table requires its corresponding indexes.
Before applying the migration on an initialized database, run
[`ops/registry-permission-indexes/install.sql`](../ops/registry-permission-indexes/install.sql)
outside a transaction. It builds missing indexes concurrently and validates the
whole set; follow its [ordered prebuild, headroom and recovery instructions](../ops/registry-permission-indexes/README.md).
The schema-migration validates every index against its actual target relation,
refuses a populated target with a missing index or any invalid definition,
then adopts a complete set without rebuilding or changing its OIDs. Empty
schemas build directly. The independent
factory-origin retention and UserRegistry implementation metadata ship in the
held content-hash rotation and require its normal full Interpret/Project
rebuild. The existing compiler also adds the migration family’s two topics at
the UserRegistry implementation address from block `11820439`; complete the
required Ingest repair first. An earlier release-wide repair boundary already
covers this start. See the [measured watch-plan change](manifests.md).
Metadata and retained origins do not replace ordinary registry
announcement admission or introduce pre-initialization approval capture.

### Wrapper expiry in the bounded names listing

The integrated expiry-selector reader attaches the stored wrapper expiry before
serving scalar or multi-window `/v1/names` pages. It uses the existing batched
reader on the page's snapshot, without loading resolution topology or expanding
the `page_size + 1` composition bound. This preserves the `ens_v1.wrapper_expires_at`
contract for backed, lapsed and unwrapped entries. The correction changes reader
sources only and does not rotate the interpreter content hash itself, add a
schema-migration or require an additional redo. It ships within the release's
existing hash rotation and schema-migration sequence described above.

### ENSv1 intermediary migration and retired registrar renewal

The migration adapter now selects the exact terminal controller-entry and
cleanup transfers while preserving earlier ordinary transfers in the same
transaction. The renewal adapter applies the existing same-lease retirement rule before
creating a new ENSv1 binding from later name readability. Actual renewal and
expiry observations remain available. This changes `crates/adapters/src` and
rotates the [interpreter content hash](glossary.md#interpreter-content-hash) for
every chain, without changing the schema, manifests or watch coverage. Deploy
matching runner and API binaries and finish the full-history Interpret redo
and the Project redo it installs before serving the new generation, following
the [planned migration and fingerprint
boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).
One redo pair under the final combined release covers this correction and
other changes in that release; an earlier hash does not. Preserve raw facts
and verify that intermediary migrations publish their current ENSv2
registration with no active ENSv1 predecessor, including after a later ENSv1
renewal of that retired lease.

### Durable search derivation rollout

The durable search schema requires a full Interpret re-derivation under the
new compiled fingerprint followed by a Project rebuild. Install the schema
first, keep API readiness fenced during re-derivation, and admit the candidate
only after both phases publish complete matching state. The migration does not
backfill a plausible search payload from old summaries or mark an empty lexical
index ready. The shared field shaper, token writer, identity hooks and Project
writer participate in the interpreter fingerprint; fresh build outputs must
prove the rotation and unchanged deployment-profile hashes.

After applying `20261005210000_durable_name_search.sql`, grant the existing
API role read access to the new lexical tables before starting the new API:

```sql
GRANT SELECT ON TABLE
    bigname_phase.name_search_documents,
    bigname_phase.name_search_postings
TO bigname_api;
```

New API roles receive these privileges in the provisioning block above. Startup
preflight refuses to serve when either table is unreadable.

### Retained ENSv1 descendant resolution paths

The shared composition now follows the current ENSv2 path before serving retained
ENSv1 descendant records. The composition, its registry/mirror/identity helpers
and Project's affected-name selection are interpreter fingerprint inputs. This
rotates the compiled hash for every chain and requires a full-history Interpret
redo followed by the Project redo it installs under matching runner/API binaries.
Use the [planned fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
and keep existing readiness fences intact. No schema-migration, watch-plan,
manifest/admission or raw-intake change is introduced by this correction. Do not
mark old rows ready under the new hash or treat a recent parent replay as a full
re-derivation. Validate retained mirror/direct paths, disconnected descendants,
wrapper rebinding/unwrap eligibility, and no-child-write expiry after publication.

### Search refresh pages by cursor

Interpret, the normalization-flag recompute and the label-preimage import
refresh the search documents of the names a write affects. Each page statement
of that refresh used to carry the whole set of affected names. A redo from a
chain's first block affects every name on the chain, so on a chain with millions
of names its first batch could not finish. The refresh now writes the affected
names once into a temporary table, and each page statement carries only a
cursor.

The stored documents and postings are the same, row for row. No schema-migration,
manifest or watch-plan change is included. The change edits
`crates/storage/src/identity_search.rs` and
`crates/storage/src/identity_search/documents.rs`. Both are inputs of the
[interpreter content hash](glossary.md#interpreter-content-hash), so the compiled
hash rotates for every chain. A database derived under the previous hash needs
the full-history Interpret redo and the Project redo it installs before the new
binaries serve it. Follow the [planned fingerprint
boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).

The temporary table belongs to the database connection. The first refresh on a
connection creates it, PostgreSQL empties it at each commit, and it goes away
when the connection closes. The database role of each of those writers must be
able to create temporary tables, which PostgreSQL allows every role by default.
A refresh over N names writes N rows of temporary table and index on the
database host until its transaction ends. Budget about 300 bytes of disk per
affected name for a full-history redo.

## Published lookup state

Apply `20261007120000_project_lookup_precomputation.sql` before deploying the
matching writer and API. It adds five Project tables and their eight indexes,
checks preexisting definitions, and leaves the old publication intact. Fresh
baseline and upgrade definitions match. Apply the explicit API SELECT grants
above for name, relation, inventory and record tables; the API does not read the
dependency table. Startup checks require those four tables to be readable.

The shared producer now persists lookup name and inventory composition, so the
interpreter content hash changes. Run the normal full retained-history Interpret
rederivation followed by the full Project rebuild under the new binary. Creating
the tables, copying reader results, or changing marker hashes is not adoption.
The API refuses stored lookup while the marker or Interpret/Project input hashes
belong to the old epoch, including the manifest-sync interval before redo starts.
It admits the new state only after the normal runner publishes a complete live
family generation. Cross-chain execution declaration changes retain the ordinary
phase dependency invalidation. No new raw intake is required.

Budget rebuild time, WAL, journal space and the current name/resource/key/relation/
dependency cardinalities before rollout. The factored representation bounds
unchanged-value writes; it still adds rows, indexes and genuine dependency fanout.
Use the matched footprint evidence for the release, rather than treating the old
frozen lookup experiment as its production storage or throughput estimate.

For rollback, stop the changed writer, restore the intended prior binary and
perform its normal compatible Interpret/Project epoch adoption and full rebuild
before admitting its API. Additive tables may remain, but their existence does
not make an older binary compatible with a newer publication hash. Preserve the
ordinary retained-undo/reorg procedures; the lookup families share that journal,
reset and repair authority and have no independent cache generation.

## Registration lifecycle status

Apply `20261008120000_registration_lifecycle.sql` before deploying the matching
phase runner and API. It adds `project_name_summary.grace_ends_at`, two partial
indexes on it, and replaces the search-payload check constraint so the payload
carries `status`. Fresh baseline and upgrade definitions match.

When the schema-migration finds the earlier shape, it also deletes every row of
the Project tables in place, including the family publication markers. It does
this under an exclusive lock on the marker table, in the migration transaction.
Raw facts and Interpret's tables are untouched. The API refuses name reads until
Project publishes again. Adopt the release by a full rebuild, or by promoting a
database that was already rebuilt under the new binary. Do not apply it to a
serving database and expect the old publication to keep answering. Budget the
delete's WAL and lock time on a large database.

The [interpreter content hash](glossary.md#interpreter-content-hash) rotates.
NameWrapper mints are interpreted at the `TransferSingle` position, and the
shared lifecycle composition under
`crates/storage/src/families/control/lifecycle` changed. Every chain needs a
full-history Interpret redo, then the Project redo it installs, at a planned
[re-derivation boundary](glossary.md#re-derivation-boundary) under matching
runner and API binaries. Do not mark old rows ready under the new hash.

The manifest-authority fingerprint changes on Ethereum Mainnet and Ethereum
Sepolia. Both `ens_v1_wrapper_l1` manifests now declare the normalized events a
NameWrapper `TransferSingle` mint produces. Synchronization records a
[manifest-authority marker](glossary.md#manifest-authority-marker) on those two
chains, so each needs the token-attested full-range Interpret redo and the
downstream stamped Project redo described under [phase-runner
configuration](#phase-runner-configuration). The one redo per chain discharges
both the hash rotation and the marker. The compiled watch plan is unchanged, so
no Ingest redo is needed. The Ethereum Mainnet `basenames_execution` authority
is unchanged, so Base needs no Project redo for that reason. Base still follows
the hash rotation above.

Registrar grace for an ENSv2 `.eth` name is keyed to the one admitted Sepolia
ETHRegistry deployment. A later deployment gets no registrar grace until the
policy in `crates/storage/src/families/control/lifecycle/policy.rs` names it.
