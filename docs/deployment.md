# Deployment

The old `bigname-indexer` runtime has been deleted. The phase runner implements
`ingest`, `interpret`, `project`, read-only `verify`, and continuous `live`
follow, and the first Stage C slice moves v2 verified execution onto its lookup
engine. The public edge flip and deletion of the retained v1 plane remain
outstanding, so production must remain on the last pre-cut release until those
gates land. Commands and environment variables for the old
indexer, backfill scheduler, reconciliation replay, and repair tools are no
longer supported by this source tree.

## Container contents

The image contains these runnable binaries:

- `bigname-api`
- `phase-runner`
- `bigname-worker`

The entrypoint selectors are:

```sh
docker run --rm ghcr.io/ensdomains/bigname:latest api
docker run --rm ghcr.io/ensdomains/bigname:latest phases-migrate
docker run --rm ghcr.io/ensdomains/bigname:latest phases
docker run --rm ghcr.io/ensdomains/bigname:latest worker
docker run --rm ghcr.io/ensdomains/bigname:latest migrate
```

`migrate` applies the retained migration history consumed by API and worker; it
prepares `public` but not the phase namespace. The one-time `phases-migrate`
command invokes `phase-runner init-schema` against the same database and
requires an empty `bigname_phase` schema. It refuses every nonempty phase schema
until a reviewed upgrade or rebuild mechanism exists. `phases` then invokes
`phase-runner run` with `bigname_phase` as its search path. It can
persist ingest-through-project output and continuously follow provider heads,
including reorg-driven downstream redo and canonical-head hydration. Its
read-only verification phase compares Base's Coinbase-loaded range with dRPC
through the `48,428,000` ingest seam and compares Ethereum with local reth only
through the finalized head. The v2 verified name, record, and ENS/60
primary-name paths consume its schema-v2 projection output through the lookup
engine. Retained v1 handlers and the worker continue to use `public`.

## Server Compose during Stage B

`docker-compose.server.yml` starts PostgreSQL, migrations, the surviving API,
and the surviving worker. It intentionally has no indexer or phase-runner
service. The stack can exercise existing read models and worker behavior, but
without the Stage C API cutover it is not the replacement fresh indexing
deployment.

```sh
cp .env.server.example .env.server
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

Before the full `up`, use the writer URL to apply `migrate` and
`phases-migrate`, then provision the non-owner `bigname_api` login below. Set
`BIGNAME_API_DATABASE_URL` to that login; Compose deliberately does not fall
back to `BIGNAME_DATABASE_URL` for the API. Migrations, the worker, and explicit
phase-runner commands continue to use the writer URL.

The API binds to the configured `BIGNAME_API_HOST` and
`BIGNAME_API_PORT`; `/healthz` remains its local readiness endpoint. The API
and worker configuration that still applies is documented in
[`production.md`](production.md) and [`development.md`](development.md).

## Phase-runner configuration

The implemented phases use:

- `BIGNAME_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT`
- `BIGNAME_PHASE_RUNNER_CHAINS`
- `BIGNAME_PHASE_RUNNER_SOURCES`
- `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`
- `BIGNAME_PHASE_RUNNER_INSTANCE_ID`

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
migration authority. Reapply and revalidate the SELECT grant after every
approved phase-schema rebuild. Do not reuse the writer credential in the
verification URL: setting a writer session's default transaction to read-only
does not remove that role's write authority, and startup rejects it.

Each `BIGNAME_PHASE_RUNNER_SOURCES` entry has the form
`CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV`; the named environment variable
contains the provider URL. Capacity, retry, and polling controls use the
`BIGNAME_PHASE_RUNNER_*` names exposed by `phase-runner --help`.
For production verification, `base-mainnet` must configure its independent RPC
source with kind `drpc`; that source can record only `cross_checked`, and its
start block and independent verification extent are fixed at the block
`48,428,000` Coinbase-to-dRPC ingest seam. A moved source start or verify redo
above that seam is rejected before redo state is created.
`ethereum-mainnet` must configure one `reth_db` source; that source records
`node_checked`. A generic RPC kind is not accepted as Base verification
authority because it does not identify the ratified independent provider.
Each completed verification batch logs its actual dRPC request count, including
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
`ethereum-mainnet`), `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` must contain a
`CHAIN=HTTP_URL` entry. A missing entry is a fatal project-phase configuration
error. The check runs before event-derived project publication or hydration
writes, so previously hydrated values remain intact while the chain is stopped
for configuration repair.

One-shot finite phase work is available through `phase-runner redo` for
`ingest`, `interpret`, `project`, `verify`, and `recompute-flags`.
`--phase all` runs ingest through verify for each selected chain, and
`--all-chains` discovers active manifest chains before dispatching the same
per-chain path. A chain failure stops its remaining phases but does not prevent
later selected chains from running; the command still exits nonzero with the
collected failures. Interpret's effective replay range is handed to Project
through the downstream redo stamp. `--phase all` refuses a chain with any
already-pending redo rather than absorbing that work. If one of its phases
fails, the error gives the phase-specific command that must complete the
durable marker before the operator reruns `--phase all`. Verify redo checks its source
and SELECT-only database configuration before phase initialization, locking,
or redo-state publication.
It rechecks only a range inside the recorded verification extent: the range
end cannot exceed the current verify cursor. Each batch is additionally
constrained to finalized lineage. Blocks above the verify cursor are covered
by normal verification resume, never by redo.
Completion restores the pre-redo normal extent; a partial redo retains its
level, while a redo covering the full retained extent can report the level
fixed by its source kind. An interrupted attempt keeps the normal resumable
redo marker and must be rerun with the same range.
Historical `live` redo is rejected because live follows only the current head.
`recompute-flags` recalculates label and name-surface normalization metadata
under the current normalizer and refreshes the scoped primary-name projection.
Names that remain active or remain shadow complete without replay. Names that
cross between active and shadow are reported and merged into the ordinary
Interpret and Project redo markers; only that replay path may create or retract
their bindings. After a shadow-to-active recompute commits, the surface has
active visibility while bindings and projections remain at their pre-transition
class. The API serves that conservative pre-transition projection state, and
the stamped markers block normal Interpret work. Run the stamped redo to make
transitions visible; until then, affected names serve their pre-transition
state. On completion the command writes one JSON object to standard output with
the same-class and transition counts plus every stamped phase range; this report
does not depend on `RUST_LOG`. An interrupted recompute resumes from its durable
marker; the
scoped Project refresh marker created by the command is likewise distinguishable
and resumable. A completed scoped refresh stays marked as "Interpret flags
pending" until Interpret completion clears or replaces it atomically, so a
restart in that handoff resumes the same command without repeating Project. An
unrelated ordinary Project redo that was already pending is widened or
preserved, never completed by the recompute session. This split
deliberately narrows the simplification plan's
bare statement that the mode runs without replay: shadow names suppress
bindings, so a class transition requires normal binding derivation or
retraction rather than a direct flag write. Project redo,
`recompute-flags`, `--phase all`, and an interpret-to-project cascade use
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` (or
`--hydration-rpc CHAIN=HTTP_URL`) for the same current-head enrichment as the
supervised project phase. `phase-runner rewind` moves the
published latest marker to an exact stored readable ancestor and uses normal
head publication to orphan the suffix, invalidate affected cache eligibility,
and stamp downstream redo.

`phase-runner inspect block-canonicality`, `stored-lineage`, and `raw-events`
provide the three read-only bounded schema-v2 operator windows. They do not
expose API routes. No drift, cache, execution-trace, or watch-plan inspection
surface is ported to the phase runner.

Before these schema-v2 operator commands are first used, run
`phase-runner init-schema` once after
the retained migrations have prepared `public`. The phase runner owns the
`bigname_phase` namespace in that database. Keeping both namespaces in one
transaction domain is required while head publication atomically marks phase
lineage orphaned and removes cache eligibility from affected retained
`public.execution_cache_outcomes`; durable traces remain in `public`.

## Verification mismatch repair

A [stored-history verification](glossary.md#stored-history-verification)
mismatch stops only the affected chain and is not retried.
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
level fixed by its source kind again. Normal verification resume then covers
the re-ingested blocks above the cursor. A mismatch in the first-ever verify
batch leaves no recorded verification extent, so no verify redo range is
expressible and a full phase-state reset is the only repair. Under the
state-preserving alternative, a failed verify redo retains its marker and is
resumed by rerunning the same redo command after repair. After a full
phase-state reset, rerun the normal pipeline instead.

## Carried-raw cutover gate

Build-plan amendment B is a separate one-time Stage D1 operation. B1 supplied
the ingest cursor model but did not run this production-data check. Before the
legacy coverage and job tables are retired, the cutover must:

1. compare every carried `(address, topic, range)` raw-log set with the retired
   coverage records that describe what the old runtime fetched;
2. record and review every disagreement before copying or deleting data;
3. seed schema-v2 ingest cursors explicitly at the Base `48,428,000` seam, at
   the reviewed historical starts for the three newly watched signature sets,
   and at the observed Ethereum carry-over head; and
4. run the production verifier over the carried Base and Ethereum raw history
   before live follow is admitted.

This repository cannot truthfully mark that gate complete without the stopped
production dataset and its retired coverage rows. The steady-state B4 verifier
does not read those legacy tables or infer their dynamic cursor seeds.

## Surviving services

The API keeps a `public`-schema pool for health checks and retained
public-schema reads, plus a `bigname_phase`-schema pool for GraphQL, startup
status-chain discovery, `/v2/status`, v2 snapshot selection, verified lookup,
primary-name projection reads, and the indexed lookup/resolver projections.
GraphQL reads `name_current`, `address_names_current`, and
`record_inventory_current` through that phase pool. Startup and status read
`chain_heads`, `chain_phase_state`, `chain_lineage`, and `service_heartbeats`;
they do not read
`public.chain_checkpoints`. V2 projection routes that remain on the public pool
retain their existing successor work. V2 record lookup
may perform only the schema-v2 guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) write;
v2 primary-name lookup writes nothing. The API database role therefore needs
`USAGE` on `bigname_phase`, `SELECT` on only the schema-v2 lookup relations
enumerated below,
and `EXECUTE` on the three guarded functions below in addition to its retained
legacy grants. These fixed-`search_path`, security-definer functions are owned
by their schema owner; their installers revoke default `PUBLIC` execution.
Grant them only to the API role, and do not grant that role `CREATE` on
`bigname_phase` or `public`. In particular, the API receives no direct `INSERT`
or `UPDATE` on
`resolution_divergences` and no `UPDATE` on the guarded head, lineage, or
projection relations.

After both schemas exist, the schema owner provisions the dedicated login with
these exact retained public-schema and schema-v2 privileges (substitute
database, role, and secret through the normal secret-management path):

```sql
CREATE ROLE bigname_api
    LOGIN PASSWORD '<secret>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE bigname TO bigname_api;
GRANT USAGE ON SCHEMA public, bigname_phase TO bigname_api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bigname_api;
GRANT INSERT ON TABLE
    public.execution_traces,
    public.execution_steps,
    public.execution_cache_outcomes
TO bigname_api;
GRANT UPDATE ON TABLE public.execution_cache_outcomes TO bigname_api;
GRANT INSERT, UPDATE ON TABLE public.raw_call_snapshots TO bigname_api;
GRANT USAGE ON SEQUENCE public.raw_call_snapshots_raw_call_snapshot_id_seq
TO bigname_api;
GRANT EXECUTE ON FUNCTION public.bigname_lock_primary_name_anchor(
    text, text, text
) TO bigname_api;
GRANT SELECT ON TABLE
    bigname_phase.chain_heads,
    bigname_phase.chain_lineage,
    bigname_phase.chain_phase_state,
    bigname_phase.service_heartbeats,
    bigname_phase.name_current,
    bigname_phase.address_names_current,
    bigname_phase.resolver_current,
    bigname_phase.name_surfaces,
    bigname_phase.resources,
    bigname_phase.surface_bindings,
    bigname_phase.token_lineages,
    bigname_phase.record_inventory_current,
    bigname_phase.primary_names_current,
    bigname_phase.manifest_versions,
    bigname_phase.manifest_contract_instances
TO bigname_api;
GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) TO bigname_api;
GRANT EXECUTE ON FUNCTION bigname_phase.write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) TO bigname_api;
```

This role cannot read raw facts, normalized events, discovery state, the
divergence table, or unrelated operational tables directly. Reapply these
explicit relation and function grants after a reviewed public migration or
phase-schema replacement; do not use ownership or schema-wide write grants as
a shortcut.

### Replacing an initialized phase schema for the v2 cutover

The current installer cannot upgrade a nonempty `bigname_phase` schema. For an
existing initialized database, this cutover therefore requires an offline
replacement and full pipeline walk:

1. Build `phase-runner` and `bigname-api` from the same commit. Stop the old
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

3. Run the new binary's `phase-runner init-schema` with `--database-url
   "$BIGNAME_DATABASE_URL"`. Reapply the verification-role `USAGE`/`SELECT`
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
future schema upgrade must use a separately reviewed migration or lossless
export/import mechanism rather than this replacement procedure.

The project-at-head guard also binds the API's compiled interpreter content
hash. `bigname-api` and `phase-runner` must therefore come from the same commit.
After any interpreter-hash rotation, deploy the new phase runner and finish its
required re-walk before deploying the matching API; deploying the API first
makes all v2 snapshot-selected reads return `409 stale` until the new project
generation is published. This includes indexed reads because snapshot
selection itself requires the matching project publication before any
projection row is admitted.

Configure
`BIGNAME_API_CHAIN_RPC_URLS` for status and both verified engines as described
in the API docs. The retained and schema-v2 request pools each use
`BIGNAME_DATABASE_MAX_CONNECTIONS`; together with the reserved readiness
connection, one API process can open at most
`2 * BIGNAME_DATABASE_MAX_CONNECTIONS + 1` PostgreSQL connections.

The worker continues to own projection rebuild/apply, hydration, verified
execution, pruning, and read-only inspection. Its continued use of projection
tables, execution artifacts, service heartbeats, historical backfill-job reads,
and normalized replay cursor reads is the reason those storage paths remain in
Stage B. They are deferred to the worker/API port; this PR does not change their
behavior.

## Removed operational surfaces

This source tree has no command for:

- old-indexer startup, live polling, or head-following
- persisted `backfill_*` job creation, leasing, advancement, or repair
- normalized-event catch-up, adapter startup synchronization, supersession, or
  coverage recovery
- the Base drop-and-rederive correction
- resolver-profile reconciliation or authority-journal draining
- old raw-code and name-normalization indexer repair commands

The corresponding SQL migrations remain immutable history. Existing rows are
not current readiness or replay authority. Where a surviving worker inspection
command still reads one of those tables, the read is historical and does not
reactivate its old writer.
