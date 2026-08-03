# Deployment

The old `bigname-indexer` runtime has been deleted. This Stage B source tree is
not yet a complete replacement production deployment: the phase runner
implements `ingest`, `interpret`, `project`, read-only `verify`, and continuous
`live` follow, while the Stage C API cutover remains outstanding. Production
must remain on the last pre-cut release until that gate lands. Commands and
environment variables for the old
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
through the finalized head. It is included for isolated Stage B
verification, not as a replacement production service until Stage C lands.
The surviving API/worker do not consume its projection output yet; they
continue to use `public`.

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
transport retries, range-splitting attempts, and target-marker checks. Record
those counts and the provider's billed volume during the production sweep; the
measured dRPC cost remains a required D3 cutover input.
For every configured chain on which canonical-head hydration runs (currently
`ethereum-mainnet`), `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` must contain a
`CHAIN=HTTP_URL` entry. A missing entry is a fatal project-phase configuration
error. The check runs before event-derived project publication or hydration
writes, so previously hydrated values remain intact while the chain is stopped
for configuration repair.

One-shot finite phase work is available through `phase-runner redo` for
`ingest`, `interpret`, `project`, and `verify`. Verify redo checks its source
and SELECT-only database configuration before phase initialization, locking,
or redo-state publication.
It rechecks the requested range, remains below the recorded finalized extent,
and persists the level reported by the production verifier. A partial redo
retains the level for the full recorded extent; a full-extent redo reports the
level fixed by its source kind. An interrupted attempt keeps the normal
resumable redo marker and must be rerun with the same range.
Historical `live` redo is rejected because live follows only the current head.
The not-yet-implemented flag recomputation path also fails explicitly. Project
redo and an interpret-to-project cascade use
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` (or
`--hydration-rpc CHAIN=HTTP_URL`) for the same current-head enrichment as the
supervised project phase. `phase-runner rewind` moves the
published latest marker to an exact stored readable ancestor and uses normal
head publication to orphan the suffix, invalidate affected cache eligibility,
and stamp downstream redo.

Before either command's first use, run `phase-runner init-schema` once after
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
for diagnosis. Then wipe the affected chain's schema-v2 data, ingest it again
from the configured sources, rebuild interpretation and projections, and rerun
verification. Do not edit immutable raw rows in place and do not mark the phase
complete manually. A normal retry resumes from the last successful verification
batch and retains the weaker level for the resulting whole extent if the
reference kind changed. A failed verify redo is resumed by rerunning the same
redo command after the wipe-and-resync repair.

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

The API remains read-only over projections and execution output except for its
documented on-demand verified-resolution persistence path. Configure
`BIGNAME_API_CHAIN_RPC_URLS` for status and verified execution as described in
the API docs.

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
