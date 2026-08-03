# Deployment

The old `bigname-indexer` runtime has been deleted. This Stage B source tree is
not yet a complete replacement production deployment: the phase runner
implements `ingest`, `interpret`, `project`, and continuous `live` follow, while
the B4 read-only `verify` implementation and the Stage C API cutover remain
outstanding. Production must remain on the last pre-cut release until those
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
including reorg-driven downstream redo and canonical-head hydration. It is
included for isolated Stage B verification, not as a replacement production
service until B4 and Stage C land. The surviving
API/worker do not consume its projection output yet; they continue to use
`public`.

## Server Compose during Stage B

`docker-compose.server.yml` starts PostgreSQL, migrations, the surviving API,
and the surviving worker. It intentionally has no indexer or phase-runner
service. The stack can exercise existing read models and worker behavior, but
without B4 verification and the Stage C API cutover it is not the replacement
fresh indexing deployment.

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
- `BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT`
- `BIGNAME_PHASE_RUNNER_CHAINS`
- `BIGNAME_PHASE_RUNNER_SOURCES`
- `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`
- `BIGNAME_PHASE_RUNNER_INSTANCE_ID`

Each `BIGNAME_PHASE_RUNNER_SOURCES` entry has the form
`CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV`; the named environment variable
contains the provider URL. Capacity, retry, and polling controls use the
`BIGNAME_PHASE_RUNNER_*` names exposed by `phase-runner --help`.
For every configured chain on which canonical-head hydration runs (currently
`ethereum-mainnet`), `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` must contain a
`CHAIN=HTTP_URL` entry. A missing entry is a fatal project-phase configuration
error. The check runs before event-derived project publication or hydration
writes, so previously hydrated values remain intact while the chain is stopped
for configuration repair.

One-shot finite phase work is available through `phase-runner redo` for
`ingest`, `interpret`, `project`, and a configured `verify` implementation.
Verify redo persists the verification level that implementation reports. The
B3 deferred verifier refuses redo in phase preflight until B4 rather than
claiming a trust level. That refusal runs before phase initialization, locking,
or redo-state publication, including when a pre-B3 verify extent already
exists, so it cannot strand a redo marker. A configured verify implementation
continues to use the ratified redo path and persists its reported level.
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
