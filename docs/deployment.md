# Deployment

The old `bigname-indexer` runtime has been deleted. This Stage B source tree is
not yet a complete production indexing deployment: the phase runner implements
`ingest` and `interpret`, while `project`, `verify`, and `live` are explicit
unavailable phases. Production must remain on the last pre-cut release until
the project/live port lands. Commands and environment variables for the old
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
persist ingest and interpret output, then fails closed when orchestration
reaches the unavailable `project` phase. It is included for isolated Stage B
verification, not as a replacement production service. The surviving
API/worker do not consume its projection output yet; they continue to use
`public`.

## Server Compose during Stage B

`docker-compose.server.yml` starts PostgreSQL, migrations, the surviving API,
and the surviving worker. It intentionally has no indexer or phase-runner
service. The stack can exercise existing read models and worker behavior, but
without a completed project/live pipeline it is not a fresh indexing
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
- `BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT`
- `BIGNAME_PHASE_RUNNER_CHAINS`
- `BIGNAME_PHASE_RUNNER_SOURCES`
- `BIGNAME_PHASE_RUNNER_INSTANCE_ID`

Each `BIGNAME_PHASE_RUNNER_SOURCES` entry has the form
`CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV`; the named environment variable
contains the provider URL. Capacity, retry, and polling controls use the
`BIGNAME_PHASE_RUNNER_*` names exposed by `phase-runner --help`.

One-shot finite phase work is available through `phase-runner redo`. Only
`ingest` and `interpret` have implementations in this build. Selecting
`project`, `verify`, `live`, or the not-yet-implemented flag recomputation path
fails explicitly.

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
