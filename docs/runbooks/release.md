# Release Runbook

Use the release smoke gate before promoting a release candidate and as the CI
release safety check. The gate is local: it validates the checked-out revision,
the configured PostgreSQL database, local pinned upstream refs, generated OpenAPI
artifact consistency, and the conformance ownership table for published OpenAPI
paths, runs focused reorg chaos, capability, and resolver-profile conformance
guards, runs the live manifest-drift audit with worker-owned alert observation
persistence, inspects the runtime watch plan, and checks the API process
readiness endpoint. It does not deploy, contact external RPC providers, contact
GitHub or Fly, or validate a remote production target.

## Command

Run the standard local release smoke gate:

```sh
scripts/release-smoke
```

Run the CI-compatible no-network subset:

```sh
scripts/release-smoke --no-network
```

Show the supported arguments and environment inputs:

```sh
scripts/release-smoke --help
```

## Local Prerequisites

- `cargo`, `diff`, `seq`, and `curl` are available on `PATH`.
- A PostgreSQL URL is available through `BIGNAME_DATABASE_URL` or
  `DATABASE_URL`. If neither is set, the script defaults to
  `postgres://bigname:bigname@127.0.0.1:5432/bigname`.
  Point this at the local PostgreSQL database used for smoke checks. The focused
  reorg chaos and dynamic resolver-profile conformance guards need a local
  PostgreSQL server where they can create, migrate, and drop temporary test
  databases; migrations, the manifest-drift audit and inspection path, runtime
  watch-plan inspection, and readiness all use the configured database even
  when `--no-network` is passed.
- The checked-in migration that creates `manifest_alert_observations` must have
  run before manifest-drift smoke checks can persist or read alert observations.
  The smoke gate runs migrations before the audit; when running
  `manifest-drift audit --json` or `inspect manifest-drift --json` by hand, run
  the worker migration first against the same database.
- The API bind address is free. Set `BIGNAME_SMOKE_API_BIND_ADDR` when
  `127.0.0.1:3000` is already in use.
- `BIGNAME_SMOKE_API_HEALTH_URL` is reachable from the operator host. By
  default it is derived from `BIGNAME_SMOKE_API_BIND_ADDR` as
  `http://<bind_addr>/healthz`.
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` when it exists, then uses the environment values above.

## Gate Coverage

`scripts/release-smoke` performs these checks, in order:

1. Validates no-network constraints when `--no-network` is passed.
2. Runs `scripts/sync-refs --check` as a local pinned upstream-ref verification
   step. This check reads the pinned refs already present under `.refs/`; it
   does not fetch, rotate, or mutate upstream refs.
3. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   reorg_chaos_drill_conformance_job -- --nocapture` as the focused reorg chaos
   conformance guard. This guard uses the configured local PostgreSQL server for
   temporary test database work and must not require external network access
   when dependencies are already cached.
4. Runs `cargo run --locked -p bigname-api -- print-openapi` and compares the
   result to `docs/api-v1.openapi.json`.
5. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   openapi` as the OpenAPI conformance-owner smoke guard. This guard reads only
   the checked-in `docs/api-v1.openapi.json` artifact and the conformance owner
   table in the conformance harness; it is no-network and no-Postgres.
6. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   capability_cutover_evidence` as the focused capability cutover evidence
   guard.
7. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   dynamic_resolver_profile -- --nocapture` as the focused dynamic
   resolver-profile conformance guard. This guard uses the configured local
   PostgreSQL server to create, migrate, and drop temporary test databases; it
   must not require external network access when dependencies are already
   cached.
8. Runs `cargo run --locked -p bigname-worker -- migrate` against the configured
   database, including the migration that creates the worker-owned
   `manifest_alert_observations` storage used by manifest-drift audit and
   inspection.
9. Runs `cargo run --locked -p bigname-worker -- manifest-drift audit --json`
   against the configured database. The audit computes live drift candidates,
   persists alert observations through worker-owned storage, and renders the
   persisted observation set.
10. Runs `cargo run --locked -p bigname-worker -- inspect watch-plan --json`
   against the configured database as a read-only runtime watch-plan inspection.
11. Starts `cargo run --locked -p bigname-api -- serve --bind-addr
   <BIGNAME_SMOKE_API_BIND_ADDR>` and probes `/healthz` until it returns
   `200` with `"status":"ready"`.

With `--no-network`, the script also sets `CARGO_NET_OFFLINE=true`, passes
`--offline` to Cargo, and rejects non-loopback smoke bind or health URLs. The
local pinned-ref check still only reads the checked-out `.refs/` state, and the
Cargo-backed conformance guards run from the local dependency cache. The gate
does not contact external network services or external RPC providers, but the
configured local PostgreSQL database must still be available. Once dependencies
are cached, the manifest-drift audit and `inspect manifest-drift --json` triage
path need no remote network; they still require the checked-out local refs and
the configured local PostgreSQL database.

Manifest-drift audit and inspection behavior:

- `manifest-drift audit --json` persists live alert candidates into the
  worker-owned `manifest_alert_observations` table, then renders the durable
  persisted observation set. The JSON reports persisted counts and
  `actionable_persisted_alert_count`; live candidate counts are diagnostic.
- `--fail-on-alert`, when used with the audit command outside the smoke script,
  fails on actionable persisted alerts. It is not a gate on transient live
  candidates that were not persisted.
- `inspect manifest-drift --json` is read-only and renders the same durable
  observation shape from the same worker-owned storage.
- Neither command fixes drift or mutates manifest truth, discovery edges,
  source-family admission, watch plans, or normalized events. Remediation
  remains explicit manifest, discovery, or source-family work.

## Pass Criteria

Treat the release smoke gate as passing only when the script exits `0` and logs
`release smoke gate passed`.

A passing gate means:

- the checked-in OpenAPI JSON matches the API generator output for this
  revision;
- the local `.refs/` checkouts match the pinned upstream-ref manifest for this
  revision;
- the focused reorg chaos conformance guard passes for this revision using local
  PostgreSQL temporary databases;
- every published OpenAPI public path has an explicit conformance harness owner
  or an explicit private/out-of-scope reason in the conformance owner table;
- the focused capability cutover evidence guard passes for this revision;
- the focused dynamic resolver-profile conformance guard passes using local
  PostgreSQL temporary databases;
- the checked-in migrations apply to the configured local database, including
  manifest alert observation storage;
- the manifest-drift audit command exits successfully against the configured
  local database, persists worker-owned alert observations, and renders the
  persisted observation set;
- the runtime watch-plan inspection command exits successfully and renders JSON
  from the configured local database;
- the API process can start from this revision; and
- the private readiness endpoint reports ready against that database.

## Failure Criteria

Any non-zero exit blocks the release candidate until triaged.

- OpenAPI drift failure: the generated artifact and checked-in artifact disagree.
  Do not promote the candidate until the API contract and checked-in artifact are
  intentionally reconciled.
- Pinned upstream-ref failure: `scripts/sync-refs --check` reported that the
  local `.refs/` checkouts do not match the pinned manifest or are unavailable.
  Do not promote until the local pinned-ref state is restored. This check is
  local verification only; it does not fetch or rotate upstream refs.
- Reorg chaos conformance failure: the focused
  `reorg_chaos_drill_conformance_job` guard failed or could not prepare its
  temporary PostgreSQL databases. Do not promote until the conformance failure or
  local database precondition is triaged. This is local conformance evidence, not
  proof of production reorg coverage.
- OpenAPI conformance-owner failure: a published public path in
  `docs/api-v1.openapi.json` lacks a conformance harness owner, an owner entry is
  blank, or an out-of-scope entry lacks an explicit reason. Do not promote until
  the route has an owning conformance harness or a deliberate private/out-of-scope
  reason.
- Dynamic resolver-profile conformance failure: the focused
  `dynamic_resolver_profile` guard failed or could not prepare its temporary
  PostgreSQL databases. Do not promote until the conformance failure or local
  database precondition is triaged.
- Migration failure: the configured database cannot apply the checked-in
  migrations. Do not promote until the migration or database precondition is
  fixed. Manifest-drift audit and inspection cannot persist or read
  `manifest_alert_observations` until this migration state exists.
- Manifest-drift audit failure: the audit command returned non-zero against the
  configured local database. Do not promote until the local manifest/discovery
  state, audit inputs, persistence path, migration state, or database
  precondition is triaged. This is not production monitoring or external RPC
  coverage, and the audit command does not auto-remediate drift.
- Manifest-drift alert failure: if an operator reruns
  `manifest-drift audit --json --fail-on-alert`, a non-zero exit means the
  persisted observation set contains actionable alerts. Inspect the durable
  shape with `inspect manifest-drift --json`; remediation remains explicit
  manifest, discovery, or source-family work before rerunning the audit.
- Watch-plan inspection failure: the read-only `inspect watch-plan --json`
  command returned non-zero against the configured local database. Do not promote
  until database reachability, manifest/discovery state, or the inspection
  command failure is triaged.
- Readiness failure: the API did not stay up or `/healthz` did not report
  ready. Do not promote until the API logs and database reachability explain the
  failure.
- No-network failure: the gate was not fully local, the bind or health URL was
  not loopback, or Cargo could not build from its local cache. Fix the operator
  environment before treating it as a release failure.

## Rollback Decision Points

Before promotion, a release smoke failure is a stop-the-line release block, not
a rollback trigger. Fix the release candidate or the local prerequisites, then
rerun the gate.

After promotion, start the rollback runbook when the promoted revision is
already serving traffic and the failure is service-impacting or cannot be
resolved quickly by correcting operator configuration. Use
`docs/runbooks/rollback.md` to validate the rollback candidate locally before
or alongside the operational rollback procedure.

Do not use this gate as proof of external integration health. It intentionally
does not exercise deploy commands, external RPC, GitHub, Fly, or remote
production endpoints.

## CI Behavior

CI runs this gate as `release smoke gate (no network)` with:

```sh
./scripts/release-smoke --no-network
```

The CI no-network subset preserves the existing OpenAPI drift and migration
checks while adding the local pinned upstream-ref check, focused reorg chaos
conformance guard, no-Postgres OpenAPI conformance-owner guard, focused
capability cutover evidence guard, focused dynamic resolver-profile conformance
guard, live manifest-drift audit with worker-owned alert persistence, runtime
watch-plan inspection, and local API readiness. It uses loopback-only smoke
URLs, offline Cargo execution, the checked-out `.refs/` state, and the
configured local PostgreSQL server/database for reorg chaos and dynamic
resolver-profile temporary databases, migrations, manifest-drift audit,
watch-plan inspection, and readiness. A CI failure has the same release-blocking
meaning as a local non-zero exit, except that missing cached dependencies are a
CI environment issue rather than a product regression.
