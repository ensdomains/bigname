# Rollback Runbook

Use the rollback smoke gate when preparing or validating a rollback candidate.
The gate is local: it validates the checked-out rollback revision, the
configured PostgreSQL database, local pinned upstream refs, generated OpenAPI
artifact consistency, migration idempotence, the conformance ownership table for
published OpenAPI paths, runs focused reorg chaos, capability, and
resolver-profile conformance guards, runs the live manifest-drift audit, inspects
the runtime watch plan, and checks the API process readiness endpoint. It does
not perform the production rollback, deploy, contact external RPC providers,
contact GitHub or Fly, or validate a remote production target.

## Command

Run the standard local rollback smoke gate:

```sh
scripts/rollback-smoke
```

Run the CI-compatible no-network subset:

```sh
scripts/rollback-smoke --no-network
```

Show the supported arguments and environment inputs:

```sh
scripts/rollback-smoke --help
```

## Local Prerequisites

- `cargo`, `diff`, `seq`, and `curl` are available on `PATH`.
- A PostgreSQL URL is available through `BIGNAME_DATABASE_URL` or
  `DATABASE_URL`. If neither is set, the script defaults to
  `postgres://bigname:bigname@127.0.0.1:5432/bigname`.
  Point this at the local PostgreSQL database used for smoke checks. The focused
  reorg chaos and dynamic resolver-profile conformance guards need a local
  PostgreSQL server where they can create, migrate, and drop temporary test
  databases; migrations, the manifest-drift audit, runtime watch-plan
  inspection, and readiness all use the configured database even when
  `--no-network` is passed.
- The API bind address is free. Set `BIGNAME_SMOKE_API_BIND_ADDR` when
  `127.0.0.1:3000` is already in use.
- `BIGNAME_SMOKE_API_HEALTH_URL` is reachable from the operator host. By
  default it is derived from `BIGNAME_SMOKE_API_BIND_ADDR` as
  `http://<bind_addr>/healthz`.
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` when it exists, then uses the environment values above.

## Gate Coverage

`scripts/rollback-smoke` performs these checks, in order:

1. Validates no-network constraints when `--no-network` is passed.
2. Runs `scripts/sync-refs --check` as a local pinned upstream-ref verification
   step. This check reads the pinned refs already present under `.refs/`; it
   does not fetch, rotate, or mutate upstream refs.
3. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   reorg_chaos_drill_conformance_job -- --nocapture` as the focused reorg chaos
   conformance guard. This guard uses the configured local PostgreSQL server for
   temporary test database work and must not require external network access
   when dependencies are already cached.
4. Runs `cargo run --locked -p bigname-worker -- migrate` against the configured
   database.
5. Runs the same migration command a second time to catch non-idempotent
   migration behavior in the rollback checkout.
6. Runs `cargo run --locked -p bigname-api -- print-openapi` and compares the
   result to `docs/api-v1.openapi.json`.
7. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   openapi` as the OpenAPI conformance-owner smoke guard. This guard reads only
   the checked-in `docs/api-v1.openapi.json` artifact and the conformance owner
   table in the conformance harness; it is no-network and no-Postgres.
8. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   capability_cutover_evidence` as the focused capability cutover evidence
   guard.
9. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   dynamic_resolver_profile -- --nocapture` as the focused dynamic
   resolver-profile conformance guard. This guard uses the configured local
   PostgreSQL server to create, migrate, and drop temporary test databases; it
   must not require external network access when dependencies are already
   cached.
10. Runs `cargo run --locked -p bigname-worker -- manifest-drift audit --json`
   against the configured database.
11. Runs `cargo run --locked -p bigname-worker -- inspect watch-plan --json`
   against the configured database as a read-only runtime watch-plan inspection.
12. Starts `cargo run --locked -p bigname-api -- serve --bind-addr
   <BIGNAME_SMOKE_API_BIND_ADDR>` and probes `/healthz` until it returns
   `200` with `"status":"ready"`.

With `--no-network`, the script also sets `CARGO_NET_OFFLINE=true`, passes
`--offline` to Cargo, and rejects non-loopback smoke bind or health URLs. The
local pinned-ref check still only reads the checked-out `.refs/` state, and the
Cargo-backed conformance guards run from the local dependency cache. The gate
does not contact external network services or external RPC providers, but the
configured local PostgreSQL database must still be available.

## Pass Criteria

Treat the rollback smoke gate as passing only when the script exits `0` and logs
`rollback smoke gate passed`.

A passing gate means:

- the rollback checkout's checked-in migrations can be run twice against the
  configured local database without failing;
- the checked-in OpenAPI JSON matches the rollback checkout's API generator
  output;
- the local `.refs/` checkouts match the pinned upstream-ref manifest for the
  rollback checkout;
- the focused reorg chaos conformance guard passes for the rollback checkout
  using local PostgreSQL temporary databases;
- every published OpenAPI public path has an explicit conformance harness owner
  or an explicit private/out-of-scope reason in the conformance owner table;
- the focused capability cutover evidence guard passes for the rollback
  checkout;
- the focused dynamic resolver-profile conformance guard passes for the rollback
  checkout using local PostgreSQL temporary databases;
- the manifest-drift audit command exits successfully against the configured
  local database;
- the runtime watch-plan inspection command exits successfully and renders JSON
  from the configured local database;
- the API process can start from the rollback checkout; and
- the private readiness endpoint reports ready against that database.

## Failure Criteria

Any non-zero exit blocks automatic rollback promotion until triaged.

- First migration failure: the rollback checkout cannot apply its checked-in
  migrations to the configured database. Stop and inspect the database state and
  migration expectations before continuing.
- Second migration failure: the rollback checkout's migration command is not
  idempotent for the current database state. Do not proceed with an automatic
  rollback until the idempotence problem is understood.
- OpenAPI drift failure: the generated artifact and checked-in artifact disagree
  in the rollback checkout. Do not promote that checkout until the artifact and
  contract state are reconciled.
- Pinned upstream-ref failure: `scripts/sync-refs --check` reported that the
  local `.refs/` checkouts do not match the pinned manifest or are unavailable.
  Do not promote the rollback checkout until the local pinned-ref state is
  restored. This check is local verification only; it does not fetch or rotate
  upstream refs.
- Reorg chaos conformance failure: the focused
  `reorg_chaos_drill_conformance_job` guard failed or could not prepare its
  temporary PostgreSQL databases. Do not promote the rollback checkout until the
  conformance failure or local database precondition is triaged. This is local
  conformance evidence, not proof of production reorg coverage.
- OpenAPI conformance-owner failure: a published public path in
  `docs/api-v1.openapi.json` lacks a conformance harness owner, an owner entry is
  blank, or an out-of-scope entry lacks an explicit reason. Do not promote the
  rollback checkout until the route has an owning conformance harness or a
  deliberate private/out-of-scope reason.
- Dynamic resolver-profile conformance failure: the focused
  `dynamic_resolver_profile` guard failed or could not prepare its temporary
  PostgreSQL databases. Do not promote the rollback checkout until the
  conformance failure or local database precondition is triaged.
- Manifest-drift audit failure: the audit command returned non-zero against the
  configured local database. Do not promote the rollback checkout until the
  local manifest/discovery state, audit inputs, or database precondition is
  triaged. This is not production monitoring or external RPC coverage.
- Watch-plan inspection failure: the read-only `inspect watch-plan --json`
  command returned non-zero against the configured local database. Do not promote
  the rollback checkout until database reachability, manifest/discovery state, or
  the inspection command failure is triaged.
- Readiness failure: the rollback API did not stay up or `/healthz` did not
  report ready. Do not treat the rollback as service-restoring until the API
  logs and database reachability explain the failure.
- No-network failure: the gate was not fully local, the bind or health URL was
  not loopback, or Cargo could not build from its local cache. Fix the operator
  environment before treating it as a rollback-candidate failure.

## Rollback Decision Points

Start rollback execution when the current release is already promoted, the
current release is unhealthy or unsafe, and a rollback candidate is expected to
restore service faster than a forward fix.

Run `scripts/rollback-smoke` against the rollback checkout before promotion when
there is time to validate locally. For urgent incidents, run it in parallel with
operational rollback preparation and use any failure as a reason to pause
automatic promotion and escalate to the owning engineer.

After the operational rollback, rerun the gate against the revision and database
state that represent the rolled-back service when local access is available. A
passing local gate is not a substitute for production health checks; it confirms
only the local migration, artifact, pinned-ref, reorg chaos, conformance-owner,
capability-cutover, dynamic resolver-profile, manifest-drift audit, watch-plan
inspection, and readiness behaviors covered above.

Do not use this gate as proof of external integration health. It intentionally
does not exercise deploy commands, external RPC, GitHub, Fly, or remote
production endpoints.

## CI Behavior

CI runs this gate as `rollback smoke gate (no network)` with:

```sh
./scripts/rollback-smoke --no-network
```

The CI no-network subset preserves the existing OpenAPI drift and migration
checks while adding the local pinned upstream-ref check, focused reorg chaos
conformance guard, double migration idempotence check, the no-Postgres OpenAPI
conformance-owner guard, focused capability cutover evidence guard, focused
dynamic resolver-profile conformance guard, live manifest-drift audit, runtime
watch-plan inspection, and local API readiness. It uses loopback-only smoke URLs,
offline Cargo execution, the checked-out `.refs/` state, and the configured
local PostgreSQL server/database for reorg chaos and dynamic resolver-profile
temporary databases, migrations, manifest-drift audit, watch-plan inspection,
and readiness. A CI failure has the same rollback-blocking meaning as a local
non-zero exit, except that missing cached dependencies are a CI environment issue
rather than a product regression.
