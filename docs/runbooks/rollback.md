# Rollback runbook

The rollback smoke gate validates a rollback candidate locally — same checks as [release](./release.md), plus a migration idempotence pass. It does not perform the production rollback or contact remote services.

## Run it

```sh
scripts/rollback-smoke              # full local gate
scripts/rollback-smoke --no-network # CI-compatible subset
scripts/rollback-smoke --help
```

Prerequisites match the release gate — see [`release.md`](./release.md) § Prerequisites.

## What it checks

Same as release, plus:

- `cargo run -p bigname-worker -- migrate` is run **twice** to catch non-idempotent migration behavior in the rollback checkout.

In order:

1. No-network constraint validation.
2. `scripts/sync-refs --check`.
3. Reorg chaos conformance.
4. Worker migrate (run 1).
5. Worker migrate (run 2) — must be a no-op.
6. OpenAPI drift.
7. OpenAPI conformance-owner table.
8. Capability cutover evidence.
9. Dynamic resolver-profile conformance.
10. Manifest-drift audit.
11. Watch-plan inspection.
12. API prebuild.
13. API readiness probe.

## Pass

Exit `0` and `rollback smoke gate passed` in the log.

That means the rollback checkout's migrations are idempotent against the configured DB, the OpenAPI/refs match, all conformance guards pass, manifest-drift audit succeeds, watch-plan inspection runs, the API binary builds, and `/healthz` reports ready.

## Fail

Same failure list as release, plus:

| Failure | What to check |
| --- | --- |
| First migration | Stop and inspect DB state vs migration expectations. |
| Second migration (idempotence) | Don't proceed with automatic rollback until the idempotence problem is understood. |

## Decision points

Start rollback when:

- the current release is already promoted,
- the current release is unhealthy or unsafe, and
- a rollback candidate is expected to restore service faster than a forward fix.

Run `scripts/rollback-smoke` against the rollback checkout before promotion when there's time. For urgent incidents, run it in parallel with operational rollback prep and use any failure as a reason to pause automatic promotion.

After the operational rollback, rerun the gate against the revision and DB state representing the rolled-back service when local access is available. A passing local gate isn't a substitute for production health checks — it confirms only the local migration, artifact, pinned-ref, conformance, audit, watch-plan, build, and readiness behaviors above.

This gate is not proof of external integration health. It doesn't run deploys, external RPC, GitHub, Fly, or remote production endpoints.

## CI

CI invokes:

```sh
./scripts/rollback-smoke --no-network
```

Same blocking meaning as release. Missing cached deps are a CI environment issue, not a product regression.
