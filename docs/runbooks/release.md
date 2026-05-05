# Release runbook

The release smoke gate is a local script that validates the checked-out revision before promotion. It does not deploy, contact RPC providers, or hit GitHub or Fly. CI runs the same gate with `--no-network` as a release-blocking check.

## Run it

```sh
scripts/release-smoke              # full local gate
scripts/release-smoke --no-network # CI-compatible subset
scripts/release-smoke --help
```

## Prerequisites

- `cargo`, `diff`, `seq`, `curl` on `PATH`.
- A PostgreSQL URL via `BIGNAME_DATABASE_URL` or `DATABASE_URL` (default `postgres://bigname:bigname@127.0.0.1:5432/bigname`). The DB must be available even with `--no-network` — conformance guards create temporary test databases there.
- Worker migrations applied (the gate runs them, but `manifest-drift audit --json` and `inspect manifest-drift --json` need `manifest_alert_observations` to exist if you run them by hand).
- The API bind address is free (override with `BIGNAME_SMOKE_API_BIND_ADDR` if `127.0.0.1:3000` is in use).
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` if present, then uses the environment values above.

## What it checks

In order:

1. No-network constraints (when `--no-network` is passed).
2. `scripts/sync-refs --check` — verifies `.refs/` matches the pinned manifest. Reads only; doesn't fetch.
3. Reorg chaos conformance: `cargo test … reorg_chaos_drill_conformance_job`.
4. OpenAPI drift: `cargo run -p bigname-api -- print-openapi` vs `docs/api-v1.openapi.json`.
5. OpenAPI conformance-owner table: every published path has an owner or an explicit out-of-scope reason.
6. Capability cutover evidence: `cargo test … capability_cutover_evidence`.
7. Dynamic resolver-profile conformance: `cargo test … dynamic_resolver_profile`.
8. `cargo run -p bigname-worker -- migrate` (creates `manifest_alert_observations`).
9. `cargo run -p bigname-worker -- manifest-drift audit --json` — computes drift candidates, persists alert observations, renders the persisted set.
10. `cargo run -p bigname-worker -- inspect watch-plan --json` — read-only runtime watch-plan inspection.
11. `cargo build -p bigname-api --bin bigname-api` (so compile time doesn't eat into the readiness probe).
12. Start the compiled `bigname-api serve` and probe `/healthz` until it returns `200` with `"status":"ready"`.

With `--no-network`, the script sets `CARGO_NET_OFFLINE=true`, passes `--offline`, and rejects non-loopback bind/health URLs.

## Pass

Exit `0` and `release smoke gate passed` in the log. That means:

- OpenAPI artifact matches the generator output.
- `.refs/` matches the pinned manifest.
- Reorg chaos, capability cutover, and dynamic resolver-profile conformance pass.
- Every published OpenAPI path has a conformance owner or an out-of-scope reason.
- Migrations apply against the configured DB.
- Manifest-drift audit persists alert observations and exits cleanly.
- Watch-plan inspection runs.
- API binary builds and `/healthz` reports ready.

## Fail

Any non-zero exit blocks the release until triaged. Common causes:

| Failure | What to check |
| --- | --- |
| OpenAPI drift | API contract vs `docs/api-v1.openapi.json` reconciled? |
| Pinned ref mismatch | `.refs/` checkouts match the manifest? Run `scripts/sync-refs`. |
| Reorg chaos / dynamic resolver-profile | DB precondition or actual conformance bug. |
| Conformance-owner gap | New published path needs an owner entry. |
| Migration failure | DB precondition; can the migrations apply at all? |
| Manifest-drift audit non-zero | Local manifest/discovery state, audit inputs, persistence path. Doesn't auto-remediate — manifest/discovery work required. |
| Manifest-drift `--fail-on-alert` | Persisted observations contain actionable alerts. Inspect with `inspect manifest-drift --json`. |
| Watch-plan inspection | DB reachability or manifest/discovery state. |
| API prebuild | Compile failure or missing offline cache. |
| Readiness | API didn't stay up; check logs and DB reachability. |
| No-network | Operator environment issue, not a product regression. |

## Rollback decision

A failed gate before promotion is a stop-the-line block — fix and rerun, don't trigger rollback.

After promotion, start [rollback](./rollback.md) when the promoted revision is serving traffic and the failure is service-impacting or can't be resolved by fixing operator config quickly.

This gate is not proof of external integration health. It doesn't run deploys, external RPC, GitHub, Fly, or remote production endpoints.

## CI

CI invokes:

```sh
./scripts/release-smoke --no-network
```

A CI failure has the same release-blocking meaning as a local non-zero exit, except missing cached dependencies are an environment issue.
