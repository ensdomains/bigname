# Release Runbook

Run the checked-in release smoke gate from the repository root:

```sh
scripts/release-smoke
```

For CI or an environment whose dependencies and Caddy image are already
cached:

```sh
scripts/release-smoke --no-network
```

The gate checks, in order:

1. local pinned upstream refs;
2. a prebuilt API binary and `/healthz` readiness contract; and
3. the public-edge policy through an ephemeral Caddy container.

The target database must already have the `bigname_phase` schema installed.
For a fresh database, run `phase-runner init-schema` once before this gate. The
installer deliberately refuses a nonempty phase schema, so schema initialization
is a separate deployment step rather than part of the repeatable smoke check. CI
performs that setup immediately before invoking this script.

The edge check reflects the C2/C3 transition. The API binary serves `/v2`,
GraphQL, and `/healthz`, while the checked-in public edge still denies `/v2`.
Removed v1 and documentation-helper paths return `404`; GraphQL POST and its
browser preflight remain admitted. The C3 edge flip is maintainer-gated.

`--no-network` makes Cargo offline and requires all HTTP endpoints to be
loopback. It does not skip the PostgreSQL, API, or Caddy checks. CI must fetch
Cargo dependencies and the configured Caddy image before entering this mode.

Required environment:

- `BIGNAME_DATABASE_URL` or `DATABASE_URL` for PostgreSQL;
- `BIGNAME_SMOKE_API_BIND_ADDR` when the default `127.0.0.1:3000` is occupied;
- `BIGNAME_SMOKE_PUBLIC_EDGE_URL` when the default `127.0.0.1:3001` is occupied;
- `BIGNAME_SMOKE_CADDY_IMAGE` to override `caddy:2-alpine`.

Do not promote when any check fails. Fix the checked-in ref, API readiness
issue, or edge-policy mismatch and rerun the entire gate. Apply reviewed
versioned migrations separately at the planned deployment boundary.

Before promotion, also require the workspace test, format, lint, build, and
e2e check gates from CI to be green.
