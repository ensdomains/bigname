# Production Docker Operations

This runbook operates the current PostgreSQL, API, phase-runner, and optional
Caddy services. The deleted indexer and worker have no containers, commands,
heartbeats, replay jobs, or migration entrypoints.

## Validate configuration

Populate `.env.server` from `.env.server.example`, including the non-owner API
login and the phase-runner sources. Validate every overlay before changing a
running host:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml config

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml config

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml config
```

The reth overlay attaches the API and phase runner to the external
`eth-archive-node_default` network and bind-mounts the configured
`RETH_DATA_DIR` into the phase runner read-only. Create or start that network
and make the canonical reth database path readable before using the overlay.

## Planned migration and fingerprint boundary

The image has no generic `migrate` command, so migrations are an operator step
run outside the containers. The migration runner is `sqlx-cli` against the
checked-in `migrations/` directory, from a checkout of the exact commit being
deployed and with the writer database URL:

```sh
cargo install sqlx-cli --no-default-features --features rustls,postgres  # once
git -C /path/to/bigname checkout <deployed-commit>
sqlx migrate info --source migrations --database-url "$BIGNAME_DATABASE_URL"
sqlx migrate run  --source migrations --database-url "$BIGNAME_DATABASE_URL"
```

Take and verify a backup first: `sqlx migrate run` applies every pending
version in order and has no down step. Raw facts dominate this database, so a
logical `pg_dump` is neither fast nor small; use the deployment's storage
snapshot or a filesystem-level base backup, sized against the current data
directory, and do not write it to the root filesystem. Run `sqlx migrate info`
again afterwards and confirm no version is still pending.

Do not hand-apply the SQL files with `psql`: the applied set is tracked in
`_sqlx_migrations`, and a file applied outside the runner leaves that ledger
out of sync. The runner still treats that version as pending, re-applying it
fails against the objects it already created, and the deploy stays down until
the ledger is reconciled by hand. A migration that drops legacy
`public`-schema tables is destructive and additionally requires an explicit
maintenance window.

Deleting interpreter inputs rotates the compiled interpreter content hash. Do
not mix new interpretation output with rows published under the old hash. For
such a release:

1. stop the API and phase runner;
2. take and verify a database backup;
3. apply the reviewed versioned migration;
4. start the new phase runner and complete the required full-history
   interpretation and projection walk for every admitted chain;
5. confirm `/v2/status` reports current phase generations and no pending redo;
6. start the API built from the same commit; and
7. run the release smoke and public-edge checks before undraining traffic.

If the phase schema itself must be replaced, follow
[`deployment.md`](../deployment.md#replacing-an-initialized-phase-schema). Do
not copy interpretation or projection rows from the old namespace into the new
one.

## Start or refresh services

Start the internal stack:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml up -d
```

Add the public edge when required:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml up -d
```

After changing only the Caddyfile, recreate the proxy explicitly:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d --no-deps --force-recreate public-proxy
```

## Verify health

Inspect container state and recent logs:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml ps

docker compose --env-file .env.server \
  -f docker-compose.server.yml logs --tail=200 api phase-runner postgres
```

Probe the host-private API listener:

```sh
curl -fsS http://127.0.0.1:3000/healthz
curl -fsS http://127.0.0.1:3000/v2/status
```

`api_status="ready"` proves the API can reach PostgreSQL. Aggregate readiness
also requires a current phase-runner heartbeat. Treat a stale phase heartbeat,
failed phase state, pending invalidation, or generation mismatch as an indexing
incident rather than masking it with an API restart.

## Pause and resume indexing

Pause the phase runner without stopping PostgreSQL or the API:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml stop phase-runner
```

Resume it with the same image and configuration:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml up -d phase-runner
```

The API remains reachable while indexing is paused, but health and status must
continue to report the stale or absent loop honestly.

## Reorg and verification incidents

Use the bounded `phase-runner inspect` commands for stored lineage, block
canonicality, and raw-event evidence. Use `phase-runner rewind` only after
identifying an exact stored readable ancestor. Verification mismatches require
the chain-scoped repair procedure in
[`deployment.md`](../deployment.md#verification-mismatch-repair); do not edit
immutable raw facts or mark a phase complete manually.

## Rollback

Run `scripts/rollback-smoke` from the exact rollback checkout before changing
binaries. A binary rollback does not recreate dropped legacy tables. If the
rollback needs deleted schema or data, restore the verified pre-migration
backup under a separately reviewed database rollback plan.

Keep the public edge on its maintainer-approved policy throughout rollback and
re-run `scripts/public-edge-smoke` before restoring traffic.
