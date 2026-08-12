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

Adding, editing, or deleting a covered interpreter input rotates the compiled
[interpreter content hash](../glossary.md#interpreter-content-hash);
`docs/storage.md` names what is covered. Covered files are hashed whole, so
editing a unit test that lives inside one rotates the hash as surely as
changing its production code. Do not mix new interpretation output with rows
published under the old hash. For such a release:

A planned [re-derivation boundary](../glossary.md#re-derivation-boundary) may
combine separately reviewed and separately merged PRs. Before merging the first
one, the release record must list the complete artifact set, intended per-chain
product and diagnostic deltas, generated watch-plan widening, historical-fetch
range, combined content hash, acceptance corpus, and rollback point. Do not
deploy any subset. In the test environment, run the slice-isolation gates and a
combined-artifact comparison that permits only the recorded deltas. Production
publication and readiness remain per chain rather than cross-chain atomic; keep
traffic drained for each affected chain until its own full re-walk, acceptance
checks, publication, and Verify phase succeed.

A phase-runner restart during a re-walk rebuilds its session cache with a full
ranked scan over all interpreted events. That scan is expensive at production
scale. Avoid restart loops; investigate the first interruption before restarting
the walk repeatedly.

The release containing Issue #400 adds baseline indexes without a schema-v2
schema-migration file. Fresh namespaces receive them from `schema-v2/baseline`.
For an initialized production namespace, keep the API and every phase-runner or
one-shot Project process stopped and apply the following statements one at a
time with the writer role before deploying the new binary. Do not wrap them in
a transaction: PostgreSQL requires each `CREATE INDEX CONCURRENTLY` to run as a
top-level statement. The `normalized_events` builds are expected to take hours
at the production corpus size; monitor them through `pg_stat_progress_create_index`,
allow each build to finish, and confirm every named index is valid in `pg_index`
before continuing. A failed concurrent build can leave an invalid index; drop
only that exact invalid index and retry its reviewed statement before proceeding.

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_chain_block_number_idx
    ON bigname_phase.normalized_events (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_alias_history_idx
    ON bigname_phase.normalized_events
       (chain_id,
        lower(COALESCE(after_state ->> 'resolver', before_state ->> 'resolver',
                       raw_fact_ref ->> 'emitting_address')),
        block_number DESC, normalized_event_id DESC)
    WHERE event_kind = 'AliasChanged'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_upgrade_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'proxy_address'),
        block_number DESC, normalized_event_id DESC)
    WHERE event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_chain_block_number_idx
    ON bigname_phase.name_surfaces (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_chain_block_number_idx
    ON bigname_phase.surface_bindings (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_chain_block_number_idx
    ON bigname_phase.resources (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS children_current_labelhash_idx
    ON bigname_phase.children_current
       (namespace, lower(labelhash), parent_logical_name_id, child_logical_name_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_current_resolver_idx
    ON bigname_phase.name_current
       ((declared_summary #>> '{resolver,chain_id}'),
        lower(declared_summary #>> '{resolver,address}'), logical_name_id)
    WHERE declared_summary #>> '{resolver,address}' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS permissions_current_resolver_scope_idx
    ON bigname_phase.permissions_current
       ((scope_detail ->> 'chain_id'),
        lower(scope_detail ->> 'resolver_address'), resource_id)
    WHERE scope_kind = 'resolver'
      AND scope_detail ->> 'resolver_address' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS record_inventory_current_resolver_idx
    ON bigname_phase.record_inventory_current
       ((provenance ->> 'chain_id'), lower(provenance ->> 'resolver_address'), resource_id)
    WHERE provenance ->> 'resolver_address' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS primary_names_current_reverse_node_idx
    ON bigname_phase.primary_names_current
       ((claim_provenance ->> 'chain_id'),
        lower(claim_provenance ->> 'reverse_node'), address, coin_type, namespace)
    WHERE claim_provenance ->> 'reverse_node' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS permissions_current_resource_wrapper_expiry_idx
    ON bigname_phase.permissions_current_resource_summary
       ((provenance ->> 'chain_id'),
        ((provenance -> 'wrapper_expiry_boundary' ->> 'expiry_seconds')::numeric),
        resource_id)
    WHERE provenance ? 'wrapper_expiry_boundary';
```

Record this manual index step, its start/end times, validity check, and the
pre/post Project tick measurements in the release record for the shared
re-derivation boundary, alongside the complete artifact set. These indexes are
additive; rollback may leave them in place.

1. stop the API and phase runner;
2. take and verify a database backup;
3. for the release containing Issue #400, apply and validate the concurrent
   baseline indexes above; otherwise skip this step;
4. if the reviewed artifact set includes a versioned schema-migration, apply it;
   otherwise skip this step;
5. if an additive schema-migration created or changed a table, reapply and
   validate the verifier's `GRANT SELECT ON ALL TABLES IN SCHEMA
   bigname_phase` before starting any one-shot or long-running runner process;
   otherwise skip this step;
6. keep the long-running phase-runner supervisor stopped. If the generated
   watch plan widened an address/topic range, use the new artifact's one-shot
   Ingest redo over every widened range; otherwise skip this step. The command
   requires the full argument set or it is rejected before fetching anything:
   `phase-runner redo --chain <chain-id> --phase ingest --from-block <from>
   --to-block <to> --source <source>` (at least one `--source`; the CLI
   refuses an ingest redo without one, and every redo requires the explicit
   block range);
7. after any required Ingest redo succeeds, resume an already-audited Interpret
   redo with its existing token and exact active chain and range. Otherwise,
   invoke the exact required full-history Interpret redo without an attestation
   flag. If a current [manifest-authority
   marker](../glossary.md#manifest-authority-marker) makes the redo reject and
   print an invalidation token, run the required historical fetch for a widened
   watch plan, or complete the required review proving that the watch plan did
   not widen, then rerun the same chain and block range with
   `--attest-watch-set-coverage <token>`. If no marker exists, let the unflagged
   redo complete. Never invent a token, reuse one after completion, or use one
   for another redo. Do not use the unattended `run` path for an attestation;
8. complete the matching full-history Project redo while the supervisor remains
   stopped;
9. start the long-running phase runner only after those one-shot redos succeed,
   let the production Verify phase complete for every affected chain, and require
   its reviewed reference path rather than omitting or bypassing Verify;
10. confirm the phase state directly in the database while the API is still
   stopped — the `project` row in `chain_phase_state` current with no pending
   redo, and Verify success from the `verify` row for each affected chain plus
   the supervisor's Verify completion output (`/v2/status` cannot be used
   here: the API is stopped, and the route reads only the Project row's
   lifecycle, redo, and heartbeat state — it does not expose Verify);
11. start the API built from the same commit and confirm `/v2/status` reports
   current phase state and no pending redo; and
12. run the release smoke and public-edge checks before undraining traffic.

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

## Recovery plays

Route from the first confirmed symptom:

- `interpret` crash-loops with an identity or derivation mismatch, or one
  chain's Interpret state is `failed` while the container stays up ->
  [stop and escalate before selecting a repair](#stop-and-escalate-an-interpreter-mismatch).
- a schema-migration deploy stops between the schema-migration and service
  start -> [recover an aborted schema-migration deploy](#recover-an-aborted-schema-migration-deploy).
- stored lineage, block canonicality, or verification disagrees ->
  [follow the reorg and verification incident play](#reorg-and-verification-incidents).
- rollback requires an older binary, deleted schema, or restored data ->
  [follow the rollback boundary](#rollback).

Use the exact Compose file set deployed on the host for every recovery command,
retaining every active overlay. Replace `<compose-files>` below with that exact
set. The tracked baselines look like these; append any host-local overlays in
their deployed order:

- internal: `-f docker-compose.server.yml`;
- public: `-f docker-compose.server.yml -f docker-compose.public.yml`;
- reth: `-f docker-compose.server.yml -f docker-compose.reth-db.yml`; or
- public and reth: `-f docker-compose.server.yml -f docker-compose.public.yml
  -f docker-compose.reth-db.yml`.

Drain public traffic before running a recovery play. Use the deployment's
maintainer-approved edge procedure and confirm that no public request reaches
the API. The repository has no generic traffic-drain command; flag a missing
deployment-specific procedure and stop. Record this step as not applicable on
an internal deployment with no public edge.

### Stop and escalate an interpreter mismatch

Apply this play when the `interpret` phase crash-loops on an identity or
derivation mismatch, or when one chain's Interpret state is `failed` with that
error while another chain continues running.

1. Record the exact image ID of the existing phase-runner container as
   `<recovery-image>`. Do not use the mutable `latest` tag. If the command
   returns no container or more than one ID, stop and escalate:

   ```sh
   docker inspect --format '{{.Image}}' \
     "$(docker compose --env-file .env.server \
       <compose-files> ps -q phase-runner)"
   ```

2. Stop the phase runner:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> stop phase-runner
   ```

3. Capture the full error and the affected chain from the logs. Record the
   affected block when the error reports one; do not infer a missing block.
   Choose `<incident-start>` early enough to include the first failure:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> logs --since <incident-start> phase-runner
   ```

4. Capture the durable Interpret and Project status, recorded heads, pending
   redo state, and full `last_error`:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> exec -T postgres \
     sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
   SELECT chain_id,
          phase_name,
          phase_status,
          current_block_number AS recorded_head,
          target_block_number,
          redo_in_progress,
          last_error,
          updated_at
   FROM bigname_phase.chain_phase_state
   WHERE phase_name IN ('interpret', 'project')
   ORDER BY chain_id, phase_name;
   SQL
   ```

5. Escalate the captured error before selecting a redo. If the Interpret row's
   `recorded_head` is `NULL`, keep the phase runner stopped: neither a scoped nor
   a full Interpret redo can start without a processed extent. Require a
   separately reviewed recovery that preserves non-rebuildable state; do not
   select phase-schema replacement from this symptom alone. If the error says
   to run `recompute-flags`, follow the
   [recompute-flags procedure](../deployment.md#phase-runner-configuration); do
   not run an ordinary Interpret redo. Otherwise, require the incident owner to
   identify the earliest affected stored block within the recorded Interpret
   extent. If that start cannot be established, skip the scoped redo and follow
   the full re-walk in step 7.
6. Run the approved Interpret redo from the affected start through the recorded
   Interpret head. Keep the long-running phase runner stopped. Interpret treats
   later rows as potentially dependent on earlier rows, so it replays from
   `<from>` through the recorded head and stamps the matching Project repair.
   Pin the one-shot container to `<recovery-image>`. Copy every source
   descriptor for the affected chain exactly from the deployed configuration
   and repeat `--source` once for each descriptor. Each descriptor has the
   `CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV` form. The explicit arguments
   override the multi-chain `BIGNAME_PHASE_RUNNER_SOURCES` value for this
   one-off redo:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never phase-runner \
     phase-runner redo --chain <chain-id> --phase interpret \
     --from-block <from> --to-block <recorded-interpret-head> \
     --source <affected-chain-source> \
     [--source <additional-affected-chain-source> ...]
   ```

7. If the mismatch reproduces during the redo, keep the phase runner stopped
   and perform the full re-walk at the [planned re-derivation
   boundary](#planned-migration-and-fingerprint-boundary). Do not widen or
   repeat the scoped redo by guesswork. Stop this play; the full re-walk has its
   own image, restart, and verification steps.
8. If the redo succeeds, restart the phase runner with the same exact image.
   Let the supervisor resume Interpret and complete the stamped Project repair:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> up -d --pull never phase-runner
   ```

9. Repeat the status query from step 4 until Interpret advances beyond the
   pre-recovery recorded head without the same `last_error`. A
   mismatch in the next uncommitted batch can appear only after restart. If the
   same mismatch returns after the scoped redo, stop the phase runner and
   perform the full re-walk in step 7. Stop this play when escalating.
10. Require the Project row to report `phase_status = 'completed'`,
    `redo_in_progress = false`, and a `recorded_head` at or beyond the recovered
    Interpret head.
11. [Verify health](#verify-health) with the same Compose file set, then restore
    traffic through the same deployment-specific edge procedure used to drain
    it.

Never hand-edit identity or normalized-event rows. An in-place database update
is not a sanctioned recovery play on this stack.

### Recover an aborted schema-migration deploy

Apply this play when a deploy containing schema-migrations is interrupted
between the schema-migration step and service start, leaving the applied
schema-migration state and service versions desynchronized and the stack down.
The restore-or-re-roll decision was validated on 2026-07-29.

1. Keep the stack down. Never hand-apply pending SQL with `psql`, and never
   edit `_sqlx_migrations` to catch up.
2. If the schema-migration command stopped before it reported success, or its
   completion cannot be proven, treat it as half-applied. Keep the stack down
   and invoke the storage owner. Restore the verified pre-deploy backup required
   by the [existing backup steps](#planned-migration-and-fingerprint-boundary)
   with the deployment-specific restore procedure recorded for that backup.
   Do not substitute a generic repository command: storage snapshots and
   filesystem base backups use different restore mechanisms. Flag a missing
   deployment-specific restore procedure and stop. Do not complete or undo the
   partial change by hand, and do not continue until the restore is verified.
3. Do not resume at service start. Re-run the deploy from the top, from the
   exact target commit, so the applied schema-migration state and service
   versions move together. Repeat the applicable schema-migration checks and
   apply steps under the [schema-migration and fingerprint
   procedure](#planned-migration-and-fingerprint-boundary), but keep service
   start blocked.
4. Apply the release's reviewed re-derivation decision before starting
   services. Any deploy that changes the interpreter content hash requires the
   full re-walk under the [planned re-derivation
   boundary](#planned-migration-and-fingerprint-boundary). A semantic change
   outside that hash can also require re-derivation; review the surfaces listed
   under [interpretation replay](../storage.md#interpretation-replay). Keep the
   stack down until every required re-derivation step completes.
5. Treat saved Interpret and Project redo progress from the prior hash as
   invalid. Preserve pending Ingest or Verify redo markers and complete the
   exact persisted work named by the runner before starting `--phase all`; do
   not delete or skip those markers.
6. If no re-derivation is required, or after the required re-derivation
   boundary completes, [start or refresh
   services](#start-or-refresh-services).
7. [Verify health](#verify-health) before restoring traffic.

### Reorg and verification incidents

Use the bounded `phase-runner inspect` commands for stored lineage, block
canonicality, and raw-event evidence. Use `phase-runner rewind` only after
identifying an exact stored readable ancestor. Verification mismatches require
the chain-scoped repair procedure in
[`deployment.md`](../deployment.md#verification-mismatch-repair); do not edit
immutable raw facts or mark a phase complete manually.

### Rollback

Run `scripts/rollback-smoke` from the exact rollback checkout before changing
binaries. A binary rollback does not recreate dropped legacy tables. If the
rollback needs deleted schema or data, restore the verified pre-migration
backup under a separately reviewed database rollback plan.

Keep the public edge on its maintainer-approved policy throughout rollback and
re-run `scripts/public-edge-smoke` before restoring traffic.
