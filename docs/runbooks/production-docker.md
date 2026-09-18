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

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  -f docker-compose.reth-db.yml config
```

The reth overlay attaches the API and phase runner to the node's external Docker
network (`eth-archive-node_default` unless `RETH_NETWORK_NAME` names another)
and gives the phase runner the mounts, user and PID namespace that direct Reth
reads need. Create or start that network and complete
[Direct Reth reader](#direct-reth-reader) before using the overlay.

## Direct Reth reader

This applies to any deployment whose intake source kind is `reth_db`: Ethereum
Mainnet, and Ethereum Sepolia once it has been
[switched from local RPC](../deployment.md#switching-sepolia-from-local-rpc-to-direct-reth-reads).
There is no separate reader container. The reader is the `phase-runner` service
of that deployment's Compose project with `docker-compose.reth-db.yml` applied,
and the same service definition runs the one-off sample and
`source-transport` commands through `docker compose run`: the image ships the
sample as `/usr/local/bin/reth-db-smoke` next to `phase-runner`, so
`docker compose ... run --rm phase-runner reth-db-smoke ethereum-sepolia
"$RETH_DATA_DIR" <block,block,...>` reads through the production mounts, user
and PID namespace ([bounded sample](../reth-db-reader.md#bounded-sample)). A Sepolia deployment
is its own Compose project with its own `.env.server`
(`BIGNAME_PHASE_RUNNER_CHAINS=ethereum-sepolia`,
`BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT=/app/manifests/sepolia`, a Sepolia
`RETH_DATA_DIR`, and `RETH_NETWORK_NAME` set to the Sepolia node's network).
[Direct Reth reader](../reth-db-reader.md#mount-contract) explains why each
requirement below exists.

1. **Same host.** Run the reader on the host that runs the Reth node, against
   the node's local filesystem. A network filesystem, a copy or a snapshot of
   the datadir is not supported.
2. **Reth version.** This image reads Reth v2.5.0 databases only; the version is
   fixed in `crates/ingest/Cargo.toml` and is not configurable. Confirm the node
   reports v2.5.0 before deploying the image, and treat a node upgrade and an
   image upgrade as one change. This includes Mainnet deployments that already
   used direct reads with an older image.
3. **Reader user.** Set `RETH_READER_USER` to the numeric `uid:gid` that owns
   the node's `db/mdbx.lck` (`stat -c '%u:%g' "$RETH_DATA_DIR/db/mdbx.lck"`).
   The image's default `bigname` user (`10001`) cannot write that file. The
   phase runner then runs as that user, so `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`
   must be writable by it; repeat the capacity preflight's create/remove check
   as that user.
4. **Wrapper directory.** Create `RETH_READER_DIR` on the host, owned by
   `RETH_READER_USER`, outside the node's datadir and outside every path the
   capacity preflight excludes. Compose does not create it. It holds only
   Reth's temporary `rocksdb-secondary-tmp-<pid>` directory and the empty mount
   points Docker creates.
5. **Mounts.** `RETH_DATA_DIR` is the node's datadir, the directory that
   contains `db`, `static_files` and `rocksdb`. The overlay binds
   `RETH_READER_DIR` read-write at that path in the container, the three
   storage directories read-only inside it, and the node's existing
   `db/mdbx.lck` read-write. Do not replace these with one bind of the whole
   datadir: read-only fails when the reader opens the lock file, and read-write
   exposes the node's data files to the reader.
6. **PID namespace.** Set `RETH_NODE_PID_NAMESPACE` to
   `container:<reth container name>` (for the Sepolia deployment,
   `container:bigname-sepolia-reth`), or to `host` when the node runs directly
   on the host. The overlay passes it to the service's `pid:` setting. Without
   it the reader can fail to open the database with
   `Resource temporarily unavailable (11)`; see
   [deployment.md](../deployment.md#switching-sepolia-from-local-rpc-to-direct-reth-reads).
   A `container:` namespace belongs to one run of the node container, so start
   the node first, and restart the phase runner whenever the node container
   restarts or is recreated.
7. **Memory.** Direct reads are memory-mapped, and the pages the reader touches
   are file page cache charged to the phase-runner container. Where the runner
   has a container memory ceiling (`BIGNAME_PHASE_RUNNER_MEMORY_LIMIT`, added
   on `main` by [PR #917](https://github.com/ensdomains/bigname/pull/917)),
   that page cache counts against it. Choose the ceiling from a measured run
   with direct reads enabled; this runbook gives no figure.
8. **Chain.** `RETH_DATA_DIR` must hold the chain the deployment indexes. The
   reader compares the datadir's stored genesis block hash with the configured
   chain's when it opens, and refuses on a mismatch with an error naming both
   hashes; a Sepolia deployment pointed at a Mainnet datadir fails to start
   rather than ingesting Mainnet facts under the Sepolia chain id. The bounded
   sample and the `source-transport` command apply the same check.

Check the result on the created container, not only the rendered file:

```sh
docker inspect "$runner_container" --format '{{json .Config.User}} {{json .HostConfig.PidMode}}'
docker inspect "$runner_container" --format '{{json .Mounts}}'
```

Require the five reth mounts with the modes above, the expected user and the
expected PID mode.

## Capacity preflight

Complete this before adopting the server configuration or recreating the runner.
The required floor/path inputs intentionally make older incomplete configurations
fail. Editing Compose does not change an already running container. This is part
of #639: it selects no fixed 100 GB reserve, database-size default or production
host measurement, and changes no direct CLI defaults or backup policy (#329).

1. Choose a positive `BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES` for the
   deployment. It must be a decimal unsigned 64-bit integer (at most
   `18446744073709551615`). Reject zero operationally: it disables the configured
   reserve, although rendering and the unchanged CLI accept it. Do not copy a
   development host's free-space figure. The optional
   `BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES` must be unset in operator inputs
   when unused; remove its assignment instead of leaving it empty. Empty, malformed
   and overflowing values must fail the real CLI parser. Ceiling zero is a limit.
2. Select a dedicated, pre-created directory on the Docker daemon host as
   `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`. It must be absolute; Compose binds that
   same source and target read/write and does not create a missing host path.
   Use a separate sibling on the database filesystem, never the data directory,
   its parent/child, a Docker volume parent, or a directory containing database,
   WAL, tablespace, socket, backup or other service content. It must contain only
   disposable probe content and no links into those trees. Reject overlap with
   every effective mount, including `RETH_DATA_DIR`.
3. Choose the container memory ceilings: `POSTGRES_MEMORY_LIMIT`,
   `BIGNAME_API_MEMORY_LIMIT`, `BIGNAME_PHASE_RUNNER_MEMORY_LIMIT` and, with the
   public overlay, `BIGNAME_PUBLIC_PROXY_MEMORY_LIMIT`, as positive Docker byte
   values (`24g`, `2048m`). The kernel charges the file page cache a container
   populates to that container's cgroup, so the cache PostgreSQL reads through
   lives inside `POSTGRES_MEMORY_LIMIT`, not beside it: that ceiling must cover
   `shared_buffers` + `maintenance_work_mem` + `max_connections` × `work_mem` ×
   a few (a backend can hold several `work_mem` allocations at once, and shared
   memory is charged too) **plus the page cache PostgreSQL is meant to have**,
   which is what `POSTGRES_EFFECTIVE_CACHE_SIZE` tells the planner it has.
   Lower `effective_cache_size` to fit the ceiling rather than the other way
   round; the Compose default of `96GB` is not a fit for a `24g` ceiling. The
   budget is the host's total RAM (`free -b`) minus an explicit host reserve
   for what runs outside any ceiling — kernel, Docker daemon, monitoring,
   shells — of at least 2 GiB or 5%, whichever is larger, minus an allowance
   for the co-resident archive node that is its own ceiling if it has one and
   otherwise a worst case, never its observed usage, which is not a bound.
   The four ceilings must sum to no more than that budget; a budget that
   forces a ceiling below its floor above means the host is too small for
   both workloads, not that the reserve can be spent. An OOM kill of one
   backend makes the postmaster restart every session. For the runner and the API, take the
   peak RSS observed on this host under catch-up and under load respectively
   and add headroom; where no observation exists yet, record that the ceiling
   is provisional and revisit it after the first catch-up. A container that
   reaches its ceiling is killed and restarted; check `docker inspect
   --format '{{.State.OOMKilled}}'` on any unexplained restart. Compose only
   refuses an empty value, and `0` renders as *no* limit, so validate the
   rendered model with every active overlay before recreating anything:

   ```sh
   scripts/check-compose-memory-limits --env-file .env.server \
     -f docker-compose.server.yml -f docker-compose.public.yml
   ```

   It fails unless every service carries a positive ceiling and `json-file`
   logging with `max-size` and `max-file`.
4. Record Docker/Compose versions, daemon host/context, Docker data root, volume
   driver/options, rootless/user-namespace settings and applicable security policy.
   Inspect PostgreSQL's effective mount rather than guessing from `postgres-data`:

   ```sh
   docker version
   docker compose version
   docker context show
   docker info --format '{{json .SecurityOptions}} {{.DockerRootDir}}'
   # Use the exact project and every active overlay for this and later commands.
   pg_container=$(docker compose --env-file .env.server -f docker-compose.server.yml ps -q postgres)
   docker inspect "$pg_container" --format '{{json .Mounts}}'
   docker volume inspect NAME_FROM_POSTGRES_MOUNT
   ```

   Preserve `postgres-data:/var/lib/postgresql/data` and its existing volume
   identity. The runner receives only the dedicated bind, never that volume or
   a host path exposing it. Resolve canonical paths on the daemon host; record
   `findmnt -T`, `stat` device/ownership/mode, applicable ACLs and `df -Pk` for
   both the actual database storage and probe. Require matching actual mounted
   filesystem/device, not merely matching path prefixes or equal free-byte counts.
   Check corresponding mount/device/`df -Pk` observations inside both containers.
5. Prepare permissions for the effective service identity, including rootless,
   user-namespace, ACL and SELinux mappings. The image's nominal UID/GID is 10001;
   do not blindly `chown 10001:10001` on the host. Verify required diagnostic
   tools on the actual host/image rather than assuming they are installed.
   From the actual candidate container as its normal service user, require a
   successful create/remove operation. Also observe the real runner creating and
   deleting `.phase-runner-capacity-probe-*` with a host filesystem event observer,
   leaving no file behind. A manual touch by a different user is insufficient.
6. Inspect effective settings before any recreation. Shell variables override
   `--env-file`; clear unintended overrides. Capture outputs privately and redact
   credentials before sharing. For each of server only, server/public,
   server/Reth and server/public/Reth, run the corresponding command above with
   both `config --format json` and `config --environment`. Require the exact floor
   and path, and an exact decimal ceiling assignment when set. A null model key
   alone does not prove runtime behavior. Inspect the actual container: an unset
   ceiling may appear as a bare variable name without `=`; `KEY=` is invalid empty.
   Confirm no configured ceiling through the CLI control below. Relative paths
   can render; their creation-time rejection remains a required control below.
   Require one dedicated read/write bind, identical absolute source/target and
   `create_host_path: false`. Both Reth sets must retain their separate reth
   mounts: the writable `RETH_READER_DIR` wrapper, the three read-only storage
   directories and the writable `db/mdbx.lck`
   ([Direct Reth reader](#direct-reth-reader)). Require the chosen memory ceiling on every service
   (`deploy.resources.limits.memory` in the rendered model — the check above —
   and `HostConfig.Memory` greater than zero on every created container) and
   the `json-file` logging options on each. No
   unrelated service environment, command, port, network or volume may
   change. Inspect the created container as well; the env file alone is not proof:

   ```sh
   runner_container=$(docker compose --env-file .env.server -f docker-compose.server.yml ps -q phase-runner)
   docker inspect "$runner_container" --format '{{json .Config.Env}}'
   docker inspect "$runner_container" --format '{{json .Mounts}}'
   docker inspect "$runner_container" --format '{{json .Config.User}} {{json .Config.Entrypoint}} {{json .Config.Cmd}}'
   docker inspect "$runner_container" --format '{{.HostConfig.Memory}} {{json .HostConfig.LogConfig}}'
   docker top "$runner_container"
   ```

   Check the actual process environment/argv and effective UID/GID. The shipped
   `phases` command executes `phase-runner run` without synthesizing capacity
   arguments. Confirm PostgreSQL files such as `PG_VERSION` are unreachable from
   the runner, including through the bind or symlinks. Record immutable image
   identity, source identity and the complete effective overlay configuration.

### Disposable acceptance before adoption

Use newly owned project, volume, container and directory names, with an explicit
runtime allocation. Preserve existing proof/production resources. Use the shipped
PostgreSQL named-volume definition, an immutable candidate image, initialized
scratch database and documented writer/verifier logins. A temporary override may
pin the image, isolate ports/restart behavior and select a byte-identical compiled
manifest deployment profile with supported sources and admitted chain names.
Respect the single-ENS-chain restriction. Do not invent an uncompiled manifest,
replace database storage, alter the probe bind or add a helper service. Attach all
temporary overrides/fixtures to evidence; keep connection secrets separate.

Run the following controls through the effective configuration and real service:

| Control | Required observation |
| --- | --- |
| Floor or path missing, then empty | Each render fails nonzero. |
| Relative path | Render or container creation rejects the nonabsolute target. |
| Missing bind source | Container creation fails; the host directory is not created. |
| Conflicting shell and env-file settings | Shell wins; effective values match the intended inputs. |
| Ceiling unset | Accept a bare variable name without `=` in Docker inspection; no ceiling assignment or generated argument. Reach the actual probe with no configured ceiling. |
| Valid integer ceiling | Exact string forwarded; reach the actual database-size breach below. |
| Empty/text/overflow ceiling or malformed/overflow floor | Actual CLI rejects that capacity input before unrelated prerequisites mask it. |
| Floor zero | Render/parser accept it, but operational admission rejects it. |
| Probe not writable | Capture `failed to write capacity probe under …`; it is retryable, not a capacity pause. Bound observation and cancel, then restore fixture permissions. |
| Writable probe on a safe second filesystem | Probe can succeed, but device mismatch fails admission. |

Supply the other required CLI inputs; `--help` and container creation alone do not
prove parsing. For accepted values, reach the intended later capacity observation.
With no ceiling, set a disposable floor just above observed free bytes and require
only `free_disk`. With a positive floor below free bytes and a tiny valid ceiling,
require only `database_size`. Record database/free/reserved-write bytes, reconcile
free bytes against contemporaneous `df -Pk` available KiB times 1024, and account
for intervening filesystem activity. Observe actual probe create/delete events.
No pressure file or material disk consumption is needed. Startup must first pass
manifest/database/verifier prerequisites. For this fresh immediately breached
fixture, require first reserve zero and observe no provider requests; do not infer
that property for arbitrary resumed state. Pause permits heartbeat/startup writes.

One logical database-size query and one filesystem probe cannot certify remote
PostgreSQL, opaque volume drivers, separate WAL, tablespaces, Docker metadata or
other constrained devices. Stop admission if the storage relationship cannot be
proved. This is a pre-batch floor with the preceding batch's write estimate, not
an absolute ENOSPC guarantee. Existing startup-below-floor/same-PID raw recovery
proof does not establish this wiring, mid-run pressure transition, full Verify,
normalized publication, API correctness, restore or production serving acceptance.

### Apply or roll back the wiring

After the effective configuration and filesystem/permission checks pass, follow
the approved deployment boundary and recreate the services whose wiring changed,
with all active overlays. A change to the runner's floor or probe path touches
only `phase-runner`. A change to the memory ceilings or log rotation touches
every service: a running container keeps its old `HostConfig` and `LogConfig`
until it is recreated, so recreating only the runner leaves PostgreSQL, the API
and the proxy unlimited and unrotated. Pause indexing first
([§ Pause and resume indexing](#pause-and-resume-indexing)), then recreate in
dependency order — PostgreSQL (a short outage for every client), the API, the
proxy where the public overlay is active, the runner — and inspect each created
container before moving on. Build the command from the deployment's exact
overlay set, the same `-f` list every other command in this runbook uses for
it: `docker-compose.server.yml` always, `docker-compose.public.yml` only where
the proxy runs, `docker-compose.reth-db.yml` only where the runner reads Reth.
Recreating with an overlay missing rebuilds the container without that
overlay's wiring — the runner loses its `eth_archive_node` network and the
read-only `RETH_DATA_DIR` bind — and an overlay added that the deployment does
not run demands variables it never set.

```sh
# Server + public + Reth shown; drop the overlays and services this deployment does not run.
compose=(docker compose --env-file .env.server \
  -f docker-compose.server.yml -f docker-compose.public.yml -f docker-compose.reth-db.yml)
scripts/check-compose-memory-limits "${compose[@]:2}"
"${compose[@]}" up -d --no-deps --force-recreate postgres
"${compose[@]}" up -d --no-deps --force-recreate api public-proxy
"${compose[@]}" up -d --no-deps --force-recreate phase-runner
for service in postgres api public-proxy phase-runner; do
  docker inspect "$("${compose[@]}" ps -q "$service")" \
    --format "$service {{.HostConfig.Memory}} {{json .HostConfig.LogConfig}} {{json .HostConfig.Binds}}"
done
```

Every line must show a memory value greater than zero and a `json-file` config
with `max-size` and `max-file`; the runner's line must still show the Reth
bind where that overlay is active. Settings are read at startup. Changing the
floor/ceiling does not require phase-row edits; genuine capacity breaches
resume automatically after capacity recovers. Preserve the PostgreSQL volume during rollback: never use
`down -v`. Restore the reviewed configuration/image and inspect the effective
settings again. The harmless dedicated probe directory may remain, but reverting
this wiring restores the old disabled/misdirected defaults and loses its protection.

## Before a from-zero or full-source walk

Do not start the walk until all of these checks pass:

1. The release commit has green cursor non-progress integration tests for all
   five phase names and valid repair-mode behavior.
2. `promtool test rules` proves the three-batch path, the two-batch/ten-minute
   path, and the legitimate exclusions.
3. The deployed metrics endpoint exposes
   `phase_runner_phase_batches_since_cursor_advance` and
   `phase_runner_phase_cursor_stall_age_seconds` for every configured chain and
   phase.
4. The host rule list contains `BignamePhaseRunnerPhaseNonProgress` and
   `BignamePhaseRunnerProgressMetricsMissing`.
5. The host retains the checked-in 15-second rule-group evaluation interval and
   configures `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` to at least 900
   seconds so the 13-minute two-batch bound remains valid.
6. The existing `severity=page` route passes the deployment's standard
   notification-path check.
7. Operators have the [manual halt procedure](pipeline-monitoring.md#phase-cursor-non-progress-response)
   open and know every active Compose overlay.
8. Record this acceptance statement in the walk log:

> **Phase livelock paging verified:** with the checked-in 15-second Prometheus
> rule interval, every executable phase/mode combination pages through the
> existing `severity=page` route no later than 13 minutes after its second
> committed [work-bearing batch](../glossary.md#work-bearing-batch) is confirmed
> at an unchanged [durable composite cursor](../glossary.md#durable-composite-cursor);
> a third pinned completion pages within 3 minutes. Intentional rescan
> and no-work shapes remain non-paging.

The equivalent operational acceptance is that livelock in any executable
phase/mode pages within 13 minutes after the second confirmed pinned completion,
or within 3 minutes after the third. Normal Ingest source movement, one Project
boundary replay, caught-up Live polling that reports no movement from the
starting durable cursor, no-head completion, capacity pause, and completed Verify revalidation do not page.

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

Run the migration session with `quote_all_identifiers` at its PostgreSQL
default, `off`. Confirm with `SHOW quote_all_identifiers` as the migration
role, and do not set it on for that role or database (`ALTER ROLE ... SET`,
`ALTER DATABASE ... SET`) or in `PGOPTIONS`. The reason is
`20260917140000_resolver_creation_self_edge.sql`: it finds the older
self-edge CHECK on `bigname_phase.discovery_edges` by searching the text
`pg_get_constraintdef` prints, and with the setting on PostgreSQL prints every
identifier in double quotes, so the search misses the rule. The file would then
leave the older rule in place beside the new one, which keeps rejecting the
resolver self-edge, or fail on the duplicate name when the new rule already
exists. That file cannot be changed to carry its own setting: it is already
applied on Sepolia and recorded in `_sqlx_migrations` with its checksum, so an
edited copy makes `sqlx migrate run` refuse the deploy as a modified applied
migration, and a fresh database would apply the edited text while the live one
keeps the original's result. The later
`20260917141000_discovery_self_edge_check_name.sql` and the index validity
checks turn the setting off themselves, transaction-locally, and put the
caller's value back; `schema-v2/apply-check.sh` proves that for each of them.

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

Before restoring traffic, run the separate
[production-scale benchmark gate](benchmark-gate.md) against a disposable
production-shaped copy and the drained new API generation. A small test database
run is not release evidence.

A phase-runner restart during a re-walk rebuilds its session cache with a full
ranked scan over all interpreted events. That scan is expensive at production
scale. Avoid restart loops; investigate the first interruption before restarting
the walk repeatedly.

The release containing Issue #400 adds baseline indexes and the versioned
schema-migrations
`20260813120000_reverse_hydration_attempt_state.sql` and
`20260813120100_reverse_hydration_attempt_state_validate.sql`, followed by
`20260814120000_project_redo_resolver_evidence.sql`. Fresh namespaces
receive the same objects from `schema-v2/baseline`. For an initialized
production namespace, keep the API and every phase-runner or one-shot Project
process stopped. Apply and validate the following concurrent indexes as step 3
below, then apply all three schema-migrations in order as step 4. The first adds the
three internal reverse-name polling selection columns, their sequence, and an
unvalidated all-null-or-complete constraint; the second validates that
constraint. The third adds the bounded Interpret-to-Project redo handoff for
resolver evidence. Before deploying the new binary, confirm the sequence, all
three columns, the handoff table, and its range index exist; also confirm that
`primary_names_current_reverse_hydration_attempt_check` has
`pg_constraint.convalidated = true`.

The release containing Issue #591 adds schema-migration
`20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql`. On an
initialized production namespace, build
`normalized_events_ens_v1_record_node_resolver_idx` concurrently in step 3
with the reviewed statement below and validate that it is ready and valid.
Then apply the schema-migration in step 4; its `IF NOT EXISTS` build is a no-op
when the concurrent index is already valid. Do not allow the versioned
schema-migration to perform the first build against a populated production
`normalized_events` table.

The release containing
`20260831150000_normalized_events_v2_expiry_scope_idx.sql` adds the bounded
ENSv2 expiry lookup used to select affected names during replay. On an
initialized production namespace, build both
`normalized_events_v2_expiry_scope_idx` and the widened
`normalized_events_subregistry_registration_history_idx` concurrently in step
3 with the reviewed statements below, and validate that both are ready and
valid. Then apply the schema-migration in step 4; its index builds are no-ops
when the concurrent indexes are already valid. Do not allow the versioned
schema-migration to perform either first build against a populated production
`normalized_events` table.

The release containing
`20260902140000_project_redo_expiry_roots.sql` adds the bounded
Interpret-to-Project handoff for logical names from deleted state-derived ENSv2
path-expiry releases. The follow-up
`20260902150000_project_redo_expiry_resources.sql` admits resource-only releases
and records the resource identifier when available. Apply both schema-migrations
in step 4 before deploying the new binary. Before starting any Project process,
confirm the handoff table, nullable identifier columns, and range index exist and
that the index is ready and valid with the query below.

The release containing
`20260911120000_normalized_events_emitter_history_idx.sql` adds the bounded
emitter lookup used by the `GET /v1/events?contract_address=` filter and the
registry-contract event count. On an initialized production namespace, build
`normalized_events_emitter_history_idx` concurrently in step 3 with the
reviewed statement below and validate that it is ready and valid. Then apply
the schema-migration in step 4; its `IF NOT EXISTS` build is a no-op when the
concurrent index is already valid. Do not allow the versioned schema-migration
to perform the first build against a populated production `normalized_events`
table.

The release containing
`20260917120000_discovery_edges_observation_history_idx.sql` adds the lookup
Interpret uses to find earlier and later observations of one discovery
relationship, including closed ones. On an initialized production namespace,
build `discovery_edges_observation_history_idx` concurrently in step 3 with the
reviewed statement below. The source of that statement is
[`ops/discovery-history-index/install.sql`](../../ops/discovery-history-index/install.sql);
the copy below must stay identical to it. `schema-v2/apply-check.sh` proves
that `install.sql`, the fresh baseline, and the schema-migration build the same
definition, but nothing checks this runbook's copy, so compare the two before
the release and treat `install.sql` as correct if they differ. Prefer running
`install.sql` itself, as
[the index runbook](../../ops/discovery-history-index/README.md) describes: it
also lifts the lock timeout, bounds the build to thirty minutes, prints the
index row, and exits non-zero unless the index is valid and ready. The
concurrent build permits writes, so it can be completed while the existing
runner is still processing, before the stop/start window opens; step 3 then
only re-checks the result.

Before continuing, require
`discovery_edges_observation_history_index_ready` from the query below to be
true. It checks that the index belongs to `bigname_phase.discovery_edges`, is
valid and ready, and has the reviewed key columns, order, and predicate.
`install.sql` ends with its own validity and definition check; run this query
as well when the statement was applied by hand.

An interrupted build, for example one cancelled or stopped by the
thirty-minute limit, leaves an invalid index under the intended name, and
`IF NOT EXISTS` then skips it. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
this index with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_observation_history_idx`,
rerun the statement or `install.sql`, and repeat the query. Recover an index
that is valid but has the wrong definition the same way. Never drop the active
discovery indexes, and never drop a valid index with the reviewed definition
merely because an installation was retried.

In the release record, keep the `install.sql` output or the statement with its
start and end times, the result of the query below, and the before and after
`EXPLAIN (ANALYZE, BUFFERS)` plans and completed-batch measurements the index
runbook asks for. Then apply the schema-migration in step 4; its
`IF NOT EXISTS` build is a no-op when the concurrent index is already valid. Do
not allow the versioned schema-migration to perform the first build against a
populated production `discovery_edges` table: an ordinary index build blocks
writes to the table until the schema-migration's transaction ends.

The release containing `20260917130000_discovery_edges_reopen_idx.sql` adds the
exact lookup Interpret uses to find a retained observation, orphaned and closed
ones included, before it inserts a new row. On an initialized production
namespace, build `discovery_edges_reopen_idx` concurrently in step 3 with the
reviewed statement below. The source of that statement is
[`ops/discovery-reopen-index/install.sql`](../../ops/discovery-reopen-index/install.sql);
the copy below must stay identical to it. `schema-v2/apply-check.sh` proves
that `install.sql`, the fresh baseline, and the schema-migration build the same
definition, but nothing checks this runbook's copy, so compare the two before
the release and treat `install.sql` as correct if they differ. Prefer running
`install.sql` itself, as
[its index runbook](../../ops/discovery-reopen-index/README.md) describes: it
also lifts the lock timeout, bounds the build to thirty minutes, prints the
index row, and exits non-zero unless the index is valid, ready, and has the
reviewed definition. This build can also be completed before the stop/start
window opens; step 3 then only re-checks the result.

Before continuing, require `discovery_edges_reopen_index_ready` from the query
below to be true. It checks that the index belongs to
`bigname_phase.discovery_edges`, is valid and ready, has the reviewed key
columns and order, and has no predicate.

An interrupted build leaves an invalid index under the intended name, and
`IF NOT EXISTS` then skips it. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
this index with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_reopen_idx`, rerun the
statement or `install.sql`, and repeat the query. Recover an index that is
valid but has the wrong definition the same way. Never drop a valid index with
the reviewed definition or the other discovery indexes.

In the release record, keep the `install.sql` output or the statement with its
start and end times, the result of the query below, the before and after
`EXPLAIN (ANALYZE, BUFFERS)` plans and completed-batch measurements, and the
`benchmark.py` JSON output the index runbook asks for. Then apply the
schema-migration in step 4; its `IF NOT EXISTS` build is a no-op when the
concurrent index is already valid. Do not allow it to perform the first build
against a populated production `discovery_edges` table.

Both discovery index schema-migrations adopt an existing index by name alone,
so on their own they would also accept the invalid index an interrupted
concurrent build leaves behind. The later
`20260917160000_discovery_edges_index_validity_check.sql` closes that gap: in
step 4 it fails, and `sqlx migrate run` stops without recording it, if either
index exists but is not valid and ready, or is valid but does not have the
reviewed definition. It also fails when `bigname_phase.discovery_edges` exists
and either name is missing or belongs to a table, view, or other relation that
is not an index. The error names the index and, for a definition mismatch,
prints the definition it found beside the expected one. It never drops or
rebuilds an index. If it
fails, follow the recovery steps above or in the matching index runbook, then
run `sqlx migrate run` again.

The release containing `20260917131000_project_scoped_history_indexes.sql` adds
the eight indexes Project uses to look up the history of changed names and
primary names. On an initialized production namespace, build them in step 3 by
running
[`ops/project-scoped-history/install.sql`](../../ops/project-scoped-history/install.sql)
as [its runbook](../../ops/project-scoped-history/README.md) describes. This
runbook carries no copy of the eight statements; `install.sql` is the only
source, and `schema-v2/apply-check.sh` proves it builds what the fresh baseline
and the schema-migration build. The builds are concurrent and permit writes, so
they can finish while the existing runner is still processing, before the
stop/start window opens; step 3 then only runs `install.sql` again as the check.

`install.sql` is its own readiness check. It exits non-zero unless all eight
names are valid and ready indexes on `bigname_phase.normalized_events` with the
reviewed definition, and it refuses before building anything when one of the
names is already taken by an invalid index, an index with another definition,
or a relation that is not an index. An interrupted build leaves such an invalid
index. To recover, first confirm in `pg_stat_progress_create_index` that no
build is still running, then drop only the index the error names with
`DROP INDEX CONCURRENTLY` and run `install.sql` again. Never drop a valid index
with the reviewed definition.

Keep the `install.sql` output with its start and end times in the release
record. Then apply the schema-migrations in step 4. The `IF NOT EXISTS` builds
in `20260917131000_project_scoped_history_indexes.sql` are no-ops when the
indexes already exist; do not allow them to perform the first build against a
populated production `normalized_events` table, because an ordinary index build
blocks writes. That file adopts an index by name alone, so the later
`20260917161000_project_scoped_history_index_validity_check.sql` makes the same
check as `install.sql`: `sqlx migrate run` stops without recording it if any of
the eight indexes is missing, invalid, not ready, on another table, not an
index, or has another definition. It never drops or rebuilds an index. If it
fails, recover as above, then run `sqlx migrate run` again.

The release containing
`20260904120000_project_redo_child_registration_history.sql` adds the bounded
Interpret-to-Project handoff for child and registry identifiers from deleted
ENSv1→ENSv2 [migration-registry](../glossary.md#migration-registry-wrapperregistry)
entry history. A fresh namespace receives the table and range index from
`schema-v2/baseline`; an initialized namespace needs the schema-migration
because its already-installed baseline is unchanged. Apply the schema-migration
in step 4 before deploying the new binary. Before starting any Project process,
confirm the handoff table and range index exist and that the index is ready and
valid with the query below.

The registry-operator projection release adds the ordered schema-migrations
`20260902160000_registry_operator_account_permissions.sql`,
`20260902160100_registry_operator_account_permissions_validate.sql`, and
`20260902160200_registry_operator_account_permissions_swap.sql`. Apply all
three in step 4 before deploying the new binary. Before starting Project,
confirm that the [account-permission state](../glossary.md#account-permission-state) table,
both account lookup indexes, the four [registry-owner binding](../glossary.md#registry-owner-binding)
summary columns, and the validated final binding constraint exist with the query below.

```sql
SELECT
    to_regclass('bigname_phase.project_redo_resolver_evidence') IS NOT NULL
        AS redo_handoff_exists,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid =
              to_regclass('bigname_phase.project_redo_resolver_evidence_range_idx')
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS redo_handoff_range_index_ready;

SELECT
    to_regclass('bigname_phase.project_redo_expiry_roots') IS NOT NULL
        AS expiry_redo_handoff_exists,
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'project_redo_expiry_roots'
          AND column_name = 'logical_name_id'
          AND is_nullable = 'YES'
    ) AS expiry_redo_logical_name_nullable,
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'project_redo_expiry_roots'
          AND column_name = 'resource_id'
          AND is_nullable = 'YES'
    ) AS expiry_redo_resource_available,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid = to_regclass(
                  'bigname_phase.project_redo_expiry_roots_range_idx'
              )
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS expiry_redo_handoff_range_index_ready;

SELECT
    to_regclass(
        'bigname_phase.project_redo_child_registration_history'
    ) IS NOT NULL AS child_registration_history_handoff_exists,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid = to_regclass(
                  'bigname_phase.project_redo_child_registration_history_range_idx'
              )
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS child_registration_history_range_index_ready;

SELECT
    to_regclass('bigname_phase.account_permission_state_current') IS NOT NULL
        AS account_permission_state_exists,
    to_regclass('bigname_phase.account_permission_state_current_active_subject_idx') IS NOT NULL
        AS active_subject_index_exists,
    to_regclass('bigname_phase.account_permission_state_current_applicability_idx') IS NOT NULL
        AS applicability_index_exists,
    (
        SELECT count(*) = 4
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'permissions_current_resource_summary'
          AND column_name IN (
              'registry_owner',
              'registry_contract',
              'registry_binding_provenance',
              'registry_binding_chain_positions'
          )
    ) AS registry_binding_columns_exist,
    EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid =
              'bigname_phase.permissions_current_resource_summary'::regclass
          AND conname =
              'permissions_current_resource_summary_registry_binding_check'
          AND convalidated
    ) AS registry_binding_constraint_validated;

SELECT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    WHERE index_relation.oid =
          to_regclass('bigname_phase.normalized_events_v2_expiry_scope_idx')
      AND index_state.indisvalid
      AND index_state.indisready
) AS normalized_events_v2_expiry_scope_index_ready,
EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    WHERE index_relation.oid = to_regclass(
              'bigname_phase.normalized_events_subregistry_registration_history_idx'
          )
      AND index_state.indisvalid
      AND index_state.indisready
      AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
          LIKE '%RegistrationReserved%'
) AS normalized_events_reserved_registration_history_index_ready,
EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    WHERE index_relation.oid =
          to_regclass('bigname_phase.normalized_events_emitter_history_idx')
      AND index_state.indisvalid
      AND index_state.indisready
) AS normalized_events_emitter_history_index_ready;

-- The next two queries compare key and predicate text as PostgreSQL prints it.
-- With quote_all_identifiers on it prints every identifier in double quotes,
-- and a healthy index would read as not ready, so turn it off for this session.
-- The quotes are not stripped from the printed text instead.
SET quote_all_identifiers = off;

-- The schema qualifier on the enum type depends on the session search_path,
-- so both spellings of the predicate are accepted. The printed text is not
-- rewritten, because a replacement would also change a string literal.
SELECT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    JOIN pg_am access_method ON access_method.oid = index_relation.relam
    WHERE index_relation.oid = to_regclass(
              'bigname_phase.discovery_edges_observation_history_idx'
          )
      AND index_state.indrelid = to_regclass('bigname_phase.discovery_edges')
      AND index_state.indisvalid
      AND index_state.indisready
      AND NOT index_state.indisunique
      AND access_method.amname = 'btree'
      AND index_state.indnkeyatts = 5
      AND index_state.indoption::text = '0 0 0 0 0'
      AND ARRAY(
              SELECT pg_get_indexdef(index_state.indexrelid, key_position, true)
              FROM generate_series(1, index_state.indnatts) AS key_position
              ORDER BY key_position
          ) = ARRAY[
              'chain_id',
              'from_contract_instance_id',
              'edge_kind',
              '(provenance ->> ''observation_key''::text)',
              'active_from_block_number'
          ]
      AND pg_get_expr(index_state.indpred, index_state.indrelid, true) IN (
              'canonicality_state <> ''orphaned''::canonicality_state',
              'canonicality_state <> ''orphaned''::bigname_phase.canonicality_state'
          )
) AS discovery_edges_observation_history_index_ready;

SELECT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    JOIN pg_am access_method ON access_method.oid = index_relation.relam
    WHERE index_relation.oid =
          to_regclass('bigname_phase.discovery_edges_reopen_idx')
      AND index_state.indrelid = to_regclass('bigname_phase.discovery_edges')
      AND index_state.indisvalid
      AND index_state.indisready
      AND NOT index_state.indisunique
      AND access_method.amname = 'btree'
      AND index_state.indnkeyatts = 5
      AND index_state.indoption::text = '0 0 0 0 0'
      AND ARRAY(
              SELECT pg_get_indexdef(index_state.indexrelid, key_position, true)
              FROM generate_series(1, index_state.indnatts) AS key_position
              ORDER BY key_position
          ) = ARRAY[
              'chain_id',
              'from_contract_instance_id',
              'edge_kind',
              'active_from_block_number',
              '(provenance ->> ''observation_key''::text)'
          ]
      AND index_state.indpred IS NULL
) AS discovery_edges_reopen_index_ready;
```

Apply the following index statements one at a time with the writer role. Do not
wrap them in a transaction: PostgreSQL requires each `CREATE INDEX CONCURRENTLY`
to run as a top-level statement. The `normalized_events` builds are expected to
take hours at the production corpus size; monitor them through
`pg_stat_progress_create_index`, allow each build to finish, and confirm every
named index is valid in `pg_index` before continuing. A failed concurrent build
can leave an invalid index; drop only that exact invalid index and retry its
reviewed statement before proceeding.

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
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'resolver'), block_number, block_hash)
       INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_before_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(before_state ->> 'resolver'), block_number, block_hash)
       INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_after_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state #>> '{scope,resolver_address}'),
        block_number, block_hash) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_before_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(before_state #>> '{scope,resolver_address}'),
        block_number, block_hash) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND before_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_subregistry_registration_history_idx;
CREATE INDEX CONCURRENTLY normalized_events_subregistry_registration_history_idx
    ON bigname_phase.normalized_events
       (chain_id, (after_state ->> 'registry_contract_instance_id'),
        block_number DESC, normalized_event_id DESC, logical_name_id)
    WHERE event_kind IN (
              'RegistrationGranted', 'RegistrationReserved',
              'RegistrationRenewed', 'RegistrationReleased'
          )
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL
      AND after_state ->> 'registry_contract_instance_id' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'node'),
        lower(COALESCE(NULLIF(after_state ->> 'resolver', ''),
                       NULLIF(raw_fact_ref ->> 'emitting_address', ''))),
        block_number, transaction_index, log_index, normalized_event_id)
    WHERE logical_name_id IS NULL
      AND source_family = 'ens_v1_resolver_l1'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v2_expiry_scope_idx
    ON bigname_phase.normalized_events
       (chain_id, ((after_state ->> 'expiry')::numeric),
        block_number, logical_name_id)
    WHERE logical_name_id IS NOT NULL
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND event_kind IN (
          'RegistrationGranted', 'RegistrationReserved',
          'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
      )
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND jsonb_typeof(after_state -> 'expiry') = 'number';
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'node'),
        lower(COALESCE(NULLIF(after_state ->> 'resolver', ''),
                       NULLIF(raw_fact_ref ->> 'emitting_address', ''))),
        block_number, transaction_index, log_index, normalized_event_id)
    WHERE logical_name_id IS NULL
      AND source_family = 'basenames_base_resolver'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_emitter_history_idx
    ON bigname_phase.normalized_events
       (lower(raw_fact_ref ->> 'emitting_address'),
        block_number DESC NULLS LAST, log_index DESC NULLS LAST,
        normalized_event_id DESC)
    WHERE raw_fact_ref ->> 'emitting_address' IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS discovery_edges_observation_history_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        (provenance ->> 'observation_key'),
        active_from_block_number
    )
    WHERE canonicality_state <> 'orphaned';
CREATE INDEX CONCURRENTLY IF NOT EXISTS discovery_edges_reopen_idx
    ON bigname_phase.discovery_edges (
        chain_id,
        from_contract_instance_id,
        edge_kind,
        active_from_block_number,
        (provenance ->> 'observation_key')
    );
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
pre/post published-head Project re-apply measurements in the release record for
the shared re-derivation boundary, alongside the complete artifact set. These
indexes are additive; rollback may leave them in place.

1. stop the API and phase runner;
2. take and verify a database backup;
   For the destructive Issue #411 Sepolia
   [source-role rollout](../glossary.md#source-role), also require the part-2
   release artifact, two distinct endpoint secrets, and an owner-approved
   rollback/restoration procedure before continuing. No narrower per-chain
   reset procedure is checked in. The
   [whole-schema replacement](../deployment.md#replacing-an-initialized-phase-schema)
   rebuilds every configured chain and is authorized only for a reviewed
   schema-migration that cannot preserve an initialized namespace; it does not
   authorize the Issue #411 source-role transition. Stop until part 3 supplies
   the reviewed per-chain reset and lossless preservation procedure. Once it is
   available, continue with steps 3–8 before that chain reset. Stop if any
   prerequisite is absent; never improvise a reset, data transfer, or rollback.
   Do not reset at this step. The optional one-shot redo instructions are not
   substitutes for the reset and full [source
   re-walk](../glossary.md#re-derivation-boundary). Execute the [owner-ratified
   rollout section](../deployment.md#owner-ratified-sepolia-source-role-rollout)
   at step 9;
3. for the release containing Issue #400, Issue #591, or
   `20260831150000_normalized_events_v2_expiry_scope_idx.sql`, or
   `20260902120000_normalized_events_basenames_record_node_resolver_idx.sql`,
   or `20260911120000_normalized_events_emitter_history_idx.sql`,
   or `20260917120000_discovery_edges_observation_history_idx.sql`,
   or `20260917130000_discovery_edges_reopen_idx.sql`,
   apply the applicable reviewed `CREATE INDEX CONCURRENTLY` statements from
   the block above, then validate each with the readiness query above it;
   for the release containing
   `20260917131000_project_scoped_history_indexes.sql`, run
   `ops/project-scoped-history/install.sql` as described above and require it
   to exit zero;
   otherwise skip this step;
   For the release containing
   `20260814130000_surface_binding_authority_arm.sql`, a populated phase schema
   cannot take the required `NOT NULL` column without the forbidden historical
   arm backfill. Before step 4, empty only the rebuildable binding rows and the
   current projections that reference them. If the installed schema predates
   `address_records_current`, omit that table from the statement:

   ```sql
   BEGIN;
   TRUNCATE TABLE
       bigname_phase.name_current,
       bigname_phase.address_names_current,
       bigname_phase.address_records_current,
       bigname_phase.surface_bindings
       CONTINUE IDENTITY RESTRICT;
   COMMIT;
   ```

   Keep the API and phase runner stopped until steps 7 and 8 complete. This is
   a targeted derived-state reset, not a phase-schema replacement: it preserves
   raw facts, manifest rows and their sequence-assigned IDs, normalized-event
   identities, and the metadata needed to resume pre-boundary cursors. Do not
   clear or rename the phase schema at this boundary. Step 4 then applies the
   required column to the empty binding table, and the mandatory full-history
   Interpret and Project redos rebuild the cleared rows;
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
   --to-block <to> --source <source> --metrics-bind-addr 0.0.0.0:9465`. Repeat
   `--source` for every configured intake-capable source key; the exact persisted
   cursor-key set is required.
   The CLI refuses an ingest redo without a source, and every redo requires the
   explicit block range. These recovery commands override redo's ephemeral
   metrics default with `0.0.0.0:9465` so the stopped supervisor's existing
   Prometheus target observes repair-mode progress. Use that fixed address only
   while the supervisor is stopped; another simultaneous redo needs its own
   stable target or the logged ephemeral default;
7. after any required Ingest redo succeeds, resume an already-audited Interpret
   redo with its existing token and exact active chain and range. Otherwise,
   invoke the exact required full-history Interpret redo without an attestation
   flag. If a current [manifest-authority
   marker](../glossary.md#manifest-authority-marker) makes the redo reject and
   print an invalidation token, run the required historical fetch for a widened
   watch plan, or complete the required review proving that the watch plan did
   not widen, then rerun the same chain and block range with
   `--attest-watch-set-coverage <token>`. If no marker exists, let the unflagged
   redo complete. Include `--metrics-bind-addr 0.0.0.0:9465` on this and the
   matching Project redo. Never invent a token, reuse one after completion, or
   use one for another redo. Do not use the unattended `run` path for an attestation;
8. complete the matching full-history Project redo while the supervisor remains
   stopped;
9. start the long-running phase runner only after those one-shot redos succeed.
   When the release also carries a versioned schema-migration or required
   replays, complete them before the Sepolia reset and full
   Ingest-through-Verify walk. Use only the reviewed part-3 per-chain reset after
   the preceding release work succeeds; stop if that procedure is unavailable.
   Before accepting any verification-only descriptor, review the deployment's
   endpoint-rotation record. If that endpoint served intake at any time during
   the retained walk—even under another key—stop and perform the reviewed
   affected-chain reset and full source re-walk under the intended
   endpoint-and-role configuration. Phase-runner does not persist endpoint
   history, so distinct current descriptors and the same-endpoint check cannot
   prove this temporal condition.
   Do not start the runner yet: deploy the part-2 binary and distinct secrets,
   validate the role-bearing configuration, perform the applicable reviewed
   reset, then run Sepolia through Verify before Live. Require
   `cross_checked`, confirm
   exactly one intake dRPC uses `ethereum_head` and start block zero, confirm
   only its cursor exists and the finalized Verify target is covered, and use
   provider/operator request accounting to confirm the verification-only key
   received zero Ingest/Live requests. For every affected chain, require the
   reviewed verification path rather than omitting or bypassing Verify. Under
   the source-role contract, other configurations use `cross_checked`
   with a distinct [verification-only](../glossary.md#source-role) dRPC,
   `node_checked` with a distinct verification-only Ethereum Mainnet reth, or
   `quick_synced` from the target-covering intake cursor without one;
10. confirm the phase state directly in the database while the API is still
   stopped — the `project` row in `chain_phase_state` current with no pending
   redo, and Verify success from the `verify` row for each affected chain plus
   the supervisor's Verify completion output (`/v1/status` cannot be used here
   because the API is stopped; after startup, the API accepts every known verification level at or above Sepolia's `quick_synced` floor and rejects unknown
   levels);
11. before starting the API, apply the explicit API-role `GRANT SELECT` inventory in [`deployment.md`](../deployment.md#surviving-services), including `account_permission_state_current`, and verify the configured login with `has_table_privilege`; then start the API built from the same commit and confirm `/v1/status` reports
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
curl -fsS http://127.0.0.1:3000/v1/status
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

The phase runner handles SIGTERM, which is what `docker compose stop` sends and
what `tini` forwards, so a stop is a clean stop rather than a kill. It observes
the request at the next batch boundary: the batch already in flight finishes
and commits, then the loop exits. Three cases exit nonzero on purpose. A stop that
lands while a chain is working through automatically required redo cannot leave
that redo looking finished, so the runner converts the cancellation into an
error, the supervisor records the chain as stopped, and the process exits
nonzero (`apps/phase-runner/src/runner.rs`, `apps/phase-runner/src/main.rs`).
That is the incomplete-redo signal, not a failed shutdown: the redo stamp
survives, the next start resumes it, and the exit code should not be read as
corruption. Expect it whenever you stop a runner mid-redo. The second case is a
stop that arrives while required work is blocked on its rows: start-up
settlement or stopped-phase recovery, or the writes that record a batch that
has already finished — its head publication, progress, completion, and
heartbeat, and the phase's completion or failure record. None of that is
abandoned, since the batch's own writes are already committed, but it is
given what is left of one ten-second stop budget and then reports a transient
error that the stopping run does not retry
(`apps/phase-runner/src/runner_chain.rs`, `bounded_recovery`;
`apps/phase-runner/src/runner_support.rs`, `StopClock`). The budget is per
phase attempt and per chain: an attempt's records and lock release share one
ten seconds that starts when the first of them observes the stop — after the
attempt's batch, which is not bounded, and independently of another chain's
or the paired phase's batch — and the chain's own waits that follow (fence
releases, the mismatch record, the marker reads that decide what a stopped
redo reports) share a second ten seconds of their own; within each budget the
waits draw it down in sequence rather than each taking ten seconds. Nothing is corrupted: for start-up work the next start
retries the same cleanup, and for a batch the durable state is the one a kill
between the batch and its progress write leaves, which the next start handles
the same way. Row contention is one cause — another process holding
`chain_phase_state` — but not the only one: the same deadline covers opening
the lock's own connection and waiting on the pool, so a stalled database or a
saturated pool reports the same way. Check connectivity before hunting for a
lock holder.

An explicit `phase-runner redo` exits nonzero on a stop for the same reason, but
it needs a different response. A stop during its setup, or at a batch boundary
once it is running, becomes an `InvalidTransition` error rather than a silent
success, so the incomplete redo cannot look finished
(`apps/phase-runner/src/runner_operator_redo.rs`, `prepared_for_redo`;
`apps/phase-runner/src/runner.rs`). Unlike the supervised runner there is no next
start to resume it. Which response is needed depends on how far it got, and
the error says which. A stop that wins before the command touched the database
exits clean and logs that the redo never started. A stop during or after
manifest synchronization exits nonzero and asks for a rerun even though no
redo was stamped, because synchronization is the command's first commit — a
changed manifest can retire derivation hashes or install required Ingest work —
and once the stop wins, whether that commit made it is not known from the
outside. A stop that wins before the redo was stamped but after the
manifests are known to be current reports that it was *cancelled before it
started* and that no unfinished redo was recorded: nothing blocks, nothing was
changed, and rerunning is a choice, not a repair. A stop after the stamp
exists reports the redo as *incomplete*: the
stamp survives and blocks the phase from normal restart until the command is
run again. That error says which command, built from the stamped mode and
range — `rerun \`phase-runner redo --chain <chain> --phase <phase>
--from-block <n> --to-block <n>\` with the chain's configured sources as --source
options …` — because the stamp records neither the sources, the verifier URL,
nor the hydration RPC, and the CLI or the Project phase rejects the bare
command without them: add back the `--source` options the chain runs with,
`--verification-database-url` when the phase is Verify or `all`, and
`--hydration-rpc` for the chain (or `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`)
whenever Project runs — Project, Interpret, `all`, and `recompute-flags`. A redo over several chains that is stopped between two of them exits
nonzero as well, reporting each chain it never started, since only a prefix
was redone and nothing was stamped for the rest; rerun the command for those
chains. Distinguish all of these from an exit `137`, which
is the grace period expiring into SIGKILL. The API's own stop path, its validated
`BIGNAME_API_STOP_GRACE_MS` bound, and what counts as graceful success are
documented under [Stop the API](#stop-the-api).

The runner therefore needs a stop grace period longer than one batch, and
Compose's 10s default is not that. `stop_grace_period` is set explicitly on the
`phase-runner` service and is tunable per deployment:

- `BIGNAME_PHASE_RUNNER_STOP_GRACE_PERIOD` (default `120s`) — it must cover
  the longest batch at this deployment's block range and hydration settings
  *plus* twenty seconds of stop budget: the batch in flight is not bounded,
  the attempt's own ten-second budget starts only when its settlement begins,
  and the chain-level waits that follow have ten seconds more, so a batch that
  finishes after 100 s under a 120 s grace leaves exactly the cleanup room
  the runner may use. Nothing in the runner bounds a batch's wall time, so the
  default is a starting value, not a derived limit. Each budget is a fixed
  ten seconds that the runner does not derive from this value, so a grace
  period at or below `10s` reaches SIGKILL before the first budget can report,
  and the bounded exit described above cannot happen. Compose accepts such a value without complaint.

A grace period that expires is a SIGKILL. Nothing is corrupted, but a batch is
not one transaction. Each phase commits its own writes before the runner
records progress: Project commits the projection swap
(`crates/project/src/engine.rs`) and may then commit a separate canonical-head
hydration transaction (`apps/phase-runner/src/project_phase.rs`); Interpret
commits its normalized-event and identity writes
(`crates/interpret/src/write.rs`); Ingest commits raw facts
(`crates/ingest/src/write/mod.rs`) and the runner then publishes chain heads.
Only after the phase returns does the runner write
`chain_phase_state.current_block_*` and the ingest source cursors
(`apps/phase-runner/src/runner.rs`). A kill inside any of those gaps leaves
committed phase output whose progress marker still points at the previous
batch. That is safe by design: the next start resumes from the durable marker
and re-executes the batch, and every write path is replay-safe — raw facts
insert if absent and are verified immutable, Interpret rows upsert on their
identities and fail closed on divergent data, and Project's publication is a
set-based delete-and-reinsert of the affected scope — so the redo costs the
batch's wall time plus fresh `observed_at` timestamps and a new hydration
attempt ordinal, and produces no duplicate or orphaned rows. The phase lock is
only released when PostgreSQL reaps the dead session, so the next start can
find the phase still held.

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
- `project` refuses a Mainnet or Sepolia projection with
  `dual_current_exact_name_authority` or `dual_current_child_authority` ->
  [follow the dual-current generation-failure runbook](dual-current-generation-failure.md),
  including its evidence-preserving child-failure escalation path.

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
   Pin the one-shot container to `<recovery-image>`. Copy every intake-capable source descriptor for the affected chain exactly from the deployed configuration and repeat `--source` once for each descriptor. The descriptor uses the
   `CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV` form. The explicit
   arguments override the multi-chain `BIGNAME_PHASE_RUNNER_SOURCES` value for
   this one-off redo:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never --use-aliases --service-ports phase-runner \
     phase-runner redo --chain <chain-id> --phase interpret \
     --from-block <from> --to-block <recorded-interpret-head> \
     --metrics-bind-addr 0.0.0.0:9465 \
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
   not delete or skip those markers. When an all-phase redo fails, follow every
   phase-specific recovery command that it reports in dependency order.
6. If no re-derivation is required, or after the required re-derivation
   boundary completes, [start or refresh
   services](#start-or-refresh-services).
7. [Verify health](#verify-health) before restoring traffic.

### Reorg and verification incidents

Use the bounded `phase-runner inspect` commands for stored lineage, block
canonicality, and raw-event evidence. Use `phase-runner rewind` only after
identifying an exact stored readable ancestor. If rewind reports an interrupted
Ingest redo whose retained end is above that ancestor, leave the retained state
intact. Complete the covering `phase-runner redo --chain <chain> --phase ingest
--from-block <retained-start> --to-block <retained-end>` command reported by the
refusal, using the same configured sources and deployment profile. A retained
checkpoint lets that repair resume without restarting the completed prefix.
After successful repair, retry the original rewind. Do not clear redo markers,
edit cursors, or lower the range end to force it through. Required Ingest work
keeps its documented Live recovery path when rewind moves its end above the
readable head; this refusal applies to operator Ingest redo.

Verification mismatches require
the chain-scoped repair procedure in
[`deployment.md`](../deployment.md#verification-mismatch-repair); do not edit
immutable raw facts or mark a phase complete manually.

### Rollback

Run `scripts/rollback-smoke` from the exact rollback checkout before changing
binaries. A binary rollback does not recreate dropped legacy tables. If the
rollback needs deleted schema or data, restore the verified pre-migration
backup under a separately reviewed database rollback plan.
For the Issue #411 Sepolia rollout, a binary-only rollback also cannot parse
the role-bearing source configuration or preserve its readiness semantics; use
the owner-approved rollback and restoration path required by the
[rollout gate](../deployment.md#owner-ratified-sepolia-source-role-rollout).

Keep the public edge on its maintainer-approved policy throughout rollback and
re-run `scripts/public-edge-smoke` before restoring traffic.

## Stop the API

```sh
docker compose --env-file .env.server -f docker-compose.server.yml stop api
```

Docker sends SIGTERM to Tini, which forwards it to the `exec`-replaced
`bigname-api serve` child. Docker waits `BIGNAME_API_STOP_GRACE_MS` (default
45000 ms) before SIGKILL. The same value is validated at API startup: it must
exceed the request timeout by at least 5000 ms. Raise both for longer requests;
external stop-timeout overrides must preserve this margin. Graceful success
requires the application's accepted-signal
log and exit 0. Exit 137 or 143, missing signal acceptance, or a grace overrun
is not graceful success.

Use disposable services for shutdown experiments, never the active production
API or database. Fixture and container tests are not rollout, restore, or
beta-launch evidence. The runner's stop command and `stop_grace_period` are
documented under [Pause and resume indexing](#pause-and-resume-indexing); the
container shutdown job checks the runner's rendered Compose stop contract but
drains only the API, so runner settlement, heartbeat, restart, and redo
behaviour under SIGTERM are not covered by that evidence.
