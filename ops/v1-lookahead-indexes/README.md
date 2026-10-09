# Lookahead loader indexes

The directory keeps its original name, but the indexes now serve the ENSv1
and ENSv2 families on Ethereum and the Basenames Base families on Base.

Interpret's [lookahead loader](../../docs/glossary.md#lookahead-loader) restores
prior adapter state for one batch by reading only the history of the names and
resources that batch can touch. It is chosen automatically for a chain whose
`active` and `deprecated` manifests all belong to source families it covers and
whose retained history holds no other family (see
[`docs/deployment.md`](../../docs/deployment.md)). Eight partial indexes on
`normalized_events` serve its reads: two per ENSv1-model family group, and four
for ENSv2:

- `normalized_events_v1_direct_node_probe_idx` selects every readable ENSv1 event
  of one name, keyed by chain, then `namespace:namehash`, then block. A registry
  `NewOwner` is filed under the child it creates, not under its parent, so reading
  a parent never enumerates its children. An event with no name field is filed
  under its logical name. The expression must stay identical to
  `crates/interpret/src/load/lookahead/events.sql`.
- `normalized_events_v1_due_probe_idx` selects registrar grants, renewals and
  token transfers by chain and parsed expiry, so Interpret can find registrations
  whose expiry plus the 90-day grace period falls inside a batch
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f). The expression
  parses any stored expiry without raising and must stay identical to
  `crates/interpret/src/load/lookahead/due_names.sql`. The same query also reads
  the block just before the batch through `normalized_events_chain_block_number_idx`,
  for registrar events that recorded an already-lapsed expiry. That query keeps both
  expiry bounds as index conditions in PostgreSQL's generic prepared plan. That
  was measured on the #903 experiment, before this release rewrote the
  installer and its checks: a mainnet read-only comparison returned the same
  17 names in 7.2 milliseconds instead of 40.2 seconds. The figure has not been
  re-measured on the current tree.
- `normalized_events_basenames_direct_node_probe_idx` and
  `normalized_events_basenames_due_probe_idx` have the same expressions over the
  `basenames_base_*` families and the `basenames_base_registrar` family. The
  Basenames Base registrar has the same 90-day grace period
  (upstream: .refs/basenames/src/util/Constants.sol:L15 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L296 @ basenames@1809bbc).
  On Ethereum these two hold no rows, and on Base the ENSv1 two hold none, but
  every lookahead chain runs both arms of each query, so every database that
  runs the lookahead loader needs all four of these name probes.
- `normalized_events_v2_direct_node_probe_idx` has the same name expression
  over the `ens_v2_*` families: it finds an ENSv2 name's events, such as the
  registrations, transfers and ENSv1→ENSv2 migrations that name it.
- `normalized_events_v2_key_probe_idx` is an inverted (GIN) index over the
  [ENSv2 state keys](../../docs/glossary.md#ensv2-state-key) each ENSv2 event is
  filed under: its registry or resolver with the token, resource and label it
  names, the whole registry, and the registry its `subregistry` value names. The
  array must stay identical to
  `crates/interpret/src/load/lookahead/v2_keys.sql`. The loader probes it once
  per requested key. The planner does not use the statistics of a partial
  expression index, so a single test for all keys would be costed by the key
  count alone.
  Without this index, each requested key scans `normalized_events`.
- `normalized_events_v2_due_probe_idx` selects ENSv2 registry and root registry
  events by chain and parsed expiry, with the same expiry expression as the
  due-name probes, so Interpret can load the tokens whose expiry falls inside a
  batch. It must stay identical to
  `crates/interpret/src/load/lookahead/v2_due_keys.sql`.
- `normalized_events_v2_lookahead_probe_idx` reads the latest ENSv2 registry
  event of a chain before a block, keyed by chain then block, for the
  timestamp a full restore would reach
  (`crates/interpret/src/load/lookahead/v2_latest_topology.sql`).

Only a chain with an ENSv2 manifest runs the ENSv2 key, due and latest-event
reads, but every lookahead chain runs the ENSv2 name arm, so every database
that runs the lookahead loader needs all eight indexes.

These change access paths only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing them.

Without the indexes the loader still returns correct state, but every batch
scans `normalized_events`, so install them before starting a release that
contains the loader on a chain that will choose it. To run such a release
without them, set `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true`.

For an initialized database, prebuild the indexes using `install.sql` with the
writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`. Do not wrap it in a
transaction. Concurrent creation permits writes, but can wait for an existing
batch transaction; inspect `pg_stat_progress_create_index` rather than restarting
that batch. The script permits that transaction wait and bounds each build to
six hours, because each pair covers most of its chain's `normalized_events`
rows. A database that already holds some of the indexes from an earlier
release reruns the same script: it accepts them unchanged and builds only the
missing ones. Retain its output in the deployment receipt.

The script checks every name twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the loader cannot use. Before it
builds anything, it refuses a name that is already taken by an index that is not
both `indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. Names that resolve to nothing pass this first check. After the
builds it makes the same check and also requires every index to exist. It
prints the index rows before the last check, so the receipt shows the flags and
definitions either way. The definition is compared exactly as `pg_get_indexdef`
prints it, read with `search_path` set to `pg_catalog` and
`quote_all_identifiers` off, so every schema name is printed and nothing in the
text has to be rewritten, with how the fresh baseline index prints, so key
order, expressions, JSON keys, and the predicate are all covered. PostgreSQL 16
prints the expiry `CASE` expression over several indented lines, and the
expected text keeps them. The check function carries its own settings, so the
session's are unchanged. On a mismatch the error prints the definition it found
beside the expected one.

An interrupted concurrent build, for example one cancelled or stopped by the
six-hour limit, leaves an invalid index under the intended name. `IF NOT EXISTS`
matches on the name alone, so it does not repair that index; rerunning the
script stops at the first check and names it. Nothing drops or rebuilds an
index automatically. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
the named index with `DROP INDEX CONCURRENTLY bigname_phase.<index name>`, as the
error's hint spells out, and rerun the script. Recover a valid index that fails
the definition check the same way. If the name belongs to a table, view, or
another table's index, remove or rename that relation first. Never drop a valid
index with the reviewed definition merely because an installation was retried,
and never drop one while a runner that uses the lookahead loader is processing
batches.

After the builds finish, run `ANALYZE bigname_phase.normalized_events` (or
confirm autovacuum has analyzed the table since). The loader's queries depend on
the table's column statistics, which exist only once the table is analyzed: in a
test database without them, reading the history of 100,000 names did not finish
in several minutes, and took under four seconds after `ANALYZE`.

The matching versioned schema-migrations
`20260917150000_normalized_events_v1_lookahead_indexes.sql` (ENSv1),
`20261001120100_normalized_events_basenames_lookahead_indexes.sql` (Basenames
Base) and `20261001130000_normalized_events_v2_lookahead_indexes.sql` (ENSv2)
each install the same definitions of their indexes on initialized databases
and are no-ops before the phase schema exists; after a live prebuild, their
`IF NOT EXISTS` is a no-op that adopts the indexes by name alone. Each therefore
ends with the script's final check, read
under the same settings and put back before the block returns, so the SQLx run
fails rather than recording success if any of its names is not an index on
`bigname_phase.normalized_events`, is not valid and ready, or does not have the
reviewed definition; recover as described above, then run the schema-migrations
again. `scripts/check-schema` proves each refusal for the script and for
each schema-migration, and that the fresh baseline, the schema-migrations, and
the script build the same definitions. Apply them through the usual SQLx
release process when adopting this source revision. The fresh baseline also
includes all eight indexes.

`20261009120000_normalized_events_v2_key_probe_subregistry.sql` replaces
`normalized_events_v2_key_probe_idx` with the definition here, which adds the
`subregistry` element (TYR-277). It drops the index only when it is valid and has
the definition the ENSv2 schema-migration installed, then builds the new one inside
the SQLx transaction. The drop holds an `ACCESS EXCLUSIVE` lock until that
transaction commits, so reads and writes of `normalized_events` wait for the build. It
ends with the same check and refuses any other relation or definition under the
name. On a large initialized database, replace the index before applying
schema-migrations. These steps need `20261001130000` applied. Where it is still
pending, first apply the schema-migrations through it with its own procedure:
run the `install.sql` of v0.2.0, or of any later release that predates the new
definition, then apply the schema-migrations with
`--target-version 20261001130000`. The current script builds the new definition,
which `20261001130000` refuses, so running it first stops the upgrade at that
schema-migration, and the recovery above does not get past that check. Then:

1. Stop every runner that uses the lookahead loader.
2. Run `DROP INDEX CONCURRENTLY bigname_phase.normalized_events_v2_key_probe_idx`.
3. Run `install.sql`. It builds the new definition and accepts the other seven.

The schema-migration then adopts the prebuilt index. A release that predates the
new definition cannot use the replaced index, so do not restart old runners on it
without `BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER=true`.

An index with any of these names built from an earlier experimental script is kept only
if `pg_get_indexdef` matches the definition here; otherwise drop and rebuild it
as above.
