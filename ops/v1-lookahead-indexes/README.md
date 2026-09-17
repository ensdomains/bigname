# ENSv1 lookahead loader indexes

Interpret's [lookahead loader](../../docs/glossary.md#lookahead-loader) restores
prior adapter state for one batch by reading only the history of the names and
resources that batch can touch. It is chosen automatically for a chain whose
manifests all belong to ENSv1 source families (see
[`docs/deployment.md`](../../docs/deployment.md)). Two partial expression indexes
on `normalized_events` serve its reads:

- `normalized_events_v1_direct_node_probe_idx` selects every readable ENSv1 event
  of one name, keyed by chain, then `namespace:namehash`, then block. A registry
  `NewOwner` is filed under the child it creates, not under its parent, so reading
  a parent never enumerates its children. An event with no name field is filed
  under its logical name. The expression must stay identical to
  `crates/interpret/src/load/lookahead/events.sql`.
- `normalized_events_v1_due_probe_idx` selects registrar grants, renewals and
  token transfers by chain and parsed expiry, so Interpret can find registrations
  whose expiry plus the 90-day grace period falls inside a batch. The expression
  parses any stored expiry without raising and must stay identical to
  `crates/interpret/src/load/lookahead/due_names.sql`. The same query also reads
  the block just before the batch through `normalized_events_chain_block_number_idx`,
  for registrar events that recorded an already-lapsed expiry. That query keeps both
  expiry bounds as index conditions in PostgreSQL's generic prepared plan; a
  mainnet read-only comparison returned the same 17 names in 7.2 milliseconds
  instead of 40.2 seconds.

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
six hours, because both indexes cover most of a mainnet `normalized_events`
table. Retain its output in the deployment receipt.

Before treating the step as complete, require exactly two index rows, both with
`indisvalid` and `indisready` true, and the expected definitions. `IF NOT EXISTS`
does not fix an invalid index from an interrupted concurrent build. If one of
these two indexes is invalid and no build is running, drop only it with
`DROP INDEX CONCURRENTLY bigname_phase.<index name>`, then rerun the installation
and validity checks. Never drop a valid index merely because an installation was
retried, and never drop one while a runner that uses the lookahead loader is
processing batches.

After both builds finish, run `ANALYZE bigname_phase.normalized_events` (or
confirm autovacuum has analyzed the table since). Expression indexes have no
statistics until the table is analyzed, and the loader's queries depend on them:
in a test database without statistics, reading the history of 100,000 names did
not finish in several minutes, and took under four seconds after `ANALYZE`.

The matching versioned schema-migration installs the same definitions on
initialized databases; after a live prebuild, its `IF NOT EXISTS` is a no-op.
Apply that schema-migration through the usual SQLx release process when adopting
this source revision. The fresh baseline also includes both indexes.

An index with either name built from an earlier experimental script is kept only
if `pg_get_indexdef` matches the definition here; otherwise drop and rebuild it
as above.
