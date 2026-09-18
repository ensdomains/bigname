# ENSv1 lookahead loader indexes

Interpret's [lookahead loader](../../docs/glossary.md#lookahead-loader) restores
prior adapter state for one batch by reading only the history of the names and
resources that batch can touch. It is chosen automatically for a chain whose
`active` and `deprecated` manifests all belong to source families it covers and
whose retained history holds no other family (see
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
  expiry bounds as index conditions in PostgreSQL's generic prepared plan. That
  was measured on the #903 experiment, before this release rewrote the
  installer and its checks: a mainnet read-only comparison returned the same
  17 names in 7.2 milliseconds instead of 40.2 seconds. The figure has not been
  re-measured on the current tree.

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

The script checks both names twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the loader cannot use. Before it
builds anything, it refuses a name that is already taken by an index that is not
both `indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. Names that resolve to nothing pass this first check. After the
builds it makes the same check and also requires both indexes to exist. It
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

After both builds finish, run `ANALYZE bigname_phase.normalized_events` (or
confirm autovacuum has analyzed the table since). Expression indexes have no
statistics until the table is analyzed, and the loader's queries depend on them:
in a test database without statistics, reading the history of 100,000 names did
not finish in several minutes, and took under four seconds after `ANALYZE`.

The matching versioned schema-migration
`20260917150000_normalized_events_v1_lookahead_indexes.sql` installs the same
definitions on initialized databases and is a no-op before the phase schema
exists; after a live prebuild, its `IF NOT EXISTS` is a no-op that adopts the
indexes by name alone. It therefore ends with the script's final check, read
under the same settings and put back before the block returns, so the SQLx run
fails rather than recording success if either name is not an index on
`bigname_phase.normalized_events`, is not valid and ready, or does not have the
reviewed definition; recover as described above, then run the schema-migrations
again. `schema-v2/apply-check.sh` proves each refusal for the script and for
the schema-migration, and that the fresh baseline, the schema-migration, and
the script build the same definitions. Apply that schema-migration through the
usual SQLx release process when adopting this source revision. The fresh
baseline also includes both indexes.

An index with either name built from an earlier experimental script is kept only
if `pg_get_indexdef` matches the definition here; otherwise drop and rebuild it
as above.
