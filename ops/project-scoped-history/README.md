# Registry handoff history indexes

This package builds two indexes on `bigname_phase.normalized_events`:

- `normalized_events_project_name_node_idx`, keyed by chain and the name key built from
  `after_state ->> 'node'`.
- `normalized_events_project_name_child_idx`, keyed by chain and the name key built from
  `after_state ->> 'child_node'`.

The name key is the namespace, a colon, and the lowercased node.

History's registry handoff read (`load_history_registry_handoffs` in
`crates/storage/src/history/registry_handoffs.rs`) is their reader. For one name it
looks up the owner events of that name in source family `ens_v1_registry_l1`:
`AuthorityTransferred` events by `node` and `SubregistryChanged` events by `child_node`.
Each lookup gives the chain and the name key, so it reads one index range instead of every
owner event of the chain.
[`docs/storage.md`](../../docs/storage.md#table-ownership) lists both indexes with that
reader.

Neither index restricts `consumer_visibility`. Both are limited to the relevant event kinds,
to readable canonicality states and to non-null keys.

The package first built eight indexes, for two Project selections that no longer exist.
No statement reads the other six. The schema-migration
`20261012130000_drop_unread_indexes.sql` drops them, and this installer no longer builds
them.

## Install

On an initialized database, prebuild the indexes with `psql -X -v ON_ERROR_STOP=1
-f ops/project-scoped-history/install.sql`, outside a transaction, before applying
the versioned schema-migrations.
[The production runbook](../../docs/runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
places this in step 3 of an upgrade. The concurrent builds permit writes but can
wait for an existing transaction. Inspect `pg_stat_progress_create_index`; do not
restart an indexing batch to satisfy the build. The script bounds each build to
thirty minutes. Record its output.

The script checks the two names twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the lookups cannot use. Before it
builds anything, it refuses a name that is already taken by an index that is not
both `indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. Names that resolve to nothing pass this first check. After the
builds it makes the same check and also requires both indexes to exist. It
prints the index rows before the last check, so the receipt shows the flags and
definitions either way. The definition is compared exactly as `pg_get_indexdef`
prints it, read with `search_path` set to `pg_catalog` and `quote_all_identifiers` off, so every schema name is
printed and nothing in the text has to be rewritten, with how the fresh baseline
index prints, so key order, expressions, JSON keys, the included column, and the
predicate are all covered. The check function carries its own settings, so
the session's are unchanged. On a mismatch the error prints the definition it
found beside the expected one.

An interrupted concurrent build, for example one cancelled or stopped by the
thirty-minute limit, leaves an invalid index under the intended name.
`IF NOT EXISTS` matches on the name alone, so it does not repair that index;
rerunning the script stops at the first check and names it. Nothing drops or
rebuilds an index automatically. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
the named index with `DROP INDEX CONCURRENTLY bigname_phase.<index name>`, as the
error's hint spells out, and rerun the script. Recover a valid index that fails
the definition check the same way. If the name belongs to a table, view, or
another table's index, remove or rename that relation first. Never drop a valid
index with the reviewed definition, and preserve the existing access paths.

This recovery covers the two indexes the script builds. The validity
schema-migration below can also name one of the other six. Drop that index the
same way, then build it with its statement from
`20260917131000_project_scoped_history_indexes.sql`. The script does not
build it.

## Schema-migrations

Three schema-migrations name these indexes. They apply in this order.

- `20260917131000_project_scoped_history_indexes.sql` builds all eight of the first
  package's indexes on an initialized database. It is a no-op before the phase schema
  exists. After a prebuild it adopts an index through `IF NOT EXISTS`, by name alone.
- `20260917161000_project_scoped_history_index_validity_check.sql` makes the script's final
  check, for all eight names. The SQLx run fails, without recording that version, if
  `bigname_phase.normalized_events` exists and one of them is missing, is not an index on
  that table, is not valid and ready, or does not have the reviewed definition. It changes
  nothing. Its error names `install.sql` for every one of the eight. For the six, use the
  statement in the first schema-migration instead, as [Install](#install) describes.
- `20261012130000_drop_unread_indexes.sql` drops the six that no statement reads.

A database that already recorded the first two applies only the third. A database that
applies all three in one run builds the six in the first and drops them in the third.

`scripts/check-schema` proves each refusal for the script and for the validity
schema-migration. It also proves that the first schema-migration and the script build the
same definitions for the two indexes.
`handoff_probes_are_indexed_by_chain_and_exact_node` in
`crates/storage/src/history/registry_handoffs_tests.rs` proves that each lookup reads its
index.
