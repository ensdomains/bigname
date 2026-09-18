# Resolver-history indexes

Project reads the `ResolverChanged` and `PermissionChanged` history of one
resolver address by chain, resolver and block when it scopes and rebuilds
`resolver_current` rows and their dependants (`crates/project/src/scope.rs`,
`crates/project/src/scope/resolver.rs`, `crates/project/src/stage.rs`). Four
partial expression indexes on `normalized_events` serve those reads, one per
event kind and pointer side:

- `normalized_events_pointer_after_resolver_history_idx` and
  `normalized_events_pointer_before_resolver_history_idx`: activated canonical
  `ResolverChanged` events keyed by chain, then the lower-cased resolver the
  pointer moved to or from, then block, including the event id.
- `normalized_events_permission_after_resolver_history_idx` and
  `normalized_events_permission_before_resolver_history_idx`: activated canonical
  `PermissionChanged` events on a resolver scope, keyed the same way, including
  the resource id.

#415 (2026-08-14) added all four to `schema-v2/baseline/05_normalized_events.sql`
with no schema-migration. Their predicates name `consumer_visibility`, which
`20260811120000_ens_v2_migration_slice_1.sql` adds, so a database that took
slice 1 in place and was never replaced from the baseline has the column and
not the indexes. [ADR 0007](../../docs/adrs/0007-v1-schema-freeze.md) records
them as carve-out 6.

These change access paths only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing them.

Without the indexes Project still produces the same rows, but each scoped
resolver read scans `normalized_events`. On a large initialized database,
prebuild the indexes using `install.sql` with the writer role and
`psql -X -v ON_ERROR_STOP=1 -f install.sql` before applying the matching
schema-migration, as [`docs/deployment.md`](../../docs/deployment.md) lists. Do
not wrap it in a transaction. Concurrent creation permits writes, but can wait
for an existing batch transaction; inspect `pg_stat_progress_create_index`
rather than restarting that batch. The script permits that transaction wait and
bounds each build to six hours. Retain its output in the deployment receipt.

The script checks all four names twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index Project cannot use. Before it builds
anything, it refuses a name that is already taken by an index that is not both
`indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. Names that resolve to nothing pass this first check. After the
builds it makes the same check and also requires all four indexes to exist. It
prints the index rows before the last check, so the receipt shows the flags and
definitions either way. The definition is compared exactly as `pg_get_indexdef`
prints it, read with `search_path` set to `pg_catalog` and
`quote_all_identifiers` off, so every schema name is printed and nothing in the
text has to be rewritten, with how the fresh baseline index prints, so key
order, expressions, JSON keys, and the predicate are all covered. The check
function carries its own settings, so the session's are unchanged. On a
mismatch the error prints the definition it found beside the expected one.

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
and never drop one while a phase runner is projecting.

The matching versioned schema-migration
`20260918120000_normalized_events_resolver_history_idx.sql` is a no-op before
the phase schema exists. Where the table exists it builds each index that is
missing — as an ordinary, write-blocking `CREATE INDEX`, which is why a large
database prebuilds first — and, where a name is already taken, applies the
script's check under the same settings, put back before the block returns, so
the SQLx run fails rather than recording success if the name is not an index
on `bigname_phase.normalized_events`, is not valid and ready, or does not have
the reviewed definition; recover as described above, then run the
schema-migrations again. `schema-v2/apply-check.sh` proves each refusal for
the script and for the schema-migration, that the schema-migration builds all
four from the slice-1 predecessor shape, and that the fresh baseline, the
schema-migration, and the script build the same definitions. Apply that
schema-migration through the usual SQLx release process when adopting this
source revision. The fresh baseline also includes all four indexes.
