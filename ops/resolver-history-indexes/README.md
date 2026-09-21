# Resolver-history indexes

The resolver-anchored event feed (`GET /v1/events?resolver=<chain>:<address>`,
`crates/storage/src/history/paging.rs`) reads the activated canonical
`ResolverChanged` events of one chain whose registry pointer moved to or from
one resolver address. Two partial expression indexes on `normalized_events`
serve that read, one per pointer side:

- `normalized_events_pointer_after_resolver_history_idx` and
  `normalized_events_pointer_before_resolver_history_idx`: activated canonical
  `ResolverChanged` events keyed by chain, then the lower-cased resolver the
  pointer moved to or from, then block, including the event id. The planner
  answers the feed with a `BitmapOr` over both; without them it scans every
  canonical `ResolverChanged` on the chain.

#415 (2026-08-14) added these two, and two more —
`normalized_events_permission_after_resolver_history_idx` and
`normalized_events_permission_before_resolver_history_idx`, keyed the same way
over `PermissionChanged` events on a resolver scope — to
`schema-v2/baseline/05_normalized_events.sql` with no schema-migration. Their
predicates name `consumer_visibility`, which
`20260811120000_ens_v2_migration_slice_1.sql` adds, so a database that took
slice 1 in place and was never replaced from the baseline has the column and
not the indexes. The `permission_*` pair has no reader whose predicate the
planner can match: Project's resolver scoping (`crates/project/src/scope`,
`crates/project/src/stage.rs`) derives the resolver address through a `CASE`
inside a lateral `VALUES` list, which no expression index serves, and nothing
else filters `PermissionChanged` by scope resolver. They are retired: dropped
by the schema-migration and removed from the baseline in the same change.
[ADR 0007](../../docs/adrs/0007-v1-schema-freeze.md) records both halves as
carve-out 6.

These change access paths only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing or dropping them.

Without the two kept indexes the feed still produces the same rows, but each
resolver-anchored page scans the chain's `ResolverChanged` history. On a large
initialized database, prebuild them and drop the retired pair using
`install.sql` with the writer role and
`psql -X -v ON_ERROR_STOP=1 -f install.sql` before applying the matching
schema-migration, as [`docs/deployment.md`](../../docs/deployment.md) lists. Do
not wrap it in a transaction. Concurrent creation and concurrent drop both
permit writes, but each can wait for an existing batch transaction; inspect
`pg_stat_progress_create_index` rather than restarting that batch. The script
permits that transaction wait and bounds each build to six hours. Retain its
output in the deployment receipt.

After both builds finish, run `ANALYZE bigname_phase.normalized_events` (or
confirm autovacuum has analyzed the table since). Both kept indexes key on a
`lower(...)` expression, and an expression index has no statistics until the
table is analyzed; without them the planner may keep scanning the chain's
`ResolverChanged` history instead of taking the `BitmapOr` over the pair. The
same applies when the schema-migration performs a build itself.

The script checks the kept names twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the feed cannot use. Both kept
predicates name `consumer_visibility`, so it first refuses a namespace whose
`normalized_events` lacks that column, naming
`20260811120000_ens_v2_migration_slice_1.sql` as the prerequisite, and one
that has no `normalized_events` at all; apply the schema-migrations through
slice 1 first, as the production runbook's step 3 describes, and never run
it on a fresh namespace, which takes the indexes from the baseline. Before it
builds anything, it refuses a kept name that is already taken by an index that
is not both `indisvalid` and `indisready`, an index on another table, an index
whose definition is not the reviewed one, or a table, view, or other relation
that is not an index, and a retired name held by anything but the index #415 built
(a table, an index on another table, an index with another definition).
Names that resolve to nothing pass this first check. After the builds
and drops it makes the same check and also requires both kept indexes to exist
and both retired names to resolve to nothing. It prints the index rows before
the last check, so the receipt shows the flags and definitions either way. The
definition is compared exactly as `pg_get_indexdef` prints it, read with
`search_path` set to `pg_catalog` and `quote_all_identifiers` off, so every
schema name is printed and nothing in the text has to be rewritten, with how
the fresh baseline index prints, so key order, expressions, JSON keys, and the
predicate are all covered. The check function carries its own settings, so the
session's are unchanged. On a mismatch the error prints the definition it
found beside the expected one.

An interrupted concurrent build, for example one cancelled or stopped by the
six-hour limit, leaves an invalid index under the intended name. `IF NOT EXISTS`
matches on the name alone, so it does not repair that index; rerunning the
script stops at the first check and names it. Nothing drops or rebuilds a kept
index automatically. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
the named index with `DROP INDEX CONCURRENTLY bigname_phase.<index name>`, as the
error's hint spells out, and rerun the script. Recover a valid index that fails
the definition check the same way. If a kept or retired name belongs to a
table, view, or another table's index, remove or rename that relation first.
Never drop a valid kept index merely because an installation was retried, and
never drop one while a phase runner is projecting. An interrupted concurrent
drop of a retired index leaves it invalid; rerunning the script drops it.

The matching versioned schema-migration
`20260924120000_normalized_events_resolver_history_idx.sql` is a no-op before
the phase schema exists. Where the table exists it builds each kept index that
is missing — as an ordinary, write-blocking `CREATE INDEX`, which is why a
large database prebuilds first — and, where a kept name is already taken,
applies the script's check under the same settings, put back before the block
returns, so the SQLx run fails rather than recording success if the name is
not an index on `bigname_phase.normalized_events`, is not valid and ready, or
does not have the reviewed definition; recover as described above, then run
the schema-migrations again. It then drops each retired index that still
exists with a plain `DROP INDEX`, which takes the table's exclusive lock for
the instant of the drop, and fails on a retired name held by anything but
that index: a table, an index on another table, or an index with another
definition is refused, not dropped. `schema-v2/apply-check.sh` proves each refusal for the script
and for the schema-migration, that the schema-migration builds both kept
indexes and drops both retired ones from the slice-1 and the #415 predecessor
shapes, and that the fresh baseline, the schema-migration, and the script
build the same definitions. Apply that schema-migration through the usual SQLx
release process when adopting this source revision. The fresh baseline
includes the two kept indexes and neither retired one.
