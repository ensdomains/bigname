# Registry permission history indexes

These three read-only indexes serve exact factory-origin and sparse unsupported-upgrade
checks for WrapperRegistry permissions. They add no Project state and rewrite no event.
The indexes use their final names; all three must be valid, ready, live, on
`bigname_phase.normalized_events`, and have the reviewed definitions before a populated
upgrade can apply `20261005200000_registry_permission_history_indexes.sql`.

Before applying that migration on an initialized database, complete preceding migrations,
reserve disk for the three final indexes plus temporary build space and WAL, and run:

```sh
psql -X -v ON_ERROR_STOP=1 "$DATABASE_URL" -f ops/registry-permission-indexes/install.sql
```

Run outside a transaction. The installer validates every occupied name before building any
missing index, then builds missing indexes sequentially with `CONCURRENTLY`. Normal writers
can continue during the prebuild; long transactions can delay its completion. The script
sets a six-hour statement timeout and does not impose a short lock-wait timeout. Monitor
`pg_stat_progress_create_index`, free space and WAL/replication lag while it runs. Sparse
indexes still scan the heap during construction; final index size alone is not peak disk
headroom. Save the printed definitions, state flags, sizes and elapsed build times.

After prebuild, apply the release's ordered schema migrations with the API, phase runner
and redo processes stopped, following the normal planned migration boundary. The migration
takes a short write-blocking table lock, validates the complete set and adopts it without
rebuilding or changing index OIDs. On a populated table, any missing or invalid member
refuses the migration before index creation. Empty and migration-first databases keep the
ordinary baseline path. The release's independent factory-origin retention and manifest
metadata changes still require the documented full Interpret/Project redo.

A rerun is safe when the indexes are healthy. An interrupted concurrent build can leave an
invalid index, which both installer and migration refuse. Inspect `pg_index`,
`pg_get_indexdef` and `pg_stat_progress_create_index` first; confirm no build is active.
For only the identified invalid or wrong index, run `DROP INDEX CONCURRENTLY` outside a
transaction, then rerun the installer. If a table or other relation occupies a required
name, inspect its ownership and purpose before renaming/removing that conflicting relation.
The scripts never remove it automatically. Preserve all healthy indexes. A failed
migration rolls back; recover the prebuild and retry the same migration.
