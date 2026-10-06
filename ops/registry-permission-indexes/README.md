# Registry permission history indexes

These five read-only indexes serve exact factory-origin, ordinary-announcement, sparse
unsupported-upgrade and parent root-grant checks for WrapperRegistry permissions. Four index
`bigname_phase.normalized_events`; `project_grant_registry_parent_idx` indexes
`bigname_phase.project_grant`. They add no Project table or derived holder rows and
rewrite no event. All use their final names and must be valid, ready, live, on their
reviewed target relation, and have the reviewed definitions before a populated
upgrade can apply `20261005200000_registry_permission_history_indexes.sql`.

Before applying that migration on an initialized database, complete preceding migrations,
reserve disk for the five final indexes plus temporary build space and WAL, and run:

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
takes short write-blocking locks on the two target tables, validates the complete set and adopts it without
rebuilding or changing index OIDs. A populated target table requires its own indexes even when the other target is
empty. Any missing required member or invalid definition refuses the migration
before index creation. Empty and migration-first databases keep the
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
