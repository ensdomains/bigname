# Project scope indexes

These indexes support scoped node history and progressive mirror dependency
traversal. They do not change normalized events, projection contents or admission.

For an existing large database, run `install.sql` through an autocommit SQL client
against the intended database before starting a binary containing the accompanying
schema-migrations. Concurrent index builds cannot run inside a transaction.
Then run `validate.sql` and compare `pg_get_indexdef` with `install.sql` for all five
indexes. An existing name is not proof of a valid or matching index. Stop on any
failure; an interrupted concurrent build can leave an invalid index and must be
reviewed before an explicitly authorized retry.

The mirror walk looks up the surface of each label suffix of a name (the name
itself, then each ancestor below the root). `name_surfaces_project_suffix_hash_idx`
serves that lookup on `(namespace, hash_array_extended(raw_labels, 0))` instead of
on the label array itself. Labels come from chain data and have no length limit,
and a btree entry cannot exceed about 2.7 KB, so indexing the array would make
the `name_surfaces` insert fail for a long enough name. The 64-bit hash has a fixed
size; the query compares the hash and then the exact label array, so a hash
collision never changes the result. `hash_array_extended` is a built-in immutable
PostgreSQL function (the array-to-text functions are not immutable and cannot be
indexed); the query spells the same expression so the planner can use the index.

An earlier version of this package built `name_surfaces_project_suffix_idx` on
`(namespace, raw_labels)`. `install.sql` drops that index concurrently before it
builds the hash index, and `validate.sql` fails while it still exists or when the
hash index has another definition. The schema-migration drops it too, which is an
ordinary (blocking, but quick) index drop when `install.sql` was not rerun first.

The ordinary schema-migrations (`20260922010000_project_node_history_idx.sql` and
`20260922010100_project_mirror_scope_indexes.sql`) cover empty/test installations
and recognise indexes prebuilt under the same names. They are not a substitute for
the concurrent prebuild on a large database. Keep the `install.sql` and
`validate.sql` output with start and end times in the release record. Each
deployment still requires its own reviewed migration, capacity and rollback checks;
see [the production runbook](../../docs/runbooks/production-docker.md).
