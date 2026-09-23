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

Two of the indexes serve lookups by label on `name_surfaces`. Labels come from
chain data and have no length limit, and an index entry larger than about 2.7 KB
makes the `name_surfaces` insert fail, so neither index stores label text:

- `name_surfaces_project_label_hashes_idx` is a GIN index on
  `bigname_phase.label_hashes(raw_labels)`, one 64-bit hash per label. The mirror
  seed lookup finds the names that contain a seed's labels with
  `label_hashes(raw_labels) @> label_hashes(seed.raw_labels)`.
- `name_surfaces_project_suffix_hash_idx` is a btree on
  `(namespace, hash_array_extended(raw_labels, 0))`, one 64-bit hash of the whole
  array. The mirror walk looks up the surface of each label suffix of a name (the
  name itself, then each ancestor below the root) by that hash.

Both queries also compare the label arrays themselves (`raw_labels @> ...` and
`raw_labels = ...`), so a hash collision never changes a result; the hashes only
narrow the rows the index returns. `hash_array_extended` and `hashtextextended`
are built-in immutable PostgreSQL functions, and `label_hashes` is an immutable
SQL function over `hashtextextended` that `install.sql` and the schema-migration
create. The array-to-text functions are not immutable and cannot be indexed. The
queries spell the same expressions as the indexes so the planner can use them.

An earlier version of this package built `name_surfaces_project_labels_idx`
(GIN on `raw_labels`) and `name_surfaces_project_suffix_idx` (btree on
`(namespace, raw_labels)`). `install.sql` builds the two hash indexes first and
then drops the old two concurrently, so the lookups always have an index.
`validate.sql` fails while either old index exists, when `label_hashes` has
another definition, or when either hash index has another definition.
`20260923140000_project_name_surfaces_label_indexes.sql` does the same on the
schema-migration path: it drops the old indexes (an ordinary, quick drop when
`install.sql` was not rerun first), builds any missing hash index, and refuses a
function or index under the reviewed name with another definition.

The ordinary schema-migrations (`20260922010000_project_node_history_idx.sql`,
`20260922010100_project_mirror_scope_indexes.sql` and
`20260923140000_project_name_surfaces_label_indexes.sql`) cover empty/test installations
and recognise indexes prebuilt under the same names. They are not a substitute for
the concurrent prebuild on a large database. Keep the `install.sql` and
`validate.sql` output with start and end times in the release record. Each
deployment still requires its own reviewed migration, capacity and rollback checks;
see [the production runbook](../../docs/runbooks/production-docker.md).
