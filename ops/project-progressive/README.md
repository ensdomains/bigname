# Project scope indexes

These indexes support scoped node history and progressive mirror dependency
traversal. They do not change normalized events, projection contents or admission.

## Release sequence

On an existing large database, in this order:

1. **Install, while the current runner is still processing.** Run `install.sql`
   with `psql -X -v ON_ERROR_STOP=1` against the intended database, outside any
   transaction (concurrent index builds cannot run inside one). It refuses before
   building anything when one of the five names is already taken by an invalid
   index, an index on another table, a relation that is not an index, or (for the
   two label-hash indexes and the `label_hashes` function) another definition. It
   then builds whatever is missing, runs `ANALYZE bigname_phase.name_surfaces`
   (expression indexes have no statistics until then, and the planner needs them
   to choose the label-hash indexes), and runs the same check again, requiring
   everything to exist. It never drops an index. The `ANALYZE` runs again on every
   rerun of the script.
2. **Validate.** Run `validate.sql`. It fails unless all five indexes are valid and
   ready on their own tables, and the two label-hash indexes and `label_hashes` have their reviewed
   definitions. It allows the earlier label-array indexes to exist, because the
   running binary still uses them. Also compare the other three `pg_get_indexdef`
   outputs with `install.sql`; an existing name is not proof of a matching index.
3. **Stop the runner**, as the production runbook's release steps describe.
4. **Update the recorded checksum of `20260922010100`, only where it is recorded.**
   See [Recorded checksum of 20260922010100](#recorded-checksum-of-20260922010100).
   Skip this step on a database that has not recorded that version.
5. **Apply the schema-migrations.**
   `20260923140000_project_name_surfaces_label_indexes.sql` drops the earlier
   label-array indexes (a quick, ordinary drop inside the stop window), finds the
   prebuilt hash indexes by name, and refuses a function or index under a reviewed
   name with another definition.
6. **Start the new binary**, after the full re-walk described below.
7. **Validate after the switch.** Run `validate.sql` again, then
   `validate-after-switch.sql`, which fails while either earlier label-array index
   still exists.

Stop on any failure. An interrupted concurrent build leaves an invalid index that
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` would skip by name. To recover, confirm
in `pg_stat_progress_create_index` that no build is still running, drop only the
index the error names with `DROP INDEX CONCURRENTLY`, and rerun `install.sql`.
Never drop a valid index with the reviewed definition. `bigname_phase.label_hashes`
is never replaced: if it has another definition, no index can depend on the
reviewed one yet, so drop it (and any index that depends on it) and rerun
`install.sql`, after review.

## Recorded checksum of 20260922010100

An earlier version of `20260922010100_project_mirror_scope_indexes.sql` also built
the two whole-array label indexes. The file no longer builds them, because an
upgrade must never build an index whose entries can exceed the btree or GIN entry
limit, so its checksum changed. sqlx stores the SHA-384 of each applied migration
file in `_sqlx_migrations.checksum` and refuses to run (`sqlx migrate run`, and the
binary at startup) when a recorded checksum differs from the file. The schema that
the earlier version applied is otherwise the same, and
`20260923140000_project_name_surfaces_label_indexes.sql` drops the two indexes it
built. So, before the schema-migrations in step 5, on a deployment that already
recorded `20260922010100` (Sepolia), confirm the earlier checksum:

```sql
SELECT encode(checksum, 'hex') FROM _sqlx_migrations WHERE version = 20260922010100;
-- expected: de4b8fb9bd900be8a4524f26f41deb3557d2cd04cc77309a2a1ebddf45769679e0b9be22f4a8a9bbc71ec3601b25c6be
```

then record the current file's checksum:

```sql
UPDATE _sqlx_migrations SET checksum = decode('ba87c9cfc8c0ff508240e4e31d0038512dcdf07dce55cb638fabe4936785f7e084b907a7b7d91ea91bc0320824f0ac63', 'hex') WHERE version = 20260922010100 AND checksum = decode('de4b8fb9bd900be8a4524f26f41deb3557d2cd04cc77309a2a1ebddf45769679e0b9be22f4a8a9bbc71ec3601b25c6be', 'hex');
```

It must report `UPDATE 1`. Stop if the first query shows any other value.
`schema-v2/apply-check.sh` proves that the new checksum here and in the production
runbook is the SHA-384 of the file as checked in.

## Full re-walk

This release changes Project SQL under `crates/project/src/scope/`, which is a
covered input of the
[interpreter content hash](../../docs/glossary.md#interpreter-content-hash). The
new binary's hash therefore differs from the running one's, and it must re-walk
the complete retained range before normal derived writes continue. Plan the stop
window for that re-walk and record the new hash in the release record.

## Label-hash indexes

Two of the indexes serve lookups by label on `name_surfaces` for the
[mirror walk and mirror seeds](../../docs/glossary.md#mirror-walk-and-mirror-seed).
Labels come from chain data and have no length limit, and an index entry larger
than about 2.7 KB makes the `name_surfaces` insert fail, so neither index stores
label text:

- `name_surfaces_project_label_hashes_idx` is a GIN index on
  `bigname_phase.label_hashes(raw_labels)`, one 64-bit hash per label. The seed
  lookup finds the names whose labels contain a seed's labels with
  `label_hashes(raw_labels) @> label_hashes(seed.raw_labels)`.
- `name_surfaces_project_suffix_hash_idx` is a btree on
  `(namespace, hash_array_extended(raw_labels, 0))`, one 64-bit hash of the whole
  array. The walk looks up the surface of each label suffix of a name (the name
  itself, then each ancestor below the root) by that hash.

Both queries also compare the label arrays themselves (`raw_labels @> ...` and
`raw_labels = ...`), so a hash collision never changes a result; the hashes only
narrow the rows the index returns. `hash_array_extended` and `hashtextextended`
are built-in immutable PostgreSQL functions, and `label_hashes` is an immutable
SQL function over `hashtextextended` that `install.sql` and the schema-migration
create. The array-to-text functions are not immutable and cannot be indexed. The
queries spell the same expressions as the indexes so the planner can use them.

An earlier version of this package built `name_surfaces_project_labels_idx` (GIN
on `raw_labels`) and `name_surfaces_project_suffix_idx` (btree on
`(namespace, raw_labels)`). Only the running binary's lookups can use them, so
they stay until the schema-migration drops them in step 5.

## Schema-migrations

The ordinary schema-migrations (`20260922010000_project_node_history_idx.sql`,
`20260922010100_project_mirror_scope_indexes.sql` and
`20260923140000_project_name_surfaces_label_indexes.sql`) cover empty/test
installations and recognise indexes prebuilt under the same names. They are not a
substitute for the concurrent prebuild on a large database. Keep the `install.sql`,
`validate.sql` and `validate-after-switch.sql` output with start and end times in
the release record. Each deployment still requires its own reviewed migration,
capacity and rollback checks; see
[the production runbook](../../docs/runbooks/production-docker.md).
