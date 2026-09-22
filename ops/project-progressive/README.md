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

The ordinary schema-migrations (`20260922010000_project_node_history_idx.sql` and
`20260922010100_project_mirror_scope_indexes.sql`) cover empty/test installations
and recognise indexes prebuilt under the same names. They are not a substitute for
the concurrent prebuild on a large database. Keep the `install.sql` and
`validate.sql` output with start and end times in the release record. Each
deployment still requires its own reviewed migration, capacity and rollback checks;
see [the production runbook](../../docs/runbooks/production-docker.md).
