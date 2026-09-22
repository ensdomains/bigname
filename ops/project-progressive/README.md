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

The ordinary schema-migrations cover empty/test installations and recognise indexes
prebuilt under the same names. They are not a substitute for the concurrent
prebuild on a large database. No index has been installed on Sepolia by this work.
The live deployment still uses the prior binary and schema. Deployment requires
its own reviewed migration, capacity and rollback checks.
