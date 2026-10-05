# Address-history catalogue indexes

`20261005170000_project_address_history_catalogue.sql` uses four replacement
indexes and two new indexes on `normalized_events`. Their final definitions are
unchanged from the fresh baseline. The replacement order serves bounded history
pages; the two new indexes serve noncanonical work discovery. This procedure
changes only access paths. The catalogue producer still requires the matching
binary's full-history Interpret and Project redos before the new API can serve.

On a populated database the migration refuses unless each required final index
already has its valid, ready, reviewed definition, or its replacement has been
prebuilt and validated. It never falls back to building the six indexes inside
the migration transaction. On an empty table it can build them normally.

## Order and capacity

First apply and record the preceding schema-migrations, following each version's
own concurrent-index instructions. In particular,
`20261003120000_normalized_events_record_id_attribution_indexes.sql` must be
recorded with its historical definition before this upgrade. Stop the migration
run at `--target-version 20261005160000`; do not apply the catalogue version yet.
Do not edit an already-recorded migration or its checksum.

When those versions are already recorded, the concurrent prebuild can run before
the planned service stop, while the old API and runner continue using the four
old indexes. If older versions still need their planned maintenance window,
complete them first. The final catalogue migration and replay remain within the
[planned deployment boundary](../../docs/runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).
The walk-index installer is for rebuilding its own set during a redo and does
not replace this pre-upgrade procedure.

Budget temporary disk space for all four replacement indexes beside the old
ones, the two new indexes, concurrent-build temporary files and WAL/replica
retention. The replacement keys are wider, so the existing indexes' sizes are
only a starting point; the general deployment free-space floor is not proof
that this additional space is available. Record the actual table/index sizes,
free space and replica lag before starting. Concurrent builds still consume
I/O, CPU and WAL; allow for their effect on normal indexing throughput.

## Prebuild and adoption

Use the schema owner/writer connection and a fresh `psql` session, outside any
transaction. Do not use `--single-transaction`:

```sh
psql -X -v ON_ERROR_STOP=1 \
  -f ops/address-history-catalogue-indexes/install.sql "$BIGNAME_DATABASE_URL"
```

The script validates all occupied candidate names before building anything.
Each missing index builds with `CREATE INDEX CONCURRENTLY`, with a six-hour
statement budget and no short lock timeout. It leaves the old four indexes
untouched. A rerun validates completed builds and builds only missing ones.
The script then validates the complete set, runs `ANALYZE`, and prints the index
identities, flags, definitions and sizes. Save its full output and exit status.

| Final index | Prebuilt name |
| --- | --- |
| `normalized_events_name_history_idx` | `ahc_name_prebuild_idx` |
| `normalized_events_resource_history_idx` | `ahc_resource_prebuild_idx` |
| `normalized_events_project_node_history_idx` | `ahc_node_prebuild_idx` |
| `normalized_events_record_id_write_idx` | `ahc_record_prebuild_idx` |
| `normalized_events_history_discovery_name_idx` | Same as final name |
| `normalized_events_history_discovery_resource_idx` | Same as final name |

Then stop the API, supervised runner and one-shot phase processes as required by
the planned boundary, and apply the catalogue migration through SQLx:

```sh
PGOPTIONS='-c lock_timeout=10s -c statement_timeout=10min' \
  sqlx migrate run --source migrations --database-url "$BIGNAME_DATABASE_URL" \
  --target-version 20261005170000
```

This is a budget for validation, metadata changes and creation of the empty
catalogue tables, not for full-table index builds. The migration stabilizes the
table while it checks every index. Only after all six pass does it drop the old
replacement names and rename their prebuilt indexes in the same transaction.
Those indexes retain their OIDs: they are adopted, not rebuilt. Already-correct
final indexes are retained; a verified redundant temporary candidate is removed.
No temporary replacement name remains after a successful adoption.

Confirm SQLx recorded the version and all six final indexes are valid and ready.
Apply the [API-role grants](../../docs/deployment.md#address-history-catalogue-role-upgrade),
then perform the documented combined-binary Interpret and Project redos. Keep
the new API unserved until its catalogue publication is complete. This index
procedure does not shorten or replace that required replay.

## Interrupted builds and refusal

An interrupted concurrent build can leave an invalid index under its intended
prebuilt name. The installer and migration refuse it. First confirm no build
for that index remains in `pg_stat_progress_create_index`, then drop only the
reported prebuilt index with `DROP INDEX CONCURRENTLY bigname_phase.<name>` and
rerun the installer. The same recovery applies to a candidate with the wrong
definition. Never drop one of the four valid old indexes to recover a temporary
candidate. If the candidate name belongs to a table or another table's index,
investigate and rename/remove that conflicting object before retrying.

A missing candidate on a populated table makes the migration fail before any
old-index drop. A lock timeout, statement timeout or disconnected migration
session rolls back the entire adoption; the old indexes and prebuilt candidates
remain available for the next attempt. Inspect the actual SQLx ledger and index
names after an uncertain outcome, rerun the installer to verify what exists,
and retry the same migration. Do not rename indexes manually or edit the ledger.
After successful adoption the installer is also safe to rerun: it accepts the
final definitions without creating replacement duplicates.
