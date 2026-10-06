# Direct account history indexes

Product history reads owner-wide approvals for their recorded owner and operator, and reverse
activity for the claimed address. The three partial indexes key those exact participants on
activated, readable normalized events. They change no rows, catalogue encoding, phase marker,
or interpreter content-hash input; no Interpret replay or Project reset is required.

## Install and verify

Use the writer role in a schema-maintenance window with no concurrent DDL on these names.
Ordinary indexing transactions may continue; concurrent builds can wait for older transactions
and snapshots. Allow space for the indexes and build WAL, record database size/WAL position,
and inspect `pg_stat_progress_create_index` and long transactions before starting.

Run outside a transaction and retain the output, elapsed time and exit status:

```sh
psql -X -v ON_ERROR_STOP=1 -f ops/history-direct-account-indexes/install.sql "$BIGNAME_DATABASE_URL"
```

The installer uses a six-hour bound and checks existing objects before any build. It refuses
invalid, unready, misplaced or differently defined indexes instead of silently accepting a
matching name. It prints exact definitions and allocated bytes after installation. A repeat
adopts matching valid indexes. The read-only verifier is independently runnable:

```sh
psql -X -v ON_ERROR_STOP=1 -f ops/history-direct-account-indexes/verify.sql "$BIGNAME_DATABASE_URL"
```

Then apply `20261006160000_normalized_events_history_direct_account_indexes.sql` through the
normal schema-migration command. A populated table must already have every exact index; the
migration adopts them without a writer lock. Missing indexes on an empty table are built only
after an immediately acquired lock. A busy table fails with installation instructions. Before
phase initialization the migration is a no-op and the baseline installs the same indexes.

## Recovery

Neither the installer nor migration drops an existing object. After a failed concurrent build,
inspect the named object and confirm no build is still running. Drop only this operation's
invalid index, outside a transaction, then rerun installation:

```sql
DROP INDEX CONCURRENTLY bigname_phase.normalized_events_history_account_owner_idx;
-- Or the specific failed account_subject_idx / reverse_address_idx name reported.
```

Review valid indexes with an unexpected definition before replacing them. Resolve a table,
view, or another table's index occupying a name with its owner. Recovery never requires changing
normalized events, publication markers, or rebuild progress.
