# Registrar lease history index

Project's `storage:history.publication_memberships` statement checks whether each registry-only
binding's lease has any non-orphaned `ens_v1_registrar_l1` event. The shared token-holder history
matcher uses the same check. `normalized_events_history_registrar_lease_idx` indexes only
`resource_id` for that exact predicate, including observed events. It changes no evidence,
membership, phase marker, catalogue version or interpreter content-hash input.

This index requires no Interpret replay or Project reset. An existing v0.5.0 rebuild can
continue from its recorded progress, and its existing binary can use the index once the
build completes and subsequent statements are planned. An already-running statement retains
its existing plan. Installation does not authorize restarting or cancelling that statement.

## Install

Use the writer role and an exclusive schema-maintenance window: no concurrent index DDL on
this name. Ordinary Project and Interpret transactions may continue. Before starting, allow
space for the index and build WAL, record database size and WAL position where available,
and inspect long transactions and `pg_stat_progress_create_index`. Concurrent creation can
wait for an old Project transaction or snapshot as well as consuming disk and I/O.

Run outside a transaction, retaining complete output, elapsed time and exit status:

```sh
psql -X -v ON_ERROR_STOP=1 -f ops/history-registrar-lease-index/install.sql "$BIGNAME_DATABASE_URL"
```

The installer bounds the build and its waits to six hours. It validates existing objects
before creation and checks the resulting index is valid, ready, on the expected table,
and has the exact reviewed definition. A matching repeat succeeds without rebuilding.
It prints the resulting definition, validity flags and index size. Record the post-build
database size and WAL position as well; concurrent ordinary writes contribute to those deltas.

Then apply `20261006080000_normalized_events_history_registrar_lease_index.sql` through the
normal schema-migration command. On populated schemas the migration requires this prebuild
and adopts it without rebuilding. A missing index on an empty table is built under an
immediately acquired table lock; a busy table fails and directs the operator to this installer.
Before phase initialization, the migration is a no-op and the baseline supplies the index.
The [walk index set](../walk-index-set/README.md) drops and restores it with the other indexes
that only Project and serving reads use.

## Recovery

A failed or interrupted concurrent build may leave an invalid index. The installer and
migration refuse invalid, unready, wrongly defined or misplaced objects. Neither drops them.
First inspect the named object and confirm in `pg_stat_progress_create_index` that no build
is still running. For an invalid index owned by this operation on `normalized_events`, use
this explicit recovery outside a transaction, then rerun installation:

```sql
DROP INDEX CONCURRENTLY bigname_phase.normalized_events_history_registrar_lease_idx;
```

For a valid index with the wrong definition, review why it exists before choosing the same
replacement. If another table's index, a table or a view occupies the name, resolve that
collision with its owner; do not use the recovery command blindly. Retain failure receipts.
No recovery here requires changing rows, stamps or rebuild progress.
