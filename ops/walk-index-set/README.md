# Walk index set

Every Interpret batch inserts its normalized events into `normalized_events`, and PostgreSQL
adds each new row to every index on that table whose predicate the row matches. On a large
database that upkeep is a large share of each insert, because the keys (block hashes, nodes,
names, addresses) arrive in random order and each new entry lands on a different index page.
Most of those indexes serve the API's history and event pages or Project's reads, and nothing
reads them while Interpret walks the chain: Project starts only after Interpret completes, and
the API refuses the routes that read them while an Interpret redo is in progress.

Two readers stay available while the set is dropped and read without it, so on a large
database they read far more of the table. The event audit, `GET /v1/diagnostics/events`, stays
available during a redo; its record attribution reads six of the dropped indexes and may
exceed the API's statement timeout (`BIGNAME_API_DB_STATEMENT_TIMEOUT_MS`) until the indexes
are rebuilt. The operator's `phase-runner inspect` block and raw-event windows count
and list normalized events by block hash, which `normalized_events_block_idx` serves.

The [walk index set](../../docs/glossary.md#walk-index-set) is the 17 indexes Interpret keeps.
`drop.sql` drops the other 34 before a from-zero walk or a full-history Interpret redo. Before
Project runs, the phase runner rebuilds any of them that is missing, with the statements
`install.sql` uses (see [The phase runner's rebuild](#the-phase-runners-rebuild)), and
`install.sql` remains the way to rebuild them by hand.
[`docs/storage.md`](../../docs/storage.md#walk-index-set) lists both sets and the rule that
splits them. Indexes are access paths: dropping or rebuilding them changes no stored row, no
[interpreter content hash](../../docs/glossary.md#interpreter-content-hash) input and no
schema-migration record, and the primary key, the `event_identity` unique key and every
foreign key stay in place.

## When drop.sql may run

The indexes belong to the table, which every chain on the database shares. `drop.sql` refuses
while any chain may be served or read by Project: a chain whose Project phase is running, has
a current block, or has a live publication in `project_family_marker`, and whose Interpret
phase has no redo in progress. Project commits a publication before it records its progress,
so the marker covers a chain whose runner stopped between the two. The check passes on a
fresh database before its first walk, and once every chain whose Project has advanced is in
an Interpret redo.

On a database that holds two chains, such as Ethereum and Base, both chains' Interpret redos
must be in progress before it runs, or run the walk with every index. A multi-chain `redo`
command runs its chains one after another, so it does not meet that condition; a redo
stopped part-way keeps `redo_in_progress`, so start and stop one chain's redo, then start
the other's.

The script checks this before the drops and again after them, and takes no phase lock, so
nothing stops an Interpret redo completing while the drops run. Run it as the walk or redo
starts, with Interpret's whole range still ahead. The one-hour timeout bounds each drop, not
the script, so there is no fixed limit on the whole sequence. Allow for all 34 drops before
Interpret can complete. The second check sees only the state when it runs: if a chain meets
the condition then, for example because its Project started during the drops and is still
running, it fails after the receipt, naming the chains, and `psql` exits non-zero; run
`install.sql` at once.

## Running the scripts

Run both scripts with the writer role, outside a transaction:

```sh
psql -X -v ON_ERROR_STOP=1 -f ops/walk-index-set/drop.sql "$BIGNAME_DATABASE_URL"
psql -X -v ON_ERROR_STOP=1 -f ops/walk-index-set/install.sql "$BIGNAME_DATABASE_URL"
```

Retain each script's complete output and exit status in the deployment receipt. Both print
the index rows, but `drop.sql` runs its second check after them, so the rows alone do not
show that it succeeded.

Run `drop.sql` as the walk or redo starts (see above). It can run while Interpret is
processing batches. `DROP INDEX CONCURRENTLY` waits for the transactions that may use the
index, such as a batch in flight, so the script lifts the lock timeout and bounds each drop
to one hour. `DROP INDEX` matches the name alone, so before any drop the script refuses a
name held by anything other than an index on `normalized_events`.

A drop that fails with an SQL error while the connection stays up, for example at the
one-hour limit, leaves its index in place, possibly marked invalid. The script goes on with
the other drops, prints the receipt and runs the second check. If that check fails, run
`install.sql` as it says; otherwise the script then fails naming the indexes left, and a
rerun of `drop.sql` drops them, skipping the names already dropped.

If the run is cut off instead, by a lost connection, a database restart or an interrupt to
`psql`, it exits non-zero without the receipt or either check, and any of the drops may have
happened. Reconnect and check the `chain_phase_state` rows the script checks. If `drop.sql`
may still run, rerun it; otherwise run `install.sql`, recovering any invalid index it names
as [Checks and recovery](#checks-and-recovery) describes.

`install.sql` builds each missing index with `CREATE INDEX CONCURRENTLY`, one at a time, each
bounded to six hours, then runs `ANALYZE bigname_phase.normalized_events`: expression and
partial indexes have no statistics until the table is analyzed. A concurrent build permits
writes but waits for transactions already running, such as an Interpret batch; inspect
`pg_stat_progress_create_index` rather than restarting it. A rerun accepts the indexes already
built and builds only the missing ones.

While a build runs, a phase runner that is interpreting cannot take the lock for its own
`ANALYZE` of the table and logs the warning
`interpret could not refresh normalized_events statistics`. That is expected: the script's
final `ANALYZE` covers it.

## Sequence

For a full-history Interpret redo run with the one-shot `phase-runner redo` command (the
[planned boundary](../../docs/runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)),
start the Interpret redo, and once the chain's `interpret` row in `chain_phase_state` shows
`redo_in_progress`, run `drop.sql`. When Interpret completes on a chain whose Interpret had
completed before the redo, the usual case, the same command goes on to the Project redo that
completion stamps, and the runner rebuilds the indexes before that Project redo's first batch.
A Project redo started with its own command rebuilds them the same way.

For a from-zero walk under the long-running runner, run `drop.sql` after `init-schema` and
before the first start. The runner rebuilds the indexes once Interpret completes, before
Project's first batch.

Either way the chain's Project waits for the builds, so the time between Interpret completing
and Project's first batch is the sum of them. To spend that time while Interpret still runs
instead, stop the command or runner while Interpret's last batches run, run `install.sql`,
and start it again. The runner then finds every index in place. A stop lets the batch in
flight finish, so Interpret can still complete before the stop, and then the runner's rebuild
covers the rest.

The set is shared, so on a database with two chains the first chain whose Project starts
rebuilds the indexes for both, while the other chain's Interpret may still be running. The
runner logs a warning naming each other chain whose Interpret has not completed, in a redo or
on its first walk, with its state. That chain's remaining batches maintain the rebuilt
indexes, as they would after a hand-run `install.sql`.

## The phase runner's rebuild

At every Project entry, normal or redo, supervised or one-shot, the runner reads the state of
every name `drop.sql` drops in the catalog. When every one is a valid, ready index on
`normalized_events` it does nothing more and logs nothing, apart from the definition warning
below. Otherwise it:

1. Opens a connection of its own and takes a session advisory lock, so that one rebuild runs
   at a time across every process on the database. Another chain's Project entry waits on the
   lock, then finds the indexes in place.
2. Reads the names again, and stops there when another chain or process built them while it
   waited. Otherwise it waits for any other index build, `VACUUM` or `ANALYZE` on the table,
   from any login, to finish. Autovacuum is not waited for: it gives way to the build, apart
   from an anti-wraparound vacuum, which the build then waits behind.
3. One index at a time, waits for storage capacity, then drops an invalid leftover with
   `DROP INDEX CONCURRENTLY` and runs `install.sql`'s `CREATE INDEX CONCURRENTLY` statement,
   outside a transaction, with `lock_timeout` 0 and each build bounded to six hours. It logs
   each index it built with its duration. The capacity check is the batch loop's, with the same
   database ceiling and free-disk floor. While a breach lasts it logs
   `the rebuild of the normalized_events indexes the walk index set scripts drop waits until
   storage capacity recovers` on every poll, with `breach_reasons`, `database_size_bytes` and
   `free_disk_bytes`, and the metric `phase_runner_walk_index_capacity_blocked{chain}` reads 1.
4. Analyzes the table, checks every name again and logs
   `every normalized_events index the walk index set scripts drop is in place`. When the
   `ANALYZE` fails or is stopped it logs
   `could not refresh normalized_events statistics after building its indexes` instead of
   retrying it: run `ANALYZE bigname_phase.normalized_events` by hand.

Before the builds its reads run with a ten-second lock timeout and a one-minute statement
timeout, so a read held behind a lock fails and the restart loop retries it. While it waits for
the lock or for another build, or builds, it records loop progress, so a long build does not
raise the stale-loop alert. While it waits for capacity it pauses a running Project row, as the
batch loop does, and records loop progress. A Project row that is idle or completed, as on a
first walk or before a redo, is left as it is and records none, so a long wait for capacity
raises the stale-loop alert.

- A valid index under one of the names is kept whatever its definition. When the definition
  differs from `install.sql`'s, the runner logs a warning with the definition it found and the
  one it expects, once per process, and Project reads the index as found. A changed definition
  takes a new name ([`docs/storage.md`](../../docs/storage.md#walk-index-set)).
- A name held by a table, a view or an index on another table stops the chain with an error
  naming the relation, and nothing is dropped. Remove or rename that relation, then restart
  the runner.
- A build or drop that fails stops the chain with an error naming the index and the cause. It is
  not retried in the process: another chain's Project entry stops with the same error instead of
  building again. Once every chain has stopped the process exits non-zero. Under a restart
  policy each start drops the invalid leftover and builds it again, so a cause that persists,
  such as a login that does not own the table, shows as a restart loop with the same error each
  time.
- A stop during any of the rebuild's statements, a read held behind a lock included, cancels
  it on the server and releases the rebuild's lock. A stop while it waits for capacity issues
  no build.
- A stop cancels the build on the server and leaves its invalid entry, which the next
  Project entry drops and builds again, in the same process too. After a one-shot Interpret
  redo, the next start is the matching Project redo, not the Interpret redo's command again.
  Indexes already built are kept. A stop whose budget has already run out sends no cancel,
  so the build carries on until it ends, and the next start waits for it.
- While it builds, the runner records loop progress only while the build's backend is
  active, so a connection that died without a reply raises the stale-loop alert.

`BIGNAME_PHASE_RUNNER_RESTORE_MAINTENANCE_WORK_MEM` and
`BIGNAME_PHASE_RUNNER_RESTORE_PARALLEL_MAINTENANCE_WORKERS` set `maintenance_work_mem` and
`max_parallel_maintenance_workers` on the rebuild's connection only. Unset, the server's values
apply. The build line logs the values in force.

## Checks and recovery

`install.sql` checks every name twice and fails, with a non-zero `psql` exit, instead of
reporting success over an index the readers cannot use, exactly as
[the lookahead loader installer](../v1-lookahead-indexes/README.md) does. Before it builds
anything, it refuses a name taken by an index that is not both `indisvalid` and `indisready`,
an index on another table, an index whose definition is not the reviewed one, or a relation
that is not an index. After the builds it makes the same check and also requires every index
to exist. The definition is compared as `pg_get_indexdef` prints it with `search_path` set to
`pg_catalog` and `quote_all_identifiers` off; the check sets both for its own reads only.

An interrupted concurrent build, for example one cancelled or stopped by the six-hour limit,
leaves an invalid index under the intended name, and `IF NOT EXISTS` matches on the name
alone, so a rerun stops at the first check and names it. Confirm in
`pg_stat_progress_create_index` that no build is still running, drop only the named index with
`DROP INDEX CONCURRENTLY bigname_phase.<index name>`, as the error's hint spells out, and
rerun the script. Recover a valid index that fails the definition check the same way. If the
name belongs to a table, view or another table's index, remove or rename that relation
first.

A runner restart while the indexes are dropped resumes the walk or redo from its marker as
usual, and Interpret never builds them. `scripts/check-schema` proves that
`drop.sql` refuses a chain with Project progress and a chain with only a live publication,
fails after its drops when a fresh chain's Project is running by then, refuses a name held by
another table's index, fails naming an index whose drop failed, and drops exactly
its list, and that `install.sql` rebuilds the fresh baseline's definitions and refuses an
invalid index, an index with other keys or another definition, and a table under one of its
names. A database test in
`crates/interpret` proves that the two lists together are every index the baseline defines
on `normalized_events`, and that a walk, a full-history redo, a flag recompute and a runner
restart's manifest sync, over ENSv1, Basenames and ENSv2 histories, scan `normalized_events`
sequentially nowhere without the dropped indexes and store the same rows.
`crates/storage/src/walk_index_set/tests.rs` holds the runner's copy of the list, statements
and definitions to these scripts, and `apps/phase-runner/tests/spine/walk_index_restore.rs`
proves the runner's rebuild, its refusals and its stop.
