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
exceed the API's statement timeout (`BIGNAME_API_DB_STATEMENT_TIMEOUT_MS`) until
`install.sql` has run. The operator's `phase-runner inspect` block and raw-event windows count
and list normalized events by block hash, which `normalized_events_block_idx` serves.

The [walk index set](../../docs/glossary.md#walk-index-set) is the 17 indexes Interpret keeps.
`drop.sql` drops the other 45 before a from-zero walk or a full-history Interpret redo, and
`install.sql` rebuilds them, with their reviewed definitions, before Project runs.
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
the script, so there is no fixed limit on the whole sequence; allow for all 45 drops before
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

## Sequence

For a full-history Interpret redo run with the one-shot `phase-runner redo` command (the
[planned boundary](../../docs/runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)),
note that the command need not stop between the phases: when Interpret completes on a chain
whose Interpret had completed before the redo, the usual case, the same command runs the
Project redo that completion stamps before it exits. Rebuild while Interpret is still
incomplete:

1. start the Interpret redo, and once the chain's `interpret` row in `chain_phase_state` shows
   `redo_in_progress`, run `drop.sql`;
2. while Interpret's last batches run, stop the command with SIGTERM or Ctrl-C, wait for it
   to exit, and check that the `interpret` row still shows `redo_in_progress`;
3. run `install.sql`;
4. rerun the same command, with the same chain, range and flags. It resumes Interpret from
   its recorded block, finishes it with every index in place; the Project redo then runs
   with them too, from this command or from its own.

A stop lets the batch in flight finish, so Interpret can still complete. If the `interpret`
row no longer shows `redo_in_progress`, the Project redo is stamped and may already have
started without the indexes. Run `install.sql`, then start the Project redo with its own
command, or rerun that command if it was running; do not rerun the Interpret command, whose
attestation token, if it had one, is spent.

For a from-zero walk under the long-running runner, run `drop.sql` after `init-schema` and
before the first start. Project starts on its own once Interpret completes, so stop the
runner while Interpret's last batches run, run `install.sql`, then start it again. In either
sequence a missed stop never changes a row, but until `install.sql` finishes Project reads
without these indexes, through sequential scans, and an API read that needs one may exceed
the API's statement timeout (`BIGNAME_API_DB_STATEMENT_TIMEOUT_MS`) on a large database. The
script can run while they do.

The set is shared, so on a database with two chains run `install.sql` before either chain's
Interpret completes: stop each chain's redo before its last batch, run `install.sql`, then
rerun each command.

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

The runner never checks or recreates these indexes, so a restart while they are dropped
resumes the walk or redo from its marker as usual. `scripts/check-schema` proves that
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
