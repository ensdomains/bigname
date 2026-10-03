# Walk index set

Every Interpret batch inserts its normalized events into `normalized_events`, and PostgreSQL
adds each new row to every index on that table whose predicate the row matches. Most of those
indexes serve the API's history and event pages or Project's reads, and nothing reads them
while Interpret walks the chain: Project starts only after Interpret completes, and the API
refuses the routes that read them while an Interpret redo is in progress. The event audit,
`GET /v1/diagnostics/events`, stays available during a redo; while the set is dropped it
returns the same rows, only slower. On a large database
their upkeep is a large share of each insert, because their keys (block hashes, nodes, names,
addresses) arrive in random order and each new entry lands on a different index page.

The [walk index set](../../docs/glossary.md#walk-index-set) is the 16 indexes Interpret keeps.
`drop.sql` drops the other 33 before a from-zero walk or a full-history Interpret redo, and
`install.sql` rebuilds them, with their reviewed definitions, before Project runs.
[`docs/storage.md`](../../docs/storage.md#walk-index-set) lists both sets and the rule that
splits them. Indexes are access paths: dropping or rebuilding them changes no stored row, no
[interpreter content hash](../../docs/glossary.md#interpreter-content-hash) input and no
schema-migration record, and the primary key, the `event_identity` unique key and every
foreign key stay in place.

## When drop.sql may run

The indexes belong to the table, which every chain on the database shares. `drop.sql` refuses
while any chain may be served: a chain whose Project phase has a current block and whose
Interpret phase has no redo in progress. It passes on a fresh database before its first walk,
and once every chain whose Project has advanced is in an Interpret redo. On a database that
holds two chains, such as Ethereum and Base, start both chains' Interpret redos before
running it, or run the walk with every index.

## Running the scripts

Run both scripts with the writer role, outside a transaction:

```sh
psql -X -v ON_ERROR_STOP=1 -f ops/walk-index-set/drop.sql "$BIGNAME_DATABASE_URL"
psql -X -v ON_ERROR_STOP=1 -f ops/walk-index-set/install.sql "$BIGNAME_DATABASE_URL"
```

Retain both outputs in the deployment receipt; each ends by printing the index rows.

`drop.sql` can run while Interpret is processing batches. `DROP INDEX CONCURRENTLY` waits for
the transactions that may use the index, such as a batch in flight, so the script lifts the
lock timeout and bounds each drop to one hour. A rerun skips the names already dropped.

`install.sql` builds each missing index with `CREATE INDEX CONCURRENTLY`, one at a time, each
bounded to six hours, then runs `ANALYZE bigname_phase.normalized_events`: expression and
partial indexes have no statistics until the table is analyzed. A concurrent build permits
writes but waits for transactions already running, such as an Interpret batch; inspect
`pg_stat_progress_create_index` rather than restarting it. A rerun accepts the indexes already
built and builds only the missing ones.

## Sequence

For a full-history Interpret redo run with the one-shot `phase-runner redo` commands (the
[planned boundary](../../docs/runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)):

1. start the Interpret redo, and once the chain's `interpret` row in `chain_phase_state` shows
   `redo_in_progress`, run `drop.sql`;
2. let the Interpret redo complete;
3. run `install.sql` before starting the Project redo; the Interpret redo installs that redo
   on completion, but with the supervisor stopped nothing runs it until its command does;
4. run the Project redo.

For a from-zero walk under the long-running runner, run `drop.sql` after `init-schema` and
before the first start. Project starts on its own once Interpret completes, so stop the
runner while Interpret's last batches run, run `install.sql`, then start it again. A missed
stop is never wrong, only slow: Project and the API then read without these indexes, through
sequential scans, until `install.sql` finishes, and it can run while they do.

The set is shared, so on a database with two chains run `install.sql` before the first chain
to finish its Interpret redo starts Project.

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
`drop.sql` refuses a served chain and drops exactly its list, and that `install.sql` rebuilds
the fresh baseline's definitions and refuses each bad name above. A database test in
`crates/interpret` proves that the two lists together are every index the baseline defines
on `normalized_events`, and that a walk, a full-history redo, a flag recompute and a runner
restart's manifest sync, over ENSv1, Basenames and ENSv2 histories, scan `normalized_events`
sequentially nowhere without the dropped indexes and store the same rows.
