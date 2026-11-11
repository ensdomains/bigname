# Record history indexes

`GET /v1/events`, name history, and address history take a `record_key` filter that keeps
the writes and clears of one record key plus record-version resets
([shared route rules](../../docs/api-v1-routes.md#shared-route-rules)). A page that no name,
registration, or address anchors, scoped by `resolver` or `contract_address` or not, reads
two arms per chain of its window: the key's `RecordChanged` rows and the chain's
`RecordVersionChanged` rows, each in page order
(`crates/storage/src/history/record_key_source.rs`). Two partial indexes on
`bigname_phase.normalized_events` serve the `RecordChanged` arm:

| Index | Keys | Rows | Serves |
| --- | --- | --- | --- |
| `normalized_events_record_key_history_idx` | `chain_id`, `left(after_state ->> 'record_key', 256)`, `block_number DESC NULLS LAST` | activated, canonical `RecordChanged` | the write arm of an unscoped page |
| `normalized_events_record_emitter_key_history_idx` | `chain_id`, `lower(raw_fact_ref ->> 'emitting_address')`, `left(after_state ->> 'record_key', 256)`, `block_number DESC NULLS LAST` | the same | the write arm of a page scoped by `resolver` or `contract_address` |

Read forward, each index returns one chain's rows newest first. Read backward, it returns them
oldest first, with rows without a block first, which is the `order=asc` page order. Each indexes
the first 256 characters of the key, at most 1,024 bytes, so a row with a key of any length can
be written. The arm matches that prefix in the index and the whole key on the row. It reads at
most one page of index entries, and none for a key with no write on the chain, whether the key
is common, rare, or unknown. The reset arm reads `normalized_events_projection_idx` and sorts
the chain's resets, which are few.

Without the indexes the API returns the same pages, but the write arm walks the chain's events
in page order and tests the key on every row, so a rare or unknown key reads the chain's whole
history and can reach the API's statement timeout. A request scoped by an emitter needs the
emitter-led index: the key index alone would walk every write of a common key looking for one
emitter, and the emitter's own index alone would walk every event a large resolver emitted
looking for one key.

Installing them changes an access path only: no normalized event, canonicality state, raw
intake, or [content hash](../../docs/glossary.md#interpreter-content-hash) input changes.

The two indexes are advisory ([walk index set](../../docs/storage.md#walk-index-set)). The
phase runner never builds them, and `ops/walk-index-set/drop.sql` leaves them in place. The API
starts and serves without them, with a WARN at startup naming each one that is not in place.

## Building the indexes

Build the two indexes on every database the build is deployed to before its
schema-migrations run. Schema-migration
`20261111120000_normalized_events_record_history_indexes.sql` adopts them by name and refuses
a populated table without them, so it never builds one in place: an ordinary `CREATE INDEX`
on a populated table would block writes to `normalized_events` for the whole build.

Run `install.sql` by hand with the writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`.
Do not wrap it in a transaction. It builds the indexes one after the other with
`CREATE INDEX CONCURRENTLY`, so the phase runner and the API keep running. Each build reads the
whole table twice and waits for every transaction older than itself, including a running
Interpret batch. Inspect `pg_stat_progress_create_index` rather than restarting that batch.
The script permits the wait and bounds each build to six hours. The previous image does not
read the indexes, and Interpret keeps them current like the table's other indexes. Retain the
script's output in the deployment receipt.

Each index holds one entry per activated, canonical record write, so its size grows with the
record writes, not with the table. PostgreSQL uses no statistics from the expressions of a
partial index, so no `ANALYZE` is needed afterwards. The arms are chosen by their index
conditions on the chain, the key prefix, and the emitter.

## Checking the indexes

The script checks the names twice and fails, with a non-zero `psql` exit, instead of reporting
success over an index the arms cannot use. Before it builds anything, it refuses a name that is
already taken by an index that is not both `indisvalid` and `indisready`, an index on another
table, an index whose definition is not the reviewed one, or a table, view, or other relation
that is not an index. A name that resolves to nothing passes this first check. After the builds
it makes the same check and also requires both indexes to exist. It prints the index rows
before the last check, so the receipt shows the flags and definitions either way. Rerunning the
script on a database that has both indexes builds nothing and repeats the check.

Before the schema-migration runs on a database, both indexes must exist there, valid and
ready, with the reviewed definitions:

```sql
SET search_path = pg_catalog;
SELECT c.relname, c.relkind, i.indrelid::regclass, i.indisvalid, i.indisready,
       pg_get_indexdef(i.indexrelid)
FROM pg_class AS c
LEFT JOIN pg_index AS i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'bigname_phase'::regnamespace
  AND c.relname IN ('normalized_events_record_key_history_idx',
                    'normalized_events_record_emitter_key_history_idx')
ORDER BY c.relname;
RESET search_path;
```

It must return two rows, each with `relkind` `i`, `indrelid` `bigname_phase.normalized_events`,
and `indisvalid` and `indisready` true. The definitions must be exactly these lines, which are
the texts the schema-migration and the script compare:

```text
CREATE INDEX normalized_events_record_emitter_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, lower((raw_fact_ref ->> 'emitting_address'::text)), "left"((after_state ->> 'record_key'::text), 256), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))
CREATE INDEX normalized_events_record_key_history_idx ON bigname_phase.normalized_events USING btree (chain_id, "left"((after_state ->> 'record_key'::text), 256), block_number DESC NULLS LAST) WHERE ((event_kind = 'RecordChanged'::text) AND (consumer_visibility = 'activated'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])))
```

## Stored keys without a colon

The API reads a `record_key` with no colon as the text record `text:<key>`, except for the keys
producers store without a family prefix, listed in `UNALIASED_RECORD_KEYS` in
`apps/api/src/v2/history_filters.rs`: `content`, `contenthash` and `name`. A stored bare key
missing from that list could no longer be asked for. Before a build that reads the alias serves
a database, list the bare keys that database stores. The query reads one entry of
`normalized_events_record_key_history_idx` per distinct stored key prefix on each chain, so it
needs the index. It lists the first 256 characters of each key. A key with no colon in those
characters is listed by its prefix, so a listed value is a whole key when it is shorter than
256 characters, and every bare key a producer stores is far shorter:

```sql
WITH RECURSIVE stored_keys(chain_id, record_key) AS (
    (SELECT chain_id, left(after_state ->> 'record_key', 256)
     FROM bigname_phase.normalized_events
     WHERE event_kind = 'RecordChanged'
       AND consumer_visibility = 'activated'
       AND canonicality_state IN ('canonical', 'safe', 'finalized')
       AND after_state ->> 'record_key' IS NOT NULL
     ORDER BY chain_id, left(after_state ->> 'record_key', 256)
     LIMIT 1)
    UNION ALL
    SELECT next_key.chain_id, next_key.record_key
    FROM stored_keys
    CROSS JOIN LATERAL (
        SELECT chain_id, left(after_state ->> 'record_key', 256) AS record_key
        FROM bigname_phase.normalized_events
        WHERE event_kind = 'RecordChanged'
          AND consumer_visibility = 'activated'
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
          AND (chain_id, left(after_state ->> 'record_key', 256))
              > (stored_keys.chain_id, stored_keys.record_key)
        ORDER BY chain_id, left(after_state ->> 'record_key', 256)
        LIMIT 1
    ) AS next_key
)
SELECT DISTINCT record_key
FROM stored_keys
WHERE strpos(record_key, ':') = 0
ORDER BY record_key;
```

It must return exactly `content`, `contenthash` and `name`, or a subset of them. Any other value
means a producer stores that key bare, or a key whose first 256 characters hold no colon: do
not serve the build on that database until the key is understood and, if a producer stores it,
joins the list. `apps/api/src/tests/v2_events_record_key.rs` runs this query on a test database
and checks that the adapters store no other bare key.

## Recovery

An interrupted concurrent build, for example one cancelled or stopped by the six-hour limit,
leaves an invalid index under the intended name. `IF NOT EXISTS` matches on the name alone, so
it does not repair that index. Rerunning the script stops at the first check and names it.
Nothing drops or rebuilds an index automatically. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only the named index
with `DROP INDEX CONCURRENTLY bigname_phase.<name>`, as the error's hint spells out, and rerun
the script. Recover a valid index that fails the definition check the same way. If the name
belongs to a table, view, or another table's index, remove or rename that relation first. Never
drop a valid index with the reviewed definition merely because an installation was retried.

The schema-migration ends with the same checks, read under the same settings and put back
before the block returns, so the SQLx run fails rather than recording success if a name is not
an index on `bigname_phase.normalized_events`, is not valid and ready, or does not have the
reviewed definition. Recover as described above, then run the schema-migrations again.
`scripts/check-schema` proves the schema-migration's build on an empty table, its adoption of
this script's indexes, and each refusal.
