# Address key indexes

`GET /v1/events?address=…`, `GET /v1/diagnostics/events?address=…` and
`GET /v1/addresses/{address}/history` first look up the names and resources an address held
in the past: registrations granted to it, tokens transferred to it and registry ownership
transfers to it (`crates/storage/src/history/address_evidence.rs`). The statement reads each
of the three address keys of `after_state` through its own subquery, which holds only that
key's match and the address length of its value, and nothing the statement filters on
afterwards. Three partial expression indexes
on `bigname_phase.normalized_events` serve those subqueries:

| Index | Key | Rows |
| --- | --- | --- |
| `normalized_events_address_to_idx` | `lower(COALESCE(after_state ->> 'to', ''))` | every row whose `to` value is 42 characters long |
| `normalized_events_address_registrant_idx` | `lower(COALESCE(after_state ->> 'registrant', ''))` | every row whose `registrant` value is 42 characters long |
| `normalized_events_address_owner_idx` | `lower(COALESCE(after_state ->> 'owner', ''))` | every row whose `owner` value is 42 characters long |

The predicates name no event kind, visibility or canonicality state. A 42-character value is
an address's length, the only value a probe can match, and the length bounds the key: a long
string under one of these keys in any event kind is left out of the index, where an unbounded
key would make its insert fail. The statement applies
those after the subqueries, so the planner cannot combine an address probe with an index
keyed by event kind or block, whatever it estimates for the address. PostgreSQL keeps no
statistics on the expressions of a partial index, so it estimates every address alike. The
expressions and predicates must stay identical to the subqueries' text.

Installing them changes an access path only: no normalized event, canonicality state, raw
intake or [content hash](../../docs/glossary.md#interpreter-content-hash) input changes.

They are part of the drop list of the [walk index set](../../docs/storage.md#walk-index-set):
`ops/walk-index-set/drop.sql` drops them before a from-zero walk, and
`ops/walk-index-set/install.sql` or the phase runner rebuilds them before Project runs.

## Building the indexes

Build the three indexes on every database the build is deployed to before its
schema-migrations run. Schema-migration
`20261112120000_normalized_events_address_key_indexes.sql` adopts them by name and refuses
a populated table without them, so it never builds one in place: an ordinary `CREATE INDEX`
on a populated table would block writes to `normalized_events` for the whole build.

Run `install.sql` by hand with the writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`.
Do not wrap it in a transaction. It builds the indexes one after the other with
`CREATE INDEX CONCURRENTLY`, smallest first (`to`, then `registrant`, then `owner`), so the
phase runner and the API keep running. Each build reads the whole table twice and waits for
every transaction older than itself, including a running Interpret batch. Inspect
`pg_stat_progress_create_index` rather than restarting that batch. The script permits the wait
and bounds each build to six hours. After each build it prints one receipt line with the
index, its size and the time the build took. The previous image does not read the indexes,
and Interpret keeps them current like the table's other indexes. Retain the script's output in
the deployment receipt.

## Checking the indexes

The script checks the names twice and fails, with a non-zero `psql` exit, instead of reporting
success over an index the subqueries cannot use. Before it builds anything, it refuses a name
that is already taken by an index that is not both `indisvalid` and `indisready`, an index on
another table, an index whose definition is not the reviewed one, or a table, view, or other
relation that is not an index. A name that resolves to nothing passes this first check. After
the builds it makes the same check and also requires all three indexes to exist. It prints the
index rows before the last check, so the receipt shows the flags and definitions either way.
Rerunning the script on a database that has the three indexes builds nothing and repeats the
check.

Before the schema-migration runs on a database, the three indexes must exist there, valid and
ready, with the reviewed definitions:

```sql
SET search_path = pg_catalog;
SELECT c.relname, c.relkind, i.indrelid::regclass, i.indisvalid, i.indisready,
       pg_get_indexdef(i.indexrelid)
FROM pg_class AS c
LEFT JOIN pg_index AS i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'bigname_phase'::regnamespace
  AND c.relname IN ('normalized_events_address_to_idx',
                    'normalized_events_address_registrant_idx',
                    'normalized_events_address_owner_idx')
ORDER BY c.relname;
RESET search_path;
```

It must return three rows, each with `relkind` `i`, `indrelid` `bigname_phase.normalized_events`,
and `indisvalid` and `indisready` true. The definitions must be exactly these lines, which are
the texts the schema-migration and the script compare:

```text
CREATE INDEX normalized_events_address_owner_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'owner'::text), ''::text))) WHERE (length((after_state ->> 'owner'::text)) = 42)
CREATE INDEX normalized_events_address_registrant_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'registrant'::text), ''::text))) WHERE (length((after_state ->> 'registrant'::text)) = 42)
CREATE INDEX normalized_events_address_to_idx ON bigname_phase.normalized_events USING btree (lower(COALESCE((after_state ->> 'to'::text), ''::text))) WHERE (length((after_state ->> 'to'::text)) = 42)
```

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
