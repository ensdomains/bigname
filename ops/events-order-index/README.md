# Event page order index

`GET /v1/events`, name history, and address history return normalized events
newest first by default, sorted by block number with events without a block
last, and oldest first with `order=asc`. The index
`normalized_events_chain_block_number_desc_idx` on
`normalized_events (chain_id, block_number DESC NULLS LAST)` returns rows of one
chain in exactly that order: read forward for newest first, read backward for
oldest first. PostgreSQL then stops after one page of rows. The older
`normalized_events_chain_block_number_idx` on `(chain_id, block_number)` read
backward puts events without a block first, which is not the page order, so
without the new index PostgreSQL reads and sorts every matching event before it
returns the first page. On Sepolia that was 2.73 million events and the request
hit the 25 second statement timeout; with a matching order the same read took
1.4 milliseconds. Continued pages also bound the index scan at the cursor's
block, so page N does not reread the rows before it.

This changes an access path only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing it. The older ascending index stays, because other
reads use it, such as the block lookup of Interpret's ENSv1
[lookahead loader](../v1-lookahead-indexes/README.md).

Without the index the API still returns correct pages, but an unfiltered
`/v1/events` read sorts the whole chain's history. Install it before, or together
with, the release that contains
`20260923130000_normalized_events_chain_block_number_desc_idx.sql`.

For an initialized database, prebuild the index using `install.sql` with the
writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`. Do not wrap it in a
transaction. Concurrent creation permits writes, but can wait for an existing
batch transaction; inspect `pg_stat_progress_create_index` rather than restarting
that batch. The script permits that transaction wait and bounds the build to six
hours. The index has one entry per normalized event, about the size of
`normalized_events_chain_block_number_idx`. Retain the script's output in the
deployment receipt. The index is on plain columns that already have statistics,
so no `ANALYZE` is needed afterwards.

The script checks the name twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the page reads cannot use. Before it
builds anything, it refuses a name that is already taken by an index that is not
both `indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. A name that resolves to nothing passes this first check. After the
build it makes the same check and also requires the index to exist. It prints the
index row before the last check, so the receipt shows the flags and definition
either way. The definition is compared exactly as `pg_get_indexdef` prints it,
read with `search_path` set to `pg_catalog` and `quote_all_identifiers` off, so
the schema name is always printed and nothing in the text has to be rewritten.
That covers key order, the descending direction, and null placement. The check
function carries its own settings, so the session's are unchanged. On a mismatch
the error prints the definition it found beside the expected one.

An interrupted concurrent build, for example one cancelled or stopped by the
six-hour limit, leaves an invalid index under the intended name. `IF NOT EXISTS`
matches on the name alone, so it does not repair that index; rerunning the
script stops at the first check and names it. Nothing drops or rebuilds an
index automatically. To recover, first confirm in `pg_stat_progress_create_index`
that no build is still running. Then drop only the named index with
`DROP INDEX CONCURRENTLY bigname_phase.normalized_events_chain_block_number_desc_idx`,
as the error's hint spells out, and rerun the script. Recover a valid index that
fails the definition check the same way. If the name belongs to a table, view, or
another table's index, remove or rename that relation first. Never drop a valid
index with the reviewed definition merely because an installation was retried.

The matching versioned schema-migration
`20260923130000_normalized_events_chain_block_number_desc_idx.sql` installs the
same definition on initialized databases and is a no-op before the phase schema
exists; after a live prebuild, its `IF NOT EXISTS` is a no-op that adopts the
index by name alone. It therefore ends with the script's final check, read under
the same settings and put back before the block returns, so the SQLx run fails
rather than recording success if the name is not an index on
`bigname_phase.normalized_events`, is not valid and ready, or does not have the
reviewed definition; recover as described above, then run the schema-migrations
again. Without a prebuild on a populated table, that schema-migration builds the
index with an ordinary `CREATE INDEX`, which blocks writes to `normalized_events`
until it finishes. `schema-v2/apply-check.sh` proves each refusal for the script
and for the schema-migration, and that the fresh baseline, the schema-migration,
and the script build the same definition. The fresh baseline also includes the
index.
