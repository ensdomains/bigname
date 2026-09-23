# Address history match indexes

`GET /v1/addresses/{address}/history` first looks up the names and resources an
address held in the past. It reads three kinds of `normalized_events` rows: a
registration granted to the address (`after_state ->> 'registrant'`), a token
transferred to it (`after_state ->> 'to'`), and a registry ownership transfer to
it (`after_state ->> 'owner'`). Three partial expression indexes key those rows by
the lowercased new holder:

- `normalized_events_address_registrant_match_idx` (`RegistrationGranted`)
- `normalized_events_address_token_holder_match_idx` (`TokenControlTransferred`)
- `normalized_events_address_registry_owner_match_idx` (`AuthorityTransferred`)

Each covers only activated rows in the `canonical`, `safe` and `finalized`
states. The expressions and predicates must stay identical to the query in
`crates/storage/src/history/address_matches.rs`. Without them the lookup still
returns the same rows, but reads every grant and transfer on the chain: about 6
seconds on the Sepolia database on 2026-09-23.

These change access paths only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing them.

For an initialized database, prebuild the indexes with `install.sql` using the
writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`, outside any
transaction. It works like
[`ops/v1-lookahead-indexes/install.sql`](../v1-lookahead-indexes/README.md): each
build is concurrent and bounded to six hours, and the script checks every name
before and after the builds. It refuses a name held by an index that is not valid
and ready, an index on another table, an index whose `pg_get_indexdef` text is
not the reviewed one, or a relation that is not an index. Retain its output in the
deployment receipt.

To recover from an interrupted build or a refused index, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only
the named index with `DROP INDEX CONCURRENTLY bigname_phase.<index name>`, as the
error's hint says, and rerun the script. If the name belongs to a table, view, or
another table's index, remove or rename that relation first. After the builds,
run `ANALYZE bigname_phase.normalized_events` so the planner has statistics for
the new expressions.

The schema-migration `20260923120000_normalized_events_address_match_indexes.sql`
installs the same definitions on initialized databases and is a no-op before the
phase schema exists. After a prebuild its `IF NOT EXISTS` adopts the indexes by
name, and it ends with the same check, so the SQLx run fails rather than
recording success over an index the read cannot use. The fresh baseline also
includes all three indexes. `schema-v2/apply-check.sh` proves that the baseline,
the schema-migration and this script build the same definitions, and proves each
refusal.
