# Mirror pointer index

Project follows a name whose ENSv2 resolver is the declared
[ENSv1 mirror resolver](../../docs/glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
by reading the ENSv1 registry's resolver pointers for the name and each of its
ancestors. It finds those pointers by the name each `ResolverChanged` event
addresses, read in the adapters' shared order `child_node`, then `namehash`, then
`node` (`V1_EVENT_NODE_FIELDS` in `crates/adapters/src/schema_v2/seam.rs`). A
state-derived pointer for a newly linked child carries its parent in `node` and
the child in `child_node`, so the node field alone files it under the parent.

The index `normalized_events_project_v1_pointer_addressed_node_idx` keys ENSv1
registry, registrar and wrapper `ResolverChanged` rows by chain, namespace, that
addressed name and block. Project's mirror dependency expansion
(`crates/project/src/scope/mirror_bulk.sql`) and mirror evidence staging
(`crates/project/src/stage/mirror_evidence.rs`) probe it once per consulted name,
and the history reader's mirror lookup
(`crates/storage/src/history/attribution/mirror.rs`) can use it too. The older
`normalized_events_project_v1_pointer_node_idx` keys `node` alone, so it cannot
serve the corrected lookups. Without the new index each probe reads every ENSv1
pointer on the chain: on an 800,000-event test table one consulted name took a
sequential scan of the whole table per probe, and with the index each probe read
one row.

This changes an access path only: no normalized event, canonicality state, raw
intake or [interpreter content hash](../../docs/glossary.md#interpreter-content-hash)
input changes by installing it. The older index stays for now; the running binary
uses it until the release that contains
`20260924120000_normalized_events_project_v1_pointer_addressed_node_idx.sql` is
started, and nothing in that release reads it.

Install it before, or together with, that release. For an initialized database,
prebuild the index using `install.sql` with the writer role and
`psql -X -v ON_ERROR_STOP=1 -f install.sql`, while the current runner is still
processing. Do not wrap it in a transaction. Concurrent creation permits writes,
but can wait for an existing batch transaction; inspect
`pg_stat_progress_create_index` rather than restarting that batch. The script
permits that transaction wait and bounds the build to six hours. The index has one
entry per ENSv1 registry-side resolver pointer, a small part of
`normalized_events`. The script then runs `ANALYZE bigname_phase.normalized_events`,
because an expression index has no statistics until the table is analyzed. Retain
the script's output in the deployment receipt.

The script checks the name twice and fails, with a non-zero `psql` exit,
instead of reporting success over an index the mirror lookups cannot use. Before
it builds anything, it refuses a name that is already taken by an index that is
not both `indisvalid` and `indisready`, an index on another table, an index whose
definition is not the reviewed one, or a table, view, or other relation that is
not an index. A name that resolves to nothing passes this first check. After the
build it makes the same check and also requires the index to exist. It prints the
index row before the last check, so the receipt shows the flags and definition
either way. The definition is compared exactly as `pg_get_indexdef` prints it,
read with `search_path` set to `pg_catalog` and `quote_all_identifiers` off, so
the schema names are always printed and nothing in the text has to be rewritten.
That covers the key order, the addressed-name expression and the predicate. The
check function carries its own settings, so the session's are unchanged. On a
mismatch the error prints the definition it found beside the expected one.

An interrupted concurrent build, for example one cancelled or stopped by the
six-hour limit, leaves an invalid index under the intended name. `IF NOT EXISTS`
matches on the name alone, so it does not repair that index; rerunning the
script stops at the first check and names it. Nothing drops or rebuilds an
index automatically. To recover, first confirm in `pg_stat_progress_create_index`
that no build is still running. Then drop only the named index with
`DROP INDEX CONCURRENTLY bigname_phase.normalized_events_project_v1_pointer_addressed_node_idx`,
as the error's hint spells out, and rerun the script. Recover a valid index that
fails the definition check the same way. If the name belongs to a table, view, or
another table's index, remove or rename that relation first. Never drop a valid
index with the reviewed definition merely because an installation was retried.

The matching versioned schema-migration
`20260924120000_normalized_events_project_v1_pointer_addressed_node_idx.sql`
installs the same definition on initialized databases and is a no-op before the
phase schema exists; after a live prebuild, its `IF NOT EXISTS` is a no-op that
adopts the index by name alone. It therefore ends with the script's final check,
read under the same settings and put back before the block returns, so the SQLx
run fails rather than recording success if the name is not an index on
`bigname_phase.normalized_events`, is not valid and ready, or does not have the
reviewed definition; recover as described above, then run the schema-migrations
again. Without a prebuild on a populated table, that schema-migration builds the
index with an ordinary `CREATE INDEX`, which blocks writes to `normalized_events`
until it finishes. `schema-v2/apply-check.sh` proves each refusal for the script
and for the schema-migration, and that the fresh baseline, the schema-migration,
and the script build the same definition. The fresh baseline also includes the
index.
