# Historical discovery lookup index

Interpret finds earlier and later observations and closes their block ranges in
`crates/interpret/src/write/discovery.rs`. These queries include previously
closed observations. The existing indexes restricted to `deactivated_at IS NULL`
cannot serve those historical predicates. A registry with many names can
therefore cause a table scan for each observation.

`discovery_edges_observation_history_idx` selects one chain, source contract,
relationship type and exact observation key, then orders that key's history by
start block. It includes all non-orphaned observations, matching the writer's
predicate. Same-block transaction/log ordering and exact target checks remain
in the existing queries. This changes access paths only: no observation, block
range, canonicality, interpreter hash or raw intake changes are required.

For an initialized database, prebuild the index using `install.sql` with the
writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`. Do not wrap it in a
transaction. Concurrent creation permits writes, but can wait for an existing
batch transaction; inspect `pg_stat_progress_create_index` rather than restarting
that batch. The script permits that transaction wait and bounds the entire build to thirty
minutes. Retain its output in the deployment receipt.

The script ends with a check that fails, with a non-zero `psql` exit, unless
the named index belongs to `bigname_phase.discovery_edges`, is both
`indisvalid` and `indisready`, and has the reviewed definition. It compares
the `pg_get_indexdef` text, read with `search_path` set to `pg_catalog` so
every schema name is printed and nothing in the text has to be rewritten, with
how the fresh baseline index prints, and on a mismatch prints the definition it
found beside the expected one. It also fails, naming the kind of relation, when a table,
view, or other relation that is not an index holds the name: remove or rename
that relation before retrying. It prints the index row first, so the receipt
shows the flags and definition either way.

An interrupted concurrent build, for example one cancelled or stopped by the
thirty-minute limit, leaves an invalid index under the intended name.
`IF NOT EXISTS` matches on the name alone, so rerunning the script skips
creation and then fails at the check. Nothing drops or rebuilds the index
automatically. To recover, first confirm in `pg_stat_progress_create_index` that
no build is still running. Then drop only this index with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_observation_history_idx`
and rerun the script. Recover a valid index that fails the definition check,
for example one left by an incorrect manual build, the same way: the intended
queries cannot use it. Do not drop the existing active indexes. Never drop a
valid index with the reviewed definition merely because an installation was
retried.

Capture representative `EXPLAIN (ANALYZE, BUFFERS)` read-only equivalents of the
historical queries before and after installation. Verify indexed access includes
the observation key and compare actual completed Interpret batches and consumed
raw logs. A faster isolated query does not prove end-to-end throughput by itself.

The matching versioned schema-migration installs the same definition on initialized
databases; after a live prebuild, its `IF NOT EXISTS` is a no-op. That file
matches on the name alone, so the later schema-migration
`20260917160000_discovery_edges_index_validity_check.sql` fails the SQLx run if
this index is missing, exists but is not valid and ready, or does not have the
reviewed definition, or if its name belongs to a relation that is not an index.
It changes nothing; recover as
described above, then run the schema-migrations again. Apply that migration
through the usual SQLx release process when adopting this source revision. The
fresh baseline also includes the index. No binary replacement or Interpret replay
is needed solely to preinstall it on the current deployment.
