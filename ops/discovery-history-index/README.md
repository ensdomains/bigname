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

Before treating the step as complete, require exactly one index row, both
`indisvalid` and `indisready` true, and the expected definition. `IF NOT EXISTS`
does not fix an invalid index from an interrupted concurrent build. If this
specific index is invalid and no build is running, drop only it with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_observation_history_idx`,
then rerun the installation and validity checks. Do not drop the existing active
indexes. Never drop a valid replacement merely because an installation was retried.

Capture representative `EXPLAIN (ANALYZE, BUFFERS)` read-only equivalents of the
historical queries before and after installation. Verify indexed access includes
the observation key and compare actual completed Interpret batches and consumed
raw logs. A faster isolated query does not prove end-to-end throughput by itself.

The matching versioned schema-migration installs the same definition on initialized
databases; after a live prebuild, its `IF NOT EXISTS` is a no-op. Apply that migration
through the usual SQLx release process when adopting this source revision. The
fresh baseline also includes the index. No binary replacement or Interpret replay
is needed solely to preinstall it on the current deployment.
