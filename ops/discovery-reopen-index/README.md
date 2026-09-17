# Exact discovery observation lookup

Interpret's `reopen_statement` in `crates/interpret/src/write/discovery.rs`
finds an exact retained observation before inserting a new row. It deliberately
includes orphaned and closed observations so replay can reactivate them. Neither
the active-only indexes nor the non-orphaned historical observation index can
serve this unrestricted lookup.

`discovery_edges_reopen_idx` selects the chain, source contract, relationship,
start block, and observation key without excluding any canonicality or interval
state. Target contract, manifest, and block-hash equality remain in the query.
The historical index remains useful for predecessor/successor lookups over
non-orphaned observations. No row, authority, canonicality, interpreter hash,
raw intake, or replay semantics change.

Preinstall on an initialized database with the writer role using
`psql -X -v ON_ERROR_STOP=1 -f install.sql`, outside a transaction. Creation is
concurrent and may wait for the current batch transaction. The script bounds
the entire operation to thirty minutes. The script ends with a check that fails,
with a non-zero `psql` exit, unless the named index belongs to
`bigname_phase.discovery_edges` and is both `indisvalid` and `indisready`. It
prints the index row first, so the receipt shows the flags either way. Record
its output. The check does not compare the definition, so also confirm the
exact definition before considering the step complete.

An interrupted concurrent build, for example one cancelled or stopped by the
thirty-minute limit, leaves an invalid index under the intended name.
`IF NOT EXISTS` matches on the name alone, so rerunning the script skips
creation and then fails at the check. Nothing drops or rebuilds the index
automatically. To recover, first confirm in `pg_stat_progress_create_index` that
no build is still running. Then drop only this index with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_reopen_idx` and rerun the
script. Do not drop a valid index or the other discovery indexes.

Capture a read-only `EXPLAIN (ANALYZE, BUFFERS)` equivalent of the exact writer
predicate before and after installation. Require the index condition to include
the observation key and start block. Separately measure completed Interpret
batches; an isolated query improvement is not an end-to-end rate measurement.

The fresh baseline and versioned schema-migration install the identical index.
Following an online prebuild, the schema-migration adopts it through `IF NOT EXISTS`
during the usual SQLx release process. That file matches on the name alone, so
the later schema-migration
`20260917160000_discovery_edges_index_validity_check.sql` fails the SQLx run if
this index exists but is not valid and ready. It changes nothing; recover as
described above, then run the schema-migrations again. No runner restart or Interpret replay is
required solely to preinstall this index.

`benchmark.py /path/to/repo --runtime podman --container bigname-test-postgres`
creates and removes its own test database. It extracts the actual reopen UPDATE,
holds the observation fixed while growing unrelated history from 1,000 to
1,000,000 rows, compares before/after results, and verifies active, closed,
orphaned, nullable-manifest, competing-target, and outside-range-close cases.
UPDATE probes run inside rolled-back transactions. The test also requires fresh,
schema-migration, and online index definitions to match. Save its JSON output as release
evidence; do not point it at a role without disposable-database privileges.
