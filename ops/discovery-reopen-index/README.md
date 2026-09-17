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
the entire operation to thirty minutes. Record its output, the exact definition,
and `indisvalid = true` and `indisready = true` before considering it complete.
`IF NOT EXISTS` does not repair a failed concurrent build: if this specific index
is invalid and no build is running, drop only it with
`DROP INDEX CONCURRENTLY bigname_phase.discovery_edges_reopen_idx`, then retry.
Do not drop a valid index or the other discovery indexes.

Capture a read-only `EXPLAIN (ANALYZE, BUFFERS)` equivalent of the exact writer
predicate before and after installation. Require the index condition to include
the observation key and start block. Separately measure completed Interpret
batches; an isolated query improvement is not an end-to-end rate measurement.

The fresh baseline and versioned schema-migration install the identical index.
Following an online prebuild, the schema-migration adopts it through `IF NOT EXISTS`
during the usual SQLx release process. No runner restart or Interpret replay is
required solely to preinstall this index.

`benchmark.py /path/to/repo --runtime podman --container bigname-test-postgres`
creates and removes its own test database. It extracts the actual reopen UPDATE,
holds the observation fixed while growing unrelated history from 1,000 to
1,000,000 rows, compares before/after results, and verifies active, closed,
orphaned, nullable-manifest, competing-target, and outside-range-close cases.
UPDATE probes run inside rolled-back transactions. The test also requires fresh,
schema-migration, and online index definitions to match. Save its JSON output as release
evidence; do not point it at a role without disposable-database privileges.
