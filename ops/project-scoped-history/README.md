# Project history lookups for changed names and primary names

Project rebuilds current reading tables from normalized event history. Two
incremental selections previously scanned whole historical event classes before
matching changed names or address tuples. Their replacement starts with each
changed key and uses four correlated index lookups. `OFFSET 0` preserves the
correlation instead of allowing the planner to flatten it into a broad join.
Before selection, Project analyzes the completed name, child, and primary-name
temporary scopes. These tables have no automatic statistics collection; default
estimates can otherwise multiply a handful of indexed probes into a large plan
cost and trigger unnecessary JIT compilation. JIT settings remain unchanged.

The selected event IDs remain identical. Name-node keys retain their existing
lowercasing; explicit alias targets retain exact string matching. Primary-name
addresses are lowercased, while coin types and namespaces retain exact matching.
Before-state references, all readable canonicality states, and the upper block
boundary remain intact. Candidate visibility is intentionally not restricted in
these evidence selections; the later serving filter still decides which events
can populate reading tables. Non-null index predicates add no exclusion because
the existing equality matches already reject null keys.

The eight expression indexes cover the four name-key forms and four primary
address-tuple forms separately. They are limited to the relevant event kinds and
non-null keys. Ordinary text records without a primary address tuple do not
populate the four primary-name indexes. Index storage and additional write work
should be recorded with the installation receipt.

## Install

On an initialized database, prebuild the indexes with `psql -X -v ON_ERROR_STOP=1
-f ops/project-scoped-history/install.sql`, outside a transaction, before applying
the versioned schema-migration or starting the changed Project binary. The
concurrent builds permit writes but can wait for an existing transaction. Inspect
`pg_stat_progress_create_index`; do not restart an indexing batch to satisfy the
build. The script bounds each build to thirty minutes.

Require all eight result rows, `indisvalid` and `indisready` true, and definitions
matching the fresh baseline. `IF NOT EXISTS` does not repair an invalid index.
If a named replacement is invalid and no build is running, drop only that invalid
index concurrently and retry its creation. Preserve valid indexes and existing
access paths. The migration uses the same definitions for initialized databases
and is a no-op before the phase schema exists.

This is an access-path change, with no new event or replay semantics. However,
Project Rust and SQL sources are inputs to the shared interpreter content hash.
The changed binary therefore rotates that fingerprint; use the normal release
and redo requirements. Preinstalling indexes alone does not rotate the running
binary's fingerprint. Do not bypass the hash guard based on an equality test.

## Validate

Run the focused tests against the normal isolated test database:

```sh
scripts/test-db -- cargo test -p bigname-project --lib stage::history::tests -- --nocapture
```

The tests compare complete old/new selection multisets, independently expected
IDs, final UNION deduplication, empty scope, another chain, and target-boundary
changes. They exercise candidate events, observed/orphaned history, before/after
references, missing/null/empty keys, mixed address tuples, and case sensitivity.
They verify the migration recreates the baseline indexes exactly and is
repeatable. Actual production SQL is used in both tests.

The scale test grows unrelated event history from 50,000 to 200,000 rows with the
changed keys fixed. Competing scans remain enabled. It requires all eight
correlated indexes and bounds examined event rows, rather than imposing a
machine-dependent time limit. Set `BIGNAME_PROJECT_HISTORY_EVIDENCE_DIR` to save
the complete executed plans and counts to `project-history-plans.json`.
Selected occurrence counts are exact. The examined-row aggregate comes from
PostgreSQL's rounded per-loop plan counters, so it is an approximation; the
index conditions show whether unrelated keys were excluded during each probe.

For a scratch PostgreSQL server without a Rust build, the following emits a SQL
check using the same production queries, fixtures, and baseline:

```sh
python3 ops/project-scoped-history/validate.py > /tmp/project-history-validation.sql
psql -XAtq -v ON_ERROR_STOP=1 "$SCRATCH_DATABASE_URL" -f /tmp/project-history-validation.sql
```

It refuses a database where `bigname_phase` exists, runs inside one transaction,
and rolls back all fixture/schema writes. Its output contains JSON execution
plans for each scale. Larger cumulative scales can be requested with
`--scales 100000 500000`. These artificial unrelated-history measurements prove
the access path; separately measure an actual complete Project pass and retain
full/incremental/redo output checks before claiming end-to-end throughput.

Relevant existing integration coverage includes `primary_names_reverse_node`,
`record_attribution_pointer_history`, `issue_360_children_nameability`, and
`record_id_resolver` in `crates/project/tests`.
