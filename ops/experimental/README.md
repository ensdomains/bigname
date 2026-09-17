# Blue-brain ENSv1 batch lookahead

This branch is an isolated blue-brain experiment. Normal deployments retain the
existing interpreter loader unless `BIGNAME_EXPERIMENTAL_V1_LOOKAHEAD=true` is
explicitly set. No separate protocol snapshot table is introduced. Interpret
continues to own normalized events and identity writes; adapters never write to
the database. Raw facts, ingestion cursors, Reth and Lighthouse are preserved.

Before enabling the experiment:

1. Build a recorded commit and run adapter, Interpret and runner checks.
2. Install `v1-lookahead-indexes.sql` against the existing Bigname database,
   outside a transaction. Both indexes must exist with `indisvalid` and
   `indisready` true. Its comments contain recovery instructions for an
   interrupted concurrent build. Do not run `init-schema`.
3. With the runner paused, run the ignored read-only test
   `load::lookahead::tests::readonly_mainnet_batches` against the failing
   checkpoint. Compare its complete output with the recorded full-state output.
   Run additional historical batches to measure memory after disjoint batches.
4. Stage both binaries with their commit and interpreter hash. Save the service
   configuration, phase state and ingestion cursors. Set the experimental flag
   only in blue-brain's runner environment and use the existing supported
   Interpret redo/adoption procedure for the new interpreter hash. Do not edit
   stored hashes or cursors to bypass adoption.
5. Verify actual completed batches, memory and range-based block/log throughput.
   API availability alone does not prove a healthy sync.

The working set includes direct node observations, explicit resource links and
due-expiry candidates. A parent request does not enumerate unrelated children.
Rows are selected by exact opaque state key, canonical block number/hash and
the existing winner ordering. Each read uses a repeatable-read snapshot; the
existing write boundary revalidates the orphaning epoch and block lineage.
Unloaded node access is rejected before publication. Dependency expansion must
finish completely; row, byte and query limits fail explicitly rather than return
partial state. Arbitrary record values still use the existing previous-value
lookup when needed. A single exceptionally large name history can exceed the
experimental limits and stop progress; this is not a guarantee of progress for
every possible dependency set.

If the experiment fails, stop the runner and retain its error and cursor. The
default loader can be selected by removing the flag on the same binary, but its
full retained state may still exceed blue-brain's memory limit. Returning to a
different interpreter hash requires the normal redo/adoption procedure; merely
swapping binaries is not a safe rollback. The optional indexes can remain while
the runner is stopped and must not be removed during an active lookahead query.

## Expiry lookup with generic prepared plans

The optional predecessor bound uses a numeric `COALESCE` lower bound so both
expiry endpoints remain index conditions in PostgreSQL's generic prepared plan.
The fallback is the signed-64-bit minimum already enforced by the query. Null
predecessors, strict grace boundaries, numeric parsing and overflow handling keep
the same behavior. A mainnet read-only comparison returned the same 17 names,
reducing the generic query from 40.2 seconds to 7.2 milliseconds. This SQL-only
access-path repair retains the approved compatibility version; completed-batch
throughput must still be measured after deployment.
