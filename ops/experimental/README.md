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

## Approved blue-brain compatibility exception

On 2026-09-17 Tate explicitly authorized treating this loader change as compatible
with the interpretation already stored on blue-brain. Build this isolated release
with `BIGNAME_BLUE_BRAIN_LOOKAHEAD_COMPATIBILITY=blue-brain-v1-lookahead-20260917`.
The build refuses the exception unless the computed source fingerprint is exactly
`keccak256:5b4e1689c140c935ce4f3fccd28dd766f761077127b24405df41d93a7e7ea7fd`.
It uses the retained interpretation version
`keccak256:e292847c25244de4a800c580f291fca2d2259b9ac6915fb828a390032c6a7f43`
for existing compatibility checks and new output. `INTERPRETER_SOURCE_HASH` and
`INTERPRETER_COMPATIBILITY_EXCEPTION` record the source fingerprint and the
exception separately; startup logs and the deployment receipt retain both.
Without this build setting, the computed fingerprint remains the interpretation
version. Other source fingerprints and arbitrary replacement hashes are refused.

Evidence: exact complete output at blocks 14,684,500–14,684,999 matched the
existing full-state baseline (48,241 normalized events); all 20 historical
batches passed; adapter, Interpret and runner checks passed. These checks support
the operator's compatibility decision but do not prove every historical output.
This exception is approved for blue-brain only, not Sepolia or other deployments.
The historical discovery index was installed separately from commit `26d253fa`;
that index alone does not require a binary change or replay.

For this exception, replace step 4's full-history redo with the following:

1. Record the exact build commit, actual source fingerprint, retained version,
   exception identifier and binary checksums. Re-run the read-only complete-output
   comparison with the final build.
2. Confirm Interpret still records the retained version at block 14,684,499,
   no redo is active, and raw ingestion and node start times match the receipt.
3. Save the current service configuration. Remove the earlier replay's
   `OnSuccess` adoption hook, set the new runner command to `phase-runner run`,
   and retain the experimental loader flag, jemalloc and 10/12 GiB limits with
   swap disabled. Resume normal interpretation at block 14,684,500. Do not
   modify any database hash, cursor, or retained fact to authorize this step.
4. Verify actual completed batches and memory. The existing API can remain on
   its matching retained interpretation version; install the matching new API
   only after the runner's resumed batches pass verification.

Any future semantic change still requires the normal full-history adoption.
Removing this build setting restores the actual source version and therefore
requires normal adoption if that version differs from the database.
