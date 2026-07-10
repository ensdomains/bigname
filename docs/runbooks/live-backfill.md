# Backfilling a Running Indexer

This runbook covers closing a raw-fact coverage hole on a deployment that is
already serving, without stopping the live tailer and without dropping the
database. Backfill mechanics live in [`../chain-intake.md`](../chain-intake.md);
deployment shape in [`../deployment.md`](../deployment.md).

Wiping and re-bootstrapping is not a substitute. A cold start reproduces the same
hole (see [Why one pass is not enough](#why-one-pass-is-not-enough)), and it is
never available on mainnet.

## When to use this

A watched target has on-chain logs that the deployment never ingested. The
common cause is a target admitted by discovery *after* the backfill job that
would have covered its history was created.

`GET /v1/status` does not detect this. `projection_lag_blocks` tracks head
reconciliation, not log coverage: `chain_lineage` can be gapless to the chain
head while `raw_logs` holds nothing for a watched target. Trust the per-target
comparison in [Verification](#verification) instead.

## Preconditions

Check each before creating a job. Two are hard gates.

1. **Backfill code observations must be activity-scoped.** Before that change,
   backfill wrote one `raw_code_hashes` row per selected target per block,
   unconditionally. A whole-chain job over a large target set writes
   `O(selected targets x blocks)` rows and can exhaust the volume. Confirm the
   deployed image scopes observations to a block's selected log emitters.
2. **The live tailer must watch the active watched chain.** Backfill does not
   observe a watched target that emits nothing in the backfilled range; the
   tailer's missing-baseline pass is the only source of a baseline observation
   for a silent target, and it only covers targets in the live watch plan
   (`apps/indexer/src/main/reconciliation/persistence.rs:544-581`).
3. **Volume headroom.** A running deployment cannot reclaim `raw_code_hashes`
   rows written under the old policy — raw facts are immutable. Size the job
   against free space before starting.
4. **No leaked backfill lease.** A `backfill_jobs` row stuck in `running` holds a
   lease. Inspect `status`, `updated_at`, and `failure_reason` before adding
   work.

## Why one pass is not enough

A backfill job freezes its selected target set at creation
([`../chain-intake.md`](../chain-intake.md), § Selector modes:
`whole_active_watched_chain` selects "every active watched target ... at job
creation"). Discovery admits targets from normalized events, which are derived
from raw facts, which is what backfill produces. So a target discovered *by* a
pass was not in that pass's own filter.

The consequence: a single whole-chain pass over a deployment whose discovery
graph is incomplete will admit new targets and leave their history unindexed.
Iterate until a pass admits no new targets and the coverage check is clean.

For source families with a topic-signature scan the circularity does not apply,
because ingest identity is the topic plan rather than a moving address list. The
ENSv1 generic resolver scan (`apps/indexer/src/main/ens_v1_resolver.rs:4`) and
the Basenames registry scan ([`../chain-intake.md`](../chain-intake.md) § Backfill
contract) already work this way.

## Procedure

Run from an operator shell against the serving database. The live tailer keeps
running throughout; raw upserts are widening and idempotent, so a range may be
re-covered safely under a fresh idempotency key
([`../chain-intake.md`](../chain-intake.md) § Selected-target intake).

**1. Record the starting state.** Capture the per-target coverage table from
[Verification](#verification) and the current `raw_code_hashes` row count.

**2. Create the job.** Omit a selector to take `whole_active_watched_chain`.

```sh
bigname-indexer backfill \
  --chain ethereum-sepolia \
  --from-block <lowest admitted start across active targets> \
  --to-block <finalized head> \
  --idempotency-key recovery-<iso8601>-<n>
```

Bound `--to-block` at the finalized head, not the canonical head, so the range
cannot be reorged out from under the job.

**3. Let normalization catch up.** Discovery edges are written from normalized
events, so new targets only appear after replay drains. Watch
`normalized_replay_cursors`: `next_block_number > target_block_number` and
`last_failure_at IS NULL`.

**4. Check for newly admitted targets.** If the active watched set grew, return
to step 2 with a fresh idempotency key. The new pass covers the targets the
previous pass discovered.

**5. Stop when a pass admits nothing new and the coverage check is clean.**

## Cost model

Hash-pinned backfill resolves one block hash per block
(`apps/indexer/src/provider/block_transaction.rs:173-192`, `eth_getBlockByNumber`).
That dominates provider cost, and it does not depend on how many targets the job
selects:

```
provider cost ~= (to_block - from_block + 1) x CU(eth_getBlockByNumber)
wall time     ~= provider cost / throughput ceiling (CU/s)
```

Batch size (`BIGNAME_INDEXER_JSON_RPC_BATCH_ITEM_LIMIT`, default 32, max 256)
does not change the total; it removes per-request latency stalls up to the
throughput ceiling. Below the ceiling the job is latency-bound and a larger batch
helps; at the ceiling only a higher throughput limit helps.

Note the block-hash resolution loop is sequential and does not consume
`BIGNAME_INDEXER_JSON_RPC_BATCH_CONCURRENCY`, which is applied only to receipt
fetches (`apps/indexer/src/provider/transaction_receipts.rs:532`).

Budget the provider's monthly quota as well as its rate: a full-range pass over a
long history can consume a large fraction of a month's allowance, and this
procedure runs the range more than once.

## Verification

Coverage is per target, not per deployment. For each active watched target
compare the highest block it has an ingested non-orphaned log for against what
the chain holds above that block.

The floor for a target with **no** ingested logs is its admission block minus
one, not the chain head. Treating "no rows" as "nothing missing" is the failure
this runbook exists to correct: a target that was never watched has no rows, and
a naive `onchain_max > ingested_max` comparison silently skips it.

```sql
-- Per-target ingested frontier. Targets with no rows come back NULL and must be
-- scanned from their admission block, not skipped.
SELECT cia.address,
       MAX(rl.block_number) AS ingested_max_block
FROM contract_instance_addresses cia
LEFT JOIN raw_logs rl
  ON LOWER(rl.emitting_address) = cia.address
 AND rl.canonicality_state <> 'orphaned'::canonicality_state
WHERE cia.deactivated_at IS NULL
GROUP BY cia.address
ORDER BY ingested_max_block NULLS FIRST;
```

Then, for each target, ask the provider for the first log strictly above that
floor within the target's active range. Use a single bounded `eth_getLogs`; do
not use the reorg-safe range helpers, which resolve every block hash in the range
and are unaffordable across a never-ingested target's full history.

A useful whole-deployment smoke check: no raw log should exist only because it
shared a transaction with a manifest-declared emitter. If every row in `raw_logs`
sits in a transaction containing a manifest-declared contract's log, no
discovery-admitted target has ever been ingested on its own.

Confirm the code-observation policy held during the run:

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT code_hash) AS distinct_code_hashes
FROM raw_code_hashes
WHERE chain_id = 'ethereum-sepolia';
```

Row count growing far faster than distinct code hashes means observations are
being written per block rather than per emission.

## Hazards

- **The tailer runs throughout.** It writes raw facts for new head blocks while
  the job writes them for historical ranges. Upserts are widening and idempotent,
  so this is safe, but a coverage check taken mid-run is a moving target. Take
  the check after the job completes.
- **A pass can widen the watch set.** The tailer's discovery refresh picks up
  newly admitted targets without a restart, so live coverage self-corrects going
  forward; only history needs another pass.
- **Never-ingested targets are the expensive case.** Their scan window is their
  entire active range. Bound the look-ahead and stop at the first hit; presence
  of a hole is a boolean, and its size does not change what you do next.
