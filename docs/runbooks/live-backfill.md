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
4. **No live range lease from another worker.** Leases live on
   `backfill_ranges`, not `backfill_jobs`. Inspect the range status and all
   three lease fields before adding work:

   ```sql
   SELECT br.backfill_job_id,
          br.backfill_range_id,
          br.range_start_block_number,
          br.range_end_block_number,
          br.checkpoint_block_number,
          br.status,
          br.lease_owner,
          br.lease_token,
          br.lease_expires_at,
          (br.lease_expires_at <= now()) AS expired_and_reclaimable,
          br.updated_at,
          br.failure_reason
   FROM backfill_ranges br
   WHERE br.status IN (
       'reserved'::backfill_lifecycle_status,
       'running'::backfill_lifecycle_status
   )
   ORDER BY br.lease_expires_at, br.backfill_job_id, br.backfill_range_id;
   ```

   A `reserved` or `running` range with `lease_expires_at > now()` has a live
   lease and must be allowed to finish or be stopped deliberately. A range with
   `lease_expires_at <= now()` is safely reclaimable by the normal reservation
   path, which atomically replaces the expired token and owner. Do not clear
   lease fields by hand: table checks require `lease_token`, `lease_owner`, and
   `lease_expires_at` together for `reserved` and `running` ranges.

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

The topic-scan exception is narrow. The hash-pinned provider path scans ENSv1
generic resolver event topics across all emitters, so that specific source is
not circular (`apps/indexer/src/main/backfill/fetching/log_ranges.rs`). The
Basenames registry becomes address-free only for a Coinbase SQL source-family
job with `--source-family basenames_base_registry` and an active manifest ABI
topic plan (`apps/indexer/src/main/backfill/coinbase_sql/planner.rs`). A
whole-active-watched-chain Coinbase SQL job remains address-filtered, as does a
hash-pinned whole-chain Base job. Those jobs still require the iterative passes
in this runbook.

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
  --manifests-root manifests/sepolia \
  --chain ethereum-sepolia \
  --from-block <lowest admitted start across active targets> \
  --to-block <finalized head> \
  --hash-pinned-adapter-sync inline \
  --idempotency-key recovery-<iso8601>-<n>
```

Pin `--manifests-root` to the chain's deployment profile. The CLI default is
`manifests/mainnet`, which is not the correct corpus for the Sepolia example.

Bound `--to-block` at the finalized head, not the canonical head, so the range
cannot be reorged out from under the job.

**3. Let the inline adapter sync finish.** The command pins
`--hash-pinned-adapter-sync inline`, so each completed chunk writes normalized
events and discovery edges directly. Manual `auto` mode has the same effective
inline behavior, but spelling it out keeps the runbook independent of that
mapping. Inline backfill does not advance `normalized_replay_cursors`; do not
wait on that table. Wait for every range belonging to the job to reach
`completed`:

```sql
SELECT status,
       COUNT(*) AS range_count,
       MIN(checkpoint_block_number) AS min_checkpoint,
       MAX(checkpoint_block_number) AS max_checkpoint,
       MAX(updated_at) AS last_updated_at
FROM backfill_ranges
WHERE backfill_job_id = <backfill_job_id>
GROUP BY status
ORDER BY status;
```

Discovery edges and the active watched set may grow while the job runs. The
authoritative signal is the watched-set comparison in step 4 after all ranges
complete.

**4. Check for newly admitted targets.** Rerun the active watched-range query in
[Verification](#verification) and compare `(source_family,
contract_instance_id, address, scan_from_block, scan_to_block)`. If the set
grew, return to step 2 with a fresh idempotency key. The new pass covers the
targets the previous pass discovered.

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

Coverage is per active watched target and per active range, not per deployment.
The stored maximum log block is not a frontier: a late-admitted target may have
live-tailed rows near head while still missing older logs below that maximum.
Verification must scan from the target's effective active-range start and prove
every provider-returned log through the verification head is stored.

The backfill selector derives its universe from `load_watched_contracts`: active
manifest declarations plus active admitted discovery edges. Use the same shape
for verification. Set these `psql` variables to the completed job's chain,
declared lower bound, and finalized verification head:

```sql
\set job_chain 'ethereum-sepolia'
\set job_from_block 10462881
\set verification_head 12345678

WITH watched_contracts AS (
    SELECT
        mv.chain,
        mv.source_family,
        cia.address,
        mci.contract_instance_id,
        CASE
            WHEN manifest_range.start_block IS NULL
                THEN cia.active_from_block_number
            WHEN cia.active_from_block_number IS NULL
                THEN manifest_range.start_block
            ELSE GREATEST(
                manifest_range.start_block,
                cia.active_from_block_number
            )
        END AS active_from_block_number,
        cia.active_to_block_number
    FROM manifest_versions mv
    JOIN manifest_contract_instances mci
      ON mci.manifest_id = mv.manifest_id
    LEFT JOIN LATERAL (
        SELECT (entry ->> 'start_block')::BIGINT AS start_block
        FROM jsonb_array_elements(
            CASE
                WHEN mci.declaration_kind = 'root'
                    THEN mv.manifest_payload -> 'roots'
                ELSE mv.manifest_payload -> 'contracts'
            END
        ) entry
        WHERE (
                mci.declaration_kind = 'root'
                AND entry ->> 'name' = mci.declaration_name
              )
           OR (
                mci.declaration_kind = 'contract'
                AND entry ->> 'role' = mci.declaration_name
              )
        ORDER BY start_block NULLS LAST
        LIMIT 1
    ) manifest_range ON TRUE
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = mci.contract_instance_id
     AND cia.chain_id = :'job_chain'
     AND cia.deactivated_at IS NULL
    WHERE mv.rollout_status = 'active'
      AND mv.chain = :'job_chain'

    UNION

    SELECT
        de.chain_id AS chain,
        COALESCE(target_mv.source_family, mv.source_family) AS source_family,
        cia.address,
        de.to_contract_instance_id AS contract_instance_id,
        CASE
            WHEN de.active_from_block_number IS NULL
                THEN cia.active_from_block_number
            WHEN cia.active_from_block_number IS NULL
                THEN de.active_from_block_number
            ELSE GREATEST(
                de.active_from_block_number,
                cia.active_from_block_number
            )
        END AS active_from_block_number,
        CASE
            WHEN de.active_to_block_number IS NULL
                THEN cia.active_to_block_number
            WHEN cia.active_to_block_number IS NULL
                THEN de.active_to_block_number
            ELSE LEAST(
                de.active_to_block_number,
                cia.active_to_block_number
            )
        END AS active_to_block_number
    FROM discovery_edges de
    JOIN manifest_versions mv
      ON mv.manifest_id = de.source_manifest_id
    LEFT JOIN manifest_versions target_mv
      ON target_mv.rollout_status = 'active'
     AND target_mv.namespace = mv.namespace
     AND target_mv.chain = de.chain_id
     AND target_mv.deployment_epoch = mv.deployment_epoch
     AND target_mv.source_family = CASE
         WHEN de.edge_kind = 'resolver'
          AND mv.source_family = 'ens_v1_registry_l1'
             THEN 'ens_v1_resolver_l1'
         WHEN de.edge_kind = 'resolver'
          AND mv.source_family = 'ens_v2_registry_l1'
             THEN 'ens_v2_resolver_l1'
         WHEN de.edge_kind = 'resolver'
          AND mv.source_family = 'basenames_base_registry'
             THEN 'basenames_base_resolver'
         ELSE NULL
     END
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = de.to_contract_instance_id
     AND cia.chain_id = :'job_chain'
     AND cia.deactivated_at IS NULL
    WHERE mv.rollout_status = 'active'
      AND de.chain_id = :'job_chain'
      AND de.deactivated_at IS NULL
      AND de.edge_kind <> 'migration'
      AND (
          de.edge_kind <> 'resolver'
          OR mv.source_family NOT IN (
              'ens_v1_registry_l1',
              'ens_v2_registry_l1',
              'basenames_base_registry'
          )
          OR target_mv.manifest_id IS NOT NULL
      )
      AND (
          de.active_from_block_number IS NULL
          OR cia.active_to_block_number IS NULL
          OR de.active_from_block_number <= cia.active_to_block_number
      )
      AND (
          cia.active_from_block_number IS NULL
          OR de.active_to_block_number IS NULL
          OR cia.active_from_block_number <= de.active_to_block_number
      )
),
target_ranges AS (
    SELECT DISTINCT
        chain,
        source_family,
        contract_instance_id,
        LOWER(address) AS address,
        GREATEST(
            COALESCE(active_from_block_number, :job_from_block),
            :job_from_block
        ) AS scan_from_block,
        LEAST(
            COALESCE(active_to_block_number, :verification_head),
            :verification_head
        ) AS scan_to_block
    FROM watched_contracts
)
SELECT tr.chain,
       tr.source_family,
       tr.contract_instance_id,
       tr.address,
       tr.scan_from_block,
       tr.scan_to_block,
       COUNT(rl.raw_log_id) AS stored_non_orphaned_log_count
FROM target_ranges tr
LEFT JOIN raw_logs rl
  ON rl.chain_id = tr.chain
 AND LOWER(rl.emitting_address) = tr.address
 AND rl.block_number BETWEEN tr.scan_from_block AND tr.scan_to_block
 AND rl.canonicality_state <> 'orphaned'::canonicality_state
WHERE tr.scan_from_block <= tr.scan_to_block
GROUP BY tr.chain,
         tr.source_family,
         tr.contract_instance_id,
         tr.address,
         tr.scan_from_block,
         tr.scan_to_block
ORDER BY tr.source_family,
         tr.contract_instance_id,
         tr.scan_from_block,
         tr.scan_to_block;
```

`active_from_block_number = NULL` has the same finite-job behavior as the
selector: `job_from_block` becomes the effective lower bound. That proves only
the explicitly declared job range, not unknown history below it. Do not claim
whole-history coverage for such a target until its historical start is pinned.

For each returned target range, issue bounded `eth_getLogs` windows beginning
at `scan_from_block`, filtered to that address, and compare every returned log
identity with `raw_logs` on the same chain. For one provider result, the storage
check is:

```sql
SELECT EXISTS (
    SELECT 1
    FROM raw_logs rl
    WHERE rl.chain_id = :'job_chain'
      AND LOWER(rl.emitting_address) = LOWER('<target_address>')
      AND LOWER(rl.block_hash) = LOWER('<provider_block_hash>')
      AND LOWER(rl.transaction_hash) = LOWER('<provider_transaction_hash>')
      AND rl.log_index = <provider_log_index>
      AND rl.canonicality_state <> 'orphaned'::canonicality_state
) AS ingested;
```

If every provider log in a window is present, advance to the next contiguous
window; do not jump to the highest stored block. The first `false` proves a
historical hole, so stop scanning that target and schedule another pass. A
target is clean only after contiguous windows cover its entire
`[scan_from_block, scan_to_block]` range without a missing provider log. Use
bounded look-ahead windows and stop on the first miss; do not use the reorg-safe
range helpers, which resolve every block hash and are unaffordable across a
never-ingested target's full history.

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
  entire active range. Bound the look-ahead and stop at the first missing
  provider log; presence of a hole is a boolean, and its size does not change
  what you do next.
