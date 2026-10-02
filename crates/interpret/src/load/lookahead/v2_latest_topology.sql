-- The timestamp of the chain's latest readable ENSv2 registry event before the batch: the
-- restored topology timestamp a restore of every event reaches. Block timestamps increase
-- with block number, so the latest block holds it. Served backwards by
-- normalized_events_v2_lookahead_probe_idx.
SELECT lineage.block_timestamp
FROM normalized_events event
JOIN LATERAL (
    SELECT lineage.block_timestamp FROM chain_lineage lineage
    WHERE lineage.chain_id = event.chain_id
      AND lineage.block_number = event.block_number
      AND lineage.block_hash = event.block_hash
      AND lineage.canonicality_state IN ('canonical','safe','finalized')
    LIMIT 1
) lineage ON TRUE
WHERE event.chain_id = $1 AND event.block_number < $2
  AND event.canonicality_state IN ('canonical','safe','finalized')
  AND event.source_family LIKE 'ens\_v2\_%'
  AND event.source_family IN ('ens_v2_registry_l1','ens_v2_root_l1')
ORDER BY event.block_number DESC
LIMIT 1
