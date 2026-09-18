-- The first of the requested source families, in name order, with a readable event on the
-- chain before the batch. Readable means what the full-state loader restores: the event and
-- its block are canonical, safe or finalized. No index on normalized_events leads with
-- source_family, so each family costs one scan of the chain's events before the batch, which
-- stops at the first match or covers every row when the family has none. The loader asks
-- only for families whose manifest is in a rollout state it does not otherwise read. The
-- family filter sits inside the lateral scan so lineage is checked only for matching rows.
SELECT requested.source_family
FROM unnest($3::text[]) requested(source_family)
JOIN LATERAL (
    SELECT 1
    FROM normalized_events event
    JOIN LATERAL (
        SELECT 1 FROM chain_lineage lineage
        WHERE lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number
          AND lineage.block_hash = event.block_hash
          AND lineage.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 1
    ) readable ON TRUE
    WHERE event.chain_id = $1 AND event.block_number < $2
      AND event.source_family = requested.source_family
      AND event.canonicality_state IN ('canonical','safe','finalized')
    LIMIT 1
) retained ON TRUE
ORDER BY requested.source_family
LIMIT 1
