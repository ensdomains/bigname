/* project:hydration.text_candidates */
SELECT row.resource_id::text, row.record_version_boundary_key,
       COALESCE(lower(row.provenance ->> 'resolver_address'), ''), surface.namehash, row.entries,
       admission.allowed, pending.ordinals
FROM record_inventory_current row
JOIN name_surfaces surface
  ON surface.logical_name_id = row.provenance ->> 'logical_name_id'
CROSS JOIN LATERAL (
    SELECT COALESCE(row.support_status = 'supported'
       AND lower(row.provenance ->> 'resolver_address') = ANY($4::text[]), false) AS allowed
) admission
CROSS JOIN LATERAL (
    SELECT array_agg(ordinal) AS ordinals
    FROM jsonb_array_elements(row.entries) WITH ORDINALITY AS elements(entry, ordinal)
    WHERE entry ->> 'record_family' = 'text'
      AND (
        (admission.allowed AND entry ->> 'unsupported_reason' = $2)
        OR (entry ? $3 AND (
            NOT admission.allowed OR NOT EXISTS (
                SELECT 1 FROM chain_lineage lineage
                WHERE lineage.chain_id = $1
                  AND lineage.block_hash = entry -> $3 ->> 'block_hash'
                  AND lineage.block_number::text = entry -> $3 ->> 'block_number'
                  AND lineage.block_number <= $5
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            )
        ))
      )
) pending
WHERE row.provenance ->> 'chain_id' = $1
  AND pending.ordinals IS NOT NULL
ORDER BY row.resource_id, row.record_version_boundary_key
