/* project:families.hydrate.text.select */
-- Preview only the owned rows changed in this block; all other keys read their stored image.
WITH value_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_node_record_value, $3)
), record_values AS (
    SELECT value.* FROM project_node_record_value value WHERE value.chain_id = $1
      AND NOT EXISTS (SELECT 1 FROM value_changes change
          WHERE (change.chain_id, change.resolver_address, change.arm, change.arm_identity,
                 change.record_key) = (value.chain_id, value.resolver_address, value.arm,
                                      value.arm_identity, value.record_key))
    UNION ALL SELECT * FROM value_changes
), partition_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_node_record_partition, $4)
), partitions AS (
    SELECT partition.* FROM project_node_record_partition partition WHERE partition.chain_id = $1
      AND NOT EXISTS (SELECT 1 FROM partition_changes change
          WHERE (change.chain_id, change.resolver_address, change.arm, change.arm_identity) =
                (partition.chain_id, partition.resolver_address, partition.arm, partition.arm_identity))
    UNION ALL SELECT * FROM partition_changes
), classification_changes AS (
    SELECT * FROM jsonb_populate_recordset(NULL::project_resolver_classification, $5)
), classifications AS (
    SELECT classification.* FROM project_resolver_classification classification
    WHERE classification.chain_id = $1 AND NOT EXISTS (
        SELECT 1 FROM classification_changes change
        WHERE (change.chain_id, change.resolver_address) =
              (classification.chain_id, classification.resolver_address))
    UNION ALL SELECT * FROM classification_changes
)
SELECT to_jsonb(value.*) || jsonb_build_object(
    '_version', partition.version_position,
    '_namehash', CASE WHEN value.arm = 'named' THEN surface.namehash ELSE value.node END,
    '_admission', jsonb_build_object('classification', classification.classification,
        'support_status', classification.support_status,
        'unsupported_reason', classification.unsupported_reason,
        'manifest_id', classification.manifest_id),
    '_readable', EXISTS (SELECT 1 FROM chain_lineage lineage
        WHERE lineage.chain_id = value.chain_id
          AND lineage.block_number = value.hydrated_at_block
          AND lineage.block_number <= $2
          AND lineage.block_hash = value.hydrated_value ->> 'block_hash'
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')),
    -- The block changed this selector's value, record version or admission: hydrate it first.
    -- Stored rows are still the prior block's, in preparation and publication alike.
    '_delta', EXISTS (SELECT 1 FROM value_changes change
            WHERE (change.resolver_address, change.arm, change.arm_identity, change.record_key) =
                  (value.resolver_address, value.arm, value.arm_identity, value.record_key))
        OR EXISTS (SELECT 1 FROM partition_changes change
            LEFT JOIN project_node_record_partition stored
              ON (stored.chain_id, stored.resolver_address, stored.arm, stored.arm_identity) =
                 (change.chain_id, change.resolver_address, change.arm, change.arm_identity)
            WHERE (change.resolver_address, change.arm, change.arm_identity) =
                  (value.resolver_address, value.arm, value.arm_identity)
              AND change.version_position IS DISTINCT FROM stored.version_position)
        OR EXISTS (SELECT 1 FROM classification_changes change
            LEFT JOIN project_resolver_classification stored
              ON (stored.chain_id, stored.resolver_address) =
                 (change.chain_id, change.resolver_address)
            WHERE change.resolver_address = value.resolver_address
              AND (change.classification, change.support_status, change.unsupported_reason,
                   change.manifest_id) IS DISTINCT FROM (stored.classification,
                   stored.support_status, stored.unsupported_reason, stored.manifest_id)))
FROM record_values value
LEFT JOIN partitions partition USING (chain_id, resolver_address, arm, arm_identity)
LEFT JOIN classifications classification USING (chain_id, resolver_address)
LEFT JOIN name_surfaces surface ON surface.logical_name_id = value.logical_name_id
WHERE value.namespace = 'ens' AND value.record_family = 'text'
  AND (value.status = 'unsupported' OR value.hydrated_value IS NOT NULL)
  AND (value.resolver_address = ANY($6::text[]) OR value.hydrated_value IS NOT NULL)
ORDER BY value.resolver_address, value.arm, value.arm_identity, value.record_key
