pub(crate) const RESOURCE_CANONICALITY_JOINS: &str = r#"
JOIN bigname_phase.resources resource
  ON resource.resource_id = ric.resource_id
JOIN bigname_phase.chain_lineage resource_lineage
  ON resource_lineage.chain_id = resource.chain_id
 AND resource_lineage.block_hash = resource.block_hash
"#;

pub(crate) const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
  AND ric.canonicality_summary ->> 'state' = 'canonical_lineage'
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = ric.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = ric.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
  AND resource.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND resource_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
"#;
