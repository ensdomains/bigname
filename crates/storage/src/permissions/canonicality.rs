pub const DEFAULT_PERMISSIONS_CURRENT_READ_FILTER: &str = r#"
  AND pc.canonicality_summary ->> 'state' IN ('canonical', 'safe', 'finalized')
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.resources resource
      JOIN bigname_phase.chain_lineage resource_lineage
        ON resource_lineage.chain_id = resource.chain_id
       AND resource_lineage.block_hash = resource.block_hash
      WHERE resource.resource_id = pc.resource_id
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
  )
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = pc.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = pc.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
"#;

pub(super) const CURRENT_PERMISSION_SUMMARY_READ_FILTER: &str = r#"
  summary.canonicality_summary ->> 'state' = 'canonical_lineage'
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.resources resource
      JOIN bigname_phase.chain_lineage resource_lineage
        ON resource_lineage.chain_id = resource.chain_id
       AND resource_lineage.block_hash = resource.block_hash
      WHERE resource.resource_id = summary.resource_id
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
  )
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = summary.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = summary.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
"#;
