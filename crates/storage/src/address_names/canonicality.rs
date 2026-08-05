pub(super) const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND surface_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR (
          token_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND token_lineage_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
      )
  )
"#;

pub(super) const DEFAULT_ADDRESS_NAMES_CURRENT_LINEAGE_JOINS: &str = r#"
  JOIN chain_lineage surface_lineage
    ON surface_lineage.chain_id = surface.chain_id
   AND surface_lineage.block_hash = surface.block_hash
  JOIN chain_lineage resource_lineage
    ON resource_lineage.chain_id = resource.chain_id
   AND resource_lineage.block_hash = resource.block_hash
  JOIN chain_lineage binding_lineage
    ON binding_lineage.chain_id = binding.chain_id
   AND binding_lineage.block_hash = binding.block_hash
  LEFT JOIN chain_lineage token_lineage_lineage
    ON token_lineage_lineage.chain_id = token_lineage.chain_id
   AND token_lineage_lineage.block_hash = token_lineage.block_hash
"#;
