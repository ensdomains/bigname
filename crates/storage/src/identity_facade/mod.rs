mod types;

/// Reusable row predicates for active reverse-identity pagination and count queries.
pub const READABLE_REVERSE_IDENTITY_CTES: &str = r#"
readable_names AS (
    SELECT nc.logical_name_id, nc.raw_name, nc.namespace, nc.namehash
    FROM bigname_phase.name_current nc
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = nc.logical_name_id
    LEFT JOIN bigname_phase.resources resource
      ON resource.resource_id = nc.resource_id
    LEFT JOIN bigname_phase.surface_bindings binding
      ON binding.surface_binding_id = nc.surface_binding_id
    LEFT JOIN bigname_phase.token_lineages token_lineage
      ON token_lineage.token_lineage_id = nc.token_lineage_id
    JOIN bigname_phase.chain_lineage surface_lineage
      ON surface_lineage.chain_id = surface.chain_id
     AND surface_lineage.block_hash = surface.block_hash
    LEFT JOIN bigname_phase.chain_lineage resource_lineage
      ON resource_lineage.chain_id = resource.chain_id
     AND resource_lineage.block_hash = resource.block_hash
    LEFT JOIN bigname_phase.chain_lineage binding_lineage
      ON binding_lineage.chain_id = binding.chain_id
     AND binding_lineage.block_hash = binding.block_hash
    LEFT JOIN bigname_phase.chain_lineage token_lineage_lineage
      ON token_lineage_lineage.chain_id = token_lineage.chain_id
     AND token_lineage_lineage.block_hash = token_lineage.block_hash
    WHERE nc.support_status = 'supported'
      AND nc.canonicality_summary ->> 'state' = 'canonical_lineage'
      AND EXISTS (
          SELECT 1 FROM bigname_phase.chain_lineage projection_lineage
          WHERE projection_lineage.chain_id = nc.provenance ->> 'chain_id'
            AND projection_lineage.block_hash =
                nc.canonicality_summary ->> 'target_block_hash'
            AND projection_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      )
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (
          nc.surface_binding_id IS NULL
          OR (
              resource.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND binding.active_to IS NULL
              AND (
                  nc.token_lineage_id IS NULL
                  OR (
                      token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND token_lineage_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
              )
          )
      )
), readable_relations AS (
    SELECT anc.*
    FROM bigname_phase.address_names_current anc
    JOIN readable_names readable_name
      ON readable_name.logical_name_id = anc.logical_name_id
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = anc.logical_name_id
    JOIN bigname_phase.resources resource
      ON resource.resource_id = anc.resource_id
    JOIN bigname_phase.surface_bindings binding
      ON binding.surface_binding_id = anc.surface_binding_id
    LEFT JOIN bigname_phase.token_lineages token_lineage
      ON token_lineage.token_lineage_id = anc.token_lineage_id
    JOIN bigname_phase.chain_lineage surface_lineage
      ON surface_lineage.chain_id = surface.chain_id
     AND surface_lineage.block_hash = surface.block_hash
    JOIN bigname_phase.chain_lineage resource_lineage
      ON resource_lineage.chain_id = resource.chain_id
     AND resource_lineage.block_hash = resource.block_hash
    JOIN bigname_phase.chain_lineage binding_lineage
      ON binding_lineage.chain_id = binding.chain_id
     AND binding_lineage.block_hash = binding.block_hash
    LEFT JOIN bigname_phase.chain_lineage token_lineage_lineage
      ON token_lineage_lineage.chain_id = token_lineage.chain_id
     AND token_lineage_lineage.block_hash = token_lineage.block_hash
    WHERE anc.support_status = 'supported'
      AND anc.canonicality_summary ->> 'state' = 'canonical_lineage'
      AND EXISTS (
          SELECT 1 FROM bigname_phase.chain_lineage projection_lineage
          WHERE projection_lineage.chain_id = anc.provenance ->> 'chain_id'
            AND projection_lineage.block_hash = anc.chain_positions ->> 'target_block_hash'
            AND projection_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      )
      AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND binding.active_to IS NULL
      AND (
          anc.token_lineage_id IS NULL
          OR (
              token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND token_lineage_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
      )
)
"#;

pub use types::{
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityPrimaryNameSnapshot, IdentityRecordInventoryRow, IndexingStatusChainRow,
    IndexingStatusRead, ReverseIdentityCursor, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};
