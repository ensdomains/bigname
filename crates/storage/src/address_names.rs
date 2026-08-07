mod count;
mod decode;
mod page;
mod query;
mod read;
mod types;
pub use count::{AddressNamesCurrentCountFilter, count_address_names_current_for_app_filter};
pub use page::{
    load_address_names_current_page, load_address_names_current_page_sorted_for_relations,
};
pub use read::{
    load_address_names_current, load_address_names_current_for_relations,
    load_address_names_current_including_noncanonical,
    load_address_names_current_including_noncanonical_for_relations,
};
pub use types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation, AddressNamesCurrentCursor,
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentPage,
    AddressNamesCurrentProvenanceSummary, AddressNamesCurrentSort, AddressNamesCurrentSortedCursor,
    AddressNamesCurrentSortedCursorValue, AddressNamesCurrentSortedPage,
    AddressNamesCurrentSummary,
};

pub(crate) const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND anc.canonicality_summary ->> 'state' = 'canonical_lineage'
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = anc.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = anc.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
  AND surface.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND surface_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
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
  AND binding.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND binding_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR (
          token_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND token_lineage_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
      )
  )
"#;

pub(crate) const DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS: &str = r#"
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
"#;
