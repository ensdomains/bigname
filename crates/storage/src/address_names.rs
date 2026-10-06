mod decode;
mod page;
mod query;
mod read;
mod resolves_to;
mod resolves_to_evm;
mod resolves_to_filter;
mod resolves_to_page;
mod source;
mod types;
mod walk;
pub use page::{
    load_address_names_current_page, load_address_names_current_page_filtered,
    load_address_names_current_page_sorted_for_relations,
};
pub(crate) use page::{
    load_address_names_filtered_ids_from, load_address_names_page_entries_from,
    load_address_names_page_from,
};
pub(crate) use query::{push_expires_at_timestamp_expr, push_registered_at_timestamp_expr};
pub(crate) use read::load_address_names_current_at_bound;
pub(crate) use read::push_address_names_current_query;
pub use read::{
    load_address_names_current, load_address_names_current_for_relations,
    load_address_names_current_including_noncanonical,
    load_address_names_current_including_noncanonical_for_relations,
};
pub use resolves_to::{
    AddressRecordCurrentEntry, AddressRecordsCurrentPage, ENSIP19_DEFAULT_ADDRESS_RECORD_KEY,
};
pub(crate) use resolves_to_evm::load_address_records_evm_page_from;
pub use resolves_to_evm::{
    AddressRecordCoinMatch, AddressRecordEvmEntry, AddressRecordsCurrentEvmPage,
    EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT, load_address_records_current_evm_page,
};
pub use resolves_to_page::load_address_records_current_page;
pub(crate) use resolves_to_page::load_address_records_page_from;
pub(crate) use source::RowSource;
pub use types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation,
    AddressNamesCurrentCappedPage, AddressNamesCurrentCursor, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentPage, AddressNamesCurrentProvenanceSummary,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    AddressNamesCurrentSortedPage, AddressNamesCurrentSummary, NameQuery, NameQueryMatch,
};
pub use walk::{AddressNamesPageRequest, load_family_address_names_capped_page};

/// The publication half of the read filter: the row's target block is on readable lineage.
macro_rules! publication_read_filter {
    () => {
        r#"
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
"#
    };
}

/// The read filter of a row with no surface, binding or token lineage to check: a surface-less
/// registry child's (`families::records::registry_children`).
pub(crate) const ADDRESS_NAMES_PUBLICATION_READ_FILTER: &str = publication_read_filter!();

// Project owns the selected binding. Interpret can close it before the next publication,
// so read eligibility checks canonicality, not its mutable active_to field.
pub const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = concat!(
    publication_read_filter!(),
    r#"  AND surface.canonicality_state IN (
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
"#
);

pub const DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS: &str = r#"
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

pub(crate) use query::push_expiry_paths_expr;

pub(crate) use query::push_json_timestamp_expr;
