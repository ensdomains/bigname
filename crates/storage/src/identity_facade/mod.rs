mod counts;
mod forward;
mod reverse;
mod reverse_feed;
mod reverse_page;
mod reverse_rows;
mod status;
mod types;

use std::collections::BTreeSet;

pub use forward::{load_identity_name_feed_records_by_names, load_identity_records_by_names};
pub use reverse::load_reverse_identity_records;
pub use reverse_feed::load_reverse_identity_feed_records;
pub use status::{
    PENDING_INVALIDATION_COUNT_CAP, load_expected_status_chain_ids, load_indexing_status,
};
pub use types::{
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityPrimaryNameSnapshot, IdentityRecordInventoryRow, IndexingStatusChainRow,
    IndexingStatusRead, ReverseIdentityCursor, ReverseIdentityFeedGroup, ReverseIdentityFeedInput,
    ReverseIdentityFeedRecordRow, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};

const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
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

const DEFAULT_ADDRESS_NAMES_CURRENT_LINEAGE_JOINS: &str = r#"
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

const DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER: &str = r#"
  AND identity_nc_surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND identity_nc_surface_lineage.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      identity_nc.surface_binding_id IS NULL
      OR (
          identity_nc_resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_resource_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding_lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding.active_to IS NULL
          AND (
              identity_nc.token_lineage_id IS NULL
              OR (
                  identity_nc_token_lineage.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND identity_nc_token_lineage_lineage.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
          )
      )
  )
"#;

const DEFAULT_IDENTITY_NAME_CURRENT_LINEAGE_JOINS: &str = r#"
  JOIN chain_lineage identity_nc_surface_lineage
    ON identity_nc_surface_lineage.chain_id = identity_nc_surface.chain_id
   AND identity_nc_surface_lineage.block_hash = identity_nc_surface.block_hash
  LEFT JOIN chain_lineage identity_nc_resource_lineage
    ON identity_nc_resource_lineage.chain_id = identity_nc_resource.chain_id
   AND identity_nc_resource_lineage.block_hash = identity_nc_resource.block_hash
  LEFT JOIN chain_lineage identity_nc_binding_lineage
    ON identity_nc_binding_lineage.chain_id = identity_nc_binding.chain_id
   AND identity_nc_binding_lineage.block_hash = identity_nc_binding.block_hash
  LEFT JOIN chain_lineage identity_nc_token_lineage_lineage
    ON identity_nc_token_lineage_lineage.chain_id = identity_nc_token_lineage.chain_id
   AND identity_nc_token_lineage_lineage.block_hash = identity_nc_token_lineage.block_hash
"#;

const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
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
"#;

const DEFAULT_RECORD_INVENTORY_CURRENT_LINEAGE_JOIN: &str = r#"
  JOIN chain_lineage resource_lineage
    ON resource_lineage.chain_id = resource.chain_id
   AND resource_lineage.block_hash = resource.block_hash
"#;

fn dedupe_in_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
