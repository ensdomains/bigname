mod expiring;
mod list;
mod migration;
mod public_authority;
mod row;
mod snapshot;
pub mod wrapper_expiry;

use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::types::Uuid;

pub use expiring::NameCurrentExpiringFilter;
pub(crate) use expiring::{expiring_page_from, parent_like_patterns, push_parent_predicate};
pub(crate) use list::{COMPOSED_NC_COLUMNS, escape_like_pattern, list_page_from};
pub use list::{
    NameCurrentAddressFilter, NameCurrentAddressRelationFilter, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListRow, NameCurrentListSort, name_current_list_cursor_from_row,
};
pub use migration::{
    MIGRATION_AUTHORITY_TRANSITION_PROOF_KIND, load_name_migration_transition_timestamps,
    name_current_authority_arm,
};
pub use public_authority::{
    name_current_is_ownerless_registry, name_current_public_authority,
    name_current_registry_generation, name_current_registry_handoff_block_number,
};
pub(crate) use public_authority::{
    public_authority_arms, push_public_authority_filter_in, push_public_authority_predicate,
};
pub use row::NameCurrentRow;
use row::decode_name_current_row;
pub use snapshot::load_name_current_for_snapshot;

pub const DEFAULT_NAME_CURRENT_LINEAGE_JOINS: &str = r#"
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
"#;

/// Compose a current exact-name row by deterministic logical name identity from the families.
pub async fn load_name_current(
    db: impl Into<crate::ReadDb<'_>>,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    crate::families::name::load_family_name(db, logical_name_id).await
}

/// Load current exact-name projection rows for a set of logical name identities.
///
/// The returned map is keyed by `logical_name_id`, so duplicate requested ids collapse into one
/// found row and missing rows are omitted. Iteration order is deterministic `BTreeMap` key order;
/// callers that need request or page order should iterate their original ids and look up into the
/// map. The rows are composed from the owned key families.
pub async fn load_name_current_by_logical_name_ids(
    db: impl Into<crate::ReadDb<'_>>,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    crate::families::name::load_family_names_by_logical_name_ids(db, logical_name_ids).await
}

/// Load the canonical representative current name for each resource (registration).
///
/// `name_current.resource_id` is 1:many; this picks one representative per resource using the
/// `canonical_display_name ASC` tie-break the rest of v2 uses and returns the picked exact-name
/// row, including its declared wrapper summary. The rows are
/// composed from the owned key families.
pub async fn load_current_names_by_resource_ids(
    db: impl Into<crate::ReadDb<'_>>,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, NameCurrentRow>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    crate::families::name::load_family_names_by_resource_ids(db, resource_ids).await
}
