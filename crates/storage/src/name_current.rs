mod expiring;
mod list;
mod migration;
mod public_authority;
mod row;
#[cfg(test)]
mod row_tests;
mod snapshot;
pub mod wrapper_expiry;

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::types::Uuid;

pub use expiring::{NameCurrentDeadline, NameCurrentExpiringFilter, NameCurrentExpiryWindow};
pub(crate) use expiring::{
    expiring_page_from, parent_labelhash_path, parent_like_patterns, push_parent_key_predicate,
    push_parent_predicate,
};
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
    push_public_authority_filter_in, push_public_authority_predicate,
};
use row::decode_name_current_row;
pub(crate) use row::has_event_linked_registry_serving_provenance;
pub use row::{
    EVENT_LINKED_REGISTRY_SERVING_BASES, NameCurrentRow,
    name_current_has_event_linked_registry_serving,
};
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

/// Current served names with the same composition, visibility and preimage rules as exact-name rows,
/// without record inventory or resolution-topology enrichment that display names do not use.
pub async fn load_current_normalized_names(
    db: impl Into<crate::ReadDb<'_>>,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, String>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = db.into().snapshot().await?;
    let mut rows = crate::families::name::load_composed_base(
        &mut snapshot,
        logical_name_ids,
        crate::families::name::CoverageShape::Plain,
    )
    .await?;
    crate::rendered_name::enrich(&mut snapshot, &mut rows).await?;
    snapshot.close().await?;
    Ok(rows
        .into_iter()
        .map(|(id, row)| (id, row.normalized_name))
        .collect())
}

/// Load the canonical representative current name for each resource (registration), reading
/// only the names bound on `chain_ids`, the chains of the caller's publication. An empty
/// `chain_ids` serves no names.
///
/// `name_current.resource_id` is 1:many; this picks one representative per resource using the
/// `canonical_display_name ASC` tie-break the rest of v2 uses and returns the picked exact-name
/// row, including its declared wrapper summary. The rows are
/// composed from the owned key families.
pub async fn load_current_names_by_resource_ids(
    db: impl Into<crate::ReadDb<'_>>,
    resource_ids: &[Uuid],
    chain_ids: &[String],
) -> Result<BTreeMap<Uuid, NameCurrentRow>> {
    if resource_ids.is_empty() || chain_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    // TYR-518: the two statements and the pick below are copied from
    // `load_family_names_by_resource_ids` (families/name/batch.rs), with the chain key added so
    // the lookup seeks project_binding_candidate_resource_idx. batch.rs is hashed on this
    // release line, so it is not changed here. The reconcile branch fixes batch.rs in place
    // (TYR-518). There this wrapper goes back to delegating to batch.rs and these copied
    // statements are deleted in the same PR. The forward-port of this change must not leave
    // both.
    let mut snapshot = db.into().snapshot().await?;
    let names: Vec<String> = sqlx::query_scalar(
        "/* storage:name_current.by_resource */
         SELECT DISTINCT candidate.logical_name_id
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.chain_id = ANY($2::text[])
           AND candidate.resource_id = ANY($1::uuid[])",
    )
    .bind(resource_ids)
    .bind(chain_ids)
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to load the names bound to resources")?;
    let ordered: Vec<String> = sqlx::query_scalar(&format!(
        "/* storage:name_current.by_resource_order */
         SELECT surface.logical_name_id FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1)
         ORDER BY {name} ASC, surface.logical_name_id ASC",
        name = crate::rendered_name::rendered_name_sql("surface")
    ))
    .bind(&names)
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to order the names bound to resources")?;
    let mut rows =
        crate::families::name::load_family_names_by_logical_name_ids(&mut *snapshot, &names)
            .await?;
    snapshot.close().await?;
    let mut out = BTreeMap::new();
    for name in ordered {
        let Some(row) = rows.remove(&name) else {
            continue;
        };
        if let Some(resource) = row.resource_id.filter(|id| resource_ids.contains(id)) {
            out.entry(resource).or_insert(row);
        }
    }
    Ok(out)
}
