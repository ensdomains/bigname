use anyhow::Result;
use sqlx::PgPool;

use super::types::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentPageFilter,
    ChildrenCurrentSummary, RegistryChildrenPage, RegistryLabelOwnerFilter,
};

pub async fn load_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    super::page::load_children_current_page_filtered(
        pool,
        parent_logical_name_id,
        &ChildrenCurrentPageFilter::default(),
        cursor,
        page_size,
        None,
    )
    .await
}

/// A page of the declared children of `parent_logical_name_id` whose ENSv2 registration was
/// emitted by `registry_address`: the labels one registry contract currently holds under the
/// name it serves, narrowed by `owner` when given. `label_count` counts every such child the
/// owner filter admits, not just the page.
pub async fn load_registry_children_current_page(
    db: impl Into<crate::ReadDb<'_>>,
    parent_logical_name_id: &str,
    registry_address: &str,
    owner: Option<RegistryLabelOwnerFilter<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<RegistryChildrenPage> {
    super::families::registry_page(
        db.into(),
        parent_logical_name_id,
        registry_address,
        owner,
        cursor,
        page_size,
    )
    .await
}

/// Exact count of the labels `registry_address` holds on `chain_id` at the current family
/// publication, under the name the registry serves there, or `None` when the registry holds more
/// than `cap` registrations under that name: the count then stops reading at `cap + 1`
/// registrations and is not computed.
pub async fn count_registry_labels_current(
    db: impl Into<crate::ReadDb<'_>>,
    chain_id: &str,
    registry_address: &str,
    cap: u64,
) -> Result<Option<i64>> {
    super::families::registry_count_current(db.into(), chain_id, registry_address, cap).await
}

pub async fn load_children_current_summaries(
    db: impl Into<crate::ReadDb<'_>>,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    if parent_logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }

    super::families::summaries(db.into(), parent_logical_name_ids).await
}
