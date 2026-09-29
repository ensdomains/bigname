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
    )
    .await
}

/// A page of the declared children of `parent_logical_name_id` whose ENSv2 registration was
/// emitted by `registry_address`: the labels one registry contract currently holds under the
/// name it serves, narrowed by `owner` when given. `label_count` counts every such child the
/// owner filter admits, not just the page.
pub async fn load_registry_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    registry_address: &str,
    owner: Option<RegistryLabelOwnerFilter<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<RegistryChildrenPage> {
    super::families::registry_page(
        pool,
        parent_logical_name_id,
        registry_address,
        owner,
        cursor,
        page_size,
    )
    .await
}

/// Exact count of the declared children of `parent_logical_name_id` whose ENSv2 registration
/// was emitted by `registry_address`.
pub async fn count_registry_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
    registry_address: &str,
) -> Result<i64> {
    super::families::registry_count(pool, parent_logical_name_id, registry_address).await
}

pub async fn load_children_current_summaries(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    if parent_logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }

    super::families::summaries(pool, parent_logical_name_ids).await
}
