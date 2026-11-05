//! Filtered, sortable declared direct child page reads.
//!
//! The unfiltered `load_children_current_page` is the `ChildrenCurrentPageFilter::default()`
//! case of `load_children_current_page_filtered`. The page is a two-stage query: a `children`
//! CTE applies the read fence, the optional served-name prefix, and the optional expiry fence
//! and computes one `sort_timestamp` column; the outer SELECT applies the keyset predicate and
//! order over that column so the cursor logic is written once per sort, not once per filter.

use anyhow::Result;

use super::types::{ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentPageFilter};

/// Load a bounded page of declared direct children narrowed and ordered by `filter`.
///
/// `total_count` counts every child the filter admits, before the cursor; with `total_cap` it
/// stops at `total_cap + 1`, and the rows are those of the uncapped page. `cursor` must have been
/// issued for the same `sort` and `order`; a cursor whose sort value kind does not match the sort
/// is rejected here rather than silently re-anchored.
pub async fn load_children_current_page_filtered(
    db: impl Into<crate::ReadDb<'_>>,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
    total_cap: Option<u64>,
) -> Result<ChildrenCurrentPage> {
    super::families::page(
        db.into(),
        parent_logical_name_id,
        filter,
        cursor,
        page_size,
        total_cap,
    )
    .await
}
