//! Filtered, sortable declared direct child page reads.
//!
//! The unfiltered `load_children_current_page` is the `ChildrenCurrentPageFilter::default()`
//! case of `load_children_current_page_filtered`. The page is a two-stage query: a `children`
//! CTE applies the read fence, the optional served-name prefix, and the optional expiry fence
//! and computes one `sort_timestamp` column; the outer SELECT applies the keyset predicate and
//! order over that column so the cursor logic is written once per sort, not once per filter.

use anyhow::Result;
use sqlx::PgPool;

use super::types::{ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentPageFilter};

/// Load a bounded page of declared direct children narrowed and ordered by `filter`.
///
/// `summary` on the returned page is always the unfiltered per-parent aggregate: callers that
/// narrowed the page use the separately computed `total_count`. `cursor` must have been issued for
/// the same `sort` and `order`; a cursor whose sort value kind does not match the sort is
/// rejected here rather than silently re-anchored.
pub async fn load_children_current_page_filtered(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    super::families::page(pool, parent_logical_name_id, filter, cursor, page_size).await
}
