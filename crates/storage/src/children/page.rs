//! Filtered, sortable declared direct child page reads.
//!
//! The unfiltered `load_children_current_page` is the `ChildrenCurrentPageFilter::default()`
//! case of `load_children_current_page_filtered`. The page is a two-stage query: a `children`
//! CTE applies the read fence, the optional served-name prefix, and the optional expiry fence
//! and computes one `sort_timestamp` column; the outer SELECT applies the keyset predicate and
//! order over that column so the cursor logic is written once per sort, not once per filter.

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::time::OffsetDateTime};

use crate::address_names::{
    escape_like_pattern, push_expires_at_timestamp_expr, push_registered_at_timestamp_expr,
};
use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page,
};

use super::{
    DECLARED_SURFACE_CLASS, DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS,
    DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    reads::{
        CHILD_DISPLAY_NAME_EXPR, CHILD_DISPLAY_PARENT_JOIN, decode_children_current_row,
        load_children_current_summary,
    },
    types::{
        ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPage,
        ChildrenCurrentPageFilter, ChildrenCurrentRow, ChildrenCurrentSort,
        ChildrenCurrentSortValue,
    },
};

/// The child's current name row, read for timestamp sorts and the expiry fence. `nc` is the
/// alias the shared `declared_summary` timestamp expressions expect.
const CHILD_NAME_CURRENT_JOIN: &str = r#"
  LEFT JOIN bigname_phase.name_current nc
    ON nc.logical_name_id = cc.child_logical_name_id
"#;

/// Load a bounded page of declared direct children narrowed and ordered by `filter`.
///
/// `summary` on the returned page is always the unfiltered per-parent aggregate: callers that
/// narrowed the page must not present it as the page's total. `cursor` must have been issued for
/// the same `sort` and `order`; a cursor whose sort value kind does not match the sort is
/// rejected here rather than silently re-anchored.
pub async fn load_children_current_page_filtered(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    let limit = checked_page_limit_i64(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size is too large",
    )?;
    let page_size = checked_page_size_usize(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size does not fit in usize",
    )?;
    if let Some(cursor) = cursor {
        ensure_cursor_matches_sort(filter.sort, cursor)?;
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH children AS (");
    push_children_cte(&mut builder, parent_logical_name_id, filter);
    builder.push(") SELECT * FROM children WHERE TRUE");
    if let Some(cursor) = cursor {
        push_cursor_after(&mut builder, filter.order, cursor);
    }
    push_order(&mut builder, filter.sort, filter.order);
    builder.push(" LIMIT ");
    builder.push_bind(limit);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load phase children_current page for {parent_logical_name_id} sort {} order {} q {:?} include_expired {}",
                sort_name(filter.sort),
                order_name(filter.order),
                filter.q,
                filter.include_expired
            )
        })?
        .into_iter()
        .map(decode_sorted_row)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        cursor_from_sorted_row(row, filter.sort)
    });
    let rows = rows.into_iter().map(|row| row.row).collect();
    let summary = load_children_current_summary(pool, parent_logical_name_id).await?;
    Ok(ChildrenCurrentPage {
        rows,
        next_cursor,
        summary,
    })
}

fn push_children_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    parent_logical_name_id: &'a str,
    filter: &ChildrenCurrentPageFilter<'a>,
) {
    builder.push(format!(
        r#"
    SELECT cc.parent_logical_name_id, cc.child_logical_name_id, cc.surface_class,
           cc.namespace,
           {CHILD_DISPLAY_NAME_EXPR} AS canonical_display_name,
           {CHILD_DISPLAY_NAME_EXPR} AS normalized_name,
           cc.namehash, cc.labelhash, cc.owner, cc.registrant, cc.provenance,
           cc.chain_positions, cc.canonicality_summary, cc.manifest_version,
           cc.last_recomputed_at,
           "#
    ));
    match filter.sort {
        ChildrenCurrentSort::Name => {
            builder.push("NULL::TIMESTAMPTZ");
        }
        ChildrenCurrentSort::ExpiresAt => push_expires_at_timestamp_expr(builder),
        ChildrenCurrentSort::RegisteredAt => push_registered_at_timestamp_expr(builder),
    }
    builder.push(" AS sort_timestamp FROM bigname_phase.children_current cc");
    builder.push(CHILD_DISPLAY_PARENT_JOIN);
    builder.push(DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS);
    if filter.joins_name_current() {
        builder.push(CHILD_NAME_CURRENT_JOIN);
    }
    builder.push(" WHERE cc.parent_logical_name_id = ");
    builder.push_bind(parent_logical_name_id);
    builder.push(" AND cc.surface_class = ");
    builder.push_bind(DECLARED_SURFACE_CLASS);
    builder.push(DEFAULT_CHILDREN_CURRENT_READ_FILTER);
    if let Some(prefix) = filter.q {
        builder.push(format!(" AND {CHILD_DISPLAY_NAME_EXPR} LIKE "));
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    if !filter.include_expired {
        // A child is expired when its current registration is released, or when the expiry the
        // `sort=expires_at` order reads is already behind the database's transaction time. A
        // child with no name row, no registration, or no expiry is not expired, so the expiry
        // comparison is folded to TRUE when it is NULL instead of dropping the row.
        builder.push(
            " AND COALESCE(nc.declared_summary #>> '{registration,status}', '') <> 'released' AND COALESCE(",
        );
        push_expires_at_timestamp_expr(builder);
        builder.push(" >= NOW(), TRUE)");
    }
}

fn push_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    order: ChildrenCurrentOrder,
    cursor: &'a ChildrenCurrentKeysetCursor,
) {
    match &cursor.sort_value {
        ChildrenCurrentSortValue::Name => {
            builder.push(" AND (canonical_display_name ");
            builder.push(strict_comparator(order));
            builder.push_bind(&cursor.canonical_display_name);
            builder.push(" OR (canonical_display_name = ");
            builder.push_bind(&cursor.canonical_display_name);
            builder.push(" AND child_logical_name_id > ");
            builder.push_bind(&cursor.child_logical_name_id);
            builder.push("))");
        }
        ChildrenCurrentSortValue::Timestamp(sort_value) => {
            let cursor_rank = null_rank(sort_value.is_none(), order);
            builder.push(" AND (");
            builder.push(null_rank_expr(order));
            builder.push(" > ");
            builder.push_bind(cursor_rank);
            builder.push(" OR (");
            builder.push(null_rank_expr(order));
            builder.push(" = ");
            builder.push_bind(cursor_rank);
            builder.push(" AND ");
            match sort_value {
                None => {
                    builder.push("sort_timestamp IS NULL AND ");
                    push_name_tie_after(builder, cursor);
                }
                Some(value) => {
                    builder.push("(sort_timestamp ");
                    builder.push(strict_comparator(order));
                    builder.push_bind(*value);
                    builder.push(" OR (sort_timestamp = ");
                    builder.push_bind(*value);
                    builder.push(" AND ");
                    push_name_tie_after(builder, cursor);
                    builder.push("))");
                }
            }
            builder.push("))");
        }
    }
}

fn push_name_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a ChildrenCurrentKeysetCursor,
) {
    builder.push("(canonical_display_name, child_logical_name_id) > (");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(", ");
    builder.push_bind(&cursor.child_logical_name_id);
    builder.push(")");
}

fn push_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: ChildrenCurrentSort,
    order: ChildrenCurrentOrder,
) {
    let direction = match order {
        ChildrenCurrentOrder::Asc => "ASC",
        ChildrenCurrentOrder::Desc => "DESC",
    };
    match sort {
        ChildrenCurrentSort::Name => {
            builder.push(format!(
                " ORDER BY canonical_display_name {direction}, child_logical_name_id ASC"
            ));
        }
        ChildrenCurrentSort::ExpiresAt | ChildrenCurrentSort::RegisteredAt => {
            builder.push(" ORDER BY ");
            builder.push(null_rank_expr(order));
            builder.push(format!(
                " ASC, sort_timestamp {direction}, canonical_display_name ASC, child_logical_name_id ASC"
            ));
        }
    }
}

/// Rows without a timestamp sort last ascending and first descending, the same convention the
/// address-name timestamp sorts use.
fn null_rank_expr(order: ChildrenCurrentOrder) -> &'static str {
    match order {
        ChildrenCurrentOrder::Asc => "CASE WHEN sort_timestamp IS NULL THEN 1 ELSE 0 END",
        ChildrenCurrentOrder::Desc => "CASE WHEN sort_timestamp IS NULL THEN 0 ELSE 1 END",
    }
}

fn null_rank(is_null: bool, order: ChildrenCurrentOrder) -> i32 {
    match (is_null, order) {
        (true, ChildrenCurrentOrder::Asc) | (false, ChildrenCurrentOrder::Desc) => 1,
        (false, ChildrenCurrentOrder::Asc) | (true, ChildrenCurrentOrder::Desc) => 0,
    }
}

fn strict_comparator(order: ChildrenCurrentOrder) -> &'static str {
    match order {
        ChildrenCurrentOrder::Asc => "> ",
        ChildrenCurrentOrder::Desc => "< ",
    }
}

fn ensure_cursor_matches_sort(
    sort: ChildrenCurrentSort,
    cursor: &ChildrenCurrentKeysetCursor,
) -> Result<()> {
    match (sort, &cursor.sort_value) {
        (ChildrenCurrentSort::Name, ChildrenCurrentSortValue::Name)
        | (
            ChildrenCurrentSort::ExpiresAt | ChildrenCurrentSort::RegisteredAt,
            ChildrenCurrentSortValue::Timestamp(_),
        ) => Ok(()),
        _ => anyhow::bail!(
            "children_current page cursor sort value does not match sort {}",
            sort_name(sort)
        ),
    }
}

struct SortedChildRow {
    row: ChildrenCurrentRow,
    sort_timestamp: Option<OffsetDateTime>,
}

fn decode_sorted_row(row: PgRow) -> Result<SortedChildRow> {
    let sort_timestamp: Option<OffsetDateTime> = row.try_get("sort_timestamp")?;
    Ok(SortedChildRow {
        row: decode_children_current_row(row)?,
        sort_timestamp,
    })
}

fn cursor_from_sorted_row(
    row: &SortedChildRow,
    sort: ChildrenCurrentSort,
) -> ChildrenCurrentKeysetCursor {
    ChildrenCurrentKeysetCursor {
        sort_value: match sort {
            ChildrenCurrentSort::Name => ChildrenCurrentSortValue::Name,
            ChildrenCurrentSort::ExpiresAt | ChildrenCurrentSort::RegisteredAt => {
                ChildrenCurrentSortValue::Timestamp(row.sort_timestamp)
            }
        },
        canonical_display_name: row.row.canonical_display_name.clone(),
        child_logical_name_id: row.row.child_logical_name_id.clone(),
    }
}

fn sort_name(sort: ChildrenCurrentSort) -> &'static str {
    match sort {
        ChildrenCurrentSort::Name => "name",
        ChildrenCurrentSort::ExpiresAt => "expires_at",
        ChildrenCurrentSort::RegisteredAt => "registered_at",
    }
}

fn order_name(order: ChildrenCurrentOrder) -> &'static str {
    match order {
        ChildrenCurrentOrder::Asc => "asc",
        ChildrenCurrentOrder::Desc => "desc",
    }
}
