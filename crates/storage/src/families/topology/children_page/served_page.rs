//! The children page as the served relation composes it at read, before the child listing
//! family stored it: the oracle the stored page and total are checked against
//! (`listing_support.rs`, `checked_page`), and the statement the relation's plan tests read
//! (`performance_tests.rs`, `registry_labels_tests.rs`). It keeps the old order, nulls first
//! ascending and last descending with name and child ties ascending, in the database's
//! collation, and the old total, an exact count over the same relation.
use anyhow::{Context, Result, bail};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentSort, ChildrenCurrentSortValue, UnixSeconds,
};

use super::{
    super::children::Parents, FamilyChildRow, FamilyChildrenPage, RegistryLabels, push_children,
};

/// One page and its exact total in one statement.
pub(crate) async fn served_page(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    registry: Option<RegistryLabels<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<FamilyChildrenPage> {
    if page_size == 0 {
        bail!("children shadow page_size must be positive");
    }
    let limit = i64::try_from(page_size).context("children shadow page_size is too large")? + 1;
    if let Some(cursor) = cursor {
        let matches = matches!(
            (filter.sort, &cursor.sort_value),
            (ChildrenCurrentSort::Name, ChildrenCurrentSortValue::Name)
                | (
                    ChildrenCurrentSort::ExpiresAt | ChildrenCurrentSort::RegisteredAt,
                    ChildrenCurrentSortValue::Timestamp(_)
                )
        );
        if !matches {
            bail!("children shadow cursor sort value does not match the sort");
        }
    }
    let mut builder = served_page_query(parent_logical_name_id, filter, registry, cursor, limit);
    let rows = builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .with_context(|| {
            format!("failed to load the children shadow of {parent_logical_name_id}")
        })?;
    let total_count = match rows.first() {
        Some(row) => u64::try_from(row.try_get::<i64, _>("total_count")?)
            .context("negative children shadow count")?,
        None => 0,
    };
    let mut decoded = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(child) = served_decode(row)? {
            decoded.push(child);
        }
    }
    let has_more = decoded.len() > usize::try_from(page_size).unwrap_or(usize::MAX);
    decoded.truncate(usize::try_from(page_size).unwrap_or(usize::MAX));
    let next_cursor = has_more
        .then(|| decoded.last())
        .flatten()
        .map(|(row, sort_timestamp)| ChildrenCurrentKeysetCursor {
            sort_value: match filter.sort {
                ChildrenCurrentSort::Name => ChildrenCurrentSortValue::Name,
                _ => ChildrenCurrentSortValue::Timestamp(*sort_timestamp),
            },
            canonical_display_name: row.canonical_display_name.clone(),
            child_logical_name_id: row.child_logical_name_id.clone(),
        });
    Ok(FamilyChildrenPage {
        total_count,
        rows: decoded.into_iter().map(|(row, _)| row).collect(),
        next_cursor,
    })
}

pub(crate) fn served_page_query<'a>(
    parent_logical_name_id: &'a str,
    filter: &'a ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
    cursor: Option<&'a ChildrenCurrentKeysetCursor>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_children(
        &mut builder,
        Parents::One(parent_logical_name_id),
        filter,
        registry,
        None,
    );
    let sort_timestamp = match filter.sort {
        ChildrenCurrentSort::Name => "NULL::NUMERIC",
        ChildrenCurrentSort::ExpiresAt => "children.expires_at",
        ChildrenCurrentSort::RegisteredAt => "children.registered_at",
    };
    builder.push(format!(
        "), sorted AS (SELECT children.*, {sort_timestamp} AS sort_timestamp FROM children),
         page AS (SELECT * FROM sorted WHERE TRUE"
    ));
    if let Some(cursor) = cursor {
        push_cursor_after(&mut builder, filter.order, cursor);
    }
    push_order(&mut builder, filter.sort, filter.order, "");
    builder.push(" LIMIT ");
    builder.push_bind(limit);
    builder.push(
        ") SELECT total.total_count, page.* FROM (SELECT count(*) AS total_count FROM children) total
           LEFT JOIN page ON TRUE",
    );
    push_order(&mut builder, filter.sort, filter.order, "page.");
    builder
}

pub(crate) fn served_decode(row: &PgRow) -> Result<Option<(FamilyChildRow, Option<UnixSeconds>)>> {
    let Some(child_logical_name_id) = row.try_get::<Option<String>, _>("child_logical_name_id")?
    else {
        return Ok(None);
    };
    Ok(Some((
        FamilyChildRow {
            parent_logical_name_id: row.try_get("parent_logical_name_id")?,
            child_logical_name_id,
            namespace: row.try_get("namespace")?,
            canonical_display_name: row.try_get("canonical_display_name")?,
            namehash: row.try_get("namehash")?,
            labelhash: row.try_get("labelhash")?,
            owner: row.try_get("owner")?,
            registrant: row.try_get("registrant")?,
            registry_authority: row.try_get("registry_authority")?,
            lifecycle_shadow: row.try_get("lifecycle_shadow")?,
            wrapper_held: row.try_get("wrapper_held")?,
            released_lease: row.try_get("released_lease")?,
            token_holder: row.try_get("token_holder")?,
        },
        row.try_get("sort_timestamp")?,
    )))
}

fn push_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    order: ChildrenCurrentOrder,
    cursor: &'a ChildrenCurrentKeysetCursor,
) {
    let strict = match order {
        ChildrenCurrentOrder::Asc => " > ",
        ChildrenCurrentOrder::Desc => " < ",
    };
    match &cursor.sort_value {
        ChildrenCurrentSortValue::Name => {
            builder.push(format!(" AND (canonical_display_name{strict}"));
            builder.push_bind(&cursor.canonical_display_name);
            builder.push(" OR (canonical_display_name = ");
            builder.push_bind(&cursor.canonical_display_name);
            builder.push(" AND child_logical_name_id > ");
            builder.push_bind(&cursor.child_logical_name_id);
            builder.push("))");
        }
        ChildrenCurrentSortValue::Timestamp(value) => {
            let rank = null_rank(value.is_none(), order);
            let rank_expr = null_rank_expr(order, "");
            builder.push(format!(" AND ({rank_expr} > "));
            builder.push_bind(rank);
            builder.push(format!(" OR ({rank_expr} = "));
            builder.push_bind(rank);
            builder.push(" AND ");
            match value {
                None => {
                    builder.push("sort_timestamp IS NULL AND ");
                    push_name_tie_after(builder, cursor);
                }
                Some(value) => {
                    builder.push(format!("(sort_timestamp{strict}"));
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
    prefix: &str,
) {
    let direction = match order {
        ChildrenCurrentOrder::Asc => "ASC",
        ChildrenCurrentOrder::Desc => "DESC",
    };
    builder.push(match sort {
        ChildrenCurrentSort::Name => format!(
            " ORDER BY {prefix}canonical_display_name {direction}, \
             {prefix}child_logical_name_id ASC"
        ),
        ChildrenCurrentSort::ExpiresAt | ChildrenCurrentSort::RegisteredAt => format!(
            " ORDER BY {} ASC, {prefix}sort_timestamp {direction}, \
             {prefix}canonical_display_name ASC, {prefix}child_logical_name_id ASC",
            null_rank_expr(order, prefix)
        ),
    });
}

/// A missing timestamp is the smallest value: rows without one sort first ascending and last
/// descending.
fn null_rank_expr(order: ChildrenCurrentOrder, prefix: &str) -> String {
    match order {
        ChildrenCurrentOrder::Asc => {
            format!("CASE WHEN {prefix}sort_timestamp IS NULL THEN 0 ELSE 1 END")
        }
        ChildrenCurrentOrder::Desc => {
            format!("CASE WHEN {prefix}sort_timestamp IS NULL THEN 1 ELSE 0 END")
        }
    }
}

fn null_rank(is_null: bool, order: ChildrenCurrentOrder) -> i32 {
    match (is_null, order) {
        (false, ChildrenCurrentOrder::Asc) | (true, ChildrenCurrentOrder::Desc) => 1,
        (true, ChildrenCurrentOrder::Asc) | (false, ChildrenCurrentOrder::Desc) => 0,
    }
}
