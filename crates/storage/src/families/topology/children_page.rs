//! The subnames page over the family child relation, with today's page semantics
//! (children/page.rs): the optional prefix, the expiry fence with its null treatment, the name and
//! timestamp sorts, and the keyset cursor. The total is an exact count over the same filtered
//! relation, taken in the same statement as the page (design section 2.2); there is no maintained
//! child count. The expiry fence reads the block clock of the family marker unless the caller
//! fixes `evaluated_at`, never the database's transaction time.
use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::time::OffsetDateTime};

use crate::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentSort, ChildrenCurrentSortValue,
    address_names::{
        escape_like_pattern, push_expires_at_timestamp_expr, push_registered_at_timestamp_expr,
    },
};

use super::children::{CHILD_DISPLAY_NAME, CHILD_SURFACE_FILTER, push_selected};

/// One served child, the wire fields of the subnames route (docs/api-v1-routes.md, subnames).
/// The per-row provenance, chain positions and target blocks `children_current` stamps are not
/// family facts (design section 2, read model) and are not reproduced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyChildRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyChildrenPage {
    /// Exact number of children the filters admit, before the cursor.
    pub total_count: u64,
    pub rows: Vec<FamilyChildRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
}

/// The shadow of `load_children_current_page_filtered`: the same rows in the same order with the
/// same cursor semantics, read from the child edge families.
pub async fn load_children_shadow_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
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
    let joins_name_current = filter.sort.is_timestamp() || !filter.include_expired;
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_selected(&mut builder, parent_logical_name_id);
    builder.push(format!(
        " children AS (
            SELECT selected.parent_logical_name_id, selected.child_logical_name_id,
                   selected.namespace, {CHILD_DISPLAY_NAME} AS canonical_display_name,
                   selected.namehash,
                   selected.labelhash, selected.owner, selected.registrant, "
    ));
    match filter.sort {
        ChildrenCurrentSort::Name => {
            builder.push("NULL::TIMESTAMPTZ");
        }
        ChildrenCurrentSort::ExpiresAt => push_expires_at_timestamp_expr(&mut builder),
        ChildrenCurrentSort::RegisteredAt => push_registered_at_timestamp_expr(&mut builder),
    }
    builder.push(
        " AS sort_timestamp
            FROM selected CROSS JOIN parent CROSS JOIN clock
            LEFT JOIN bigname_phase.name_surfaces child_surface
              ON child_surface.logical_name_id = selected.child_logical_name_id",
    );
    if joins_name_current {
        builder.push(
            " LEFT JOIN bigname_phase.name_current nc
                ON nc.logical_name_id = selected.child_logical_name_id",
        );
    }
    builder.push(" WHERE selected.pair_rank = 1");
    builder.push(CHILD_SURFACE_FILTER);
    if let Some(prefix) = filter.q {
        builder.push(format!(" AND {CHILD_DISPLAY_NAME} LIKE "));
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    if !filter.include_expired {
        // A child with no name row, no registration or no expiry is not expired.
        builder.push(
            " AND COALESCE(nc.declared_summary #>> '{registration,status}', '') <> 'released' \
             AND COALESCE(",
        );
        push_expires_at_timestamp_expr(&mut builder);
        builder.push(" >= ");
        match filter.evaluated_at {
            Some(evaluated_at) => {
                builder.push_bind(evaluated_at);
            }
            None => {
                builder.push("clock.block_timestamp");
            }
        }
        builder.push(", TRUE)");
    }
    builder.push("), page AS (SELECT * FROM children WHERE TRUE");
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
    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!("failed to load the children shadow of {parent_logical_name_id}")
    })?;
    let total_count = match rows.first() {
        Some(row) => u64::try_from(row.try_get::<i64, _>("total_count")?)
            .context("negative children shadow count")?,
        None => 0,
    };
    let mut decoded = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(child) = decode(row)? {
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

fn decode(row: &PgRow) -> Result<Option<(FamilyChildRow, Option<OffsetDateTime>)>> {
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

/// Rows without a timestamp sort last ascending and first descending, as children/page.rs does.
fn null_rank_expr(order: ChildrenCurrentOrder, prefix: &str) -> String {
    match order {
        ChildrenCurrentOrder::Asc => {
            format!("CASE WHEN {prefix}sort_timestamp IS NULL THEN 1 ELSE 0 END")
        }
        ChildrenCurrentOrder::Desc => {
            format!("CASE WHEN {prefix}sort_timestamp IS NULL THEN 0 ELSE 1 END")
        }
    }
}

fn null_rank(is_null: bool, order: ChildrenCurrentOrder) -> i32 {
    match (is_null, order) {
        (true, ChildrenCurrentOrder::Asc) | (false, ChildrenCurrentOrder::Desc) => 1,
        (false, ChildrenCurrentOrder::Asc) | (true, ChildrenCurrentOrder::Desc) => 0,
    }
}
