//! The subnames page, the registry labels page and the child counts over the family child
//! relation, with the page semantics of crates/storage/src/children/page.rs and reads.rs: the
//! optional prefix or substring filter, the expiry fence with its null treatment, the name and timestamp sorts, the
//! keyset cursor, and a registry's labels as the ENSv2 children its subregistry holds, optionally
//! narrowed by the owner each serves (`project_name_summary.owner`). The total
//! is an exact count over the same filtered relation, taken in the same statement as the page;
//! there is no maintained child count, because eligibility depends on the parent's current
//! state. The expiry fence reads the family marker's block timestamp unless the caller fixes
//! `evaluated_at`, never the database's transaction time.
//!
//! The registration and expiry times the timestamp sorts and the fence use, the released
//! status the fence checks and the owner the labels' owner filter reads are the child's name summary (`project_name_summary`), which the
//! family step writes from the child's composed `declared_summary`.
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::{
    PgConnection, PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::time::OffsetDateTime,
};

use crate::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentSort, ChildrenCurrentSortValue, RegistryLabelOwnerFilter,
    families::name::ensure_published,
};

use super::{
    children::{CHILD_DISPLAY_NAME, CHILD_SURFACE_FILTER, Parents, push_selected},
    name_summary::CHILD_SUMMARY_JOIN,
};

/// One served child, the wire fields of the subnames route (docs/api-v1-routes.md, subnames).
/// The row carries no per-row provenance, chain positions or target blocks: those are not family
/// facts.
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

/// A registry's labels: the ENSv2 children whose registration `registry` emitted, narrowed by
/// the owner each serves when `owner` is given.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistryLabels<'a> {
    pub(crate) registry: &'a str,
    pub(crate) owner: Option<RegistryLabelOwnerFilter<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyChildrenPage {
    /// Exact number of children the filters admit, before the cursor.
    pub total_count: u64,
    pub rows: Vec<FamilyChildRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
}

/// One page of `parent_logical_name_id`'s children from the child edge families, with the
/// children route's filters, order and cursor semantics.
pub async fn load_children_shadow_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<FamilyChildrenPage> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection")?;
    page(
        &mut conn,
        parent_logical_name_id,
        filter,
        None,
        cursor,
        page_size,
    )
    .await
}

/// The exact unfiltered child count of each parent, in `parent_logical_name_ids` order, the
/// count `load_children_current_summaries` serves; one count per parent, all in one read-only
/// snapshot.
pub async fn count_children_shadow(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<(String, u64)>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open the count snapshot")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to fix the count snapshot")?;
    let mut counts = Vec::with_capacity(parent_logical_name_ids.len());
    for parent in parent_logical_name_ids {
        counts.push((parent.clone(), count(&mut transaction, parent, None).await?));
    }
    transaction
        .commit()
        .await
        .context("failed to close the count snapshot")?;
    Ok(counts)
}

/// Fails with [`FamilyPublicationUnavailable`] when a surface of one of `parent_logical_name_ids`
/// is on a chain whose family marker the publication fence would refuse (missing, not `live`,
/// another build's, or on a block a reorg orphaned): the composed name reads' check
/// (`ensure_published`), so a child read under the switch answers the stale 409 instead of an
/// empty list while a rebuild populates the families. Run it in the caller's snapshot, before
/// the reads.
pub(crate) async fn require_publication(
    conn: &mut PgConnection,
    parent_logical_name_ids: &[String],
) -> Result<()> {
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:families.topology.children_publication */
         SELECT DISTINCT surface.chain_id FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1)
         ORDER BY surface.chain_id",
    )
    .bind(parent_logical_name_ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to read the chains of the child reads' parents")?;
    ensure_published(conn, &chains).await?;
    Ok(())
}

/// The exact unfiltered child count of one parent, or of the labels its registry `registry`
/// holds.
pub(crate) async fn count(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    registry: Option<&str>,
) -> Result<u64> {
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_children(
        &mut builder,
        Parents::One(parent_logical_name_id),
        &ChildrenCurrentPageFilter::default(),
        registry.map(|registry| RegistryLabels {
            registry,
            owner: None,
        }),
    );
    builder.push(") SELECT count(*) FROM children");
    let count: i64 = builder
        .build_query_scalar()
        .fetch_one(&mut *conn)
        .await
        .with_context(|| {
            format!("failed to count the children shadow of {parent_logical_name_id}")
        })?;
    u64::try_from(count).context("negative children shadow count")
}

/// The exact unfiltered child count of each of `parent_logical_name_ids` in one statement, keyed
/// by parent; a parent with no child (or no readable surface) has no entry.
pub(crate) async fn counts(
    conn: &mut PgConnection,
    parent_logical_name_ids: &[String],
) -> Result<BTreeMap<String, u64>> {
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_children(
        &mut builder,
        Parents::Many(parent_logical_name_ids),
        &ChildrenCurrentPageFilter::default(),
        None,
    );
    builder.push(
        ") SELECT parent_logical_name_id, count(*) FROM children
         GROUP BY parent_logical_name_id",
    );
    let rows: Vec<(String, i64)> = builder
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .context("failed to count the children shadow of the parents")?;
    rows.into_iter()
        .map(|(parent, count)| {
            Ok((
                parent,
                u64::try_from(count).context("negative children shadow count")?,
            ))
        })
        .collect()
}

/// One page and its exact total in one statement.
pub(crate) async fn page(
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
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_children(
        &mut builder,
        Parents::One(parent_logical_name_id),
        filter,
        registry,
    );
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

/// The selected children CTEs and the `children` relation (left open, closed by the caller)
/// after the read filter, the prefix, the expiry fence and, for a registry's labels, the registry
/// and owner filters, with each child's served fields and `sort_timestamp`.
fn push_children<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    parents: Parents<'a>,
    filter: &ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
) {
    push_selected(builder, parents);
    let sort_timestamp = match filter.sort {
        ChildrenCurrentSort::Name => "NULL::TIMESTAMPTZ",
        ChildrenCurrentSort::ExpiresAt => "summary.expires_at",
        ChildrenCurrentSort::RegisteredAt => "summary.registered_at",
    };
    builder.push(format!(
        " children AS (
            SELECT selected.parent_logical_name_id, selected.child_logical_name_id,
                   selected.namespace, {CHILD_DISPLAY_NAME} AS canonical_display_name,
                   selected.namehash, selected.labelhash, selected.owner, selected.registrant,
                   {sort_timestamp} AS sort_timestamp
            FROM selected
            JOIN parent ON parent.logical_name_id = selected.parent_logical_name_id
            JOIN clock ON clock.chain_id = parent.chain_id
            LEFT JOIN bigname_phase.name_surfaces child_surface
              ON child_surface.logical_name_id = selected.child_logical_name_id
            {CHILD_SUMMARY_JOIN}
            WHERE selected.pair_rank = 1{CHILD_SURFACE_FILTER}"
    ));
    if let Some(labels) = registry {
        builder.push(" AND selected.registry_address = ");
        builder.push_bind(labels.registry);
        // The owner a label serves is its composed name row's (apps/api/src/v2/subnames.rs,
        // `build_subname`); the ENSv2 arm's child row carries none, so a label without a summary
        // or a summary owner is ownerless and only the exclusion admits it.
        match labels.owner {
            Some(RegistryLabelOwnerFilter::Owner(owner)) => {
                builder.push(" AND summary.owner = ");
                builder.push_bind(owner);
            }
            Some(RegistryLabelOwnerFilter::ExcludeOwner(owner)) => {
                builder.push(" AND summary.owner IS DISTINCT FROM ");
                builder.push_bind(owner);
            }
            None => {}
        }
    }
    if let Some(q) = filter.q {
        builder.push(format!(" AND {CHILD_DISPLAY_NAME} LIKE "));
        builder.push_bind(q.like_pattern());
        builder.push(" ESCAPE '\\'");
    }
    if !filter.include_expired {
        // A child with no name summary, no registration or no expiry is not expired.
        builder.push(
            " AND COALESCE(summary.registration_status, '') <> 'released' \
             AND COALESCE(summary.expires_at >= ",
        );
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

/// Rows without a timestamp sort last ascending and first descending, as
/// crates/storage/src/children/page.rs does.
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
