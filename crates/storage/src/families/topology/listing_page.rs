//! The subnames page, the registry labels page and the child counts over the child listing
//! family (`project_child_listing`, F17) and its counts (`project_child_listing_count`), which
//! Project writes at every publication (`listing.rs`). A page is a keyset over the sort's index:
//! the parent's rows from the cursor's stored key on, in the requested order, the first
//! `page_size` the filters admit. No child is composed to answer it.
//!
//! Each sort reads one index, ascending forwards and descending backwards, so a descending page
//! is the exact reverse of the ascending one, ties included ([`DESCENDING_TIES`]). The name sort
//! orders the stored key in C collation. A missing timestamp is stored as
//! [`MISSING_TIMESTAMP`], so it sorts first ascending and last descending.
//!
//! The unfiltered total is a primary-key read of the counts, and a filtered total counts the
//! parent's rows through the same index the page reads, stopping one row past a caller's cap. A
//! registry's label count is a primary-key read of the counts too. A page row whose stored name keeps a
//! label placeholder is spelled again by the served relation, narrowed to those children, so an
//! imported label serves healed while the row keeps its stored key ([`heal`]).
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentSort, ChildrenCurrentSortValue, NameQueryMatch, RegistryLabelOwnerFilter,
    UnixSeconds,
};

use super::{
    children::Parents,
    children_page::{FamilyChildRow, FamilyChildrenPage, RegistryLabels, push_children},
    listing::{DISPLAY_KEY_CHARS, MISSING_TIMESTAMP},
};

/// The direction of a descending page's ties after its sort key: the stored key and then the
/// child id run backwards too, so one backward index scan serves the page.
const DESCENDING_TIES: &str = "DESC";

/// The parent's rows: its chain is its surface's.
const PARENT_ROWS: &str = "
    FROM bigname_phase.project_child_listing listing
    WHERE listing.chain_id = (SELECT surface.chain_id FROM bigname_phase.name_surfaces surface
                              WHERE surface.logical_name_id = ";

const PAGE_COLUMNS: &str = "listing.parent_logical_name_id, listing.child_logical_name_id,
    listing.namespace, listing.display_key, listing.display_name, listing.namehash,
    listing.labelhash, listing.owner, listing.registrant, listing.registry_authority,
    listing.lifecycle_shadow, listing.wrapper_held, listing.released_lease, listing.token_holder";

/// The largest code point: a key that starts with a prefix sorts below the prefix followed by it.
const KEY_CEILING: char = '\u{10FFFF}';

/// The count of parent `$1` (all its children for registry `''`, else the labels of registry
/// `$2`): one primary-key read.
pub(super) const COUNT: &str = "/* storage:families.topology.child_listing_count */
    SELECT counter.child_count
    FROM bigname_phase.name_surfaces surface
    JOIN bigname_phase.project_child_listing_count counter
      ON counter.chain_id = surface.chain_id
     AND counter.parent_logical_name_id = surface.logical_name_id
     AND counter.registry_address = $2
    WHERE surface.logical_name_id = $1";

/// The child count of each parent of `$1` that has a child: one primary-key read each.
pub(super) const COUNTS: &str = "/* storage:families.topology.child_listing_counts */
    SELECT counter.parent_logical_name_id, counter.child_count
    FROM bigname_phase.name_surfaces surface
    JOIN bigname_phase.project_child_listing_count counter
      ON counter.chain_id = surface.chain_id
     AND counter.parent_logical_name_id = surface.logical_name_id
     AND counter.registry_address = ''
    WHERE surface.logical_name_id = ANY($1)";

/// The stored sort key of `sort`, `None` for the name sort.
const fn sort_key(sort: ChildrenCurrentSort) -> Option<&'static str> {
    match sort {
        ChildrenCurrentSort::Name => None,
        ChildrenCurrentSort::ExpiresAt => Some("listing.expires_key"),
        ChildrenCurrentSort::RegisteredAt => Some("listing.registered_key"),
    }
}

/// One page and its total, in the caller's snapshot. The total is exact, or `total_cap + 1` when
/// it is above `total_cap`; the rows and cursor are those of the uncapped page.
pub(crate) async fn page(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    registry: Option<RegistryLabels<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
    total_cap: Option<u64>,
) -> Result<FamilyChildrenPage> {
    if page_size == 0 {
        bail!("children shadow page_size must be positive");
    }
    let limit = i64::try_from(page_size).context("children shadow page_size is too large")? + 1;
    let total_limit = total_cap.map(past).transpose()?;
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
    let rows = page_query(parent_logical_name_id, filter, registry, cursor, limit)
        .build()
        .fetch_all(&mut *conn)
        .await
        .with_context(|| format!("failed to load the child listing of {parent_logical_name_id}"))?;
    let mut listed = rows.iter().map(decode).collect::<Result<Vec<_>>>()?;
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    let has_more = listed.len() > page_size;
    listed.truncate(page_size);
    let next_cursor =
        has_more
            .then(|| listed.last())
            .flatten()
            .map(|row| ChildrenCurrentKeysetCursor {
                sort_value: match filter.sort {
                    ChildrenCurrentSort::Name => ChildrenCurrentSortValue::Name,
                    _ => ChildrenCurrentSortValue::Timestamp(row.sort_timestamp),
                },
                // The stored key, never the served name: the next page continues from the index.
                canonical_display_name: row.display_key.clone(),
                child_logical_name_id: row.child.child_logical_name_id.clone(),
            });
    let mut rows: Vec<FamilyChildRow> = listed.into_iter().map(|row| row.child).collect();
    heal(conn, parent_logical_name_id, &mut rows).await?;
    let total_count = total(conn, parent_logical_name_id, filter, registry, total_limit).await?;
    Ok(FamilyChildrenPage {
        total_count,
        rows,
        next_cursor,
    })
}

/// The exact unfiltered child count of one parent, or of the labels its registry `registry`
/// holds: one primary-key read of the counts.
pub(crate) async fn count(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    registry: Option<&str>,
) -> Result<u64> {
    let count: Option<i64> = sqlx::query_scalar(COUNT)
        .bind(parent_logical_name_id)
        .bind(registry.unwrap_or_default())
        .fetch_optional(&mut *conn)
        .await
        .with_context(|| format!("failed to count the children of {parent_logical_name_id}"))?;
    u64::try_from(count.unwrap_or_default()).context("negative child count")
}

/// The exact number of labels `registry` holds under `parent_logical_name_id`, one primary-key
/// read of the counts, or `None` when they are more than `cap`.
pub(crate) async fn count_registry_labels(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    registry: &str,
    cap: u64,
) -> Result<Option<u64>> {
    let labels = count(conn, parent_logical_name_id, Some(registry)).await?;
    Ok((labels <= cap).then_some(labels))
}

/// The bind of a `LIMIT` that keeps one row past `cap`.
fn past(cap: u64) -> Result<i64> {
    i64::try_from(cap)
        .ok()
        .and_then(|cap| cap.checked_add(1))
        .context("children count cap is too large")
}

/// True when the filter admits every child the stored counts count.
const fn admits_every_child(filter: &ChildrenCurrentPageFilter<'_>) -> bool {
    filter.q.is_none() && filter.include_expired
}

/// The exact unfiltered child count of each of `parent_logical_name_ids` in one statement, keyed
/// by parent; a parent with no child (or no surface) has no entry.
pub(crate) async fn counts(
    conn: &mut PgConnection,
    parent_logical_name_ids: &[String],
) -> Result<BTreeMap<String, u64>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(COUNTS)
        .bind(parent_logical_name_ids)
        .fetch_all(&mut *conn)
        .await
        .context("failed to count the children of the parents")?;
    rows.into_iter()
        .map(|(parent, count)| {
            Ok((
                parent,
                u64::try_from(count).context("negative child count")?,
            ))
        })
        .collect()
}

/// The total the filters admit, before the cursor: exact, or `total_limit` when it reaches that
/// bind. With no filter it is the parent's (or the registry's) stored count. A registry's labels
/// excluding one owner are its count less the owner's labels, read from the owner index.
async fn total(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    registry: Option<RegistryLabels<'_>>,
    total_limit: Option<i64>,
) -> Result<u64> {
    if !admits_every_child(filter) {
        return filtered_count(conn, parent_logical_name_id, filter, registry, total_limit).await;
    }
    let registry_address = registry.map(|labels| labels.registry);
    let total = match registry.and_then(|labels| labels.owner) {
        None => count(conn, parent_logical_name_id, registry_address).await?,
        Some(RegistryLabelOwnerFilter::Owner(_)) => {
            return filtered_count(conn, parent_logical_name_id, filter, registry, total_limit)
                .await;
        }
        Some(RegistryLabelOwnerFilter::ExcludeOwner(owner)) => {
            let labels = count(conn, parent_logical_name_id, registry_address).await?;
            // The difference needs the owner's exact count.
            let owned = filtered_count(
                conn,
                parent_logical_name_id,
                filter,
                registry.map(|labels| RegistryLabels {
                    owner: Some(RegistryLabelOwnerFilter::Owner(owner)),
                    ..labels
                }),
                None,
            )
            .await?;
            labels
                .checked_sub(owned)
                .context("an owner holds more labels than its registry counts")?
        }
    };
    Ok(match total_limit.map(u64::try_from).transpose()? {
        Some(limit) => total.min(limit),
        None => total,
    })
}

async fn filtered_count(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    registry: Option<RegistryLabels<'_>>,
    total_limit: Option<i64>,
) -> Result<u64> {
    let count: i64 = capped_count_query(parent_logical_name_id, filter, registry, total_limit)
        .build_query_scalar()
        .fetch_one(&mut *conn)
        .await
        .with_context(|| format!("failed to count the children of {parent_logical_name_id}"))?;
    u64::try_from(count).context("negative child count")
}

pub(super) fn count_query<'a>(
    parent_logical_name_id: &'a str,
    filter: &ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
) -> QueryBuilder<'a, Postgres> {
    capped_count_query(parent_logical_name_id, filter, registry, None)
}

/// The filtered count, over at most `total_limit` of the parent's rows when it is given.
pub(super) fn capped_count_query<'a>(
    parent_logical_name_id: &'a str,
    filter: &ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
    total_limit: Option<i64>,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::<Postgres>::new(match total_limit {
        None => format!(
            "/* storage:families.topology.child_listing_total */ SELECT count(*){PARENT_ROWS}"
        ),
        Some(_) => format!(
            "/* storage:families.topology.child_listing_capped_total */
             SELECT count(*) FROM (SELECT 1{PARENT_ROWS}"
        ),
    });
    push_parent(&mut builder, parent_logical_name_id);
    push_filters(&mut builder, filter, registry);
    if let Some(total_limit) = total_limit {
        builder.push(" LIMIT ");
        builder.push_bind(total_limit);
        builder.push(") capped");
    }
    builder
}

pub(super) fn page_query<'a>(
    parent_logical_name_id: &'a str,
    filter: &'a ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
    cursor: Option<&'a ChildrenCurrentKeysetCursor>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    let sort_timestamp = sort_key(filter.sort).map_or_else(
        || "NULL::numeric".to_owned(),
        |key| format!("NULLIF({key}, {MISSING_TIMESTAMP})"),
    );
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "/* storage:families.topology.child_listing_page */
         SELECT {PAGE_COLUMNS}, {sort_timestamp} AS sort_timestamp{PARENT_ROWS}"
    ));
    push_parent(&mut builder, parent_logical_name_id);
    push_filters(&mut builder, filter, registry);
    if let Some(cursor) = cursor {
        push_after(&mut builder, filter, cursor);
    }
    push_order(&mut builder, filter);
    builder.push(" LIMIT ");
    builder.push_bind(limit);
    builder
}

fn push_parent<'a>(builder: &mut QueryBuilder<'a, Postgres>, parent_logical_name_id: &'a str) {
    builder.push_bind(parent_logical_name_id);
    builder.push(") AND listing.parent_logical_name_id = ");
    builder.push_bind(parent_logical_name_id);
}

/// The registry and owner filters of a registry's labels, the prefix or substring filter, and
/// the expiry fence, over the stored columns.
fn push_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
) {
    if let Some(labels) = registry {
        builder.push(" AND listing.registry_address = ");
        builder.push_bind(labels.registry);
        // A label without a summary or a summary owner is ownerless, so only the exclusion
        // admits it.
        match labels.owner {
            Some(RegistryLabelOwnerFilter::Owner(owner)) => {
                builder.push(" AND listing.summary_owner = ");
                builder.push_bind(owner);
            }
            Some(RegistryLabelOwnerFilter::ExcludeOwner(owner)) => {
                builder.push(" AND listing.summary_owner IS DISTINCT FROM ");
                builder.push_bind(owner);
            }
            None => {}
        }
    }
    if let Some(q) = filter.q {
        if q.mode == NameQueryMatch::Prefix {
            // A name that starts with the prefix has a key that starts with its first
            // characters (a long name's key is its first characters), so the key's range bounds
            // the scan and the name rechecks it.
            let floor: String = q
                .text
                .chars()
                .take(usize::try_from(DISPLAY_KEY_CHARS).unwrap_or(usize::MAX))
                .collect();
            let ceiling = format!("{floor}{KEY_CEILING}");
            builder.push(" AND listing.display_key >= ");
            builder.push_bind(floor);
            builder.push(" AND listing.display_key < ");
            builder.push_bind(ceiling);
        }
        builder.push(" AND listing.display_name LIKE ");
        builder.push_bind(q.like_pattern());
        builder.push(" ESCAPE '\\'");
    }
    if !filter.include_expired {
        // A child with no name summary or no registration remains listable. The stored status
        // is the publication clock's, with the protocol's own equality boundary.
        builder.push(
            " AND COALESCE(listing.registration_status, '') NOT IN ('expired', 'released')
              AND NOT listing.released_lease",
        );
        // The caller's additional fixed-time fence: API snapshots pass the publication time,
        // and a direct storage caller may deliberately evaluate later.
        if let Some(evaluated_at) = filter.evaluated_at {
            builder.push(format!(
                " AND (listing.expires_key = {MISSING_TIMESTAMP} OR listing.expires_key >= "
            ));
            builder.push_bind(UnixSeconds::from(evaluated_at));
            builder.push(")");
        }
    }
}

/// The rows strictly after the cursor in the page's order: one row comparison on the sort's
/// index key, which the index bounds.
fn push_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: &'a ChildrenCurrentKeysetCursor,
) {
    let strict = match filter.order {
        ChildrenCurrentOrder::Asc => ">",
        ChildrenCurrentOrder::Desc => "<",
    };
    match (sort_key(filter.sort), &cursor.sort_value) {
        (Some(key), ChildrenCurrentSortValue::Timestamp(value)) => {
            builder.push(format!(
                " AND ({key}, listing.display_key, listing.child_logical_name_id) {strict} \
                 (COALESCE("
            ));
            builder.push_bind(*value);
            builder.push(format!(", {MISSING_TIMESTAMP}), "));
        }
        _ => {
            builder.push(format!(
                " AND (listing.display_key, listing.child_logical_name_id) {strict} ("
            ));
        }
    }
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(", ");
    builder.push_bind(&cursor.child_logical_name_id);
    builder.push(")");
}

fn push_order(builder: &mut QueryBuilder<'_, Postgres>, filter: &ChildrenCurrentPageFilter<'_>) {
    let (direction, ties) = match filter.order {
        ChildrenCurrentOrder::Asc => ("ASC", "ASC"),
        ChildrenCurrentOrder::Desc => ("DESC", DESCENDING_TIES),
    };
    let key = sort_key(filter.sort).map_or_else(String::new, |key| format!("{key} {direction}, "));
    builder.push(format!(
        " ORDER BY {key}listing.display_key {ties}, listing.child_logical_name_id {ties}"
    ));
}

struct Listed {
    child: FamilyChildRow,
    display_key: String,
    sort_timestamp: Option<UnixSeconds>,
}

fn decode(row: &PgRow) -> Result<Listed> {
    Ok(Listed {
        child: FamilyChildRow {
            parent_logical_name_id: row.try_get("parent_logical_name_id")?,
            child_logical_name_id: row.try_get("child_logical_name_id")?,
            namespace: row.try_get("namespace")?,
            canonical_display_name: row.try_get("display_name")?,
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
        display_key: row.try_get("display_key")?,
        sort_timestamp: row.try_get("sort_timestamp")?,
    })
}

/// Spell again, from the served relation, the page rows whose stored name keeps a label
/// placeholder. The stored name reads chain-observed preimages only, and the served relation
/// reads imported ones too, so only such a row can serve another name. The relation is narrowed
/// to those children's nodes, at most a page of them.
async fn heal(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    rows: &mut [FamilyChildRow],
) -> Result<()> {
    let nodes: Vec<String> = rows
        .iter()
        .filter(|row| row.canonical_display_name.contains('['))
        .map(|row| row.namehash.to_ascii_lowercase())
        .collect();
    if nodes.is_empty() {
        return Ok(());
    }
    let mut builder =
        QueryBuilder::<Postgres>::new("/* storage:families.topology.child_listing_heal */ WITH ");
    push_children(
        &mut builder,
        Parents::One(parent_logical_name_id),
        &ChildrenCurrentPageFilter::default(),
        None,
        Some(nodes.as_slice()),
    );
    builder.push(
        ") SELECT children.child_logical_name_id, children.canonical_display_name FROM children",
    );
    let served: BTreeMap<String, String> = builder
        .build_query_as::<(String, String)>()
        .fetch_all(&mut *conn)
        .await
        .with_context(|| format!("failed to spell the children of {parent_logical_name_id}"))?
        .into_iter()
        .collect();
    for row in rows {
        if let Some(name) = served.get(&row.child_logical_name_id) {
            row.canonical_display_name.clone_from(name);
        }
    }
    Ok(())
}
