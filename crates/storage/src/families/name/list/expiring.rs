//! The expiring listing of /v1/names: one exact selection, then one composition.
//!
//! No composed row is stored, but the family step stores each name's listing selector on its
//! name summary (`project_name_summary.expiry_listable`, `expires_at` and `public_authority`),
//! written in the publication's own transaction from the same composition this reader runs. A
//! page is therefore read in two steps on one snapshot:
//!
//! 1. [`select_expiring_names`] picks the first `page_size + 1` listable names of the request
//!    in the public order, with every filter and the cursor applied before the limit;
//! 2. [`compose_expiring_page`] composes exactly those names and runs the page statement over
//!    them once.
//!
//! The work a page composes is bounded by its size, whatever the namespace holds. Selecting can
//! still read many narrow keys: a sparse `parent`, or a second that many names share.
//!
//! Each [`ExpiringSelection`] is one window. A disjoint union merges at most `page_size + 1`
//! narrow keys from each window in one SQL statement, takes the global first `page_size + 1`,
//! and composes them once. Disjoint bounds and the surface's logical-name primary key prevent
//! a name from consuming more than one slot.
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{batch, source_row};
use crate::{
    NameCurrentExpiringFilter, NameCurrentExpiryWindow, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListOrder, NameCurrentListPage, UnixSeconds,
    families::name::CoverageShape,
    name_current::{expiring_page_from, push_parent_predicate},
};

/// What one selection reads: the listable names of `namespace` whose expiry is in the half-open
/// window `[expires_after, expires_before)`, optionally of the listed public authorities and
/// exactly one label below `parent`, after `cursor` in `order`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExpiringSelection<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) expires_after: Option<UnixSeconds>,
    pub(crate) expires_before: Option<UnixSeconds>,
    pub(crate) authorities: Option<&'a [String]>,
    pub(crate) parent: Option<&'a str>,
    pub(crate) order: NameCurrentListOrder,
    pub(crate) cursor: Option<&'a NameCurrentListCursor>,
}

impl<'a> ExpiringSelection<'a> {
    pub(crate) fn of(
        filter: &'a NameCurrentExpiringFilter,
        window: NameCurrentExpiryWindow,
        order: NameCurrentListOrder,
        cursor: Option<&'a NameCurrentListCursor>,
    ) -> Self {
        Self {
            namespace: &filter.namespace,
            expires_after: window.expires_after,
            expires_before: window.expires_before,
            authorities: filter.authorities.as_deref(),
            parent: filter.parent.as_deref(),
            order,
            cursor,
        }
    }
}

/// The composed expiring page of /v1/names: the first `page_size` names of `filter`'s window
/// after `cursor` in `order`, and the cursor of the next page when a further name exists.
///
/// `chains` are the chains the request selected for `filter.namespace`: their markers are read
/// first, so a rebuild, which empties the family tables, refuses rather than answers an empty
/// page. The read runs on one snapshot of `db`. It composes at most `page_size + 1` names.
pub async fn load_family_expiring_page(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chains: &[String],
) -> Result<NameCurrentListPage> {
    filter.validate_windows()?;
    ensure!(
        page_size > 0,
        "the composed expiring page_size must be positive"
    );
    let limit = page_size
        .checked_add(1)
        .context("the composed expiring page_size is too large")?;
    let mut snapshot = db.into().snapshot().await?;
    batch::ensure_published(&mut snapshot, chains).await?;
    let names = if let [window] = filter.windows.as_slice() {
        let selection = ExpiringSelection::of(filter, *window, order, cursor);
        select_expiring_names(&mut snapshot, &selection, limit).await?
    } else {
        expiring_union_query("", filter, order, cursor, limit)?
            .build_query_scalar()
            .fetch_all(&mut *snapshot)
            .await
            .context("failed to select the disjoint expiring windows")?
    };
    let page =
        compose_expiring_page(&mut snapshot, &names, filter, order, cursor, page_size).await?;
    snapshot.close().await?;
    Ok(page)
}

/// The first `limit` names of `selection`, in the listing's public order: expiry in the
/// requested direction, then namespace, normalized name and namehash ascending.
///
/// A name is selected when its summary marks it listable and its surface is one the composition
/// reads (`loaders::surfaces`, at or before its chain's family block). The keys are the ones the
/// page statement orders by: a listable summary's `expires_at` is the expiry the composed row
/// serves, and an active surface's `raw_name` is its normalized name. They compare in the
/// database's own collation and as exact numerics, as the page statement compares them.
pub(crate) async fn select_expiring_names(
    conn: &mut PgConnection,
    selection: &ExpiringSelection<'_>,
    limit: u64,
) -> Result<Vec<String>> {
    expiring_names_query("", selection, limit)?
        .build_query_scalar()
        .fetch_all(conn)
        .await
        .context("failed to select the expiring names")
}

/// The statement of [`select_expiring_names`] after `prefix` (`EXPLAIN` in the plan test).
fn expiring_names_query<'a>(
    prefix: &str,
    selection: &ExpiringSelection<'a>,
    limit: u64,
) -> Result<QueryBuilder<'a, Postgres>> {
    let mut builder = QueryBuilder::<Postgres>::new(prefix);
    push_expiring_selection(&mut builder, selection, limit)?;
    Ok(builder)
}

/// One statement merges bounded narrow-key arms in the database's exact public order. The
/// outer limit applies before any full row is composed, even for sparse separated windows.
fn expiring_union_query<'a>(
    prefix: &str,
    filter: &'a NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&'a NameCurrentListCursor>,
    limit: u64,
) -> Result<QueryBuilder<'a, Postgres>> {
    let mut builder = QueryBuilder::<Postgres>::new(prefix);
    builder.push("/* storage:families.name.expiring_union */ SELECT logical_name_id FROM (");
    for (index, window) in filter.windows.iter().enumerate() {
        if index > 0 {
            builder.push(" UNION ALL ");
        }
        builder.push("(");
        push_expiring_selection(
            &mut builder,
            &ExpiringSelection::of(filter, *window, order, cursor),
            limit,
        )?;
        builder.push(")");
    }
    builder.push(") selected ORDER BY expires_at ");
    builder.push(if order == NameCurrentListOrder::Asc {
        "ASC"
    } else {
        "DESC"
    });
    builder.push(", namespace ASC, raw_name ASC, namehash ASC LIMIT ");
    builder.push_bind(i64::try_from(limit).context("expiring union limit exceeds i64")?);
    Ok(builder)
}

fn push_expiring_selection<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    selection: &ExpiringSelection<'a>,
    limit: u64,
) -> Result<()> {
    let after = match selection.cursor.map(|cursor| &cursor.sort_value) {
        None => None,
        Some(NameCurrentListCursorValue::Timestamp(Some(at))) => Some(*at),
        Some(_) => bail!("name_current expiring page cursor must carry an expiry timestamp"),
    };
    let ascending = selection.order == NameCurrentListOrder::Asc;
    builder.push(
        "/* storage:families.name.expiring_names */
         SELECT summary.logical_name_id, summary.expires_at, summary.namespace,
                surface.raw_name, surface.namehash
         FROM bigname_phase.project_name_summary summary
         JOIN bigname_phase.name_surfaces surface
           ON surface.chain_id = summary.chain_id
          AND surface.logical_name_id = summary.logical_name_id
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN bigname_phase.project_family_marker marker ON marker.chain_id = summary.chain_id
         WHERE summary.expiry_listable AND summary.expires_at IS NOT NULL
           AND surface.visibility_state = 'active' AND surface.raw_name <> ''
           AND surface.block_number <= marker.current_block_number
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND summary.namespace = ",
    );
    builder.push_bind(selection.namespace);
    match selection.authorities {
        None => {}
        // One value as an equality, so the authority index can return it in expiry order.
        Some([authority]) => {
            builder.push(" AND summary.public_authority = ");
            builder.push_bind(authority);
        }
        Some(authorities) => {
            builder.push(" AND summary.public_authority = ANY(");
            builder.push_bind(authorities);
            builder.push(")");
        }
    }
    if let Some(parent) = selection.parent {
        push_parent_predicate(builder, "surface.raw_name", parent);
    }
    if let Some(expires_after) = selection.expires_after {
        builder.push(" AND summary.expires_at >= ");
        builder.push_bind(expires_after);
    }
    if let Some(expires_before) = selection.expires_before {
        builder.push(" AND summary.expires_at < ");
        builder.push_bind(expires_before);
    }
    if let (Some(at), Some(cursor)) = (after, selection.cursor) {
        // The first bound is implied by the keyset after it. It is spelled out so the cursor is
        // an index condition: a continuation starts reading at its expiry, not at the window's
        // first one. The tie keys ascend in both directions, as the page statement's keyset
        // does.
        builder.push(" AND summary.expires_at ");
        builder.push(if ascending { ">=" } else { "<=" });
        builder.push(" ");
        builder.push_bind(at);
        builder.push(" AND (summary.expires_at ");
        builder.push(if ascending { ">" } else { "<" });
        builder.push(" ");
        builder.push_bind(at);
        builder.push(" OR (summary.expires_at = ");
        builder.push_bind(at);
        builder.push(" AND (summary.namespace, surface.raw_name, surface.namehash) > (");
        builder.push_bind(&cursor.namespace);
        builder.push(", ");
        builder.push_bind(&cursor.normalized_name);
        builder.push(", ");
        builder.push_bind(&cursor.namehash);
        builder.push(")))");
    }
    builder.push(" ORDER BY summary.expires_at ");
    builder.push(if ascending { "ASC" } else { "DESC" });
    builder.push(", summary.namespace ASC, surface.raw_name ASC, surface.namehash ASC LIMIT ");
    builder.push_bind(i64::try_from(limit).context("expiring selection limit exceeds i64")?);
    Ok(())
}

/// The page over `names`, which [`select_expiring_names`] returned for `page_size + 1`: composes
/// them once, without the declared topology (no listing row serves it), and runs the page
/// statement over those rows. The statement applies the listing's own predicates and order to
/// the composed rows, so it must return the selected names in the selected order; anything else
/// means the stored selector and the composition disagree, and the read fails rather than serve
/// a page that may be missing a name.
pub(crate) async fn compose_expiring_page(
    conn: &mut PgConnection,
    names: &[String],
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<NameCurrentListPage> {
    if names.is_empty() {
        return Ok(NameCurrentListPage {
            rows: Vec::new(),
            next_cursor: None,
            total_count: None,
        });
    }
    crate::families::name::seams::note_composed_names(names.len());
    let composed = batch::load_base(conn, names, CoverageShape::Plain).await?;
    let source = Value::Array(composed.values().map(source_row).collect());
    crate::families::name::seams::note_submitted_rows(composed.len());
    let page = expiring_page_from(&mut *conn, filter, order, cursor, page_size, &source).await?;
    let served = usize::try_from(page_size)
        .unwrap_or(usize::MAX)
        .min(names.len());
    ensure!(
        page.next_cursor.is_some() == (names.len() > served)
            && page
                .rows
                .iter()
                .map(|row| row.row.logical_name_id.as_str())
                .eq(names[..served].iter().map(String::as_str)),
        "the expiry selector of {} and its composed rows disagree: selected {:?}, composed page {:?}",
        filter.namespace,
        &names[..served],
        page.rows
            .iter()
            .map(|row| row.row.logical_name_id.as_str())
            .collect::<Vec<_>>()
    );
    Ok(page)
}

#[cfg(test)]
#[path = "expiring_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "expiring/union_tests.rs"]
mod union_tests;
