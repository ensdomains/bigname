//! The composed name listings: the /v1/search page and the
//! expiring listing of /v1/names, served from composed name rows at family publication.
//!
//! Both page through a `filtered_names` CTE populated from composed rows, with shared predicates
//! and keyset ordering. Their candidate walks differ because no composed row is stored:
//!
//! - search walks the readable name surfaces (an input table) in the page order, which is the
//!   surface's raw name then namespace and namehash, so the first `page_size + 1` composed rows
//!   that pass the filters are final;
//! - the expiring listing walks the retained lifecycle events (F2a) and the NameWrapper states
//!   (F2b) by expiry. A name's registration expiry is always one of those expiries
//!   (control::lifecycle::served, the registration expiry), so a name first seen at expiry `x`
//!   has an expiry at or past `x` in walk order, and the walk stops once `page_size + 1` rows
//!   sort strictly before the walk position. Events whose expiry is a JSON number that is not an
//!   integral second carry no indexed expiry and are always considered. The walk key stays
//!   numeric: a NameWrapper expiry keeps the full u64 range, past the largest bigint.
//!
//! A page is read in one snapshot (`batch::read_snapshot`): the walk, every batch's composition
//! and the page statement see the same family block.
use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Row};

use super::{CoverageShape, batch};
use crate::{
    NameCurrentExpiringFilter, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage, NameCurrentListSort,
    NameCurrentRow, UnixSeconds,
    name_current::{
        escape_like_pattern, expiring_page_from, list_page_from, parent_like_patterns,
        public_authority_arms,
    },
    name_current_list_cursor_from_row,
};

const BATCH_FLOOR: usize = 200;

fn batch_size(page_size: u64) -> usize {
    super::seams::batch_size(
        usize::try_from(page_size)
            .unwrap_or(usize::MAX / 8)
            .saturating_add(1)
            .saturating_mul(4)
            .max(BATCH_FLOOR),
    )
}

/// One composed row as the list CTE binds it (name_current/list.rs, `COMPOSED_NC_COLUMNS`).
pub(super) fn source_row(row: &NameCurrentRow) -> Value {
    let unsupported = row.coverage.get("status").and_then(Value::as_str) == Some("unsupported");
    json!({
        "logical_name_id": row.logical_name_id,
        "namespace": row.namespace,
        "raw_name": row.normalized_name,
        "display_name": row.canonical_display_name,
        "namehash": row.namehash,
        "surface_binding_id": row.surface_binding_id,
        "resource_id": row.resource_id,
        "serving_resource_id": row.serving_resource_id,
        "token_lineage_id": row.token_lineage_id,
        "binding_kind": row.binding_kind.map(|kind| kind.as_str()),
        "declared_summary": row.declared_summary,
        "provenance": row.provenance,
        "support_status": if unsupported { "unsupported" } else { "supported" },
        "unsupported_reason": row.coverage.get("unsupported_reason"),
        "chain_positions": row.chain_positions,
        "canonicality_summary": row.canonicality_summary,
        "manifest_version": row.manifest_version,
        "last_recomputed_at": crate::time::format_timestamp(row.last_recomputed_at),
    })
}

/// Composed rows gathered across candidate batches, each name once.
#[derive(Default)]
struct Gathered {
    names: BTreeSet<String>,
    rows: Vec<Value>,
}

impl Gathered {
    async fn add(&mut self, conn: &mut PgConnection, names: Vec<String>) -> Result<()> {
        let fresh: Vec<String> = names
            .into_iter()
            .filter(|name| self.names.insert(name.clone()))
            .collect();
        super::seams::note_composed_names(fresh.len());
        let composed = batch::load(conn, &fresh, CoverageShape::Plain).await?;
        self.rows.extend(composed.values().map(source_row));
        Ok(())
    }

    fn source(&self) -> Value {
        super::seams::note_submitted_rows(self.rows.len());
        Value::Array(self.rows.clone())
    }
}

/// The composed /v1/search page: `filter` must carry no address filter (the search route has
/// none), and the page is sorted by name ascending.
pub async fn load_family_search_page(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<NameCurrentListPage> {
    anyhow::ensure!(
        filter.address.is_none(),
        "the composed search page has no address filter"
    );
    let order = (NameCurrentListSort::Name, NameCurrentListOrder::Asc);
    let batch = batch_size(page_size);
    // The page sorts by normalized name, the surfaces' order; the cursor's display name is only
    // echoed back to the client.
    let mut after = cursor.map(|cursor| {
        (
            cursor.normalized_name.clone(),
            cursor.namespace.clone(),
            cursor.namehash.clone(),
        )
    });
    let mut snapshot = batch::read_snapshot(pool).await?;
    let mut gathered = Gathered::default();
    loop {
        let candidates = search_candidates(&mut snapshot, filter, after.as_ref(), batch).await?;
        let exhausted = candidates.len() < batch;
        after = candidates
            .last()
            .map(|(_, name, namespace, namehash)| {
                (name.clone(), namespace.clone(), namehash.clone())
            })
            .or(after);
        gathered
            .add(
                &mut snapshot,
                candidates.into_iter().map(|(id, ..)| id).collect(),
            )
            .await?;
        let source = gathered.source();
        let page =
            list_page_from(&mut *snapshot, filter, order, cursor, page_size, &source).await?;
        if exhausted || page.next_cursor.is_some() {
            snapshot.commit().await?;
            return Ok(page);
        }
    }
}

/// Bound by `search_candidates`; the plan test prepares this exact text. The length bound matches
/// `name_surfaces_name_order_idx`'s predicate, so the walk reads that index in page order. With no
/// cursor the keyset bound is `('', '', '')`, below every row (namespaces are never empty), so a
/// generic plan still binds the cursor as an index condition.
pub(crate) const SEARCH_CANDIDATES_SQL: &str = r"/* storage:families.name.search_candidates */
     SELECT surface.logical_name_id, surface.raw_name, surface.namespace, surface.namehash
     FROM bigname_phase.name_surfaces surface
     JOIN bigname_phase.chain_lineage lineage
       ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
     JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
     WHERE surface.visibility_state = 'active' AND surface.raw_name <> ''
       AND octet_length(surface.raw_name) <= 2000
       AND surface.block_number <= marker.current_block_number
       AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
       AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
       AND ($1::text[] IS NULL OR surface.namespace = ANY($1))
       AND ($2::text IS NULL OR surface.raw_name = $2)
       AND ($3::text IS NULL OR surface.raw_name LIKE $3 ESCAPE '\')
       AND (surface.raw_name, surface.namespace, surface.namehash)
           > (COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''))
     ORDER BY surface.raw_name ASC, surface.namespace ASC, surface.namehash ASC
     LIMIT $7";

/// The next readable surfaces after `after` in the search page's order that the filter's name
/// predicates admit: (logical_name_id, raw_name, namespace, namehash).
async fn search_candidates(
    conn: &mut PgConnection,
    filter: &NameCurrentListFilter,
    after: Option<&(String, String, String)>,
    limit: usize,
) -> Result<Vec<(String, String, String, String)>> {
    let namespaces: Option<Vec<String>> = match (&filter.namespaces, &filter.namespace) {
        (Some(namespaces), _) if !namespaces.is_empty() => Some(namespaces.clone()),
        (_, Some(namespace)) => Some(vec![namespace.clone()]),
        _ => None,
    };
    let like = filter
        .prefix
        .as_deref()
        .map(|prefix| format!("{}%", escape_like_pattern(prefix)))
        .or_else(|| {
            filter
                .contains
                .as_deref()
                .map(|contains| format!("%{}%", escape_like_pattern(contains)))
        })
        .or_else(|| {
            filter.contains_nocase.as_deref().map(|contains| {
                format!("%{}%", escape_like_pattern(&contains.to_ascii_lowercase()))
            })
        });
    let rows = sqlx::query(SEARCH_CANDIDATES_SQL)
        .bind(namespaces)
        .bind(filter.name.as_deref())
        .bind(like)
        .bind(after.map(|(name, ..)| name.as_str()))
        .bind(after.map(|(_, namespace, _)| namespace.as_str()))
        .bind(after.map(|(.., namehash)| namehash.as_str()))
        .bind(i64::try_from(limit).context("search batch exceeds i64")?)
        .fetch_all(conn)
        .await
        .context("failed to load the search candidates")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("logical_name_id")?,
                row.try_get("raw_name")?,
                row.try_get("namespace")?,
                row.try_get("namehash")?,
            ))
        })
        .collect()
}

/// The composed expiring page of /v1/names.
/// `chains` are the chains the request selected for `filter.namespace`: their markers are read
/// before the walk, which reads family tables a rebuild empties, so a rebuild refuses rather than
/// answers an empty page. Only names of `filter.namespace` are walked and composed.
pub async fn load_family_expiring_page(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chains: &[String],
) -> Result<NameCurrentListPage> {
    anyhow::ensure!(
        filter.expires_after.is_some() || filter.expires_before.is_some(),
        "the composed expiring page requires an expires_after or expires_before bound"
    );
    let ascending = order == NameCurrentListOrder::Asc;
    // The walk window, in whole seconds: every expiry whose second can fall in the filter's
    // window or past the cursor.
    let cursor_second = cursor.and_then(|cursor| match cursor.sort_value {
        NameCurrentListCursorValue::Timestamp(Some(at)) => Some(at.unix_timestamp()),
        _ => None,
    });
    let mut low = filter.expires_after.map(UnixSeconds::unix_timestamp);
    let mut high = filter
        .expires_before
        .map(|at| at.unix_timestamp() + i128::from(at.nanosecond() != 0));
    if let Some(second) = cursor_second {
        if ascending {
            low = Some(low.map_or(second, |low| low.max(second)));
        } else {
            high = Some(high.map_or(second + 1, |high| high.min(second + 1)));
        }
    }
    let batch = batch_size(page_size);
    let mut snapshot = db.into().snapshot().await?;
    batch::ensure_published(&mut snapshot, chains).await?;
    let namespace = filter.namespace.as_str();
    let mut gathered = Gathered::default();
    let inexact = inexact_expiry_names(&mut snapshot, namespace).await?;
    gathered.add(&mut snapshot, inexact).await?;
    let mut position: Option<(i128, String)> = None;
    loop {
        let pairs = expiry_pairs(
            &mut snapshot,
            filter,
            (low, high),
            ascending,
            position.as_ref(),
            batch,
        )
        .await?;
        let exhausted = pairs.len() < batch;
        position = pairs.last().cloned().or(position);
        gathered
            .add(
                &mut snapshot,
                pairs.into_iter().map(|(_, name)| name).collect(),
            )
            .await?;
        let source = gathered.source();
        let page = expiring_page_from(
            &mut *snapshot,
            filter,
            order,
            cursor,
            page_size + 1,
            &source,
        )
        .await?;
        let settled = page.rows.len() as u64 > page_size
            && match (page.rows.last().and_then(|row| row.expiry_date), &position) {
                (Some(last), Some((walked, _))) => {
                    let last = last.unix_timestamp();
                    if ascending {
                        last < *walked
                    } else {
                        last > *walked
                    }
                }
                _ => false,
            };
        if exhausted || settled {
            snapshot.close().await?;
            return Ok(truncate(page, page_size));
        }
    }
}

/// The expiring page over every name of `filter.namespace` composed at once: what the walk must
/// return, whatever it costs. A test oracle only.
#[cfg(any(test, feature = "test-support"))]
pub async fn load_family_expiring_page_unbounded(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chains: &[String],
) -> Result<NameCurrentListPage> {
    let mut snapshot = db.into().snapshot().await?;
    batch::ensure_published(&mut snapshot, chains).await?;
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT logical_name_id FROM bigname_phase.name_surfaces WHERE namespace = $1",
    )
    .bind(filter.namespace.as_str())
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to load the namespace's names")?;
    let composed = batch::load_base(&mut snapshot, &names, CoverageShape::Plain).await?;
    let source = Value::Array(composed.values().map(source_row).collect());
    let page = expiring_page_from(&mut *snapshot, filter, order, cursor, page_size, &source).await?;
    snapshot.close().await?;
    Ok(page)
}

/// A page read with one extra row, cut to `page_size` with its continuation.
fn truncate(mut page: NameCurrentListPage, page_size: u64) -> NameCurrentListPage {
    let size = usize::try_from(page_size).unwrap_or(usize::MAX);
    if page.rows.len() > size {
        page.rows.truncate(size);
        page.next_cursor = page
            .rows
            .last()
            .map(|row| name_current_list_cursor_from_row(row, NameCurrentListSort::ExpiryDate));
    } else {
        page.next_cursor = None;
    }
    page
}

/// The names a retained lifecycle event or NameWrapper state (`hits`) can load
/// (control::lifecycle::load): the event's own names, the triple's name, and the names whose
/// binding candidates, associations or key states name the resource it sits on. Both walks use
/// it, so a name reached only through its resource is found whether its expiry is integral or
/// not. `$NAMESPACE` stands for the namespace's parameter and `$PRUNE` for further predicates on
/// the name's `surface`.
const EXPIRY_HIT_NAMES: &str = "
             CROSS JOIN LATERAL (
                 SELECT hits.original_logical_name_id
                 UNION SELECT hits.decoded_logical_name_id
                 UNION SELECT hits.state_key::jsonb ->> 0 WHERE hits.state_kind = 'triple'
                 UNION SELECT candidate.logical_name_id
                 FROM bigname_phase.project_binding_candidate candidate
                 WHERE candidate.chain_id = hits.chain_id
                   AND hits.resource_id IN (
                       candidate.resource_id, candidate.wrapped_registrar_resource_id,
                       candidate.predecessor_resource_id, candidate.lease_resource_id)
                 UNION SELECT association.logical_name_id
                 FROM bigname_phase.project_lifecycle_association association
                 WHERE association.chain_id = hits.chain_id
                   AND association.target_resource_id = hits.resource_id
                 UNION SELECT state.logical_name_id
                 FROM bigname_phase.project_lifecycle_key_state state
                 WHERE state.chain_id = hits.chain_id AND state.resource_id = hits.resource_id
             ) name(logical_name_id)
             WHERE name.logical_name_id IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM bigname_phase.name_surfaces surface
                   WHERE surface.logical_name_id = name.logical_name_id
                     AND surface.namespace = $NAMESPACE$PRUNE)";

/// Names of `namespace` a retained lifecycle event whose expiry is a JSON number but not an
/// integral second can load ([`EXPIRY_HIT_NAMES`]): the walk cannot place them, so they are
/// always considered.
async fn inexact_expiry_names(conn: &mut PgConnection, namespace: &str) -> Result<Vec<String>> {
    let sql = format!(
        "/* storage:families.name.inexact_expiry_names */
         WITH hits AS (
             SELECT event.chain_id, event.state_kind, event.state_key,
                    CASE WHEN event.state_kind = 'resource' THEN event.state_key::uuid END
                        AS resource_id,
                    event.original_logical_name_id, event.decoded_logical_name_id
             FROM bigname_phase.project_lifecycle_event event
             WHERE event.expiry_seconds IS NULL AND jsonb_typeof(event.expiry) = 'number'
         )
         SELECT DISTINCT name.logical_name_id
         FROM hits{}",
        EXPIRY_HIT_NAMES
            .replace("$NAMESPACE", "$1")
            .replace("$PRUNE", "")
    );
    sqlx::query_scalar(&sql)
        .bind(namespace)
        .fetch_all(conn)
        .await
        .context("failed to load the names with an inexact expiry")
}

/// Keeps the walk's names that `filter` can list: with `authorities`, a name whose stored
/// summary selects an arm that can serve one of them (the composed row decides, at the same
/// publication); with `parent`, a name one label below it. The stored spelling is exact for
/// `parent`: Interpret activates a surface only when every label is already normalized.
const EXPIRY_PAIRS_PRUNE: &str = "
                     AND ($7::text[] IS NULL OR EXISTS (
                         SELECT 1 FROM bigname_phase.project_name_summary summary
                         WHERE summary.chain_id = surface.chain_id
                           AND summary.logical_name_id = surface.logical_name_id
                           AND summary.authority_arm = ANY($7::text[])))
                     AND ($8::text IS NULL OR (surface.raw_name LIKE $8 ESCAPE '\\'
                         AND surface.raw_name NOT LIKE $9 ESCAPE '\\'))";

/// The next (expiry second, name) pairs of the walk after `after`: every retained lifecycle
/// event and NameWrapper state whose expiry is in `[low, high)`, paired with each name of
/// `filter.namespace` it can load ([`EXPIRY_HIT_NAMES`]) that [`EXPIRY_PAIRS_PRUNE`] keeps.
async fn expiry_pairs(
    conn: &mut PgConnection,
    filter: &NameCurrentExpiringFilter,
    (low, high): (Option<i128>, Option<i128>),
    ascending: bool,
    after: Option<&(i128, String)>,
    limit: usize,
) -> Result<Vec<(i128, String)>> {
    let (compare, direction) = if ascending {
        (">", "ASC")
    } else {
        ("<", "DESC")
    };
    let sql = format!(
        "/* storage:families.name.expiry_pairs */
         WITH hits AS (
             SELECT event.chain_id, event.expiry_seconds::numeric AS at, event.state_kind,
                    event.state_key,
                    CASE WHEN event.state_kind = 'resource' THEN event.state_key::uuid END
                        AS resource_id,
                    event.original_logical_name_id, event.decoded_logical_name_id
             FROM bigname_phase.project_lifecycle_event event
             WHERE event.expiry_seconds IS NOT NULL
               AND ($1::numeric IS NULL OR event.expiry_seconds >= $1::numeric)
               AND ($2::numeric IS NULL OR event.expiry_seconds < $2::numeric)
             UNION ALL
             SELECT wrapper.chain_id, FLOOR(wrapper.expiry_seconds), 'resource',
                    wrapper.resource_id::text, wrapper.resource_id, wrapper.logical_name_id, NULL
             FROM bigname_phase.project_wrapper_state wrapper
             WHERE wrapper.expiry_seconds IS NOT NULL
               AND ($1::numeric IS NULL OR wrapper.expiry_seconds >= $1::numeric)
               AND ($2::numeric IS NULL OR wrapper.expiry_seconds < $2::numeric)
         ), pairs AS (
             SELECT DISTINCT hits.at, name.logical_name_id
             FROM hits{names}
         )
         SELECT at::text AS at, logical_name_id FROM pairs
         WHERE $3::numeric IS NULL OR (at, logical_name_id) {compare} ($3::numeric, $4)
         ORDER BY pairs.at {direction}, pairs.logical_name_id {direction}
         LIMIT $5",
        names = EXPIRY_HIT_NAMES
            .replace("$NAMESPACE", "$6")
            .replace("$PRUNE", EXPIRY_PAIRS_PRUNE)
    );
    let arms = filter.authorities.as_deref().map(public_authority_arms);
    let parent = filter.parent.as_deref().map(parent_like_patterns);
    // Decimal strings keep the i128 bounds exact. SQLx binds them as TEXT, so every
    // numeric comparison above casts its parameter, not only the null guard.
    let rows = sqlx::query(&sql)
        .bind(low.map(|value| value.to_string()))
        .bind(high.map(|value| value.to_string()))
        .bind(after.map(|(at, _)| at.to_string()))
        .bind(after.map(|(_, name)| name.as_str()))
        .bind(i64::try_from(limit).context("expiry batch exceeds i64")?)
        .bind(filter.namespace.as_str())
        .bind(arms)
        .bind(parent.as_ref().map(|(one_below, _)| one_below.as_str()))
        .bind(parent.as_ref().map(|(_, deeper)| deeper.as_str()))
        .fetch_all(conn)
        .await
        .context("failed to walk the expiry candidates")?;
    rows.into_iter()
        .map(|row| {
            let at: String = row.try_get("at")?;
            Ok((
                at.parse::<i128>()
                    .with_context(|| format!("expiry walk position {at} is not an integer"))?,
                row.try_get("logical_name_id")?,
            ))
        })
        .collect()
}
