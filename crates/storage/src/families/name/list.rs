//! The composed name listings: the /v1/search page and the
//! expiring listing of /v1/names, served from composed name rows at family publication.
//!
//! Both page through a `filtered_names` CTE populated from composed rows, with shared predicates
//! and keyset ordering. They find the names to compose differently because no composed row is
//! stored:
//!
//! - search walks the readable name surfaces (an input table) in the page order, which is the
//!   surface's served name then namespace and namehash, so the first `page_size + 1` composed
//!   rows that pass the filters are final. A surface with raw bytes is served under its raw name
//!   and one without under the name built from its label hashes (`rendered`), and each kind has
//!   its own arm of the walk;
//! - the expiring listing (`expiring`) selects its page's names from the stored name summary,
//!   which carries each name's exact listing selector, and composes only those.
//!
//! A page is read in one snapshot (`batch::read_snapshot`): the selection or walk, the
//! composition and the page statement see the same family block.
mod expiring;

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Row};

use super::{CoverageShape, batch, rendered::composed_surface_sql};
pub use expiring::load_family_expiring_page;

use crate::families::topology::{
    rendered_lateral_sql, textless_surface_sql, textless_surfaces_exist_sql,
};
use crate::{
    NameCurrentListCursor, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListSort, NameCurrentRow,
    name_current::{escape_like_pattern, list_page_from},
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
    let unsupported = super::list_keys::unsupported(&row.coverage);
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

/// The statement `search_candidates` runs: [`SEARCH_CANDIDATES_SQL`], unchanged, for the surfaces
/// with raw bytes, then the same walk over the surfaces without them under their served name,
/// merged in page order. The second arm has no length bound, which only the first arm's index
/// needs.
pub(crate) fn search_candidates_sql() -> String {
    format!(
        r"({SEARCH_CANDIDATES_SQL})
     UNION ALL
     (SELECT surface.logical_name_id, rendered.name, surface.namespace, surface.namehash
      FROM bigname_phase.name_surfaces surface
      JOIN bigname_phase.chain_lineage lineage
        ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
      JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
      {rendered}
      WHERE {exist} AND {textless} AND {composed}
        AND surface.block_number <= marker.current_block_number
        AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        AND ($1::text[] IS NULL OR surface.namespace = ANY($1))
        AND ($2::text IS NULL OR rendered.name = $2)
        AND ($3::text IS NULL OR rendered.name LIKE $3 ESCAPE '\')
        AND (rendered.name, surface.namespace, surface.namehash)
            > (COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''))
      ORDER BY rendered.name ASC, surface.namespace ASC, surface.namehash ASC
      LIMIT $7)
     ORDER BY raw_name ASC, namespace ASC, namehash ASC
     LIMIT $7",
        rendered = rendered_lateral_sql(),
        exist = textless_surfaces_exist_sql(),
        textless = textless_surface_sql("surface"),
        composed = composed_surface_sql("surface"),
    )
}

/// The next readable surfaces after `after` in the search page's order that the filter's name
/// predicates admit: (logical_name_id, served name, namespace, namehash).
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
    let rows = sqlx::query(&search_candidates_sql())
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

/// The expiring page over every name of the `ens` namespace composed at once: what the listing
/// must return, whatever it costs. A test oracle only. It composes without the declared
/// topology, which changes fields the listing does not serve; for `ens` that is one
/// `declared_summary` key, for Basenames it also rewrites row metadata, so other namespaces are
/// refused rather than compared.
#[cfg(any(test, feature = "test-support"))]
pub async fn load_family_expiring_page_unbounded(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &crate::NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chains: &[String],
) -> Result<NameCurrentListPage> {
    use crate::name_current::expiring_page_from;
    anyhow::ensure!(
        filter.namespace == "ens",
        "the unbounded expiring oracle covers only the ens namespace"
    );
    let mut snapshot = db.into().snapshot().await?;
    batch::ensure_published(&mut snapshot, chains).await?;
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT logical_name_id FROM bigname_phase.name_surfaces WHERE namespace = $1",
    )
    .bind(filter.namespace.as_str())
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to load the namespace's names")?;
    let mut composed = batch::load_base(&mut snapshot, &names, CoverageShape::Plain).await?;
    super::rendered::enrich(&mut snapshot, &mut composed).await?;
    let source = Value::Array(composed.values().map(source_row).collect());
    let page =
        expiring_page_from(&mut *snapshot, filter, order, cursor, page_size, &source).await?;
    snapshot.close().await?;
    Ok(page)
}
