//! Test support for the bound-name listing: the page read from the name summary
//! ([`super::load_family_bound_names`]) against the walk it replaced
//! ([`super::bound_walk::load_bound_names_by_walk`]), and the stored bound-name values against a
//! fresh composition. The listing holds the walk's set of names, in the order of their stored
//! order names: the walk ordered a name without raw bytes by the name it is shown under, and a
//! name whose surface changed its raw bytes after its summary was composed by its new raw name.
//! So the walk's whole list is put in stored order before the pages are compared with it, and
//! the stored order names are checked on their own against the surfaces as the publication's
//! block reads them. It is compiled for tests only.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

use super::{
    batch,
    bound::{BoundName, load_family_bound_names},
    bound_rules::ORDER_NAME_BYTES,
    bound_walk::load_bound_names_by_walk,
    rendered::placeholder_name,
};
use crate::{NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow};

/// The page sizes every parity check reads the whole list in.
const PAGE_SIZES: [i64; 4] = [1, 2, 7, 50];

/// The summary columns the bound-name page selects by.
const STORED_BOUND_COLUMNS: [&str; 7] = [
    "resolver_chain_id",
    "resolver_address",
    "resolver_listing_eligible",
    "resolver_support_exempt",
    "raw_name",
    "order_name",
    "namehash",
];

/// Checks the stored summaries ([`assert_stored_bound_fields_are_fresh`]), then reads the bound
/// names of `resolver_address` on `chain_id` on one snapshot, page by page at each of
/// [`PAGE_SIZES`], and fails unless the pages chain, row for row, to the walk's whole list in the
/// listing's order, and every page's count is the walk's whole list. Returns the listed names as
/// served, in page order.
pub async fn assert_bound_names_match_the_walk(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
) -> Result<Vec<String>> {
    // First, so a name the summary lost fails as a missing summary rather than as a walked name
    // with no stored order name.
    assert_stored_bound_fields_are_fresh(pool, chain_id).await?;
    let mut snapshot = crate::begin_read_snapshot(pool).await?;
    let walked = load_bound_names_by_walk(
        &mut *snapshot,
        chain_id,
        resolver_address,
        namespace,
        None,
        i64::from(u32::MAX),
    )
    .await?;
    let whole = in_listing_order(&mut snapshot, chain_id, walked).await?;
    let expected = u64::try_from(whole.len())?;
    let unasked = load_family_bound_names(
        &mut *snapshot,
        chain_id,
        resolver_address,
        namespace,
        None,
        1,
        false,
    )
    .await?;
    ensure!(
        unasked.total_count.is_none(),
        "a page asked for no count carries one"
    );
    // One page of the whole list first, so a difference names the rows it is about.
    let listed = load_family_bound_names(
        &mut *snapshot,
        chain_id,
        resolver_address,
        namespace,
        None,
        i64::try_from(whole.len())? + 1,
        false,
    )
    .await?;
    if listed.rows != whole {
        let shown: Vec<&str> = listed
            .rows
            .iter()
            .map(|name| name.row.logical_name_id.as_str())
            .collect();
        let missing: Vec<&str> = whole
            .iter()
            .map(|name| name.row.logical_name_id.as_str())
            .filter(|id| !shown.contains(id))
            .collect();
        let stored: Vec<Value> = sqlx::query_scalar(
            "SELECT jsonb_build_object('summary', to_jsonb(summary), 'surface',
                 jsonb_build_object('raw_name', surface.raw_name, 'block', surface.block_number,
                     'visibility', surface.visibility_state,
                     'canonicality', surface.canonicality_state,
                     'witness', surface.preimage_event_identity),
                 'marker', (SELECT marker.current_block_number
                            FROM bigname_phase.project_family_marker marker
                            WHERE marker.chain_id = surface.chain_id),
                 'preimages', (SELECT jsonb_agg(jsonb_build_array(event.block_number,
                                   event.event_identity, event.canonicality_state))
                               FROM bigname_phase.normalized_events event
                               WHERE event.logical_name_id = surface.logical_name_id
                                 AND event.event_kind = 'PreimageObserved'))
             FROM bigname_phase.name_surfaces surface
             LEFT JOIN bigname_phase.project_name_summary summary
               ON summary.logical_name_id = surface.logical_name_id
             WHERE surface.logical_name_id = ANY($1)",
        )
        .bind(&missing)
        .fetch_all(&mut *snapshot)
        .await?;
        anyhow::bail!(
            "the summary lists {:?}, the walk lists {:?}. The walk's names the summary leaves \
             out: {stored:#?}",
            names(&listed.rows),
            names(&whole)
        );
    }
    for size in PAGE_SIZES {
        let mut cursor: Option<NameCurrentListCursor> = None;
        let mut served: Vec<BoundName> = Vec::new();
        for page_number in 0.. {
            let page = load_family_bound_names(
                &mut *snapshot,
                chain_id,
                resolver_address,
                namespace,
                cursor.as_ref(),
                size + 1,
                true,
            )
            .await?;
            ensure!(
                page.total_count == Some(expected),
                "page {page_number} of size {size} counts {:?}, the walk lists {expected}",
                page.total_count
            );
            let offset = served.len();
            let lookahead = whole.iter().skip(offset).take(usize::try_from(size + 1)?);
            ensure!(
                page.rows.iter().eq(lookahead),
                "page {page_number} of size {size} after {cursor:?} differs from the walk: \
                 summary {:?}, walk {:?}",
                names(&page.rows),
                names(&whole[offset.min(whole.len())..])
            );
            let more = page.rows.len() > usize::try_from(size)?;
            let mut rows = page.rows;
            rows.truncate(usize::try_from(size)?);
            cursor = rows.last().map(cursor_after);
            served.extend(rows);
            if !more {
                break;
            }
        }
        ensure!(
            served == whole,
            "pages of size {size} chain to {:?}, the walk lists {:?}",
            names(&served),
            names(&whole)
        );
    }
    // A cursor on the last name reads an empty page with the same count.
    if let Some(last) = whole.last() {
        let after = load_family_bound_names(
            &mut *snapshot,
            chain_id,
            resolver_address,
            namespace,
            Some(&cursor_after(last)),
            2,
            true,
        )
        .await?;
        ensure!(
            after.rows.is_empty() && after.total_count == Some(expected),
            "after the last name the page serves {:?}, counted {:?}",
            names(&after.rows),
            after.total_count
        );
    }
    snapshot.rollback().await?;
    Ok(names(&whole))
}

/// The walk's rows in the listing's order: their stored order names, then namespace and
/// namehash, compared by the database. A walked name with no stored order name fails.
async fn in_listing_order(
    conn: &mut sqlx::PgConnection,
    chain_id: &str,
    rows: Vec<NameCurrentRow>,
) -> Result<Vec<BoundName>> {
    let ids: Vec<String> = rows.iter().map(|row| row.logical_name_id.clone()).collect();
    let ordered: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT listed.id, summary.order_name
         FROM unnest($2::text[]) AS listed(id)
         LEFT JOIN bigname_phase.project_name_summary summary
           ON summary.chain_id = $1 AND summary.logical_name_id = listed.id
         ORDER BY summary.order_name, summary.namespace, summary.namehash",
    )
    .bind(chain_id)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut by_id: std::collections::BTreeMap<String, NameCurrentRow> = rows
        .into_iter()
        .map(|row| (row.logical_name_id.clone(), row))
        .collect();
    ordered
        .into_iter()
        .map(|(id, order_name)| {
            let order_name =
                order_name.with_context(|| format!("{id} has no stored order name"))?;
            let row = by_id.remove(&id).context("a walked row twice")?;
            Ok(BoundName { row, order_name })
        })
        .collect()
}

/// The order name of a surface from chain data, built here apart from the family step's
/// (`bound_rules::order_name`): its raw name, else every label as `[<labelhash>]`, cut to
/// [`ORDER_NAME_BYTES`].
fn chain_data_order_name(raw_name: Option<String>, labelhashes: &[String]) -> Result<String> {
    if let Some(raw_name) = raw_name {
        return Ok(raw_name);
    }
    let mut name = placeholder_name(labelhashes)?;
    let mut end = name.len().min(ORDER_NAME_BYTES);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name.truncate(end);
    Ok(name)
}

/// The names of `chain_id` whose witness repair released their raw bytes after their summary was
/// composed, before the family step composes them again, and whether the marker still records
/// another Interpret revision. Such a surface names no witness, so the publication's block reads
/// it as stored. The sweep composes those names on the first block applied under a new Interpret
/// revision, so a name is in the window only while Interpret is in redo or the marker still
/// records another revision. After that a stale raw name is a missed sweep. A redo that reaches
/// a published block stamps a Project redo, so while the revision differs a summary can also be
/// missing or stored for a name until that redo replays the block.
async fn released_window(
    conn: &mut sqlx::PgConnection,
    chain_id: &str,
) -> Result<(bool, Vec<String>)> {
    let open: bool = sqlx::query_scalar(
        "WITH interpret AS (
             SELECT input_content_hash, redo_attempt_generation, redo_in_progress
             FROM bigname_phase.chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'interpret'
         )
         SELECT marker.interpret_redo_in_progress IS NULL
                OR COALESCE((SELECT interpret.redo_in_progress FROM interpret), FALSE)
                OR marker.interpret_input_content_hash
                   IS DISTINCT FROM (SELECT interpret.input_content_hash FROM interpret)
                OR marker.interpret_redo_attempt
                   IS DISTINCT FROM (SELECT interpret.redo_attempt_generation FROM interpret)
         FROM bigname_phase.project_family_marker marker
         WHERE marker.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;
    if !open {
        return Ok((false, Vec::new()));
    }
    let released = sqlx::query_scalar(
        "SELECT summary.logical_name_id
         FROM bigname_phase.project_name_summary summary
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = summary.logical_name_id
         WHERE summary.chain_id = $1
           AND surface.raw_name IS NULL AND surface.preimage_event_identity IS NULL
           AND summary.raw_name IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok((true, released))
}

/// Fails unless the name summaries of `chain_id` are those a fresh composition at the family
/// marker's publication writes. Every name with a surface of the chain is composed, so a name
/// the summary lost fails as well as a stale one. Each stored row holds the fresh bound-name
/// columns, and an order name its surface's chain data gives at the publication's block: the
/// raw bytes count from the block of the surface's preimage witness, here read apart from the
/// family step's loader. A name the released window holds ([`released_window`]) is checked for
/// every column but its raw and order names. While the window is open a summary may be missing
/// or left over.
pub async fn assert_stored_bound_fields_are_fresh(pool: &PgPool, chain_id: &str) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let publication = batch::publication(&mut conn, chain_id)
        .await?
        .with_context(|| format!("{chain_id} has no servable family publication"))?;
    let stored: std::collections::BTreeMap<String, Value> = sqlx::query_as::<_, (String, Value)>(
        "SELECT logical_name_id, to_jsonb(summary)
             FROM bigname_phase.project_name_summary summary WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    // The raw bytes and the shadow state as the block reads them: a witness that is a readable
    // event at or below the block, or none at all.
    let surfaces: std::collections::BTreeMap<String, (Option<String>, Vec<String>)> =
        sqlx::query_as::<_, (String, Option<String>, Vec<String>)>(
            "SELECT surface.logical_name_id,
                    CASE WHEN witness.readable THEN surface.raw_name END,
                    surface.labelhashes
             FROM bigname_phase.name_surfaces surface
             CROSS JOIN LATERAL (
                 SELECT surface.preimage_event_identity IS NULL OR EXISTS (
                     SELECT 1 FROM bigname_phase.normalized_events event
                     WHERE event.event_identity = surface.preimage_event_identity
                       AND event.chain_id = surface.chain_id
                       AND event.block_number <= $2
                       AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                       AND EXISTS (
                           SELECT 1 FROM bigname_phase.chain_lineage lineage
                           WHERE lineage.chain_id = event.chain_id
                             AND lineage.block_hash = event.block_hash
                             AND lineage.block_number = event.block_number
                             AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
                 ) AS readable
             ) witness
             WHERE surface.chain_id = $1",
        )
        .bind(chain_id)
        .bind(publication.block_number)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|(id, raw_name, labelhashes)| (id, (raw_name, labelhashes)))
        .collect();
    let ids: Vec<String> = stored
        .keys()
        .chain(surfaces.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let fresh = super::compose_name_summaries(&mut conn, &publication, &ids).await?;
    let (open, released) = released_window(&mut conn, chain_id).await?;
    let mut stale = Vec::new();
    if !open {
        for id in fresh.keys().filter(|id| !stored.contains_key(*id)) {
            stale.push(format!(
                "{id}: no stored summary, a fresh composition has one"
            ));
        }
    }
    for (id, row) in &stored {
        let Some(fresh_row) = fresh.get(id) else {
            if !open {
                stale.push(format!(
                    "{id}: a stored summary, a fresh composition has none"
                ));
            }
            continue;
        };
        // Inside the window the stored raw and order names are the composed ones and the
        // surface has moved on: they are checked again once the family step composes the name.
        let in_window = released.contains(id);
        if !in_window && !row["order_name"].is_null() {
            let (raw_name, labelhashes) = surfaces.get(id).cloned().unwrap_or_default();
            let expected = chain_data_order_name(raw_name, &labelhashes)?;
            if row["order_name"] != Value::String(expected.clone()) {
                stale.push(format!(
                    "{id}.order_name: stored {}, from its surface {expected}",
                    row["order_name"]
                ));
            }
        }
        for column in STORED_BOUND_COLUMNS {
            if in_window && matches!(column, "raw_name" | "order_name") {
                continue;
            }
            let fresh_value = fresh_row.get(column);
            if row.get(column) != fresh_value {
                stale.push(format!(
                    "{id}.{column}: stored {:?}, fresh {fresh_value:?}",
                    row.get(column)
                ));
            }
        }
    }
    ensure!(
        stale.is_empty(),
        "stored bound-name values differ from a fresh composition: {stale:#?}"
    );
    Ok(())
}

fn names(rows: &[BoundName]) -> Vec<String> {
    rows.iter()
        .map(|name| name.row.normalized_name.clone())
        .collect()
}

/// The cursor after `name`, as the resolver route builds it.
fn cursor_after(name: &BoundName) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(name.row.canonical_display_name.clone()),
        namespace: name.row.namespace.clone(),
        normalized_name: name.order_name.clone(),
        namehash: name.row.namehash.clone(),
    }
}
