//! The composed bound-name listing of `GET /v1/resolvers/{chain_id}/{address}`: the names whose
//! composed row serves the resolver and passes the bound-name rules (`bound_rules.rs`), in page
//! order, with an optional exact count.
//!
//! The page order is the name as the composed row renders it from chain data alone, then
//! namespace and namehash: the raw name, or for a surface that stores no raw bytes each label
//! as `[<labelhash>]`. An imported label preimage changes how a name is shown, never where it
//! sorts or where a cursor continues.
//!
//! No composed row is stored, but the family step stores each name's bound-name values and order
//! name on its name summary (`project_name_summary`), from the same composition and rule text. A
//! page is read in two steps on one snapshot (`batch::read_snapshot`):
//!
//! 1. The resolver's binding-enumeration support is read from its classification row at request
//!    time. [`bound_page_sql`] then reads one partial index in page order from the cursor:
//!    `project_name_summary_bound_order_idx` when the support is `supported`, so every listed
//!    name passes the capability gate, else `project_name_summary_bound_exempt_order_idx`,
//!    which holds only the names exempt from the gate. Either way a page reads no name the gate
//!    drops.
//! 2. Only those names are composed, and the whole rule set runs over the composed rows once. A
//!    page shows each name as served, imported preimages included.
//!
//! A name whose surface gained or lost its raw bytes after the summary was composed is listed at
//! its stored order name and shown as served, until the family step composes it again: in the
//! block that observes the preimage, or on its first block after an Interpret redo
//! (`RAW_NAME_SWEEP` in Project's summary writer). A selected name whose composed row fails the
//! rules is a stored summary that disagrees with its composition, and the read fails.
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgConnection, Row};

use super::{
    CoverageShape, batch,
    bound_rules::{
        BOUND_CAPABILITY_EXEMPT_SQL, BOUND_LISTING_SQL, BOUND_RESOLVER_ADDRESS_SQL,
        BOUND_RESOLVER_CHAIN_SQL, COMPOSED_NC_COLUMNS,
    },
};
use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    families::topology::{FAMILY_RESOLVER_SERVED_ROWS, FAMILY_RESOLVER_SUMMARY},
    name_current::DEFAULT_NAME_CURRENT_LINEAGE_JOINS,
};

/// A page of bound names, and the exact count of the whole list when it was asked for.
#[derive(Clone, Debug, Default)]
pub struct BoundNamesPage {
    pub rows: Vec<BoundName>,
    pub total_count: Option<u64>,
}

/// One bound name as served, and its order name, which a cursor after it continues from in
/// place of the served name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundName {
    pub row: NameCurrentRow,
    pub order_name: String,
}

/// The capability gate's live part, the resolver family's binding-enumeration support, read
/// once per statement from the classification row of resolver `$2` on chain `$1`. The table's
/// key makes it one row at most.
fn capability_supported_sql() -> String {
    format!(
        "(SELECT {FAMILY_RESOLVER_SUMMARY} #>> '{{bindings,status}}'
          FROM bigname_phase.project_resolver_classification classification_row
          WHERE classification_row.chain_id = $1
            AND classification_row.resolver_address = lower($2)
            AND {FAMILY_RESOLVER_SERVED_ROWS}) = 'supported'"
    )
}

/// Every bound-name rule over a composed row `nc`, with `$1` the chain and `$2` the resolver.
fn bound_name_predicates() -> String {
    format!(
        "({BOUND_LISTING_SQL})
         AND {BOUND_RESOLVER_CHAIN_SQL} = $1
         AND {BOUND_RESOLVER_ADDRESS_SQL} = lower($2)
         AND ({BOUND_CAPABILITY_EXEMPT_SQL} OR {})",
        capability_supported_sql()
    )
}

/// The stored bound-name values that select the summary row `summary` for resolver `$2` on
/// chain `$1`.
const STORED_SELECTOR_SQL: &str = "summary.resolver_chain_id = $1
        AND summary.resolver_address = lower($2)
        AND summary.resolver_listing_eligible";

/// The surface fences of the walk the listing replaced, over the summary row `summary`: an
/// active surface at or below the family marker, on readable lineage. The keys it reads make it
/// one row at most.
const SURFACE_FENCES_SQL: &str = "FROM bigname_phase.name_surfaces surface
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
        JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
        WHERE surface.logical_name_id = summary.logical_name_id
          AND surface.visibility_state = 'active'
          AND surface.block_number <= marker.current_block_number
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')";

/// The listed summary rows of resolver `$2` on chain `$1`, optionally in namespace `$3`, with
/// the surface fences as `fenced`, for a resolver whose binding-enumeration support is
/// `supported` or not. The capability gate then holds for every row, or only for the exempt
/// ones, and the clause it leaves is the one in the predicate of the index the page reads. The
/// length bound matches both indexes' predicates.
fn listed_sql(fenced: &str, supported: bool) -> String {
    let gate = if supported {
        ""
    } else {
        "AND summary.resolver_support_exempt"
    };
    format!(
        "FROM bigname_phase.project_name_summary summary
     WHERE {STORED_SELECTOR_SQL}
       AND octet_length(summary.order_name) <= 2000
       {gate}
       AND ($3::text IS NULL OR summary.namespace = $3)
       AND {fenced}"
    )
}

/// The names of a page, `$7` of them after the cursor `($4, $5, $6)`:
/// (logical_name_id, order name, namespace, namehash). With no cursor the keyset bound is
/// `('', '', '')`, below every row (namespaces are never empty). Only the summary is scanned:
/// the surface fences are a scalar subquery, run once for each row the page reads in index
/// order, which the planner cannot turn into a join over every surface. So either plan reads
/// the partial index `supported` picks (see [`listed_sql`]) in page order and stops after `$7`
/// rows pass.
pub(crate) fn bound_page_sql(supported: bool) -> String {
    format!(
        "/* storage:families.name.bound_page */
     SELECT summary.logical_name_id, summary.order_name AS name, summary.namespace,
            summary.namehash
     {listed}
       AND (summary.order_name, summary.namespace, summary.namehash)
           > (COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''))
     ORDER BY summary.order_name, summary.namespace, summary.namehash
     LIMIT $7",
        listed = listed_sql(
            &format!("COALESCE((SELECT TRUE {SURFACE_FENCES_SQL}), FALSE)"),
            supported
        ),
    )
}

/// The number of names the list holds, over the page's relation with no cursor: the fences as
/// `EXISTS`, which the planner can run as one semi-join, since the count reads every listed row.
pub(crate) fn bound_count_sql(supported: bool) -> String {
    format!(
        "/* storage:families.name.bound_count */ SELECT count(*) {}",
        listed_sql(
            &format!("EXISTS (SELECT 1 {SURFACE_FENCES_SQL})"),
            supported
        )
    )
}

/// Whether the binding enumeration of resolver `resolver_address` on `chain_id` is `supported`
/// ([`capability_supported_sql`]), read once on the page's snapshot.
async fn binding_supported(
    conn: &mut PgConnection,
    (chain_id, resolver_address): (&str, &str),
) -> Result<bool> {
    sqlx::query_scalar(&format!(
        "/* storage:families.name.bound_support */ SELECT COALESCE({}, FALSE)",
        capability_supported_sql()
    ))
    .bind(chain_id)
    .bind(resolver_address)
    .fetch_one(conn)
    .await
    .with_context(|| format!("failed to read the binding support of {chain_id}:{resolver_address}"))
}

/// The bound-name listing over the composed rows: up to `limit` names bound to
/// `resolver_address` on `chain_id`, after `cursor`, in page order, and with `total_count` the
/// exact number of names in the whole list, counted from the same relation on the same snapshot.
pub async fn load_family_bound_names(
    db: impl Into<crate::ReadDb<'_>>,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
    cursor: Option<&NameCurrentListCursor>,
    limit: i64,
    total_count: bool,
) -> Result<BoundNamesPage> {
    // The cursor's name is the order name of the row it follows.
    let after = cursor
        .map(|cursor| match &cursor.sort_value {
            NameCurrentListCursorValue::Name(_) => Ok((
                cursor.normalized_name.clone(),
                cursor.namespace.clone(),
                cursor.namehash.clone(),
            )),
            _ => bail!("composed bound-name cursor must use name ordering"),
        })
        .transpose()?;
    let mut snapshot = db.into().snapshot().await?;
    // The summary is family state, which a rebuild empties: read the marker first, so a rebuild
    // refuses rather than answers a resolver with no names.
    batch::ensure_published(&mut snapshot, &[chain_id.to_owned()]).await?;
    let supported = binding_supported(&mut snapshot, (chain_id, resolver_address)).await?;
    let total_count = if total_count {
        Some(
            count(
                &mut snapshot,
                (chain_id, resolver_address, namespace),
                supported,
            )
            .await?,
        )
    } else {
        None
    };
    let selected = select(
        &mut snapshot,
        (chain_id, resolver_address, namespace),
        supported,
        after.as_ref(),
        Some(limit.max(0)),
    )
    .await?;
    let ids: Vec<String> = selected.iter().map(|name| name.id.clone()).collect();
    super::seams::note_composed_names(ids.len());
    let mut composed = batch::load(&mut snapshot, &ids, CoverageShape::Plain).await?;
    let source = Value::Array(composed.values().map(super::list::source_row).collect());
    let admitted: BTreeSet<String> = admitted(&mut snapshot, (chain_id, resolver_address), &source)
        .await?
        .into_iter()
        .collect();
    let mut rows = Vec::with_capacity(selected.len());
    for name in selected {
        let row = composed
            .remove(&name.id)
            .filter(|_| admitted.contains(&name.id))
            .with_context(|| {
                format!(
                    "the bound-name selector of {} at {chain_id}:{resolver_address} and its \
                     composed row disagree",
                    name.id
                )
            })?;
        rows.push(BoundName {
            row,
            order_name: name.key.0,
        });
    }
    snapshot.close().await?;
    Ok(BoundNamesPage { rows, total_count })
}

/// One selected name: its id and its page key (order name, namespace, namehash).
struct Selected {
    id: String,
    key: (String, String, String),
}

/// The selected names after `after` in page order, at most `limit` (all with `None`).
async fn select(
    conn: &mut PgConnection,
    (chain_id, resolver_address, namespace): (&str, &str, Option<&str>),
    supported: bool,
    after: Option<&(String, String, String)>,
    limit: Option<i64>,
) -> Result<Vec<Selected>> {
    let rows = sqlx::query(&bound_page_sql(supported))
        .bind(chain_id)
        .bind(resolver_address)
        .bind(namespace)
        .bind(after.map(|(name, ..)| name.as_str()))
        .bind(after.map(|(_, namespace, _)| namespace.as_str()))
        .bind(after.map(|(.., namehash)| namehash.as_str()))
        .bind(limit)
        .fetch_all(conn)
        .await
        .with_context(|| {
            format!("failed to select the bound names of {chain_id}:{resolver_address}")
        })?;
    rows.into_iter()
        .map(|row| {
            Ok(Selected {
                id: row.try_get("logical_name_id")?,
                key: (
                    row.try_get("name")?,
                    row.try_get("namespace")?,
                    row.try_get("namehash")?,
                ),
            })
        })
        .collect()
}

/// The number of names in the whole list ([`bound_count_sql`]).
///
/// The count is sent unprepared, so PostgreSQL plans each count with the resolver's own values. A
/// cached generic plan guesses a few hundred names and probes every name's surfaces in a nested
/// loop, which takes seconds at a million names.
pub(crate) async fn count(
    conn: &mut PgConnection,
    (chain_id, resolver_address, namespace): (&str, &str, Option<&str>),
    supported: bool,
) -> Result<u64> {
    let total: i64 = sqlx::query_scalar(&bound_count_sql(supported))
        .bind(chain_id)
        .bind(resolver_address)
        .bind(namespace)
        .persistent(false)
        .fetch_one(conn)
        .await
        .with_context(|| {
            format!("failed to count the bound names of {chain_id}:{resolver_address}")
        })?;
    Ok(u64::try_from(total).unwrap_or_default())
}

/// The ids of the composed rows in `source` the bound-name rules admit.
async fn admitted(
    conn: &mut PgConnection,
    (chain_id, resolver_address): (&str, &str),
    source: &Value,
) -> Result<Vec<String>> {
    let query = format!(
        "/* storage:families.name.bound_admitted */
         SELECT nc.logical_name_id
         FROM JSONB_TO_RECORDSET($3::jsonb) AS {COMPOSED_NC_COLUMNS}
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = nc.logical_name_id
         LEFT JOIN bigname_phase.resources resource
           ON resource.resource_id = nc.resource_id
         LEFT JOIN bigname_phase.surface_bindings binding
           ON binding.surface_binding_id = nc.surface_binding_id
         LEFT JOIN bigname_phase.token_lineages token_lineage
           ON token_lineage.token_lineage_id = nc.token_lineage_id
         {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
         WHERE {predicates}",
        predicates = bound_name_predicates(),
    );
    sqlx::query_scalar(&query)
        .bind(chain_id)
        .bind(resolver_address)
        .bind(source)
        .fetch_all(conn)
        .await
        .with_context(|| {
            format!("failed to filter the composed bound names of {chain_id}:{resolver_address}")
        })
}
