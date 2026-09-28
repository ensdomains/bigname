//! The composed bound-name listing of `GET /v1/resolvers/{chain_id}/{address}`: the names whose
//! composed resolver block names the resolver, with the listing's predicates, order and keyset.
//!
//! A composed name's resolver comes from one of three pointers (`serving.rs`,
//! `resolver_block`): an F5 resource pointer or an F4 registry-node pointer whose event names the
//! name, or, for an ENSv2 root-registry TLD, an F5 root-registry pointer at the name's namehash.
//! The candidates are therefore the names those pointers reach when they name the resolver,
//! walked through the readable surfaces in the page order (raw name, namespace, namehash). Each
//! batch is composed and bound as the name relation under the predicate text
//! (`BOUND_NAME_PREDICATES`), so the rows a batch admits are final and the walk stops once the
//! page is full.
//!
//! A page is read in one snapshot (`batch::read_snapshot`).
//!
//! The serving-only capability gate reads the resolver's binding support from its F3
//! classification row (`topology::overview`).
use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};

use super::{CoverageShape, batch};
use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    families::topology::{FAMILY_RESOLVER_SERVED_ROWS, FAMILY_RESOLVER_SUMMARY},
    name_current::{COMPOSED_NC_COLUMNS, DEFAULT_NAME_CURRENT_LINEAGE_JOINS},
    phase_projection_reads::BOUND_NAME_PREDICATES,
};

/// At least this many candidates are composed per walk step.
const BATCH_FLOOR: i64 = 200;

/// The bound-name listing over the composed rows: up to `limit` names
/// bound to `resolver_address` on `chain_id`, after `cursor`, in name order.
pub async fn load_family_bound_names(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
    cursor: Option<&NameCurrentListCursor>,
    limit: i64,
) -> Result<Vec<NameCurrentRow>> {
    let mut after = cursor
        .map(|cursor| match &cursor.sort_value {
            NameCurrentListCursorValue::Name(_) => Ok((
                cursor.normalized_name.clone(),
                cursor.namespace.clone(),
                cursor.namehash.clone(),
            )),
            _ => bail!("composed bound-name cursor must use name ordering"),
        })
        .transpose()?;
    let wanted = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
    let batch = i64::try_from(super::seams::batch_size(
        usize::try_from(limit.saturating_add(1).saturating_mul(4).max(BATCH_FLOOR))
            .unwrap_or(usize::MAX),
    ))
    .unwrap_or(i64::MAX);
    let mut snapshot = batch::read_snapshot(pool).await?;
    // The walk reads family tables, which a rebuild empties: read the marker first, so a rebuild
    // refuses rather than answers a resolver with no names.
    batch::ensure_published(&mut snapshot, &[chain_id.to_owned()]).await?;
    let mut out = Vec::new();
    while out.len() < wanted {
        let candidates = candidates(
            &mut snapshot,
            (chain_id, resolver_address, namespace),
            after.as_ref(),
            batch,
        )
        .await?;
        let exhausted = i64::try_from(candidates.len()).unwrap_or(i64::MAX) < batch;
        let Some((_, name, space, hash)) = candidates.last() else {
            break;
        };
        after = Some((name.clone(), space.clone(), hash.clone()));
        let ids: Vec<String> = candidates.into_iter().map(|(id, ..)| id).collect();
        let mut composed = batch::load(&mut snapshot, &ids, CoverageShape::Plain).await?;
        let source = Value::Array(composed.values().map(super::list::source_row).collect());
        let remaining = i64::try_from(wanted - out.len()).unwrap_or(i64::MAX);
        for id in admitted(
            &mut snapshot,
            (chain_id, resolver_address),
            &source,
            remaining,
        )
        .await?
        {
            out.push(
                composed
                    .remove(&id)
                    .with_context(|| format!("composed bound name {id} vanished"))?,
            );
        }
        if exhausted {
            break;
        }
    }
    snapshot.commit().await?;
    Ok(out)
}

/// The next names after `after` in the page order that a pointer naming the resolver reaches:
/// (logical_name_id, raw_name, namespace, namehash). A candidate is a superset: `admitted` keeps
/// the names whose composed row serves the resolver.
async fn candidates(
    conn: &mut PgConnection,
    (chain_id, resolver_address, namespace): (&str, &str, Option<&str>),
    after: Option<&(String, String, String)>,
    limit: i64,
) -> Result<Vec<(String, String, String, String)>> {
    let rows = sqlx::query(
        "/* storage:families.name.bound_candidates */
         WITH reached AS (
             SELECT event.logical_name_id
             FROM bigname_phase.project_resource_pointer pointer
             JOIN bigname_phase.normalized_events event
               ON event.event_identity = pointer.pointer_position ->> 'event_identity'
             WHERE pointer.chain_id = $1 AND pointer.resolver_address = lower($2)
               AND event.logical_name_id IS NOT NULL
             UNION
             SELECT pointer.namespace || ':' || lower(pointer.namehash)
             FROM bigname_phase.project_resource_pointer pointer
             WHERE pointer.chain_id = $1 AND pointer.resolver_address = lower($2)
               AND pointer.source_family = 'ens_v2_root_l1'
               AND pointer.namespace IS NOT NULL AND pointer.namehash IS NOT NULL
             UNION
             SELECT event.logical_name_id
             FROM bigname_phase.project_registry_pointer pointer
             JOIN bigname_phase.normalized_events event
               ON event.event_identity = pointer.event_identity
             WHERE pointer.chain_id = $1 AND pointer.resolver_address = lower($2)
               AND pointer.resource_id IS NULL AND event.logical_name_id IS NOT NULL
             UNION
             -- Named F5 keys retain a name's own latest pointer when the resource's latest
             -- pointer names another name. The resolver index reads retained pointer keys,
             -- never the history of ResolverChanged events at this resolver.
             SELECT pointer.logical_name_id
             FROM bigname_phase.project_named_resource_pointer pointer
             WHERE pointer.chain_id = $1 AND pointer.resolver_address = lower($2)
         )
         SELECT surface.logical_name_id, surface.raw_name, surface.namespace, surface.namehash
         FROM reached
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = reached.logical_name_id
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
         WHERE surface.visibility_state = 'active'
           AND surface.block_number <= marker.current_block_number
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND ($3::text IS NULL OR surface.namespace = $3)
           AND ($4::text IS NULL
                OR (surface.raw_name, surface.namespace, surface.namehash) > ($4, $5, $6))
         ORDER BY surface.raw_name, surface.namespace, surface.namehash
         LIMIT $7",
    )
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
        format!("failed to walk the bound-name candidates of {chain_id}:{resolver_address}")
    })?;
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

/// The ids of the composed rows in `source` the served predicates admit, in page order, at most
/// `limit`.
async fn admitted(
    conn: &mut PgConnection,
    (chain_id, resolver_address): (&str, &str),
    source: &Value,
    limit: i64,
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
         LEFT JOIN LATERAL (
             SELECT {FAMILY_RESOLVER_SUMMARY} AS declared_summary
             FROM bigname_phase.project_resolver_classification classification_row
             WHERE classification_row.chain_id = $1
               AND classification_row.resolver_address = lower($2)
               AND {FAMILY_RESOLVER_SERVED_ROWS}
         ) resolver_capability ON TRUE
         {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
         WHERE {BOUND_NAME_PREDICATES}
         ORDER BY nc.raw_name, nc.namespace, nc.namehash
         LIMIT $4"
    );
    sqlx::query_scalar(&query)
        .bind(chain_id)
        .bind(resolver_address)
        .bind(source)
        .bind(limit)
        .fetch_all(conn)
        .await
        .with_context(|| {
            format!("failed to filter the composed bound names of {chain_id}:{resolver_address}")
        })
}
