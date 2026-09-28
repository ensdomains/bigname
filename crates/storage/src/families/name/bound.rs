//! The composed bound-name listing of `GET /v1/resolvers/{chain_id}/{address}` (TYR-36 step 7b,
//! E1d): the names whose composed resolver block names the resolver, with the served listing's
//! predicates, order and keyset (`load_phase_resolver_bound_name_rows`).
//!
//! A composed name's resolver comes from one of three pointers (`serving.rs`,
//! `resolver_block`): an F5 resource pointer or an F4 registry-node pointer whose event names the
//! name, or, for an ENSv2 root-registry TLD, an F5 root-registry pointer at the name's namehash.
//! The candidates are therefore the names those pointers reach when they name the resolver,
//! walked through the readable surfaces in the page order (raw name, namespace, namehash). Each
//! batch is composed and bound in place of `name_current` under the served predicate text
//! (`BOUND_NAME_PREDICATES`), so the rows a batch admits are final and the walk stops once the
//! page is full.
//!
//! A page is read in one snapshot (`batch::read_snapshot`).
//!
//! Interim: the serving-only capability gate still reads the resolver's served row
//! (`resolver_current.declared_summary.bindings.status`), as the route's resolver overview does;
//! both move with the resolver reads (packet E5).
use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};

use super::{CoverageShape, batch};
use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    name_current::{COMPOSED_NC_COLUMNS, DEFAULT_NAME_CURRENT_LINEAGE_JOINS},
    phase_projection_reads::BOUND_NAME_PREDICATES,
};

/// At least this many candidates are composed per walk step.
const BATCH_FLOOR: i64 = 200;

/// `load_phase_resolver_bound_name_rows`'s contract over the composed rows: up to `limit` names
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
               AND event.logical_name_id IS NOT NULL
             UNION
             -- A selected resource always comes from one of the name's binding candidates.
             -- If its latest pointer belongs to another name, compose the candidate name to
             -- inspect its own pointer. This superset deliberately does not filter by resolver:
             -- that pointer can name a different resolver from the resource's latest pointer.
             -- Discovery reads family keys and one event by identity per resource, rather than
             -- rescanning the resolver's entire historical event range for every batch.
             SELECT candidate.logical_name_id
             FROM bigname_phase.project_binding_candidate candidate
             JOIN bigname_phase.project_resource_pointer pointer
               ON pointer.chain_id = candidate.chain_id
              AND pointer.resource_id = candidate.resource_id
             LEFT JOIN bigname_phase.normalized_events latest
               ON latest.event_identity = pointer.pointer_position ->> 'event_identity'
             WHERE candidate.chain_id = $1
               AND latest.logical_name_id IS DISTINCT FROM candidate.logical_name_id
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
         LEFT JOIN bigname_phase.resolver_current resolver_capability
           ON resolver_capability.chain_id = $1
          AND lower(resolver_capability.resolver_address) = lower($2)
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
