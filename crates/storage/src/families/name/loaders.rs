//! The per-batch input loaders of the composed name reader: one statement per input for a
//! batch of names.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::{
    FamilyPublication, NameHistory, compose::Surface, selection::MigrationProof,
    serving::PointerRow,
};
use crate::families::records::FamilyPosition;

pub(super) async fn surfaces(conn: &mut PgConnection, ids: &[String]) -> Result<Vec<Surface>> {
    let rows = sqlx::query(
        "/* storage:families.name.surfaces */
         SELECT surface.logical_name_id, surface.namespace, surface.raw_name, surface.namehash,
                surface.chain_id, surface.block_number
         FROM bigname_phase.name_surfaces surface
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         WHERE surface.logical_name_id = ANY($1::text[])
           AND surface.visibility_state = 'active' AND surface.raw_name <> ''
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load name surfaces")?;
    rows.into_iter()
        .map(|row| {
            Ok(Surface {
                logical_name_id: row.try_get("logical_name_id")?,
                namespace: row.try_get("namespace")?,
                raw_name: row.try_get("raw_name")?,
                namehash: row.try_get("namehash")?,
                chain_id: row.try_get("chain_id")?,
                block_number: row.try_get("block_number")?,
            })
        })
        .collect()
}

pub(super) async fn histories(
    conn: &mut PgConnection,
    chain_id: &str,
    ids: &[String],
) -> Result<BTreeMap<String, NameHistory>> {
    let rows = sqlx::query(
        "/* storage:families.name.histories */
         SELECT logical_name_id, first_block_number, to_jsonb(created_at) AS created_at,
                has_ens_v2_events, event_arms
         FROM bigname_phase.project_name_history
         WHERE chain_id = $1 AND logical_name_id = ANY($2::text[])",
    )
    .bind(chain_id)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load name histories")?;
    rows.into_iter()
        .map(|row| {
            let arms: Value = row.try_get("event_arms")?;
            Ok((
                row.try_get("logical_name_id")?,
                NameHistory {
                    first_block_number: row.try_get("first_block_number")?,
                    created_at: row.try_get("created_at")?,
                    has_ens_v2_events: row.try_get("has_ens_v2_events")?,
                    event_arms: arms
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|arm| arm.as_str().map(str::to_owned))
                        .collect(),
                },
            ))
        })
        .collect()
}

/// Each name's latest MigrationApplied, its generated id and correlation id read back by
/// identity.
pub(super) async fn migrations(
    conn: &mut PgConnection,
    chain_id: &str,
    ids: &[String],
) -> Result<BTreeMap<String, MigrationProof>> {
    let rows = sqlx::query(
        "/* storage:families.name.migrations */
         SELECT state.logical_name_id, state.migration_position ->> 'event_identity' AS identity,
                event.normalized_event_id, event.migration_correlation_ids[1] AS transition_id
         FROM bigname_phase.project_name_state state
         LEFT JOIN bigname_phase.normalized_events event
           ON event.event_identity = state.migration_position ->> 'event_identity'
         WHERE state.chain_id = $1 AND state.logical_name_id = ANY($2::text[])
           AND state.migration_position IS NOT NULL",
    )
    .bind(chain_id)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load name migrations")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("logical_name_id")?,
                MigrationProof {
                    event_identity: row.try_get("identity")?,
                    normalized_event_id: row.try_get("normalized_event_id")?,
                    transition_id: row.try_get("transition_id")?,
                },
            ))
        })
        .collect()
}

/// The readable resources among `$1` at the publication `($2, $3)`.
pub(crate) const RESOURCES_SQL: &str = "/* storage:families.name.resources */
     SELECT resource.resource_id::text AS resource_id, resource.token_lineage_id,
            (resource.token_lineage_id IS NULL OR EXISTS (
                SELECT 1 FROM bigname_phase.token_lineages token
                JOIN bigname_phase.chain_lineage token_lineage
                  ON token_lineage.chain_id = token.chain_id
                 AND token_lineage.block_hash = token.block_hash
                WHERE token.token_lineage_id = resource.token_lineage_id
                  AND token.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            )) AS token_readable
     FROM bigname_phase.resources resource
     JOIN bigname_phase.chain_lineage lineage
       ON lineage.chain_id = resource.chain_id AND lineage.block_hash = resource.block_hash
     WHERE resource.resource_id = ANY($1::uuid[])
       AND resource.chain_id = $2 AND resource.block_number <= $3
       AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
       AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')";

/// The readable resources among `resources` at the publication, with their token lineage and
/// whether that lineage is readable.
pub(super) async fn resources(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    resources: &[String],
) -> Result<BTreeMap<String, (Option<Uuid>, bool)>> {
    let rows = sqlx::query(RESOURCES_SQL)
        .bind(resources)
        .bind(&publication.chain_id)
        .bind(publication.block_number)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load readable resources")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("resource_id")?,
                (
                    row.try_get("token_lineage_id")?,
                    row.try_get("token_readable")?,
                ),
            ))
        })
        .collect()
}

fn pointer_of(row: &sqlx::postgres::PgRow, position: Option<Value>) -> Result<Option<PointerRow>> {
    let Some(position) = position.as_ref().and_then(FamilyPosition::from_json) else {
        return Ok(None);
    };
    Ok(Some(PointerRow {
        resource_id: row.try_get("resource_id")?,
        resolver_address: row.try_get("resolver_address")?,
        position,
        source_family: row
            .try_get::<Option<String>, _>("source_family")?
            .unwrap_or_default(),
        logical_name_id: row.try_get("event_name")?,
        normalized_event_id: row.try_get("event_id")?,
    }))
}

/// F5 pointers by resource, and the root-registry pointers naming each `(namespace, namehash)`.
pub(super) async fn resource_pointers(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[String],
    nodes: &[(String, String)],
) -> Result<(
    BTreeMap<String, PointerRow>,
    BTreeMap<(String, String), Vec<PointerRow>>,
)> {
    let (namespaces, namehashes): (Vec<String>, Vec<String>) = nodes.iter().cloned().unzip();
    let rows = sqlx::query(
        "/* storage:families.name.resource_pointers */
         SELECT pointer.resource_id::text AS resource_id, pointer.resolver_address,
                pointer.pointer_position, pointer.source_family, pointer.namespace,
                pointer.namehash, event.logical_name_id AS event_name,
                event.normalized_event_id AS event_id
         FROM bigname_phase.project_resource_pointer pointer
         LEFT JOIN bigname_phase.normalized_events event
           ON event.event_identity = pointer.pointer_position ->> 'event_identity'
         WHERE pointer.chain_id = $1
           AND (pointer.resource_id = ANY($2::uuid[])
                OR (pointer.source_family = 'ens_v2_root_l1'
                    AND (pointer.namespace, pointer.namehash) IN (
                        SELECT * FROM unnest($3::text[], $4::text[]))))",
    )
    .bind(chain_id)
    .bind(resources)
    .bind(&namespaces)
    .bind(&namehashes)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load resource pointers")?;
    let mut by_resource = BTreeMap::new();
    let mut roots: BTreeMap<(String, String), Vec<PointerRow>> = BTreeMap::new();
    for row in &rows {
        let Some(pointer) = pointer_of(row, row.try_get("pointer_position")?)? else {
            continue;
        };
        let resource: String = row.try_get("resource_id")?;
        if pointer.source_family == "ens_v2_root_l1"
            && let (Some(namespace), Some(namehash)) = (
                row.try_get::<Option<String>, _>("namespace")?,
                row.try_get::<Option<String>, _>("namehash")?,
            )
        {
            roots
                .entry((namespace, namehash))
                .or_default()
                .push(pointer.clone());
        }
        by_resource.insert(resource, pointer);
    }
    Ok((by_resource, roots))
}

/// The latest named `ResolverChanged` for each requested (resource, name) pair. F5 keeps the
/// resource's latest pointer independently; these owned rows keep each name's own pointer,
/// including clears, in the family's canonical event order. Only pairs whose F5 row names a
/// different name are requested. The primary key bounds reads to those pairs.
pub(super) async fn named_resource_pointers(
    conn: &mut PgConnection,
    chain_id: &str,
    target: i64,
    pairs: &[(String, String)],
) -> Result<BTreeMap<(String, String), PointerRow>> {
    if pairs.is_empty() {
        return Ok(BTreeMap::new());
    }
    let (resources, names): (Vec<String>, Vec<String>) = pairs.iter().cloned().unzip();
    let rows = sqlx::query(
        "/* storage:families.name.named_resource_pointers */
         SELECT pointer.resource_id::text AS resource_id, pointer.resolver_address,
                jsonb_build_object('block_number', pointer.block_number,
                    'transaction_index', pointer.transaction_index,
                    'log_index', pointer.log_index,
                    'event_identity', pointer.event_identity) AS pointer_position,
                pointer.source_family, pointer.logical_name_id AS event_name,
                pointer.normalized_event_id AS event_id
         FROM bigname_phase.project_named_resource_pointer pointer
         JOIN unnest($3::text[], $4::text[]) wanted(resource_id, logical_name_id)
           ON pointer.resource_id = wanted.resource_id::uuid
          AND pointer.logical_name_id = wanted.logical_name_id
         WHERE pointer.chain_id = $1 AND pointer.block_number <= $2",
    )
    .bind(chain_id)
    .bind(target)
    .bind(&resources)
    .bind(&names)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the named resource pointers")?;
    let mut out = BTreeMap::new();
    for row in &rows {
        let Some(pointer) = pointer_of(row, row.try_get("pointer_position")?)? else {
            continue;
        };
        let (Some(resource), Some(name)) =
            (pointer.resource_id.clone(), pointer.logical_name_id.clone())
        else {
            continue;
        };
        out.insert((resource, name), pointer);
    }
    Ok(out)
}

/// F4 pointers by `(namespace, node)`.
pub(super) async fn node_pointers(
    conn: &mut PgConnection,
    chain_id: &str,
    nodes: &[(String, String)],
) -> Result<BTreeMap<(String, String), PointerRow>> {
    let (namespaces, namehashes): (Vec<String>, Vec<String>) = nodes.iter().cloned().unzip();
    let rows = sqlx::query(
        "/* storage:families.name.node_pointers */
         SELECT pointer.namespace, pointer.node, pointer.resource_id::text AS resource_id,
                NULLIF(pointer.resolver_address, '') AS resolver_address, pointer.source_family,
                jsonb_build_object('block_number', pointer.block_number,
                    'transaction_index', pointer.transaction_index,
                    'log_index', pointer.log_index,
                    'event_identity', pointer.event_identity) AS position,
                event.logical_name_id AS event_name, pointer.normalized_event_id AS event_id
         FROM bigname_phase.project_registry_pointer pointer
         LEFT JOIN bigname_phase.normalized_events event
           ON event.event_identity = pointer.event_identity
         WHERE pointer.chain_id = $1
           AND (pointer.namespace, pointer.node) IN (SELECT * FROM unnest($2::text[], $3::text[]))",
    )
    .bind(chain_id)
    .bind(&namespaces)
    .bind(&namehashes)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load registry node pointers")?;
    let mut out = BTreeMap::new();
    for row in &rows {
        if let Some(pointer) = pointer_of(row, row.try_get("position")?)? {
            out.insert((row.try_get("namespace")?, row.try_get("node")?), pointer);
        }
    }
    Ok(out)
}

/// The latest root-registry release position of each resource.
pub(super) async fn root_releases(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[String],
) -> Result<BTreeMap<String, (i64, i64, i64)>> {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "/* storage:families.name.root_releases */
         SELECT event.resource_id::text, event.block_number,
                COALESCE(event.transaction_index, -1), COALESCE(event.log_index, -1)
         FROM bigname_phase.project_lifecycle_event event
         WHERE event.chain_id = $1 AND event.state_kind = 'resource'
           AND event.state_key = ANY($2::text[])
           AND event.source_family = 'ens_v2_root_l1'
           AND event.event_kind = 'RegistrationReleased'",
    )
    .bind(chain_id)
    .bind(resources)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load root releases")?;
    let mut out: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
    for (resource, block, transaction, log) in rows {
        let at = (block, transaction, log);
        let entry = out.entry(resource).or_insert(at);
        *entry = (*entry).max(at);
    }
    Ok(out)
}
