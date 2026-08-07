use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot_selection::{
    ChainPosition, ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

use super::{
    boundary_key::{boundary_has_event_pointer, boundary_str, record_version_boundary_storage_key},
    canonicality::{DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER, RESOURCE_CANONICALITY_JOINS},
    row_decode::{RecordInventoryCurrentRow, decode_record_inventory_current_row},
};

/// Load one record-inventory projection row by resource and exact version boundary.
pub async fn load_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current boundary key for resource_id {resource_id}"
        )
    })?;

    let row = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            jsonb_build_object('observed_selectors', true, 'capability_declared_families', true, 'globally_enumerable', false) AS enumeration_basis,
            ric.selectors,
            '[]'::jsonb AS explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            CASE WHEN ric.support_status = 'supported' THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted') ELSE jsonb_build_object('status', 'unsupported', 'exhaustiveness', 'not_asserted', 'unsupported_reason', ric.unsupported_reason) END AS coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM bigname_phase.record_inventory_current ric
        {RESOURCE_CANONICALITY_JOINS}
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary_key = $2
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row).transpose()
}

/// Load one record-inventory projection row by resource and version boundary, falling back to a
/// row that shares the boundary's anchor when the exact key misses. A caller-derived boundary is
/// pointer-less (`normalized_event_id`/`event_kind` null), while the projection may key its row
/// with the anchoring event pointer filled in; both describe the same anchor (`logical_name_id` +
/// chain position), so the fallback matches on the anchor and rejects ambiguous multi-row matches.
/// Callers whose boundary already carries a pointer get the exact read only.
pub async fn load_record_inventory_current_with_anchor_fallback(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    if let Some(row) =
        load_record_inventory_current(pool, resource_id, record_version_boundary).await?
    {
        return Ok(Some(row));
    }
    if boundary_has_event_pointer(record_version_boundary) {
        return Ok(None);
    }

    let Some(anchored_boundary) =
        find_record_inventory_boundary_by_anchor(pool, resource_id, record_version_boundary)
            .await?
    else {
        return Ok(None);
    };
    load_record_inventory_current(pool, resource_id, &anchored_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current anchor for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

/// Batch variant of [`load_record_inventory_current_with_anchor_fallback`]: resolve many
/// `(resource_id, boundary)` pairs in a single exact query, then apply the per-row anchor fallback
/// only to the exact-key misses (the common case is an exact hit, so the fallback runs rarely).
/// Returns rows aligned to `keys` (`None` = no row for that pair). This is what the GraphQL
/// `Domain.resolver` DataLoader calls so a page of N domains costs one query plus a handful of
/// fallbacks instead of N point reads.
pub async fn load_record_inventory_current_batch(
    pool: &PgPool,
    keys: &[(Uuid, Value)],
) -> Result<Vec<Option<RecordInventoryCurrentRow>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Derive the storage boundary key for each pair up front; this is the same key the exact
    // single-row read binds, so the batched `IN` matches the identical rows.
    let mut resource_ids = Vec::with_capacity(keys.len());
    let mut boundary_keys = Vec::with_capacity(keys.len());
    for (resource_id, boundary) in keys {
        let boundary_key =
            record_version_boundary_storage_key(boundary, *resource_id).with_context(|| {
                format!(
                    "failed to derive record_inventory_current boundary key for resource_id {resource_id}"
                )
            })?;
        resource_ids.push(*resource_id);
        boundary_keys.push(boundary_key);
    }

    // Stage 1: one query for every exact `(resource_id, boundary_key)` hit. `unnest` zips the two
    // bound arrays into the composite-key set the `IN` filters on.
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary_key,
            ric.record_version_boundary,
            jsonb_build_object('observed_selectors', true, 'capability_declared_families', true, 'globally_enumerable', false) AS enumeration_basis,
            ric.selectors,
            '[]'::jsonb AS explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            CASE WHEN ric.support_status = 'supported' THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted') ELSE jsonb_build_object('status', 'unsupported', 'exhaustiveness', 'not_asserted', 'unsupported_reason', ric.unsupported_reason) END AS coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM bigname_phase.record_inventory_current ric
        {RESOURCE_CANONICALITY_JOINS}
        WHERE (ric.resource_id, ric.record_version_boundary_key) IN (
            SELECT * FROM unnest($1::uuid[], $2::text[])
        )
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(&resource_ids)
    .bind(&boundary_keys)
    .fetch_all(pool)
    .await
    .context("failed to batch-load record_inventory_current rows")?;

    // Key the map by the DB-returned `record_version_boundary_key` so the zip-back below can look
    // each input up by its derived key. This relies on the stored key being canonical, which
    // `decode_record_inventory_current_row` validates (it re-derives the key from the row's
    // boundary JSON and bails on mismatch) — keep that validation if this insert is ever refactored.
    let mut by_key = HashMap::<(Uuid, String), RecordInventoryCurrentRow>::new();
    for row in rows {
        let resource_id = crate::sql_row::get::<Uuid>(&row, "resource_id")?;
        let boundary_key = crate::sql_row::get::<String>(&row, "record_version_boundary_key")?;
        by_key.insert(
            (resource_id, boundary_key),
            decode_record_inventory_current_row(row)?,
        );
    }

    // Assemble output aligned to the input. Misses fall back to the shared single-row anchor logic
    // (only when the boundary is pointer-less), so the fallback semantics and the ambiguity guard
    // stay identical to the non-batched path — including treating a matched-but-unloadable anchor as
    // a hard error rather than silently serving `None`.
    let mut output = Vec::with_capacity(keys.len());
    for ((resource_id, boundary), boundary_key) in keys.iter().zip(boundary_keys) {
        if let Some(row) = by_key.get(&(*resource_id, boundary_key)) {
            output.push(Some(row.clone()));
        } else if boundary_has_event_pointer(boundary) {
            output.push(None);
        } else if let Some(anchored_boundary) =
            find_record_inventory_boundary_by_anchor(pool, *resource_id, boundary).await?
        {
            let row = load_record_inventory_current(pool, *resource_id, &anchored_boundary)
                .await?
                .with_context(|| {
                    format!(
                        "matched record_inventory_current anchor for resource_id {resource_id} but the projection row was not loadable"
                    )
                })?;
            output.push(Some(row));
        } else {
            output.push(None);
        }
    }
    Ok(output)
}

/// Find the persisted boundary of the (at most one) projection row sharing the caller boundary's
/// anchor. Errors on an ambiguous match — the projection holds one row per resource, so two rows
/// with the same anchor mean the table is in a state the caller must not silently pick from.
async fn find_record_inventory_boundary_by_anchor(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<Value>> {
    let logical_name_id =
        boundary_str(record_version_boundary, &["logical_name_id"]).with_context(|| {
            format!(
                "record inventory anchor lookup for resource_id {resource_id} requires logical_name_id"
            )
        })?;
    let chain_id = boundary_str(record_version_boundary, &["chain_position", "chain_id"])
        .with_context(|| {
            format!(
                "record inventory anchor lookup for resource_id {resource_id} requires chain_position.chain_id"
            )
        })?;
    let block_number = record_version_boundary
        .get("chain_position")
        .and_then(|position| position.get("block_number"))
        .and_then(Value::as_i64)
        .with_context(|| {
            format!(
                "record inventory anchor lookup for resource_id {resource_id} requires chain_position.block_number"
            )
        })?;
    let block_hash = boundary_str(record_version_boundary, &["chain_position", "block_hash"])
        .with_context(|| {
            format!(
                "record inventory anchor lookup for resource_id {resource_id} requires chain_position.block_hash"
            )
        })?;
    let timestamp = boundary_str(record_version_boundary, &["chain_position", "timestamp"])
        .with_context(|| {
            format!(
                "record inventory anchor lookup for resource_id {resource_id} requires chain_position.timestamp"
            )
        })?;

    let boundaries = sqlx::query(&format!(
        r#"
        SELECT ric.record_version_boundary
        FROM bigname_phase.record_inventory_current ric
        {RESOURCE_CANONICALITY_JOINS}
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary ->> 'logical_name_id' = $2
          AND ric.record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (ric.record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND ric.record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND ric.record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
          {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        ORDER BY
          (ric.record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (ric.record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    ))
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to locate record_inventory_current anchor boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        crate::sql_row::get::<Value>(&row, "record_version_boundary")
    })
    .collect::<Result<Vec<Value>>>()?;

    if boundaries.len() > 1 {
        anyhow::bail!(
            "record inventory anchor lookup for resource_id {resource_id} matched multiple projection rows"
        );
    }
    Ok(boundaries.into_iter().next())
}

/// Load one record-inventory projection row only if it is eligible for the selected snapshot.
///
/// A present row with different chain-position context is reported as `stale`
/// instead of being joined into an exact-name response for another snapshot.
pub async fn load_record_inventory_current_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<RecordInventoryCurrentRow>, SnapshotSelectionError>
{
    let row = load_record_inventory_current(pool, resource_id, record_version_boundary)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    if phase_projection_target(&row)?.is_some() {
        if !record_inventory_projection_covers_selected_snapshot(
            pool,
            &row,
            selected_chain_positions,
        )
        .await?
        {
            return Err(SnapshotSelectionError::stale(
                "record_inventory_current projection does not match the selected snapshot",
            ));
        }
    } else if let Err(error) = ensure_projection_chain_positions_match(
        "record_inventory_current",
        &row.chain_positions,
        selected_chain_positions,
    ) && !record_inventory_projection_covers_selected_snapshot(
        pool,
        &row,
        selected_chain_positions,
    )
    .await?
    {
        return Err(error);
    }
    Ok(SnapshotProjectionRead::Found(row))
}

async fn record_inventory_projection_covers_selected_snapshot(
    pool: &PgPool,
    row: &RecordInventoryCurrentRow,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<bool, SnapshotSelectionError> {
    if let Some((chain_id, target_block_number, target_block_hash)) = phase_projection_target(row)?
    {
        let selected_by_chain_id = positions_by_chain_id(selected_chain_positions)?;
        let Some(selected_position) = selected_by_chain_id.get(chain_id) else {
            return Ok(false);
        };
        if selected_position.block_number < target_block_number {
            return Ok(false);
        }
        if selected_position.block_number == target_block_number {
            return Ok(selected_position.block_hash == target_block_hash);
        }
        if !phase_target_is_canonical_lineage_member(
            pool,
            chain_id,
            target_block_number,
            target_block_hash,
        )
        .await?
        {
            return Ok(false);
        }
        return record_inventory_has_newer_projection_inputs(
            pool,
            row,
            chain_id,
            target_block_number,
            selected_position.block_number,
        )
        .await
        .map(|has_newer_inputs| !has_newer_inputs);
    }

    let projected = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "record_inventory_current projection has unusable chain_positions: {}",
            error.message()
        ))
    })?;

    let projected_by_chain_id = positions_by_chain_id(&projected)?;
    let selected_by_chain_id = positions_by_chain_id(selected_chain_positions)?;

    for (chain_id, selected_position) in &selected_by_chain_id {
        let Some(projected_position) = projected_by_chain_id.get(chain_id) else {
            return Ok(false);
        };
        if selected_position.block_number < projected_position.block_number {
            return Ok(false);
        }
        if selected_position.block_number == projected_position.block_number {
            if selected_position.block_hash != projected_position.block_hash
                || selected_position.timestamp != projected_position.timestamp
            {
                return Ok(false);
            }
            continue;
        }
        if !position_is_canonical_lineage_member(pool, chain_id, projected_position).await? {
            return Ok(false);
        }
        if !position_is_canonical_lineage_member(pool, chain_id, selected_position).await? {
            return Ok(false);
        }
        if record_inventory_has_newer_projection_inputs(
            pool,
            row,
            chain_id,
            projected_position.block_number,
            selected_position.block_number,
        )
        .await?
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn phase_projection_target(
    row: &RecordInventoryCurrentRow,
) -> std::result::Result<Option<(&str, i64, &str)>, SnapshotSelectionError> {
    let Some(target_block_number) = row
        .chain_positions
        .get("target_block_number")
        .and_then(Value::as_i64)
    else {
        return Ok(None);
    };
    let target_block_hash = row
        .chain_positions
        .get("target_block_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SnapshotSelectionError::stale(
                "record_inventory_current projection has no target block hash",
            )
        })?;
    let chain_id = row
        .record_version_boundary
        .pointer("/chain_position/chain_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SnapshotSelectionError::stale(
                "record_inventory_current projection target has no chain id",
            )
        })?;
    Ok(Some((chain_id, target_block_number, target_block_hash)))
}

fn positions_by_chain_id(
    positions: &ChainPositions,
) -> std::result::Result<BTreeMap<String, &ChainPosition>, SnapshotSelectionError> {
    let mut by_chain_id = BTreeMap::new();
    for position in positions.as_map().values() {
        if by_chain_id
            .insert(position.chain_id.clone(), position)
            .is_some()
        {
            return Err(SnapshotSelectionError::stale(format!(
                "record_inventory_current projection repeats chain_id {} in chain_positions",
                position.chain_id
            )));
        }
    }
    Ok(by_chain_id)
}

async fn position_is_canonical_lineage_member(
    pool: &PgPool,
    chain_id: &str,
    position: &ChainPosition,
) -> std::result::Result<bool, SnapshotSelectionError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM bigname_phase.chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
              AND canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
        )
        "#,
    )
    .bind(chain_id)
    .bind(&position.block_hash)
    .bind(position.block_number)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check record_inventory_current chain position block {} on chain {chain_id}: {error}",
            position.block_hash
        ))
    })
}

async fn phase_target_is_canonical_lineage_member(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
) -> std::result::Result<bool, SnapshotSelectionError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM bigname_phase.chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
              AND canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
        )
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check record_inventory_current target block {block_hash} on chain {chain_id}: {error}"
        ))
    })
}

async fn record_inventory_has_newer_projection_inputs(
    pool: &PgPool,
    row: &RecordInventoryCurrentRow,
    chain_id: &str,
    projected_block_number: i64,
    selected_block_number: i64,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let logical_name_id = row
        .record_version_boundary
        .get("logical_name_id")
        .and_then(Value::as_str);

    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_events ne
            JOIN bigname_phase.chain_lineage ne_lineage
              ON ne_lineage.chain_id = ne.chain_id
             AND ne_lineage.block_hash = ne.block_hash
            WHERE ne.chain_id = $1
              AND ne.block_number > $2
              AND ne.block_number <= $3
              AND ne.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND ne_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND ne.event_kind IN (
                  'RecordChanged',
                  'RecordVersionChanged',
                  'ResolverChanged'
              )
              AND (
                  ne.resource_id = $4
                  OR ($5::TEXT IS NOT NULL AND ne.logical_name_id = $5)
              )
            LIMIT 1
        )
        "#,
    )
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .bind(row.resource_id)
    .bind(logical_name_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check record_inventory_current normalized-event invalidation for resource_id {}: {error}",
            row.resource_id
        ))
    })
}
