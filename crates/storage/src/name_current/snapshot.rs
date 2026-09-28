use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::snapshot_selection::{
    ChainPosition, ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

use super::{NameCurrentRow, load_name_current};

/// Load one exact-name projection row only if it is eligible for the selected snapshot.
///
/// Missing rows stay distinguishable from stale rows so API callers can preserve
/// the route-specific `not_found` behavior without filling stale snapshots from
/// raw facts.
///
/// Under the publication switch the row is composed from the owned key families and describes
/// the family marker's publication only, so a selected position on the name's chain other than
/// the publication (an `at` below it) is stale: no per-row position is kept to prove an older
/// read (ruling J5).
pub async fn load_name_current_for_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<NameCurrentRow>, SnapshotSelectionError> {
    if crate::publication_source::serve_from_families() {
        return family_name_for_snapshot(pool, logical_name_id, selected_chain_positions).await;
    }
    let row = load_name_current(pool, logical_name_id)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load name_current row for logical_name_id {logical_name_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    match ensure_projection_chain_positions_match(
        "name_current",
        &row.chain_positions,
        selected_chain_positions,
    ) {
        Ok(()) => {}
        Err(error) => {
            if !name_current_projection_covers_selected_snapshot(
                pool,
                &row,
                selected_chain_positions,
            )
            .await?
            {
                return Err(error);
            }
        }
    }
    Ok(SnapshotProjectionRead::Found(row))
}

async fn family_name_for_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<NameCurrentRow>, SnapshotSelectionError> {
    let row = crate::families::name::load_family_name(pool, logical_name_id)
        .await
        .map_err(|error| {
            if crate::families::name::is_publication_unavailable(&error) {
                // A rebuild in flight: the stale answer the fence gives.
                return SnapshotSelectionError::stale(format!(
                    "name data is unavailable while the families rebuild: {error}"
                ));
            }
            SnapshotSelectionError::internal(format!(
                "failed to compose the name row for logical_name_id {logical_name_id}: {error}"
            ))
        })?;
    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };
    let publication = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "composed name row has unusable chain_positions: {}",
            error.message()
        ))
    })?;
    let selected = positions_by_chain_id(selected_chain_positions)?;
    let at_publication =
        positions_by_chain_id(&publication)?
            .into_iter()
            .all(|(chain_id, published)| {
                selected.get(&chain_id).is_some_and(|position| {
                    position.block_number == published.block_number
                        && position.block_hash == published.block_hash
                })
            });
    if !at_publication {
        return Err(SnapshotSelectionError::stale(
            "name data is unavailable at the selected historical position",
        ));
    }
    Ok(SnapshotProjectionRead::Found(row))
}

async fn name_current_projection_covers_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let projected = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "name_current projection has unusable chain_positions: {}",
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
        if name_current_has_newer_projection_inputs(
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
                "name_current projection repeats chain_id {} in chain_positions",
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
            "failed to check name_current chain position block {} on chain {chain_id}: {error}",
            position.block_hash
        ))
    })
}

async fn name_current_has_newer_projection_inputs(
    pool: &PgPool,
    row: &NameCurrentRow,
    chain_id: &str,
    projected_block_number: i64,
    selected_block_number: i64,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let newer_event = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_events ne
            JOIN bigname_phase.chain_lineage ne_lineage
              ON ne_lineage.chain_id = ne.chain_id
             AND ne_lineage.block_hash = ne.block_hash
            WHERE ne.chain_id = $1
              AND ne.consumer_visibility = 'activated'
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
              AND (
                  ne.logical_name_id = $4
                  OR ($5::UUID IS NOT NULL AND ne.resource_id = $5)
              )
            LIMIT 1
        )
        "#,
    )
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .bind(&row.logical_name_id)
    .bind(row.record_serving_resource_id())
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check name_current normalized-event invalidation for {}: {error}",
            row.logical_name_id
        ))
    })?;
    if newer_event {
        return Ok(true);
    }

    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM surface_bindings sb
            JOIN bigname_phase.chain_lineage sb_lineage
              ON sb_lineage.chain_id = sb.chain_id
             AND sb_lineage.block_hash = sb.block_hash
            WHERE sb.logical_name_id = $1
              AND sb.chain_id = $2
              AND sb.block_number > $3
              AND sb.block_number <= $4
              AND sb.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
              AND sb_lineage.canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              )
            LIMIT 1
        )
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check name_current surface-binding invalidation for {}: {error}",
            row.logical_name_id
        ))
    })
}
