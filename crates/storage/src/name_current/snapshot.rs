use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::snapshot_selection::{
    ChainPosition, ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
};

use super::NameCurrentRow;

/// Load one exact-name projection row only if it is eligible for the selected snapshot.
///
/// Missing rows stay distinguishable from stale rows so API callers can preserve
/// the route-specific `not_found` behavior without filling stale snapshots from
/// raw facts.
///
/// The row is composed from the owned key families and describes
/// the family marker's publication only, so a selected position on the name's chain other than
/// the publication (an `at` below it) is stale: no per-row position is kept to prove an older
/// read.
pub async fn load_name_current_for_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<NameCurrentRow>, SnapshotSelectionError> {
    family_name_for_snapshot(pool, logical_name_id, selected_chain_positions).await
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
        // No row composed: the name may be one a rebuild has yet to reach, so the selected
        // chains' markers must be servable before the name is called absent.
        let chain_ids: Vec<String> = selected_chain_positions
            .as_map()
            .values()
            .map(|position| position.chain_id.clone())
            .collect();
        let publications = crate::families::name::ensure_family_publications(pool, &chain_ids)
            .await
            .map_err(|error| {
                if crate::families::name::is_publication_unavailable(&error) {
                    return SnapshotSelectionError::stale(format!(
                        "name data is unavailable while the families rebuild: {error}"
                    ));
                }
                SnapshotSelectionError::internal(format!(
                    "failed to read the family markers for logical_name_id {logical_name_id}: \
                     {error}"
                ))
            })?;
        // As for a composed row: only the publication itself can say the name is absent.
        let selected = positions_by_chain_id(selected_chain_positions)?;
        let at_publication = publications.iter().all(|publication| {
            selected.get(&publication.chain_id).is_some_and(|position| {
                position.block_number == publication.block_number
                    && position.block_hash == publication.block_hash
            })
        });
        if !at_publication {
            return Err(SnapshotSelectionError::stale(
                "name data is unavailable at the selected historical position",
            ));
        }
        return Ok(SnapshotProjectionRead::NotFound);
    };
    let publication = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "composed name row has unusable chain_positions: {}",
            error.message()
        ))
    })?;
    let selected = positions_by_chain_id(selected_chain_positions)?;
    let authoritative_chain = row.provenance["chain_id"].as_str().ok_or_else(|| {
        SnapshotSelectionError::internal("composed name row has no authoritative chain")
    })?;
    let published = positions_by_chain_id(&publication)?;
    // Indexed Basenames reads select Base alone. The composed topology may also carry an
    // Ethereum execution position, which becomes a read dependency only when selected for
    // verified resolution. Always require the authoritative publication, and check every
    // auxiliary position that the request did select.
    let at_publication = selected.contains_key(authoritative_chain)
        && published.contains_key(authoritative_chain)
        && published.iter().all(|(chain_id, published)| {
            selected.get(chain_id).is_none_or(|position| {
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
