use std::collections::BTreeMap;

use crate::v2::{V2Error, V2Result};
use bigname_storage::{ChainPositions, NameCurrentListRow, SelectedSnapshot};
use serde_json::Value;

pub(super) fn require_phase_name_snapshot(
    row: &NameCurrentListRow,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let projected = ChainPositions::from_value(&row.row.chain_positions)
        .map_err(|_| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    let slot = match row.row.namespace.as_str() {
        "ens" if projected.get("ethereum-sepolia").is_some() => "ethereum-sepolia",
        "ens" => "ethereum",
        "basenames" => "base",
        _ => {
            return Err(V2Error::stale(
                "resolver data is unavailable at the selected snapshot",
            ));
        }
    };
    let projected = projected
        .get(slot)
        .ok_or_else(|| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    let selected = selected_snapshot
        .chain_positions
        .get(slot)
        .ok_or_else(|| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    if projected.chain_id == selected.chain_id
        && projected.block_number <= selected.block_number
        && (projected.block_number != selected.block_number
            || projected.block_hash == selected.block_hash)
    {
        Ok(())
    } else {
        Err(V2Error::stale(
            "resolver data is unavailable at the selected snapshot",
        ))
    }
}

pub(super) fn require_phase_target_snapshot(
    chain_positions: &Value,
    chain_id: &str,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let number = chain_positions
        .get("target_block_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    let hash = chain_positions
        .get("target_block_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    let selected = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == chain_id)
        .ok_or_else(|| V2Error::stale("resolver data is unavailable at the selected snapshot"))?;
    if number > selected.block_number
        || (number == selected.block_number && hash != selected.block_hash)
    {
        return Err(V2Error::stale(
            "resolver data is unavailable at the selected snapshot",
        ));
    }
    Ok(())
}

/// Recheck a generation that was readable when this request was admitted. A missing generation
/// here means that readability changed during the request, including a publication at a newer
/// block/hash. Only unpinned reads can retry against that new publication; an explicit `at`
/// keeps its selected-position error. SQL failures are never treated as publication movement.
pub(super) async fn revalidate_project_generations(
    pool: &sqlx::PgPool,
    selected: &SelectedSnapshot,
    expected: &BTreeMap<String, String>,
    explicit_at: bool,
    pinned_generation_changed_message: &str,
) -> V2Result<()> {
    #[cfg(test)]
    super::generation_test_hooks::run(pool).await?;
    let current =
        crate::v2::support::load_selected_project_generations_for_read(pool, selected, true)
            .await
            .map_err(|_| V2Error::internal_error("failed to validate lookup data"))?;
    match current.as_ref() {
        Some(current) if current == expected => Ok(()),
        _ if !explicit_at => Err(crate::v2::collection_snapshot::changed_during_read()),
        None => Err(V2Error::stale(
            "served data is not available at the selected snapshot",
        )),
        Some(_) => Err(V2Error::stale(pinned_generation_changed_message)),
    }
}
