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
