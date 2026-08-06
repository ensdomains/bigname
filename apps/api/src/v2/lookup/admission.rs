use bigname_storage::{
    ChainPositions, IdentityNameRecordRow, NameCurrentRow, ReverseIdentityRecordRow,
    SelectedSnapshot,
};

use crate::v2::{V2Error, V2Result};

pub(super) fn require_name_records_at_served_head(
    records: &[IdentityNameRecordRow],
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    for record in records {
        require_record_at_served_head(record, selected_snapshot)?;
    }
    Ok(())
}

pub(super) fn require_reverse_records_at_served_head(
    records: &[ReverseIdentityRecordRow],
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    for record in records {
        require_record_at_served_head(&record.name_record, selected_snapshot)?;
        if let Some(target) = &record.primary_chain_positions {
            require_flat_target_at_or_before_served_head(
                target,
                &record.name_record.row.namespace,
                selected_snapshot,
            )?;
        }
    }
    Ok(())
}

fn require_record_at_served_head(
    record: &IdentityNameRecordRow,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    require_name_projection_at_served_head(
        &record.row.chain_positions,
        &record.row.namespace,
        selected_snapshot,
    )?;
    if let Some(inventory) = record.record_inventory_current.as_ref() {
        require_flat_target_at_or_before_served_head(
            &inventory.chain_positions,
            &record.row.namespace,
            selected_snapshot,
        )?;
    }
    for relation in &record.relations {
        require_flat_target_at_or_before_served_head(
            &relation.chain_positions,
            &record.row.namespace,
            selected_snapshot,
        )?;
    }
    Ok(())
}

pub(crate) fn require_name_current_at_served_head(
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    require_name_projection_at_served_head(&row.chain_positions, &row.namespace, selected_snapshot)
}

pub(crate) fn require_name_projection_at_served_head(
    chain_positions: &serde_json::Value,
    namespace: &str,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let projected = ChainPositions::from_value(chain_positions)
        .map_err(|_| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    let slot = match namespace {
        "ens" if projected.get("ethereum-sepolia").is_some() => "ethereum-sepolia",
        "ens" => "ethereum",
        "basenames" => "base",
        _ => {
            return Err(V2Error::stale(
                "lookup data is unavailable at the selected snapshot",
            ));
        }
    };
    let selected = selected_snapshot
        .chain_positions
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    let projected = projected
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    if projected.chain_id != selected.chain_id
        || projected.block_number > selected.block_number
        || (projected.block_number == selected.block_number
            && projected.block_hash != selected.block_hash)
    {
        return Err(V2Error::stale(
            "lookup data is unavailable at the selected snapshot",
        ));
    }
    Ok(())
}

pub(crate) fn require_flat_target_at_or_before_served_head(
    chain_positions: &serde_json::Value,
    namespace: &str,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let number = chain_positions
        .get("target_block_number")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    let hash = chain_positions
        .get("target_block_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    let expected_chain_id = match namespace {
        "basenames" => "base-mainnet",
        "ens"
            if selected_snapshot
                .chain_positions
                .get("ethereum-sepolia")
                .is_some() =>
        {
            "ethereum-sepolia"
        }
        "ens" => "ethereum-mainnet",
        _ => {
            return Err(V2Error::stale(
                "lookup data is unavailable at the selected snapshot",
            ));
        }
    };
    let selected = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == expected_chain_id)
        .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    if number > selected.block_number
        || (number == selected.block_number && hash != selected.block_hash)
    {
        return Err(V2Error::stale(
            "lookup data is unavailable at the selected snapshot",
        ));
    }
    Ok(())
}
