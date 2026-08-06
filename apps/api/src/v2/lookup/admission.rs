use bigname_storage::{
    ChainPositions, IdentityNameRecordRow, ReverseIdentityRecordRow, SelectedSnapshot,
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
                "lookup primary name",
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
    let projected = ChainPositions::from_value(&record.row.chain_positions)
        .map_err(|_| V2Error::stale("lookup projection has unusable chain positions"))?;
    let slot = match record.row.namespace.as_str() {
        "ens" if projected.get("ethereum-sepolia").is_some() => "ethereum-sepolia",
        "ens" => "ethereum",
        "basenames" => "base",
        _ => {
            return Err(V2Error::stale(
                "lookup projection has no served chain position",
            ));
        }
    };
    let selected = selected_snapshot
        .chain_positions
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup projection is outside the served snapshot"))?;
    let projected = projected
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup projection has no served chain position"))?;
    if projected.chain_id != selected.chain_id
        || projected.block_number > selected.block_number
        || (projected.block_number == selected.block_number
            && projected.block_hash != selected.block_hash)
    {
        return Err(V2Error::stale(
            "lookup phase projection is outside the served project publication",
        ));
    }
    if let Some(inventory) = record.record_inventory_current.as_ref() {
        require_flat_target_at_or_before_served_head(
            "lookup record inventory",
            &inventory.chain_positions,
            &record.row.namespace,
            selected_snapshot,
        )?;
    }
    for relation in &record.relations {
        require_flat_target_at_or_before_served_head(
            "lookup address-name relation",
            &relation.chain_positions,
            &record.row.namespace,
            selected_snapshot,
        )?;
    }
    Ok(())
}

pub(super) fn require_flat_target_at_or_before_served_head(
    projection_family: &str,
    chain_positions: &serde_json::Value,
    namespace: &str,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let number = chain_positions
        .get("target_block_number")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} has no target block number")))?;
    let hash = chain_positions
        .get("target_block_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} has no target block hash")))?;
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
            return Err(V2Error::stale(format!(
                "{projection_family} has no served chain"
            )));
        }
    };
    let selected = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == expected_chain_id)
        .ok_or_else(|| V2Error::stale(format!("{projection_family} is outside the snapshot")))?;
    if number > selected.block_number
        || (number == selected.block_number && hash != selected.block_hash)
    {
        return Err(V2Error::stale(format!(
            "{projection_family} is outside the served project publication"
        )));
    }
    Ok(())
}
