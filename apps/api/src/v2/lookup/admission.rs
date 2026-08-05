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
    let projected = projected
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup projection has no served chain position"))?;
    let selected = selected_snapshot
        .chain_positions
        .get(slot)
        .ok_or_else(|| V2Error::stale("lookup projection is outside the served snapshot"))?;
    if projected.chain_id != selected.chain_id
        || projected.block_number != selected.block_number
        || projected.block_hash != selected.block_hash
    {
        return Err(V2Error::stale(
            "lookup projection does not match the served phase head",
        ));
    }
    Ok(())
}
