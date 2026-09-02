use bigname_storage::{
    ChainPositions, IdentityNameRecordRow, NameCurrentRow, ReverseIdentityRecordRow,
    SelectedSnapshot,
};

use crate::v2::{V2Error, V2Result, name_record::identity_row_has_current_registration};

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
        if record.primary_name.as_ref().is_some_and(|primary| {
            primary.claim_status == bigname_storage::PrimaryNameClaimStatus::Success
        }) && record.primary_chain_positions.is_none()
        {
            return Err(V2Error::stale(
                "lookup data is unavailable at the selected snapshot",
            ));
        }
        if let Some(target) = &record.primary_chain_positions {
            require_name_projection_at_served_head(
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
    if identity_row_has_current_registration(&record.row)
        && let Some(inventory) = record.record_inventory_current.as_ref()
    {
        require_name_projection_at_served_head(
            &inventory.chain_positions,
            &record.row.namespace,
            selected_snapshot,
        )?;
    }
    for relation in &record.relations {
        require_name_projection_at_served_head(
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
    let slot = match namespace {
        "ens" => "ethereum",
        "basenames" => "base",
        _ => {
            return Err(V2Error::stale(
                "lookup data is unavailable at the selected snapshot",
            ));
        }
    };
    if let Some(target_block_number) = chain_positions
        .get("target_block_number")
        .and_then(serde_json::Value::as_i64)
    {
        let slot = match namespace {
            "ens"
                if selected_snapshot
                    .chain_positions
                    .get("ethereum-sepolia")
                    .is_some() =>
            {
                "ethereum-sepolia"
            }
            _ => slot,
        };
        let target_block_hash = chain_positions
            .get("target_block_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
        let selected = selected_snapshot
            .chain_positions
            .get(slot)
            .ok_or_else(|| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
        if target_block_number > selected.block_number
            || (target_block_number == selected.block_number
                && target_block_hash != selected.block_hash)
        {
            return Err(V2Error::stale(
                "lookup data is unavailable at the selected snapshot",
            ));
        }
        return Ok(());
    }

    let projected = ChainPositions::from_value(chain_positions)
        .map_err(|_| V2Error::stale("lookup data is unavailable at the selected snapshot"))?;
    let slot = match namespace {
        "ens" if projected.get("ethereum-sepolia").is_some() => "ethereum-sepolia",
        _ => slot,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bigname_storage::{ChainPosition, ChainPositions, SelectedSnapshot, SnapshotConsistency};
    use serde_json::json;

    use super::require_name_projection_at_served_head;

    #[test]
    fn flat_ens_target_uses_selected_sepolia_slot() {
        let selected_snapshot = SelectedSnapshot {
            chain_positions: ChainPositions::new(BTreeMap::from([(
                "ethereum-sepolia".to_owned(),
                ChainPosition {
                    slot: "ethereum-sepolia".to_owned(),
                    chain_id: "ethereum-sepolia".to_owned(),
                    block_number: 100,
                    block_hash: "0xabc".to_owned(),
                    timestamp: bigname_storage::parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
                        .unwrap(),
                },
            )])),
            consistency: SnapshotConsistency::Head,
        };
        let flat_target = json!({
            "target_block_number": 100,
            "target_block_hash": "0xabc",
        });

        require_name_projection_at_served_head(&flat_target, "ens", &selected_snapshot).unwrap();
    }
}
