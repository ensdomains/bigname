//! An ENSv1 name serves the resolver its ENSv1 registry record holds, and that resolver's records,
//! read at request time. The composed row and its record inventory take the resolver from the
//! name's resource pointer, which can lag the node's registry pointer
//! (`FamilyRecordInventory::ens_v1_registry_resolver`). Name detail and the records route read
//! through the registry pointer whenever the two name different resolvers, a registry clear
//! included. Rows where the two agree are served as composed.
use bigname_storage::{
    ChainPositions, NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot,
    SnapshotSelectionError, families::records::FamilyRecordInventory,
};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use crate::v2::name_record::row_has_current_registration;

fn row_chain_id(row: &NameCurrentRow) -> Option<String> {
    ChainPositions::from_value(&row.chain_positions)
        .ok()?
        .as_map()
        .values()
        .map(|position| position.chain_id.clone())
        .next()
}

/// Replaces `row`'s served resolver with the registry's when the two differ.
pub(crate) async fn serve_ens_v1_registry_resolver(
    conn: &mut PgConnection,
    row: &mut NameCurrentRow,
) -> anyhow::Result<()> {
    if !row_has_current_registration(row) {
        return Ok(());
    }
    let served = row
        .declared_summary
        .pointer("/resolver/address")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let Some(registry) =
        FamilyRecordInventory::ens_v1_registry_resolver(conn, row, served.as_deref()).await?
    else {
        return Ok(());
    };
    let chain_id = registry.as_ref().and_then(|_| row_chain_id(row));
    let latest_event_kind = row
        .declared_summary
        .pointer("/resolver/latest_event_kind")
        .cloned()
        .unwrap_or(Value::Null);
    row.declared_summary["resolver"] = json!({
        "chain_id": chain_id,
        "address": registry,
        "latest_event_kind": latest_event_kind,
    });
    Ok(())
}

/// `composed`, the record inventory of `row`'s serving resource, or the inventory the registry's
/// resolver holds for the node when the registry names another resolver. A registry clear serves
/// no inventory, as a cleared resource pointer does.
pub(crate) async fn ens_v1_registry_record_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    composed: Option<RecordInventoryCurrentRow>,
) -> Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    if !row_has_current_registration(row) {
        return Ok(composed);
    }
    let internal = |error: anyhow::Error| {
        SnapshotSelectionError::internal(format!(
            "failed to read the ENSv1 registry resolver of {}: {error}",
            row.logical_name_id
        ))
    };
    let served = composed
        .as_ref()
        .and_then(|inventory| inventory.provenance.get("resolver_address"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut snapshot = bigname_storage::begin_read_snapshot(pool)
        .await
        .map_err(internal)?;
    let registry =
        FamilyRecordInventory::ens_v1_registry_resolver(&mut snapshot, row, served.as_deref())
            .await
            .map_err(internal)?;
    let inventory = match registry {
        None => composed,
        Some(None) => None,
        Some(Some(resolver)) => {
            let chain_id = row_chain_id(row).ok_or_else(|| {
                internal(anyhow::anyhow!("composed name row carries no chain position"))
            })?;
            let resource_id = composed
                .as_ref()
                .map(|inventory| inventory.resource_id)
                .or_else(|| row.record_serving_resource_id());
            FamilyRecordInventory::load_resolver_node_for_snapshot(
                &mut snapshot,
                &chain_id,
                &resolver,
                &row.namespace,
                &row.logical_name_id,
                &row.namehash,
                None,
                &selected_snapshot.chain_positions,
            )
            .await?
            .map(|mut inventory| {
                if let Some(resource_id) = resource_id {
                    inventory.resource_id = resource_id;
                }
                inventory
            })
        }
    };
    snapshot
        .commit()
        .await
        .map_err(|error| internal(error.into()))?;
    Ok(inventory)
}
