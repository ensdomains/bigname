//! The record inventory of one resource, assembled from the owned key families
//! (record_inventory.rs, `BUILD_RECORD_INVENTORY`, and record_inventory/mirror.rs).
//!
//! The read takes the resource's F5 pointer, applies the ENSv1 mirror substitution over F4,
//! evaluates the outer gate, admits the F6 partitions by the four attribution arms and the F7
//! values through the link selection, takes the combined version boundary as the latest of the
//! admitted partitions' version events and the selected link events, and then the latest eligible
//! write per record key across the union. No boundary and a link boundary admit every write; only
//! an ordinary `RecordVersionChanged` cuts off the writes before it. When the latest eligible write
//! of a key is the `AddrChanged` half of a coin-60 pair whose `AddressChanged` half is eligible too,
//! the served value, event and position are the `AddressChanged` half's. An ENSv1 `setAddr` for
//! coin 60 emits `AddressChanged` and then `AddrChanged` in one call, which makes the pair.
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59-L62 @ ens_v1@91c966f)
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::inventory_publication::compose_inventories_at;
pub use super::inventory_types::{FamilyAttribution, FamilyRecordInventory};
use crate::RecordInventoryCurrentRow;

/// The record inventory row served for `resource_id`, built from the families. `None` when the
/// resource has no pointer or its current pointer is a clear.
pub async fn load_family_record_inventory(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<RecordInventoryCurrentRow>> {
    Ok(
        load_family_record_inventory_detail(pool, chain_id, resource_id, FamilyAttribution::Load)
            .await?
            .map(|inventory| inventory.row),
    )
}

/// [`load_family_record_inventory`] with the compatibility pairs it served, read in one
/// snapshot (`families::read_snapshot`).
pub async fn load_family_record_inventory_detail(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
    attribution: FamilyAttribution,
) -> Result<Option<FamilyRecordInventory>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let inventory =
        load_family_record_inventory_detail_on(&mut snapshot, chain_id, resource_id, attribution)
            .await?;
    snapshot.commit().await?;
    Ok(inventory)
}

/// The record inventory `GET /v1/names/{name}/records` reads: the family inventory of the resource `row` serves records through, when the
/// row has a record-inventory lookup key (`resolution_record_inventory_lookup_key_any_chain`), at
/// the family marker's publication. The composed name row describes that publication only, so a
/// selected position other than it is stale, as is a chain whose marker is not
/// servable. `None` when the row has no lookup key or the resource no serving pointer.
pub async fn load_family_record_inventory_for_snapshot(
    pool: &PgPool,
    row: &crate::NameCurrentRow,
    selected: &crate::ChainPositions,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, crate::SnapshotSelectionError> {
    let key = crate::resolution_record_inventory_lookup_key_any_chain(row);
    family_record_inventory_for_key(pool, row, key.map(|(resource, _)| resource), selected).await
}

/// [`load_family_record_inventory_for_snapshot`] with the mainnet-profile lookup key
/// (`resolution_record_inventory_lookup_key`) the served supported readback uses: the records
/// diagnostic.
pub async fn load_family_supported_record_inventory_for_snapshot(
    pool: &PgPool,
    row: &crate::NameCurrentRow,
    selected: &crate::ChainPositions,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, crate::SnapshotSelectionError> {
    let key = crate::resolution_record_inventory_lookup_key(row);
    family_record_inventory_for_key(pool, row, key.map(|(resource, _)| resource), selected).await
}

async fn family_record_inventory_for_key(
    pool: &PgPool,
    row: &crate::NameCurrentRow,
    resource_id: Option<Uuid>,
    selected: &crate::ChainPositions,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, crate::SnapshotSelectionError> {
    use crate::SnapshotSelectionError;
    let internal = |error: anyhow::Error| {
        if crate::families::name::is_publication_unavailable(&error) {
            return SnapshotSelectionError::stale(format!(
                "record data is unavailable while the families rebuild: {error}"
            ));
        }
        SnapshotSelectionError::internal(format!(
            "failed to assemble the family record inventory of {}: {error}",
            row.logical_name_id
        ))
    };
    let Some(resource_id) = resource_id else {
        return Ok(None);
    };
    let mut snapshot = crate::families::read_snapshot(pool)
        .await
        .map_err(internal)?;
    let chain_id: String =
        sqlx::query_scalar("SELECT chain_id FROM bigname_phase.resources WHERE resource_id = $1")
            .bind(resource_id)
            .fetch_one(&mut *snapshot)
            .await
            .map_err(|error| internal(error.into()))?;
    let publication = crate::families::name::servable_publication(&mut snapshot, &chain_id)
        .await
        .map_err(internal)?;
    let at_publication = selected.as_map().values().any(|position| {
        position.chain_id == chain_id
            && position.block_number == publication.block_number
            && position.block_hash == publication.block_hash
    });
    if !at_publication {
        return Err(SnapshotSelectionError::stale(
            "record data is unavailable at the selected historical position",
        ));
    }
    let inventory = load_family_record_inventory_detail_on(
        &mut snapshot,
        &chain_id,
        resource_id,
        FamilyAttribution::Omit,
    )
    .await
    .map_err(internal)?;
    snapshot
        .commit()
        .await
        .map_err(|error| internal(error.into()))?;
    Ok(inventory.map(|inventory| {
        let mut row = inventory.row;
        row.chain_positions = serde_json::json!({
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        });
        row
    }))
}

/// The public record selector count of each composed name row
/// (`count_record_inventory_selectors_by_lookup_keys` over the families): the selectors of the
/// family inventory of the resource the row serves records through, `None` when the row has no
/// lookup key or the resource no inventory. Read in one snapshot at each chain's publication.
pub async fn load_family_record_counts(
    db: impl Into<crate::ReadDb<'_>>,
    rows: &[&crate::NameCurrentRow],
) -> Result<Vec<Option<u64>>> {
    let mut snapshot = db.into().snapshot().await?;
    let mut wanted = Vec::with_capacity(rows.len());
    let mut by_chain: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
    for row in rows {
        let Some((resource_id, boundary)) =
            crate::resolution_record_inventory_lookup_key_any_chain(row)
        else {
            wanted.push(None);
            continue;
        };
        // The composed topology carries the selected resolver version (or link) boundary.
        // The generic lookup key has no event identity and is only the unversioned fallback.
        let boundary = row
            .declared_summary
            .get("topology")
            .map(crate::projected_resolution_boundaries_from_topology)
            .transpose()?
            .map_or(boundary, |(_, record)| record);
        let wanted_key = crate::record_version_boundary_storage_key(&boundary, resource_id)?;
        let chain_id = crate::ChainPositions::from_value(&row.chain_positions)
            .ok()
            .and_then(|positions| {
                positions
                    .as_map()
                    .values()
                    .map(|position| position.chain_id.clone())
                    .next()
            })
            .ok_or_else(|| anyhow::anyhow!("composed name row carries no chain position"))?;
        if !by_chain.contains_key(&chain_id) {
            crate::families::name::servable_publication(&mut snapshot, &chain_id).await?;
        }
        by_chain
            .entry(chain_id.clone())
            .or_default()
            .insert(resource_id);
        wanted.push(Some((chain_id, resource_id, wanted_key)));
    }
    let mut inventories = BTreeMap::new();
    for (chain_id, resources) in by_chain {
        let resources: Vec<Uuid> = resources.into_iter().collect();
        for (resource_id, inventory) in load_family_record_inventories_on(
            &mut snapshot,
            &chain_id,
            &resources,
            FamilyAttribution::Given(BTreeSet::new()),
        )
        .await?
        {
            inventories.insert((chain_id.clone(), resource_id), inventory);
        }
    }
    let counts = wanted
        .into_iter()
        .map(|wanted| {
            let (chain_id, resource_id, wanted_key) = wanted?;
            inventories
                .get(&(chain_id, resource_id))
                .filter(|inventory| inventory.record_version_boundary_key == wanted_key)
                .map(|inventory| {
                    inventory
                        .row
                        .selectors
                        .as_array()
                        .map_or(0, |selectors| selectors.len() as u64)
                })
        })
        .collect();
    snapshot.close().await?;
    Ok(counts)
}

/// [`load_family_record_inventory_detail`] on `conn`, which the caller holds in one snapshot.
pub async fn load_family_record_inventory_detail_on(
    conn: &mut PgConnection,
    chain_id: &str,
    resource_id: Uuid,
    attribution: FamilyAttribution,
) -> Result<Option<FamilyRecordInventory>> {
    Ok(
        load_family_record_inventories_on(conn, chain_id, &[resource_id], attribution)
            .await?
            .remove(&resource_id),
    )
}

/// [`load_family_record_inventory_detail_on`] for every resource in `resource_ids` on
/// `chain_id`, keyed by resource; a resource with no pointer or whose current pointer is a clear
/// has no entry. The read runs a fixed number of statements whatever the resource count, plus a
/// few per mirror pointer for its registry walk.
pub async fn load_family_record_inventories_on(
    conn: &mut PgConnection,
    chain_id: &str,
    resource_ids: &[Uuid],
    attribution: FamilyAttribution,
) -> Result<BTreeMap<Uuid, FamilyRecordInventory>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let publication = crate::families::name::servable_publication(conn, chain_id).await?;
    Ok(
        compose_inventories_at(conn, &publication, resource_ids, attribution)
            .await?
            .into_iter()
            .filter_map(|(id, value)| Some((id, value.inventory?)))
            .collect(),
    )
}
