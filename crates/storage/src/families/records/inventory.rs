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

use super::{
    FamilyPosition,
    assemble::{self, Assembly, BoundaryEvent, ServedRecord},
    facts::{ResolverClassification, load_classification_on as load_classification, probe_events},
    links::load_family_link_selection_on as load_family_link_selection,
    mirror::{MirrorSelection, evaluate_family_mirror, is_mirror_pointer},
    pointer::load_family_resource_pointer_on,
    rows::{RecordCandidate, admitted_partitions, load_partitions, load_record_id_values},
    serving::{ServingPointer, family_pointer_eligibility, serving_pointer},
};
use crate::{RecordInventoryCurrentRow, history::load_attribution_map};

/// Where a family row's `provenance.attributed_event_ids` comes from. It is the history
/// attribution, not a family fact.
#[derive(Clone, Debug)]
pub enum FamilyAttribution {
    /// Read it with `load_bounded_record_attribution` at the current publication.
    Load,
    /// Use the given ids, read by the caller for many resources at once.
    Given(BTreeSet<i64>),
}

/// A coin-60 pair a row serves: the `AddressChanged` half is the value event, the `AddrChanged`
/// half one log later its compatibility sibling, emitted in that order by one `setAddr`
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59-L62 @ ens_v1@91c966f).
/// The row's provenance lists only the value event, as today's does; the sibling is carried here
/// for the design's provenance, which names both (step 7).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityPair {
    pub record_key: String,
    pub value_event_id: Option<i64>,
    pub value_position: FamilyPosition,
    pub sibling_event_id: Option<i64>,
    pub sibling_position: FamilyPosition,
}

/// A family inventory row with what the comparison checks beside it.
#[derive(Clone, Debug)]
pub struct FamilyRecordInventory {
    pub row: RecordInventoryCurrentRow,
    pub record_version_boundary_key: String,
    pub compatibility_pairs: Vec<CompatibilityPair>,
    /// The mirror decision, for rows read through a mirror resolver.
    pub mirrored: bool,
}

/// The record inventory row today's reader serves for `resource_id`, built from the families.
/// `None` when the resource has no pointer or its current pointer is a clear, which today's
/// record-serving reads answer the same way.
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
/// (`resolution_record_inventory_lookup_key`) the served supported readback uses: name detail's
/// verified source and the records diagnostic.
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
        FamilyAttribution::Load,
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
    pool: &PgPool,
    rows: &[&crate::NameCurrentRow],
) -> Result<Vec<Option<u64>>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let mut counts = Vec::with_capacity(rows.len());
    let mut published = BTreeSet::new();
    for row in rows {
        let Some((resource_id, boundary)) =
            crate::resolution_record_inventory_lookup_key_any_chain(row)
        else {
            counts.push(None);
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
        if published.insert(chain_id.clone()) {
            crate::families::name::servable_publication(&mut snapshot, &chain_id).await?;
        }
        let inventory = load_family_record_inventory_detail_on(
            &mut snapshot,
            &chain_id,
            resource_id,
            FamilyAttribution::Given(BTreeSet::new()),
        )
        .await?;
        counts.push(
            inventory
                .filter(|inventory| inventory.record_version_boundary_key == wanted_key)
                .map(|inventory| {
                    inventory
                        .row
                        .selectors
                        .as_array()
                        .map_or(0, |selectors| selectors.len() as u64)
                }),
        );
    }
    snapshot.commit().await?;
    Ok(counts)
}

/// [`load_family_record_inventory_detail`] on `conn`, which the caller holds in one snapshot.
pub async fn load_family_record_inventory_detail_on(
    conn: &mut PgConnection,
    chain_id: &str,
    resource_id: Uuid,
    attribution: FamilyAttribution,
) -> Result<Option<FamilyRecordInventory>> {
    let Some(pointer) = load_family_resource_pointer_on(conn, chain_id, resource_id).await? else {
        return Ok(None);
    };
    let Some(serving) = serving_pointer(conn, &pointer).await? else {
        return Ok(None);
    };
    let attributed = match attribution {
        FamilyAttribution::Given(ids) => ids,
        FamilyAttribution::Load => {
            // At the block the families stand at, as Project publishes it at its target.
            let marker: Option<i64> = sqlx::query_scalar(
                "SELECT current_block_number FROM bigname_phase.project_family_marker
                 WHERE chain_id = $1",
            )
            .bind(chain_id)
            .fetch_optional(&mut *conn)
            .await?
            .flatten();
            let bound = marker.map(|block| BTreeMap::from([(chain_id.to_owned(), block)]));
            load_attribution_map(conn, &[resource_id], bound.as_ref())
                .await?
                .remove(&resource_id)
                .unwrap_or_default()
        }
    };
    let classification = load_classification(conn, chain_id, &serving.resolver_address).await?;
    if is_mirror_pointer(&serving, classification.as_ref()) {
        let mirror =
            evaluate_family_mirror(conn, chain_id, &serving, classification.unwrap_or_default())
                .await?;
        return match mirror.substituted(&serving) {
            Some(substituted) => {
                let classification =
                    load_classification(conn, chain_id, &substituted.resolver_address).await?;
                let mut inventory = select(
                    conn,
                    chain_id,
                    &substituted,
                    &serving,
                    classification.as_ref(),
                    attributed,
                )
                .await?;
                assemble::finish_mirrored(&mut inventory.row, &mirror, &serving);
                inventory.mirrored = true;
                Ok(Some(inventory))
            }
            None => Ok(Some(
                unsupported_mirror(conn, chain_id, &serving, &mirror).await?,
            )),
        };
    }
    Ok(Some(
        select(
            conn,
            chain_id,
            &serving,
            &serving,
            classification.as_ref(),
            attributed,
        )
        .await?,
    ))
}

async fn unsupported_mirror(
    conn: &mut PgConnection,
    chain_id: &str,
    serving: &ServingPointer,
    mirror: &MirrorSelection,
) -> Result<FamilyRecordInventory> {
    let stamps = super::facts::block_stamps(conn, chain_id, &[serving.block_number]).await?;
    let (row, record_version_boundary_key) =
        assemble::unsupported_mirror_row(chain_id, serving, mirror, &stamps)?;
    Ok(FamilyRecordInventory {
        row,
        record_version_boundary_key,
        compatibility_pairs: Vec::new(),
        mirrored: true,
    })
}

/// A version boundary candidate: its position, its event kind and, for a link, its event id.
type Boundary = (FamilyPosition, &'static str, Option<i64>);

/// The combined version boundary, the latest partition version event or selected link in the
/// canonical event order, and the cutoff it sets: only an ordinary `RecordVersionChanged` cuts
/// off the writes before it.
fn combined_boundary(boundaries: Vec<Boundary>) -> (Option<Boundary>, Option<FamilyPosition>) {
    let boundary = boundaries.into_iter().max_by(|a, b| a.0.cmp(&b.0));
    let cutoff = boundary
        .as_ref()
        .filter(|(_, kind, _)| *kind == "RecordVersionChanged")
        .map(|(position, _, _)| position.clone());
    (boundary, cutoff)
}

/// Whether a write at `position` is after the cutoff, when there is one.
fn eligible(cutoff: Option<&FamilyPosition>, position: &FamilyPosition) -> bool {
    cutoff.is_none_or(|cut| position > cut)
}

/// The latest eligible write per record key across the union, in the canonical event order.
fn latest_eligible(
    candidates: Vec<RecordCandidate>,
    cutoff: Option<&FamilyPosition>,
) -> BTreeMap<String, RecordCandidate> {
    let mut winners: BTreeMap<String, RecordCandidate> = BTreeMap::new();
    for candidate in candidates
        .into_iter()
        .filter(|candidate| eligible(cutoff, &candidate.position))
    {
        match winners.get(&candidate.record_key) {
            Some(current) if current.position >= candidate.position => {}
            _ => {
                winners.insert(candidate.record_key.clone(), candidate);
            }
        }
    }
    winners
}

/// Select the served records of `pointer` (the serving pointer after any mirror substitution);
/// `link_pointer` is the pointer before substitution, whose resolver the link selection reads.
async fn select(
    conn: &mut PgConnection,
    chain_id: &str,
    pointer: &ServingPointer,
    link_pointer: &ServingPointer,
    classification: Option<&ResolverClassification>,
    attributed: BTreeSet<i64>,
) -> Result<FamilyRecordInventory> {
    let eligibility = family_pointer_eligibility(&pointer.namespace, classification);
    let partitions = admitted_partitions(pointer, classification);
    let (versions, mut candidates) =
        load_partitions(conn, chain_id, &pointer.resolver_address, &partitions).await?;
    let links = load_family_link_selection(
        conn,
        chain_id,
        &link_pointer.resolver_address,
        &link_pointer.namehash,
    )
    .await?;
    let linked = match links.as_ref().and_then(|links| links.record_id.clone()) {
        Some(record_id) => {
            load_record_id_values(conn, chain_id, &link_pointer.resolver_address, &record_id)
                .await?
        }
        None => Vec::new(),
    };
    candidates.extend(linked.iter().cloned());

    let mut boundaries: Vec<Boundary> = versions
        .into_iter()
        .map(|position| (position, "RecordVersionChanged", None))
        .collect();
    if let Some(links) = &links {
        for link in links.contributing_links() {
            boundaries.push((
                link.position.clone(),
                "ResolverRecordLinked",
                link.normalized_event_id,
            ));
        }
    }
    let (boundary, cutoff) = combined_boundary(boundaries);
    let winners = latest_eligible(candidates, cutoff.as_ref());

    // Read back the events the family rows name only by position.
    let mut identities: Vec<String> = winners
        .values()
        .filter_map(|winner| winner.pair_sibling())
        .filter(|sibling| eligible(cutoff.as_ref(), sibling))
        .map(|sibling| sibling.event_identity.clone())
        .collect();
    if let Some((position, "RecordVersionChanged", _)) = &boundary {
        identities.push(position.event_identity.clone());
    }
    let probed = probe_events(conn, &identities).await?;

    let mut pairs = Vec::new();
    let mut served = Vec::new();
    for winner in winners.into_values() {
        let sibling = winner
            .pair_sibling()
            .filter(|sibling| eligible(cutoff.as_ref(), sibling))
            .and_then(|sibling| {
                probed
                    .get(&sibling.event_identity)
                    .map(|event| (sibling.clone(), event))
            });
        match sibling {
            Some((sibling, event)) => {
                pairs.push(CompatibilityPair {
                    record_key: winner.record_key.clone(),
                    value_event_id: Some(event.normalized_event_id),
                    value_position: sibling.clone(),
                    sibling_event_id: winner.normalized_event_id,
                    sibling_position: winner.position.clone(),
                });
                served.push(ServedRecord {
                    record_key: winner.record_key,
                    position: sibling.clone(),
                    normalized_event_id: Some(event.normalized_event_id),
                    source_family: event.source_family.clone(),
                    stored_status: None,
                    payload: event.after_state.clone(),
                });
            }
            None => served.push(ServedRecord {
                record_key: winner.record_key,
                position: winner.position,
                normalized_event_id: winner.normalized_event_id,
                source_family: winner.source_family,
                stored_status: Some(winner.status),
                payload: winner.payload,
            }),
        }
    }
    let boundary = boundary.map(|(position, kind, id)| BoundaryEvent {
        normalized_event_id: id.or_else(|| {
            probed
                .get(&position.event_identity)
                .map(|event| event.normalized_event_id)
        }),
        position,
        kind,
    });
    let (row, record_version_boundary_key) = assemble::assemble(
        conn,
        Assembly {
            chain_id,
            pointer,
            classification,
            eligibility,
            boundary,
            served,
            links: links.as_ref(),
            linked: &linked,
            attributed,
        },
    )
    .await?;
    Ok(FamilyRecordInventory {
        row,
        record_version_boundary_key,
        compatibility_pairs: pairs,
        mirrored: false,
    })
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
