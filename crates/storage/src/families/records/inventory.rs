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
use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::{
    FamilyPosition, LinkSelection,
    assemble::{self, Assembly, AssemblyReads, BoundaryEvent, ServedRecord},
    facts::{ProbedEvent, ResolverClassification, load_classifications_on, probe_events},
    links::{link_key, load_family_link_selections_on},
    mirror::{MirrorSelection, evaluate_family_mirror, is_mirror_pointer},
    pointer::load_family_resource_pointers_on,
    rows::{
        PartitionKey, RecordCandidate, admitted_partitions, load_partitions, load_record_id_values,
    },
    serving::{ServingPointer, family_pointer_eligibility, serving_pointers},
};
use crate::{RecordInventoryCurrentRow, history::load_attribution_map};

/// Where a family row's `provenance.attributed_event_ids` comes from. It is the history
/// attribution, not a family fact.
#[derive(Clone, Debug)]
pub enum FamilyAttribution {
    /// Read it with `load_bounded_record_attribution` at the current publication.
    Load,
    /// Use the given ids for every resource read, as read by the caller.
    Given(BTreeSet<i64>),
}

/// A family inventory row and the storage key of its record version boundary.
#[derive(Clone, Debug)]
pub struct FamilyRecordInventory {
    pub row: RecordInventoryCurrentRow,
    pub record_version_boundary_key: String,
    /// The mirror decision, for rows read through a mirror resolver.
    pub mirrored: bool,
}

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
    Ok(
        load_family_record_inventories_on(conn, chain_id, &[resource_id], attribution)
            .await?
            .remove(&resource_id),
    )
}

/// One resource's records to select: the serving pointer after any mirror substitution, the
/// pointer before it (whose resolver the link selection reads), and the mirror decision.
struct Plan {
    pointer: ServingPointer,
    link_pointer: ServingPointer,
    mirror: Option<MirrorSelection>,
}

/// A plan with everything read for it, before its events are read back and it is assembled.
struct Selected {
    plan: Plan,
    classification: Option<ResolverClassification>,
    eligibility: (bool, Option<String>),
    links: Option<LinkSelection>,
    linked: Vec<RecordCandidate>,
    boundary: Option<Boundary>,
    cutoff: Option<FamilyPosition>,
    winners: BTreeMap<String, RecordCandidate>,
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
    super::seams::note_inventory_read(resource_ids.len());
    let mut out = BTreeMap::new();
    let resource_ids: Vec<Uuid> = resource_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if resource_ids.is_empty() {
        return Ok(out);
    }
    let pointers = load_family_resource_pointers_on(conn, chain_id, &resource_ids).await?;
    let servings = serving_pointers(conn, pointers.values()).await?;
    if servings.is_empty() {
        return Ok(out);
    }
    let served_ids: Vec<Uuid> = servings.keys().copied().collect();
    let mut attributed: BTreeMap<Uuid, BTreeSet<i64>> = match attribution {
        FamilyAttribution::Given(ids) => served_ids.iter().map(|id| (*id, ids.clone())).collect(),
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
            load_attribution_map(conn, &served_ids, bound.as_ref()).await?
        }
    };
    let resolvers: Vec<String> = servings
        .values()
        .map(|serving| serving.resolver_address.clone())
        .collect();
    let mut classifications = load_classifications_on(conn, chain_id, &resolvers).await?;
    let classification_of = |classifications: &HashMap<String, ResolverClassification>,
                             address: &str| {
        classifications.get(&address.to_ascii_lowercase()).cloned()
    };

    // The mirror walk decides a mirror pointer's substituted pointer, or its unsupported row.
    let mut plans = Vec::new();
    let mut unsupported = Vec::new();
    for serving in servings.into_values() {
        let classification = classification_of(&classifications, &serving.resolver_address);
        if !is_mirror_pointer(&serving, classification.as_ref()) {
            plans.push(Plan {
                pointer: serving.clone(),
                link_pointer: serving,
                mirror: None,
            });
            continue;
        }
        let mirror =
            evaluate_family_mirror(conn, chain_id, &serving, classification.unwrap_or_default())
                .await?;
        match mirror.substituted(&serving) {
            Some(substituted) => plans.push(Plan {
                pointer: substituted,
                link_pointer: serving,
                mirror: Some(mirror),
            }),
            None => unsupported.push((serving, mirror)),
        }
    }
    let unread: Vec<String> = plans
        .iter()
        .filter(|plan| plan.mirror.is_some())
        .map(|plan| plan.pointer.resolver_address.to_ascii_lowercase())
        .filter(|address| {
            !resolvers
                .iter()
                .any(|read| read.eq_ignore_ascii_case(address))
        })
        .collect();
    classifications.extend(load_classifications_on(conn, chain_id, &unread).await?);

    // The admitted partitions, the link selections and the linked record ids of every plan.
    let classified: Vec<(Plan, Option<ResolverClassification>)> = plans
        .into_iter()
        .map(|plan| {
            let classification =
                classification_of(&classifications, &plan.pointer.resolver_address);
            (plan, classification)
        })
        .collect();
    let partitions: Vec<Vec<PartitionKey>> = classified
        .iter()
        .map(|(plan, classification)| {
            admitted_partitions(&plan.pointer, classification.as_ref())
                .into_iter()
                .map(|(arm, identity)| (plan.pointer.resolver_address.clone(), arm, identity))
                .collect()
        })
        .collect();
    let partition_rows = load_partitions(conn, chain_id, &partitions.concat()).await?;
    let link_requests: Vec<(String, String)> = classified
        .iter()
        .map(|(plan, _)| {
            (
                plan.link_pointer.resolver_address.clone(),
                plan.link_pointer.namehash.clone(),
            )
        })
        .collect();
    let mut link_selections =
        load_family_link_selections_on(conn, chain_id, &link_requests).await?;
    let links: Vec<Option<LinkSelection>> = link_requests
        .iter()
        .map(|(resolver, namehash)| {
            link_selections
                .get_mut(&link_key(resolver, namehash))
                .and_then(Option::take)
        })
        .collect();
    let record_ids: Vec<Option<(String, String)>> = classified
        .iter()
        .zip(&links)
        .map(|((plan, _), links)| {
            links
                .as_ref()
                .and_then(|links| links.record_id.clone())
                .map(|record_id| (plan.link_pointer.resolver_address.clone(), record_id))
        })
        .collect();
    let linked_rows = load_record_id_values(
        conn,
        chain_id,
        &record_ids.iter().flatten().cloned().collect::<Vec<_>>(),
    )
    .await?;

    // The combined boundary and the latest eligible write per record key of every plan.
    let mut selected = Vec::new();
    let mut identities: Vec<String> = Vec::new();
    for ((((plan, classification), partitions), links), record_id) in classified
        .into_iter()
        .zip(partitions)
        .zip(links)
        .zip(record_ids)
    {
        let eligibility =
            family_pointer_eligibility(&plan.pointer.namespace, classification.as_ref());
        let (versions, mut candidates) = partition_rows.of(&partitions);
        let linked = record_id
            .and_then(|key| linked_rows.get(&key).cloned())
            .unwrap_or_default();
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
        // The events the family rows name only by position are read back for every plan at once.
        identities.extend(
            winners
                .values()
                .filter_map(|winner| winner.pair_sibling())
                .filter(|sibling| eligible(cutoff.as_ref(), sibling))
                .map(|sibling| sibling.event_identity.clone()),
        );
        if let Some((position, "RecordVersionChanged", _)) = &boundary {
            identities.push(position.event_identity.clone());
        }
        selected.push(Selected {
            plan,
            classification,
            eligibility,
            links,
            linked,
            boundary,
            cutoff,
            winners,
        });
    }
    let probed = probe_events(conn, &identities).await?;

    // Each plan's served records and boundary, then one read of every block stamp and collation
    // order the rows use.
    let mut held = Vec::new();
    let mut owned = Vec::new();
    for selected in selected {
        let Selected {
            plan,
            classification,
            eligibility,
            links,
            linked,
            boundary,
            cutoff,
            winners,
        } = selected;
        let served = served_records(winners, cutoff.as_ref(), &probed);
        let boundary = boundary.map(|(position, kind, id)| BoundaryEvent {
            normalized_event_id: id.or_else(|| {
                probed
                    .get(&position.event_identity)
                    .map(|event| event.normalized_event_id)
            }),
            position,
            kind,
        });
        let attributed = attributed
            .remove(&plan.pointer.resource_id)
            .unwrap_or_default();
        held.push((plan, classification, links, linked));
        owned.push((eligibility, boundary, served, attributed));
    }
    let assemblies: Vec<(&Plan, Assembly<'_>)> = held
        .iter()
        .zip(owned)
        .map(
            |(
                (plan, classification, links, linked),
                (eligibility, boundary, served, attributed),
            )| {
                (
                    plan,
                    Assembly {
                        chain_id,
                        pointer: &plan.pointer,
                        classification: classification.as_ref(),
                        eligibility,
                        boundary,
                        served,
                        links: links.as_ref(),
                        linked,
                        attributed,
                    },
                )
            },
        )
        .collect();
    let mut blocks: BTreeSet<i64> = unsupported
        .iter()
        .map(|(serving, _)| serving.block_number)
        .collect();
    let mut texts = BTreeSet::new();
    for (_, assembly) in &assemblies {
        blocks.extend(assembly.blocks());
        texts.extend(assembly.texts());
    }
    let reads = AssemblyReads::load(conn, chain_id, blocks, texts).await?;
    for (plan, assembly) in assemblies {
        let (mut row, record_version_boundary_key) = assemble::assemble(assembly, &reads)?;
        let mirrored = match &plan.mirror {
            Some(mirror) => {
                assemble::finish_mirrored(&mut row, mirror, &plan.link_pointer);
                true
            }
            None => false,
        };
        out.insert(
            plan.pointer.resource_id,
            FamilyRecordInventory {
                row,
                record_version_boundary_key,
                mirrored,
            },
        );
    }
    for (serving, mirror) in unsupported {
        let (row, record_version_boundary_key) =
            assemble::unsupported_mirror_row(chain_id, &serving, &mirror, &reads.stamps)?;
        out.insert(
            serving.resource_id,
            FamilyRecordInventory {
                row,
                record_version_boundary_key,
                mirrored: true,
            },
        );
    }
    Ok(out)
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

/// The served record of each winner: the `AddressChanged` half of an eligible coin-60 pair, read
/// back from its event, else the winner's own row.
fn served_records(
    winners: BTreeMap<String, RecordCandidate>,
    cutoff: Option<&FamilyPosition>,
    probed: &HashMap<String, ProbedEvent>,
) -> Vec<ServedRecord> {
    let mut served = Vec::new();
    for winner in winners.into_values() {
        let sibling = winner
            .pair_sibling()
            .filter(|sibling| eligible(cutoff, sibling))
            .and_then(|sibling| {
                probed
                    .get(&sibling.event_identity)
                    .map(|event| (sibling.clone(), event))
            });
        match sibling {
            Some((sibling, event)) => served.push(ServedRecord {
                record_key: winner.record_key,
                position: sibling,
                normalized_event_id: Some(event.normalized_event_id),
                source_family: event.source_family.clone(),
                stored_status: None,
                payload: event.after_state.clone(),
            }),
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
    served
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
