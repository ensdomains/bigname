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
//! the served value, event and position are the `AddressChanged` half's.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    FamilyPosition,
    assemble::{self, Assembly, BoundaryEvent, ServedRecord},
    facts::{ResolverClassification, load_classification, probe_events},
    load_family_link_selection, load_family_resource_pointer,
    mirror::{MirrorSelection, evaluate_family_mirror, is_mirror_pointer},
    rows::{RecordCandidate, admitted_partitions, load_partitions, load_record_id_values},
    serving::{ServingPointer, family_pointer_eligibility, serving_pointer},
};
use crate::{RecordInventoryCurrentRow, load_bounded_record_attribution};

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
/// half one log later its compatibility sibling.
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

/// [`load_family_record_inventory`] with the compatibility pairs it served.
pub async fn load_family_record_inventory_detail(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
    attribution: FamilyAttribution,
) -> Result<Option<FamilyRecordInventory>> {
    let Some(pointer) = load_family_resource_pointer(pool, chain_id, resource_id).await? else {
        return Ok(None);
    };
    let Some(serving) = serving_pointer(pool, &pointer).await? else {
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
            .fetch_optional(pool)
            .await?
            .flatten();
            let bound = marker.map(|block| BTreeMap::from([(chain_id.to_owned(), block)]));
            load_bounded_record_attribution(pool, &[resource_id], bound.as_ref())
                .await?
                .remove(&resource_id)
                .unwrap_or_default()
        }
    };
    let classification = load_classification(pool, chain_id, &serving.resolver_address).await?;
    if is_mirror_pointer(&serving, classification.as_ref()) {
        let mirror =
            evaluate_family_mirror(pool, chain_id, &serving, classification.unwrap_or_default())
                .await?;
        return match mirror.substituted(&serving) {
            Some(substituted) => {
                let classification =
                    load_classification(pool, chain_id, &substituted.resolver_address).await?;
                let mut inventory = select(
                    pool,
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
                unsupported_mirror(pool, chain_id, &serving, &mirror).await?,
            )),
        };
    }
    Ok(Some(
        select(
            pool,
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
    pool: &PgPool,
    chain_id: &str,
    serving: &ServingPointer,
    mirror: &MirrorSelection,
) -> Result<FamilyRecordInventory> {
    let stamps = super::facts::block_stamps(pool, chain_id, &[serving.block_number]).await?;
    let (row, record_version_boundary_key) =
        assemble::unsupported_mirror_row(chain_id, serving, mirror, &stamps)?;
    Ok(FamilyRecordInventory {
        row,
        record_version_boundary_key,
        compatibility_pairs: Vec::new(),
        mirrored: true,
    })
}

/// Select the served records of `pointer` (the serving pointer after any mirror substitution);
/// `link_pointer` is the pointer before substitution, whose resolver the link selection reads.
async fn select(
    pool: &PgPool,
    chain_id: &str,
    pointer: &ServingPointer,
    link_pointer: &ServingPointer,
    classification: Option<&ResolverClassification>,
    attributed: BTreeSet<i64>,
) -> Result<FamilyRecordInventory> {
    let eligibility = family_pointer_eligibility(&pointer.namespace, classification);
    let partitions = admitted_partitions(pointer, classification);
    let (versions, mut candidates) =
        load_partitions(pool, chain_id, &pointer.resolver_address, &partitions).await?;
    let links = load_family_link_selection(
        pool,
        chain_id,
        &link_pointer.resolver_address,
        &link_pointer.namehash,
    )
    .await?;
    let linked = match links.as_ref().and_then(|links| links.record_id.clone()) {
        Some(record_id) => {
            load_record_id_values(pool, chain_id, &link_pointer.resolver_address, &record_id)
                .await?
        }
        None => Vec::new(),
    };
    candidates.extend(linked.iter().cloned());

    // The combined version boundary: the latest partition version event or selected link.
    let mut boundaries: Vec<(FamilyPosition, &'static str, Option<i64>)> = versions
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
    let boundary = boundaries.into_iter().max_by(|a, b| a.0.cmp(&b.0));
    let cutoff = boundary
        .as_ref()
        .filter(|(_, kind, _)| *kind == "RecordVersionChanged")
        .map(|(position, _, _)| position.clone());
    let eligible = |position: &FamilyPosition| cutoff.as_ref().is_none_or(|cut| position > cut);

    // The latest eligible write per record key across the union.
    let mut winners: BTreeMap<String, RecordCandidate> = BTreeMap::new();
    for candidate in candidates
        .into_iter()
        .filter(|candidate| eligible(&candidate.position))
    {
        match winners.get(&candidate.record_key) {
            Some(current) if current.position >= candidate.position => {}
            _ => {
                winners.insert(candidate.record_key.clone(), candidate);
            }
        }
    }

    // Read back the events the family rows name only by position.
    let mut identities: Vec<String> = winners
        .values()
        .filter_map(|winner| winner.pair_sibling())
        .filter(|sibling| eligible(sibling))
        .map(|sibling| sibling.event_identity.clone())
        .collect();
    if let Some((position, "RecordVersionChanged", _)) = &boundary {
        identities.push(position.event_identity.clone());
    }
    let probed = probe_events(pool, &identities).await?;

    let mut pairs = Vec::new();
    let mut served = Vec::new();
    for winner in winners.into_values() {
        let sibling = winner
            .pair_sibling()
            .filter(|sibling| eligible(sibling))
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
        pool,
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
