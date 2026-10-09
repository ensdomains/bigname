//! The record inventory a resolver holds for one node, read by (resolver, node) instead of through
//! a resource's resolver pointer: `GET /v1/resolvers/{chain_id}/{address}/records`.
//!
//! The record families keep every admitted resolver write for any node, whatever the registry
//! points at (`crates/project/src/families/records.rs`). The resource-keyed read applies the
//! pointer only to choose the resolver and the partitions. This read takes the resolver from the
//! caller and stands in a pointer for it: the queried node and name, and the pointer family of the
//! resolver's own family, so the partitions are the ones the resolver's writes are kept in. The
//! selection after that is `inventory_selection::select` for one pointer, with the same mirror
//! substitution, cutoff and winner rules and the same assembly. The two must change together.
use std::collections::BTreeSet;

use anyhow::Result;
use sqlx::PgConnection;
use uuid::Uuid;

use super::cutoff::{Boundary, combined_boundary, eligible, latest_eligible, served_records};
use crate::families::records::{
    assemble::{self, Assembly, AssemblyReads, BoundaryEvent},
    facts::{ResolverClassification, load_classifications_at, probe_events},
    inventory_types::FamilyRecordInventory,
    links::{link_key, load_family_link_selections_on},
    mirror::{evaluate_family_mirror_at, is_mirror_pointer},
    rows::{
        PartitionKey, admitted_partitions, load_partitions, load_record_id_values, select_values,
    },
    serving::{ServingPointer, family_pointer_eligibility},
};
use crate::{RecordInventoryCurrentRow, SnapshotSelectionError, families::name::FamilyPublication};

/// The resolver and the node a resolver-anchored read asks about. `resolver_address` and `node`
/// are compared lower-cased, as the record families store them.
struct ResolverNode<'a> {
    resolver_address: &'a str,
    namespace: &'a str,
    logical_name_id: &'a str,
    node: &'a str,
}

impl FamilyRecordInventory {
    /// The inventory `GET /v1/resolvers/{chain_id}/{address}/records` serves: what
    /// `resolver_address` holds for `node`, named `logical_name_id` in `namespace`, read on
    /// `conn` at the family publication of `chain_id`. Only that publication is servable, so any
    /// other selected position is stale, as on the name records route. With `keys`, only those
    /// record keys are read and assembled. `None` when the resolver has no classification.
    #[allow(clippy::too_many_arguments)]
    pub async fn load_resolver_node_for_snapshot(
        conn: &mut PgConnection,
        chain_id: &str,
        resolver_address: &str,
        namespace: &str,
        logical_name_id: &str,
        node: &str,
        keys: Option<&BTreeSet<String>>,
        selected: &crate::ChainPositions,
    ) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
        let internal = |error: anyhow::Error| {
            if crate::families::name::is_publication_unavailable(&error) {
                return SnapshotSelectionError::stale(format!(
                    "record data is unavailable while the families rebuild: {error}"
                ));
            }
            SnapshotSelectionError::internal(format!(
                "failed to assemble the record inventory of resolver {resolver_address} for \
                 {logical_name_id}: {error}"
            ))
        };
        let publication = crate::families::name::servable_publication(conn, chain_id)
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
        let key = ResolverNode {
            resolver_address,
            namespace,
            logical_name_id,
            node,
        };
        let inventory = node_inventory_at(conn, &publication, &key, keys)
            .await
            .map_err(internal)?;
        Ok(inventory.map(|inventory| {
            let mut row = inventory.row;
            row.chain_positions = serde_json::json!({
                "target_block_number": publication.block_number,
                "target_block_hash": publication.block_hash,
            });
            row
        }))
    }

    /// The family inventory `resolver_address` holds for `node`, read on `conn`, which the
    /// caller holds in one snapshot, at the current family publication.
    pub async fn load_resolver_node_on(
        conn: &mut PgConnection,
        chain_id: &str,
        resolver_address: &str,
        namespace: &str,
        logical_name_id: &str,
        node: &str,
    ) -> Result<Option<Self>> {
        let publication = crate::families::name::servable_publication(conn, chain_id).await?;
        let key = ResolverNode {
            resolver_address,
            namespace,
            logical_name_id,
            node,
        };
        node_inventory_at(conn, &publication, &key, None).await
    }
}

/// The pointer family whose partitions hold a resolver family's writes (`admitted_partitions`).
fn pointer_family(classification: &ResolverClassification) -> &'static str {
    match classification.field("source_family") {
        Some("ens_v1_resolver_l1") => "ens_v1_registry_l1",
        Some("basenames_base_resolver") => "basenames_base_registry",
        Some("ens_v2_resolver_l1") => "ens_v2_registry_l1",
        _ => "",
    }
}

async fn node_inventory_at(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    key: &ResolverNode<'_>,
    keys: Option<&BTreeSet<String>>,
) -> Result<Option<FamilyRecordInventory>> {
    let chain_id = publication.chain_id.as_str();
    let resolver = key.resolver_address.to_ascii_lowercase();
    let Some(classification) = load_classifications_at(
        conn,
        chain_id,
        std::slice::from_ref(&resolver),
        Some(publication),
    )
    .await?
    .remove(&resolver) else {
        return Ok(None);
    };
    // No pointer event stands behind this pointer: assembly falls back to the publication block
    // where the resource-keyed read uses the pointer's.
    let serving = ServingPointer {
        resource_id: Uuid::nil(),
        logical_name_id: key.logical_name_id.to_owned(),
        namespace: key.namespace.to_owned(),
        source_family: pointer_family(&classification).to_owned(),
        namehash: key.node.to_ascii_lowercase(),
        resolver_address: resolver,
        pointer_event_id: None,
        block_number: publication.block_number,
    };

    // The mirror walk decides a mirror resolver's substituted pointer, or its unsupported row.
    let (pointer, mirror, classification) = if is_mirror_pointer(&serving, Some(&classification)) {
        let mirror =
            evaluate_family_mirror_at(conn, chain_id, &serving, classification, publication)
                .await?;
        let Some(substituted) = mirror.substituted(&serving) else {
            let reads = AssemblyReads::load(
                conn,
                chain_id,
                BTreeSet::from([serving.block_number]),
                BTreeSet::new(),
            )
            .await?;
            let (row, record_version_boundary_key) =
                assemble::unsupported_mirror_row(chain_id, &serving, &mirror, &reads, false)?;
            return Ok(Some(FamilyRecordInventory {
                row,
                record_version_boundary_key,
                mirrored: true,
            }));
        };
        let address = substituted.resolver_address.to_ascii_lowercase();
        let classification = load_classifications_at(
            conn,
            chain_id,
            std::slice::from_ref(&address),
            Some(publication),
        )
        .await?
        .remove(&address);
        (substituted, Some(mirror), classification)
    } else {
        (serving.clone(), None, Some(classification))
    };

    // The admitted partitions, the link selection and the linked record id.
    let partitions: Vec<PartitionKey> = admitted_partitions(&pointer, classification.as_ref())
        .into_iter()
        .map(|(arm, identity)| (pointer.resolver_address.clone(), arm, identity))
        .collect();
    let partition_keys = keys.map(|keys| {
        partitions
            .iter()
            .flat_map(|partition| keys.iter().map(|key| (partition.clone(), key.clone())))
            .collect::<BTreeSet<_>>()
    });
    let partition_rows = load_partitions(
        conn,
        chain_id,
        &partitions,
        publication.block_number,
        partition_keys.as_ref(),
    )
    .await?;
    let link_request = (serving.resolver_address.clone(), serving.namehash.clone());
    let links = load_family_link_selections_on(conn, chain_id, std::slice::from_ref(&link_request))
        .await?
        .remove(&link_key(&link_request.0, &link_request.1))
        .flatten();
    let record_id = links
        .as_ref()
        .and_then(|links| links.record_id.clone())
        .map(|record_id| (serving.resolver_address.clone(), record_id));
    let linked_keys = keys.map(|keys| {
        record_id
            .iter()
            .flat_map(|record_id| keys.iter().map(|key| (record_id.clone(), key.clone())))
            .collect::<BTreeSet<_>>()
    });
    let linked_rows = load_record_id_values(
        conn,
        chain_id,
        &record_id.iter().cloned().collect::<Vec<_>>(),
        linked_keys.as_ref(),
    )
    .await?;

    // The combined boundary and the latest eligible write per record key.
    let eligibility = family_pointer_eligibility(&pointer.namespace, classification.as_ref());
    let (versions, mut candidates) = partition_rows.of(&partitions, keys);
    let linked = select_values(
        record_id.as_ref().and_then(|key| linked_rows.get(key)),
        keys,
    );
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

    let assembly = Assembly {
        chain_id,
        pointer: &pointer,
        classification: classification.as_ref(),
        eligibility,
        boundary,
        served,
        links: links.as_ref(),
        linked: &linked,
        attributed: None,
    };
    let reads = AssemblyReads::load(
        conn,
        chain_id,
        assembly.blocks().into_iter().collect(),
        assembly.texts().collect(),
    )
    .await?;
    let (mut row, record_version_boundary_key, _) = assemble::assemble(assembly, &reads)?;
    if let Some(mirror) = &mirror {
        assemble::finish_mirrored(&mut row, mirror, &serving);
    }
    Ok(Some(FamilyRecordInventory {
        row,
        record_version_boundary_key,
        mirrored: mirror.is_some(),
    }))
}
