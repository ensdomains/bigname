//! Inventory selection at an explicit publication, shared by readers and the Project writer.
use super::{
    FamilyPosition, LinkSelection,
    assemble::{self, Assembly, AssemblyReads, BoundaryEvent},
    facts::{ResolverClassification, load_classifications_at, probe_events},
    inventory::{FamilyAttribution, FamilyRecordInventory},
    links::{link_key, load_family_link_selections_on},
    mirror::{MirrorSelection, evaluate_family_mirror_at, is_mirror_pointer},
    pointer::load_family_resource_pointers_on,
    rows::{
        PartitionKey, RecordCandidate, admitted_partitions, load_partitions, load_record_id_values,
    },
    serving::{ServingPointer, family_pointer_eligibility, serving_pointers},
};
use crate::history::load_attribution_map;
use anyhow::Result;
use cutoff::{Boundary, combined_boundary, eligible, latest_eligible, served_records};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

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

/// Shared lookup publication, including absent inventories and negative dependency keys.
/// It uses the caller's transaction and supplied boundary without reading a serving marker.
pub async fn compose_lookup_inventories_at(
    conn: &mut PgConnection,
    publication: &crate::families::name::FamilyPublication,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, crate::families::lookup::LookupInventoryPublication>> {
    let mut out = BTreeMap::new();
    for ids in resource_ids.chunks(256) {
        out.extend(compose_inventories_at(conn, publication, ids, FamilyAttribution::Omit).await?);
    }
    Ok(out)
}

pub(super) async fn compose_inventories_at(
    conn: &mut PgConnection,
    publication: &crate::families::name::FamilyPublication,
    resource_ids: &[Uuid],
    attribution: FamilyAttribution,
) -> Result<BTreeMap<Uuid, crate::families::lookup::LookupInventoryPublication>> {
    use crate::families::lookup::{
        LookupInventoryDependency as Dependency, LookupInventoryPublication,
    };
    let chain_id = publication.chain_id.as_str();
    super::seams::note_inventory_read(resource_ids.len());
    let mut out: BTreeMap<_, _> = resource_ids
        .iter()
        .map(|id| (*id, LookupInventoryPublication::absent(*id)))
        .collect();
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
    let mut attributed: Option<BTreeMap<Uuid, BTreeSet<i64>>> = match attribution {
        FamilyAttribution::Omit => None,
        FamilyAttribution::Given(ids) => {
            Some(served_ids.iter().map(|id| (*id, ids.clone())).collect())
        }
        FamilyAttribution::Load => {
            let bound = BTreeMap::from([(chain_id.to_owned(), publication.block_number)]);
            Some(load_attribution_map(conn, &served_ids, Some(&bound)).await?)
        }
    };
    let resolvers: Vec<String> = servings
        .values()
        .map(|serving| serving.resolver_address.clone())
        .collect();
    let mut classifications =
        load_classifications_at(conn, chain_id, &resolvers, Some(publication.block_number)).await?;
    let classification_of = |classifications: &HashMap<String, ResolverClassification>,
                             address: &str| {
        classifications.get(&address.to_ascii_lowercase()).cloned()
    };

    // The mirror walk decides a mirror pointer's substituted pointer, or its unsupported row.
    let mut plans = Vec::new();
    let mut unsupported = Vec::new();
    for serving in servings.into_values() {
        let dependencies = &mut out
            .get_mut(&serving.resource_id)
            .expect("requested resource")
            .dependencies;
        dependencies.insert(Dependency::Identity {
            logical_name_id: serving.logical_name_id.clone(),
        });
        dependencies.insert(Dependency::Classification {
            resolver_address: serving.resolver_address.to_ascii_lowercase(),
        });
        let classification = classification_of(&classifications, &serving.resolver_address);
        if !is_mirror_pointer(&serving, classification.as_ref()) {
            plans.push(Plan {
                pointer: serving.clone(),
                link_pointer: serving,
                mirror: None,
            });
            continue;
        }
        let mirror = evaluate_family_mirror_at(
            conn,
            chain_id,
            &serving,
            classification.unwrap_or_default(),
            Some(publication.block_number),
        )
        .await?;
        dependencies.extend(mirror.consulted_nodes.iter().map(|(namespace, node)| {
            Dependency::RegistryNode {
                namespace: namespace.clone(),
                node: node.clone(),
            }
        }));
        if let Some(nearest) = &mirror.nearest {
            dependencies.insert(Dependency::Classification {
                resolver_address: nearest.mirrored_resolver_address.to_ascii_lowercase(),
            });
        }
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
    classifications.extend(
        load_classifications_at(conn, chain_id, &unread, Some(publication.block_number)).await?,
    );

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
    let partition_rows = load_partitions(
        conn,
        chain_id,
        &partitions.concat(),
        publication.block_number,
    )
    .await?;
    let link_requests: Vec<(String, String)> = classified
        .iter()
        .map(|(plan, _)| {
            (
                plan.link_pointer.resolver_address.clone(),
                plan.link_pointer.namehash.clone(),
            )
        })
        .collect();
    // Distinct resources can share a (resolver, node), for example one name's ENSv1 and ENSv2
    // resources at the same resolver, so every plan gets its own copy of the selection.
    let link_selections = load_family_link_selections_on(conn, chain_id, &link_requests).await?;
    let links: Vec<Option<LinkSelection>> = link_requests
        .iter()
        .map(|(resolver, namehash)| {
            link_selections
                .get(&link_key(resolver, namehash))
                .cloned()
                .flatten()
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

    for (((plan, _), partitions), record_id) in classified.iter().zip(&partitions).zip(&record_ids)
    {
        let dependencies = &mut out
            .get_mut(&plan.pointer.resource_id)
            .expect("requested resource")
            .dependencies;
        dependencies.extend(partitions.iter().map(|(resolver, arm, identity)| {
            Dependency::Partition {
                resolver_address: resolver.to_ascii_lowercase(),
                arm: (*arm).to_owned(),
                arm_identity: identity.clone(),
            }
        }));
        for node in [
            &plan.link_pointer.namehash,
            &super::DEFAULT_RECORD_NODE.to_owned(),
        ] {
            dependencies.insert(Dependency::Link {
                resolver_address: plan.link_pointer.resolver_address.to_ascii_lowercase(),
                node: node.clone(),
            });
        }
        if let Some((resolver, record_id)) = record_id {
            dependencies.insert(Dependency::RecordId {
                resolver_address: resolver.to_ascii_lowercase(),
                record_id: record_id.clone(),
            });
        }
    }

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
            .as_mut()
            .map(|map| map.remove(&plan.pointer.resource_id).unwrap_or_default());
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
        let (mut row, record_version_boundary_key, records) = assemble::assemble(assembly, &reads)?;
        let mirrored = match &plan.mirror {
            Some(mirror) => {
                assemble::finish_mirrored(&mut row, mirror, &plan.link_pointer);
                true
            }
            None => false,
        };
        let result = out
            .get_mut(&plan.pointer.resource_id)
            .expect("requested resource");
        result.inventory = Some(FamilyRecordInventory {
            row,
            record_version_boundary_key,
            mirrored,
        });
        result.records = records;
    }
    let attributing = attributed.is_some();
    for (serving, mirror) in unsupported {
        let (row, record_version_boundary_key) =
            assemble::unsupported_mirror_row(chain_id, &serving, &mirror, &reads, attributing)?;
        out.get_mut(&serving.resource_id)
            .expect("requested resource")
            .inventory = Some(FamilyRecordInventory {
            row,
            record_version_boundary_key,
            mirrored: true,
        });
    }
    Ok(out)
}

#[path = "inventory_cutoff.rs"]
mod cutoff;
