//! Inventory selection at an explicit publication, shared by readers and the Project writer.
use super::inventory_cutoff::{
    Boundary, combined_boundary, eligible, latest_eligible, served_records,
};
use super::{
    LinkSelection,
    assemble::{BoundaryEvent, ServedRecord},
    facts::{ResolverClassification, load_classifications_at, probe_events},
    links::{link_key, load_family_link_selections_on},
    mirror::{MirrorSelection, evaluate_family_mirror_at, is_mirror_pointer},
    pointer::load_family_resource_pointers_on,
    rows::{
        PartitionKey, RecordCandidate, admitted_partitions, load_partitions, load_record_id_values,
        select_values,
    },
    serving::{ServingPointer, family_pointer_eligibility, serving_pointers},
};
use anyhow::Result;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

use crate::families::{lookup::LookupInventoryPublication, name::FamilyPublication};
/// One resource's records to select: the serving pointer after any mirror substitution, the
/// pointer before it (whose resolver the link selection reads), and the mirror decision.
pub(super) struct Plan {
    pub(super) pointer: ServingPointer,
    pub(super) link_pointer: ServingPointer,
    pub(super) mirror: Option<MirrorSelection>,
}

/// A plan with everything read for it, before its events are read back and it is assembled.
pub(super) struct Selected {
    pub(super) plan: Plan,
    pub(super) classification: Option<ResolverClassification>,
    pub(super) eligibility: (bool, Option<String>),
    pub(super) links: Option<LinkSelection>,
    pub(super) linked: Vec<RecordCandidate>,
    pub(super) boundary: Option<BoundaryEvent>,
    pub(super) served: Vec<ServedRecord>,
}

pub(super) struct Prepared {
    pub(super) publications: BTreeMap<Uuid, LookupInventoryPublication>,
    pub(super) unsupported: Vec<(ServingPointer, MirrorSelection)>,
    pub(super) selected: Vec<Selected>,
}

/// None selects all keys. Some selects only the exact keys for each resource.
pub(super) type RequestedKeys = BTreeMap<Uuid, BTreeSet<String>>;

pub(super) async fn select(
    conn: &mut PgConnection,
    publication: &crate::families::name::FamilyPublication,
    resource_ids: &[Uuid],
    keys: Option<&RequestedKeys>,
) -> Result<Prepared> {
    use crate::families::lookup::{
        LookupInventoryDependency as Dependency, LookupInventoryPublication,
    };
    let started = super::seams::lookup_work_timer();
    let chain_id = publication.chain_id.as_str();
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
        return Ok(Prepared {
            publications: out,
            unsupported: Vec::new(),
            selected: Vec::new(),
        });
    }
    let pointers = load_family_resource_pointers_on(conn, chain_id, &resource_ids).await?;
    let servings = serving_pointers(conn, pointers.values()).await?;
    if servings.is_empty() {
        return Ok(Prepared {
            publications: out,
            unsupported: Vec::new(),
            selected: Vec::new(),
        });
    }
    let resolvers: Vec<String> = servings
        .values()
        .map(|serving| serving.resolver_address.clone())
        .collect();
    let mut classifications =
        load_classifications_at(conn, chain_id, &resolvers, Some(publication)).await?;
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
            publication,
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
    classifications
        .extend(load_classifications_at(conn, chain_id, &unread, Some(publication)).await?);

    let classified: Vec<(Plan, Option<ResolverClassification>)> = plans
        .into_iter()
        .map(|plan| {
            let classification =
                classification_of(&classifications, &plan.pointer.resolver_address);
            (plan, classification)
        })
        .collect();
    let note = WorkNote {
        resources: resource_ids.len(),
        started,
    };
    let (selected, reads) = select_plans(
        conn,
        publication,
        classified,
        |plan| keys.map(|keys| &keys[&plan.pointer.resource_id]),
        keys.is_some(),
        Some(note),
    )
    .await?;
    for (selected, reads) in selected.iter().zip(&reads) {
        let plan = &selected.plan;
        let dependencies = &mut out
            .get_mut(&plan.pointer.resource_id)
            .expect("requested resource")
            .dependencies;
        dependencies.extend(reads.partitions.iter().map(|(resolver, arm, identity)| {
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
        if let Some((resolver, record_id)) = &reads.record_id {
            dependencies.insert(Dependency::RecordId {
                resolver_address: resolver.to_ascii_lowercase(),
                record_id: record_id.clone(),
            });
        }
    }
    Ok(Prepared {
        publications: out,
        unsupported,
        selected,
    })
}

/// What [`select_plans`] read for one plan beside its [`Selected`]: the admitted partitions and
/// the linked record id, which the resource-keyed selection records as lookup dependencies.
pub(super) struct PlanReads {
    pub(super) partitions: Vec<PartitionKey>,
    pub(super) record_id: Option<(String, String)>,
}

/// The lookup work a resource-keyed selection notes: how many resources it selects for and when
/// it started.
pub(super) struct WorkNote {
    pub(super) resources: usize,
    pub(super) started: Option<std::time::Instant>,
}

/// The records of each plan once its pointer is chosen, the same for every reader. The admitted
/// partitions, the link selections and the linked record ids of every plan are read at once.
/// Then each plan gets its combined version boundary, the cutoff it sets, and the latest eligible
/// write per record key. `requested` gives a plan's exact record keys when `key_only`, and the
/// loads read only those keys. The resource-keyed selection ([`select`]) and the
/// resolver-anchored read (`node_inventory.rs`) both select through it.
pub(super) async fn select_plans<'k>(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    classified: Vec<(Plan, Option<ResolverClassification>)>,
    requested: impl Fn(&Plan) -> Option<&'k BTreeSet<String>>,
    key_only: bool,
    note: Option<WorkNote>,
) -> Result<(Vec<Selected>, Vec<PlanReads>)> {
    let chain_id = publication.chain_id.as_str();
    // The admitted partitions, the link selections and the linked record ids of every plan.
    let partitions: Vec<Vec<PartitionKey>> = classified
        .iter()
        .map(|(plan, classification)| {
            admitted_partitions(&plan.pointer, classification.as_ref())
                .into_iter()
                .map(|(arm, identity)| (plan.pointer.resolver_address.clone(), arm, identity))
                .collect()
        })
        .collect();
    let partition_keys = key_only.then(|| {
        classified
            .iter()
            .zip(&partitions)
            .flat_map(|((plan, _), partitions)| {
                partitions.iter().flat_map(|partition| {
                    requested(plan)
                        .into_iter()
                        .flatten()
                        .map(|key| (partition.clone(), key.clone()))
                })
            })
            .collect::<BTreeSet<_>>()
    });
    let partition_started = super::seams::lookup_work_timer();
    let partition_rows = load_partitions(
        conn,
        chain_id,
        &partitions.concat(),
        publication.block_number,
        partition_keys.as_ref(),
    )
    .await?;
    let partition_elapsed = partition_started.map(|t| t.elapsed().as_secs_f64() * 1000.0);
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
    let linked_keys = key_only.then(|| {
        classified
            .iter()
            .zip(&record_ids)
            .flat_map(|((plan, _), record_id)| {
                record_id.iter().flat_map(|record_id| {
                    requested(plan)
                        .into_iter()
                        .flatten()
                        .map(|key| (record_id.clone(), key.clone()))
                })
            })
            .collect::<BTreeSet<_>>()
    });
    let linked_started = super::seams::lookup_work_timer();
    let linked_rows = load_record_id_values(
        conn,
        chain_id,
        &record_ids.iter().flatten().cloned().collect::<Vec<_>>(),
        linked_keys.as_ref(),
    )
    .await?;

    let linked_elapsed = linked_started.map(|t| t.elapsed().as_secs_f64() * 1000.0);
    if let Some(note) = &note {
        super::seams::note_lookup_work(|| {
            serde_json::json!({
                "stage":"structural_selection", "resources":note.resources, "key_only":key_only,
                "elapsed_ms_including_source_loads":note.started.map(|t| t.elapsed().as_secs_f64()*1000.0),
                "partition_load_ms":partition_elapsed, "record_id_load_ms":linked_elapsed,
            })
        });
    }
    let winner_started = super::seams::lookup_work_timer();
    // The combined boundary and the latest eligible write per record key of every plan.
    let mut selected = Vec::new();
    let mut reads = Vec::new();
    let mut identities: Vec<String> = Vec::new();
    for ((((plan, classification), partitions), links), record_id) in classified
        .into_iter()
        .zip(partitions)
        .zip(links)
        .zip(record_ids)
    {
        let eligibility =
            family_pointer_eligibility(&plan.pointer.namespace, classification.as_ref());
        let requested = requested(&plan).filter(|_| key_only);
        let (versions, mut candidates) = partition_rows.of(&partitions, requested);
        let linked = select_values(
            record_id.as_ref().and_then(|key| linked_rows.get(key)),
            requested,
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
        reads.push(PlanReads {
            partitions,
            record_id,
        });
        selected.push((
            plan,
            classification,
            eligibility,
            links,
            linked,
            boundary,
            cutoff,
            winners,
        ));
    }
    let probed = probe_events(conn, &identities).await?;

    let selected: Vec<_> = selected
        .into_iter()
        .map(
            |(plan, classification, eligibility, links, linked, boundary, cutoff, winners)| {
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
                Selected {
                    plan,
                    classification,
                    eligibility,
                    links,
                    linked,
                    boundary,
                    served,
                }
            },
        )
        .collect();
    if let Some(note) = &note {
        super::seams::note_lookup_work(|| {
            serde_json::json!({
                "stage":"winner_selection", "resources":note.resources, "key_only":key_only,
                "selected_components":selected.iter().map(|s| s.served.len()).sum::<usize>(),
                "elapsed_ms_including_sibling_probe":winner_started.map(|t| t.elapsed().as_secs_f64()*1000.0),
            })
        });
    }
    Ok((selected, reads))
}
