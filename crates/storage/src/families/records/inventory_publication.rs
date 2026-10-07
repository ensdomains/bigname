//! Inventory selection at an explicit publication, shared by readers and the Project writer.
use super::{
    assemble::{self, Assembly, AssemblyReads},
    inventory_selection::{self, Plan, Prepared, RequestedKeys, Selected},
    inventory_types::{FamilyAttribution, FamilyRecordInventory},
};
use crate::{families::lookup::LookupRecordEntry, history::load_attribution_map};
use anyhow::Result;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

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
    super::seams::note_inventory_read(resource_ids.len());
    let chain_id = publication.chain_id.as_str();
    let Prepared {
        publications: mut out,
        unsupported,
        selected,
    } = inventory_selection::select(conn, publication, resource_ids, None).await?;
    let served_ids: Vec<Uuid> = selected
        .iter()
        .map(|s| s.plan.pointer.resource_id)
        .chain(unsupported.iter().map(|(pointer, _)| pointer.resource_id))
        .collect();
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
            served,
        } = selected;
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

/// Reselect exact keys with the same structural plan, cutoff and winner rules as full inventory
/// composition. Every requested key has an explicit result; None removes a no-longer-selected key.
/// Metadata and dependencies are not partially recomposed or returned by this interface.
pub async fn compose_lookup_record_keys_at(
    conn: &mut PgConnection,
    publication: &crate::families::name::FamilyPublication,
    requested: &BTreeMap<Uuid, BTreeSet<String>>,
) -> Result<BTreeMap<Uuid, BTreeMap<String, Option<LookupRecordEntry>>>> {
    let mut out: BTreeMap<_, BTreeMap<_, _>> = requested
        .iter()
        .map(|(id, keys)| (*id, keys.iter().map(|key| (key.clone(), None)).collect()))
        .collect();
    let ids: Vec<_> = requested.keys().copied().collect();
    for ids in ids.chunks(256) {
        let keys: RequestedKeys = ids.iter().map(|id| (*id, requested[id].clone())).collect();
        let selected = inventory_selection::select(conn, publication, ids, Some(&keys)).await?;
        let started = super::seams::lookup_work_timer();
        let mut constructed = 0usize;
        for selected in selected.selected {
            let records = out
                .get_mut(&selected.plan.pointer.resource_id)
                .expect("requested resource");
            for record in selected.served {
                constructed += 1;
                *records
                    .get_mut(&record.record_key)
                    .expect("requested record key") =
                    Some(assemble::record_component(&record, &selected.plan.pointer));
            }
        }
        super::seams::note_lookup_work(|| {
            serde_json::json!({
                "stage":"key_components", "resources":ids.len(),
                "requested_resource_keys":keys.values().map(|keys| keys.len()).sum::<usize>(),
                "constructed_components":constructed,
                "construction_ms":started.map(|t| t.elapsed().as_secs_f64()*1000.0),
            })
        });
    }
    Ok(out)
}
