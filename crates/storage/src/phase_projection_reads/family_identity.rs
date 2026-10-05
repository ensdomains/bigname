//! Batch lookup's identity and inventory views of the family publication.
use crate::{
    IdentityNameCurrentRow, IdentityNameRecordRow, IdentityRecordInventoryRow,
    families::{
        name::{CoverageShape, load_composed},
        records::{FamilyAttribution, load_family_record_inventories_on, name_relations_on},
    },
};
use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) async fn load(
    pool: &PgPool,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let out = load_on(&mut snapshot, ids, include_inventory).await?;
    snapshot.commit().await?;
    Ok(out)
}

pub(crate) async fn load_on(
    conn: &mut PgConnection,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let names = load_composed(&mut *conn, ids, CoverageShape::Plain).await?;
    let mut relations = name_relations_on(&mut *conn, &names).await?;
    // Every inventory the rows serve, read once per chain for all of them. A resource shared by
    // several rows is read on the chain of, and stamped with the positions of, the first.
    let mut first: BTreeMap<Uuid, (String, serde_json::Value)> = BTreeMap::new();
    if include_inventory {
        for row in names.values() {
            let chain = row.provenance["chain_id"]
                .as_str()
                .context("composed name has no chain")?;
            if let Some(resource) = row.record_serving_resource_id() {
                first
                    .entry(resource)
                    .or_insert_with(|| (chain.to_owned(), row.chain_positions.clone()));
            }
        }
    }
    let mut by_chain: BTreeMap<&str, Vec<Uuid>> = BTreeMap::new();
    for (resource, (chain, _)) in &first {
        by_chain.entry(chain.as_str()).or_default().push(*resource);
    }
    let mut inventories: BTreeMap<Uuid, IdentityRecordInventoryRow> = BTreeMap::new();
    for (chain, resources) in by_chain {
        let loaded = load_family_record_inventories_on(
            &mut *conn,
            chain,
            &resources,
            FamilyAttribution::Omit,
        )
        .await?;
        for (resource, inventory) in loaded {
            let publication_positions = first[&resource].1.clone();
            let row = inventory.row;
            inventories.insert(
                resource,
                IdentityRecordInventoryRow {
                    resource_id: resource,
                    record_version_boundary_key: inventory.record_version_boundary_key,
                    support_status: if row.coverage["status"] == "projected" {
                        "supported"
                    } else {
                        "unsupported"
                    }
                    .to_owned(),
                    unsupported_reason: row.coverage["unsupported_reason"]
                        .as_str()
                        .map(str::to_owned),
                    selectors: row.selectors,
                    entries: row.entries,
                    provenance: row.provenance,
                    unsupported_families: row.unsupported_families,
                    chain_positions: publication_positions,
                    last_recomputed_at: row.last_recomputed_at,
                },
            );
        }
    }
    let mut out = Vec::new();
    for (id, row) in names {
        let normalized = super::names::normalize_phase_name(&id, &row.normalized_name)?;
        let labelhash = super::names::phase_labelhash(&normalized);
        let labelhash_count = i32::try_from(normalized.labels.len()).ok();
        row.provenance["chain_id"]
            .as_str()
            .context("composed name has no chain")?;
        let inventory = row
            .serving_resource_id
            .or(row.resource_id)
            .filter(|_| include_inventory)
            .and_then(|resource| inventories.get(&resource).cloned());
        out.push(IdentityNameRecordRow {
            row: IdentityNameCurrentRow {
                logical_name_id: id.clone(),
                namespace: row.namespace,
                // The composed row's display form: a surface without raw bytes serves its
                // rendered name there, which re-normalizing would beautify.
                canonical_display_name: row.canonical_display_name,
                normalized_name: normalized.normalized_name,
                namehash: row.namehash,
                labelhash,
                labelhash_count,
                surface_binding_id: row.surface_binding_id,
                resource_id: row.resource_id,
                serving_resource_id: row.serving_resource_id,
                binding_kind: row.binding_kind,
                record_inventory_boundary_key: None,
                coverage: row.coverage,
                declared_summary: row.declared_summary,
                provenance: row.provenance,
                chain_positions: row.chain_positions,
                last_recomputed_at: row.last_recomputed_at,
            },
            record_inventory_current: inventory,
            relations: relations.remove(&id).unwrap_or_default(),
        });
    }
    Ok(out)
}
