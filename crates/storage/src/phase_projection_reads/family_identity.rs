//! Batch lookup's identity and inventory views of the family publication.
use crate::{
    IdentityNameCurrentRow, IdentityNameRecordRow, IdentityRecordInventoryRow,
    families::{
        name::{CoverageShape, load_composed},
        records::{FamilyAttribution, load_family_record_inventory_detail_on, name_relations_on},
    },
};
use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool};
use std::collections::BTreeMap;

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
    let mut inventories = BTreeMap::new();
    let mut out = Vec::new();
    for (id, row) in names {
        let normalized = super::names::normalize_phase_name(&id, &row.normalized_name)?;
        let labelhash = super::names::phase_labelhash(&normalized);
        let labelhash_count = i32::try_from(normalized.normalized_labels.len()).ok();
        let chain = row.provenance["chain_id"]
            .as_str()
            .context("composed name has no chain")?;
        let inventory = if let Some(resource) = row
            .serving_resource_id
            .or(row.resource_id)
            .filter(|_| include_inventory)
        {
            if let std::collections::btree_map::Entry::Vacant(entry) = inventories.entry(resource) {
                let inventory = load_family_record_inventory_detail_on(
                    &mut *conn,
                    chain,
                    resource,
                    FamilyAttribution::Load,
                )
                .await?;
                entry.insert(inventory.map(|inventory| {
                    let publication_positions = row.chain_positions.clone();
                    let row = inventory.row;
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
                    }
                }));
            }
            inventories.get(&resource).cloned().flatten()
        } else {
            None
        };
        out.push(IdentityNameRecordRow {
            row: IdentityNameCurrentRow {
                logical_name_id: id.clone(),
                namespace: row.namespace,
                canonical_display_name: normalized.canonical_display_name,
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
