//! Bounded reads of Project-owned lookup facts on one admitted snapshot. Missing preparation is
//! an integrity error; this path never repairs or composes production lookup state.
use super::{LookupNameCore, LookupRelation};
use crate::{
    AddressNameRelation, IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    families::name::{
        FamilyPublication, FamilyPublicationUnavailable, all_servable_publications,
        rendered::{composed_surface_sql, rendered_name_sql},
        servable_publication,
    },
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Row};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Admit the selected chains before an empty answer is possible. Manifest sync invalidates
/// phase input hashes before redo; the old live marker alone cannot admit stored lookup facts.
pub async fn ensure_publications(
    conn: &mut PgConnection,
    chains: Option<&[String]>,
) -> Result<BTreeMap<String, FamilyPublication>> {
    let publications = if let Some(chains) = chains {
        let mut out = Vec::new();
        for chain in chains {
            out.push(servable_publication(conn, chain).await?);
        }
        out
    } else {
        all_servable_publications(conn).await?
    };
    let mut out = BTreeMap::new();
    for publication in publications {
        let current: bool = sqlx::query_scalar(
            "/* storage:families.lookup.inputs */
            SELECT count(*)=2 FROM bigname_phase.chain_phase_state
            WHERE chain_id=$1 AND phase_name IN ('interpret','project') AND input_content_hash=$2",
        )
        .bind(&publication.chain_id)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_one(&mut *conn)
        .await
        .context("failed to admit lookup input hashes")?;
        if !current {
            return Err(FamilyPublicationUnavailable {
                chain_id: publication.chain_id,
            }
            .into());
        }
        out.insert(publication.chain_id.clone(), publication);
    }
    crate::families::name::seams::after_publication().await;
    Ok(out)
}

pub async fn load_lookup_records(
    pool: &PgPool,
    ids: &[String],
    include_inventory: bool,
    chains: Option<&[String]>,
) -> Result<Vec<IdentityNameRecordRow>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let publications = ensure_publications(&mut snapshot, chains).await?;
    let rows = load_at(&mut snapshot, ids, include_inventory, &publications).await?;
    snapshot.commit().await?;
    Ok(rows)
}

pub(crate) async fn load_on(
    conn: &mut PgConnection,
    ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    // Internal reverse readers already hold a complete snapshot. Restrict this admission to
    // the IDs' source chains so an unrelated unpublished chain cannot widen the request scope.
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:families.lookup.name_chains */
        SELECT DISTINCT chain_id FROM bigname_phase.name_surfaces WHERE logical_name_id=ANY($1)",
    )
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;
    let publications = ensure_publications(conn, Some(&chains)).await?;
    load_at(conn, ids, include_inventory, &publications).await
}

pub(crate) async fn load_at(
    conn: &mut PgConnection,
    ids: &[String],
    include_inventory: bool,
    publications: &BTreeMap<String, FamilyPublication>,
) -> Result<Vec<IdentityNameRecordRow>> {
    let mut out = BTreeMap::new();
    for ids in ids.chunks(256) {
        let identities = sqlx::query(&format!("/* storage:families.lookup.names */
            SELECT surface.logical_name_id, surface.chain_id, surface.namespace, surface.namehash,
                   surface.raw_name, {rendered} AS rendered_name, surface.block_number,
                   stored.logical_name_id IS NOT NULL AS prepared, stored.core
            FROM bigname_phase.name_surfaces surface
            JOIN bigname_phase.chain_lineage lineage ON lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
            LEFT JOIN bigname_phase.project_lookup_name stored ON stored.chain_id=surface.chain_id AND stored.logical_name_id=surface.logical_name_id
            WHERE surface.logical_name_id=ANY($1) AND {composed}
              AND surface.canonicality_state IN ('canonical','safe','finalized')
              AND lineage.canonicality_state IN ('canonical','safe','finalized')",
            rendered=rendered_name_sql("surface"), composed=composed_surface_sql("surface")))
            .bind(ids).fetch_all(&mut *conn).await.context("failed to load lookup identities")?;
        let relation_rows: Vec<(String, String, String)> = sqlx::query_as(
            "/* storage:families.lookup.relations */
            SELECT logical_name_id, address, relation FROM bigname_phase.project_lookup_relation
            WHERE logical_name_id=ANY($1) AND chain_id=ANY($2)
            ORDER BY logical_name_id, CASE relation WHEN 'token_holder' THEN 0 ELSE 1 END, address",
        )
        .bind(ids)
        .bind(publications.keys().cloned().collect::<Vec<_>>())
        .fetch_all(&mut *conn)
        .await
        .context("failed to load exact lookup relations")?;
        let mut relations: BTreeMap<String, Vec<LookupRelation>> = BTreeMap::new();
        for (name, address, relation) in relation_rows {
            relations
                .entry(name)
                .or_default()
                .push(LookupRelation { address, relation });
        }
        for identity in identities {
            let chain: String = identity.try_get("chain_id")?;
            let Some(publication) = publications.get(&chain) else {
                continue;
            };
            if identity.try_get::<i64, _>("block_number")? > publication.block_number {
                continue;
            }
            let id: String = identity.try_get("logical_name_id")?;
            ensure!(
                identity.try_get::<bool, _>("prepared")?,
                "published lookup name {id} has no prepared state"
            );
            let Some(core) = identity.try_get::<Option<Value>, _>("core")? else {
                continue;
            };
            let mut core: LookupNameCore =
                serde_json::from_value(core).context("invalid stored lookup name")?;
            core.provenance["surface_block_number"] =
                json!(identity.try_get::<i64, _>("block_number")?);
            let spelling: String = identity.try_get("rendered_name")?;
            let rendered = crate::rendered_name::parse(&spelling)
                .context("invalid lookup identity spelling")?;
            let display = if identity.try_get::<Option<String>, _>("raw_name")?.is_some() {
                rendered.canonical_display_name.clone()
            } else {
                spelling
            };
            let positions = positions(publication);
            let related = relations
                .remove(&id)
                .unwrap_or_default()
                .into_iter()
                .map(|relation| {
                    Ok(IdentityAddressRelationRow {
                        address: relation.address,
                        logical_name_id: id.clone(),
                        relation: relation_kind(&relation.relation)?,
                        chain_positions: positions.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            out.insert(
                id.clone(),
                IdentityNameRecordRow {
                    row: IdentityNameCurrentRow {
                        logical_name_id: id,
                        namespace: identity.try_get("namespace")?,
                        namehash: identity.try_get("namehash")?,
                        canonical_display_name: display,
                        normalized_name: rendered.normalized_name,
                        labelhash: rendered
                            .labelhashes
                            .first()
                            .map(|hash| format!("0x{}", alloy_primitives::hex::encode(hash))),
                        labelhash_count: i32::try_from(rendered.labels.len()).ok(),
                        surface_binding_id: core.surface_binding_id,
                        resource_id: core.resource_id,
                        serving_resource_id: core.serving_resource_id,
                        binding_kind: core
                            .binding_kind
                            .as_deref()
                            .map(crate::SurfaceBindingKind::parse)
                            .transpose()?,
                        record_inventory_boundary_key: None,
                        declared_summary: core.declared_summary,
                        provenance: core.provenance,
                        coverage: core.coverage,
                        chain_positions: positions,
                        last_recomputed_at: publication.block_timestamp,
                    },
                    record_inventory_current: None,
                    relations: related,
                },
            );
        }
    }
    if include_inventory {
        let mut resources: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
        for record in out.values() {
            if let Some(resource) = record_resource(record) {
                let chain = record.row.provenance["chain_id"]
                    .as_str()
                    .context("lookup name has no chain")?;
                resources.entry(chain.into()).or_default().insert(resource);
            }
        }
        let mut inventories = BTreeMap::new();
        for (chain, resources) in resources {
            let publication = &publications[&chain];
            inventories.extend(
                super::read_inventory::load(
                    conn,
                    publication,
                    &resources.into_iter().collect::<Vec<_>>(),
                )
                .await?,
            );
        }
        for record in out.values_mut() {
            record.record_inventory_current =
                record_resource(record).and_then(|id| inventories.get(&id).cloned());
        }
    }
    Ok(out.into_values().collect())
}

pub(crate) fn record_resource(record: &IdentityNameRecordRow) -> Option<Uuid> {
    let summary = &record.row.declared_summary;
    if summary["unresolvable_reason"].is_string()
        || summary["resolution_unsupported_reason"].is_string()
    {
        return None;
    }
    record.row.serving_resource_id.or(record.row.resource_id)
}

pub(crate) fn relation_kind(relation: &str) -> Result<AddressNameRelation> {
    match relation {
        "token_holder" => Ok(AddressNameRelation::TokenHolder),
        "effective_controller" => Ok(AddressNameRelation::EffectiveController),
        _ => anyhow::bail!("invalid stored lookup relation {relation}"),
    }
}

pub(crate) fn positions(publication: &FamilyPublication) -> Value {
    let slot = match publication.chain_id.as_str() {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        chain => chain,
    };
    json!({slot:{"chain_id":publication.chain_id,"block_number":publication.block_number,
        "block_hash":publication.block_hash,"timestamp":publication.block_timestamp_json}})
}
