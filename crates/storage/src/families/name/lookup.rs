//! Lookup's bounded name publication uses the name compositor without diagnostic heads or
//! execution topology. Project and reference reads share this exact core and relation fold.
use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgConnection;

use super::{CoverageShape, FamilyPublication, batch::load_chain, loaders::surfaces};
use crate::families::lookup::{LookupNameCore, LookupNamePublication, LookupRelation};

pub async fn compose_lookup_names_at(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, LookupNamePublication>> {
    let mut out = BTreeMap::new();
    // Bounded internally as well as at callers: neither publication nor a large request can
    // accidentally load an entire rebuild into the compositor's maps.
    for ids in logical_name_ids.chunks(256) {
        let surfaces: Vec<_> = surfaces(conn, ids)
            .await?
            .into_iter()
            .filter(|surface| {
                surface.chain_id == publication.chain_id
                    && surface.block_number <= publication.block_number
            })
            .collect();
        let composed =
            load_chain(conn, publication, &surfaces, CoverageShape::Plain, false).await?;
        super::wrapper_fields::attach_published_wrapper_expiries(
            conn,
            composed.values_mut().filter_map(|name| name.row.as_mut()),
        )
        .await?;
        let rows: Vec<_> = composed
            .values()
            .filter_map(|name| name.row.as_ref())
            .collect();
        let mut relations =
            crate::families::records::name_relations_at(conn, publication, &rows).await?;
        for id in ids {
            let (core, recompose_at) = match composed.get(id) {
                Some(name) => (name.row.as_ref().map(stable_core), name.recompose_at),
                None => (None, None),
            };
            let relations = relations
                .remove(id)
                .unwrap_or_default()
                .into_iter()
                .map(|relation| LookupRelation {
                    address: relation.address.to_ascii_lowercase(),
                    relation: relation.relation.as_str().to_owned(),
                })
                .collect();
            out.insert(
                id.clone(),
                LookupNamePublication {
                    core,
                    relations,
                    recompose_at,
                },
            );
        }
    }
    Ok(out)
}

fn stable_core(row: &crate::NameCurrentRow) -> LookupNameCore {
    let mut declared_summary = row.declared_summary.clone();
    if let Some(summary) = declared_summary.as_object_mut() {
        summary.remove("history");
        summary.remove("topology");
    }
    LookupNameCore {
        surface_binding_id: row.surface_binding_id,
        resource_id: row.resource_id,
        serving_resource_id: row.serving_resource_id,
        record_serving_resource_id: row.record_serving_resource_id(),
        binding_kind: row.binding_kind.map(|kind| kind.as_str().to_owned()),
        declared_summary,
        provenance: row.provenance.clone(),
        coverage: row.coverage.clone(),
    }
}
