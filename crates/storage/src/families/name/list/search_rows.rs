//! Search dictionary rows reuse authoritative composition without binding diagnostics or ENS
//! resolution topology. Other readers keep their full composition and enrichment.
use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgConnection;

use super::super::{CoverageShape, batch, compose::Surface, loaders, rendered, seams, topology};
use crate::NameCurrentRow;

pub(super) async fn load(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let mut rows = BTreeMap::new();
    if ids.is_empty() {
        return Ok(rows);
    }
    let mut by_chain: BTreeMap<String, Vec<Surface>> = BTreeMap::new();
    for surface in loaders::surfaces(conn, ids).await? {
        by_chain
            .entry(surface.chain_id.clone())
            .or_default()
            .push(surface);
    }
    for (chain, surfaces) in by_chain {
        let publication = batch::servable_publication(conn, &chain).await?;
        seams::after_publication().await;
        let surfaces: Vec<Surface> = surfaces
            .into_iter()
            .filter(|surface| surface.block_number <= publication.block_number)
            .collect();
        if surfaces.is_empty() {
            continue;
        }
        rows.extend(
            batch::load_chain(conn, &publication, &surfaces, CoverageShape::Plain, false)
                .await?
                .into_iter()
                .filter_map(|(name, composed)| Some((name, composed.row?))),
        );
    }
    rendered::enrich(conn, &mut rows).await?;
    // Basenames topology adds the Ethereum timestamp used by the created_at fallback. Keep
    // its complete existing enrichment, and preserve it for any other non-ENS namespace.
    let (mut retained_topology, mut ens): (BTreeMap<_, _>, BTreeMap<_, _>) = rows
        .into_iter()
        .partition(|(_, row)| row.namespace != "ens");
    topology::enrich_all(conn, &mut retained_topology).await?;
    super::super::wrapper_fields::attach_published_wrapper_expiries(conn, ens.values_mut()).await?;
    ens.extend(retained_topology);
    Ok(ens)
}
