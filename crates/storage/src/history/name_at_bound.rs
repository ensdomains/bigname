//! Whether a name existed at a history read's bound block.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// Whether the name's surface was first observed above the bound block of its chain. Such a
/// name did not exist at the bound, even when a current name row for it already exists. A
/// surface on a chain outside the bound is not judged here.
pub async fn name_surface_observed_above_bound(
    pool: &PgPool,
    logical_name_id: &str,
    block_bounds: &BTreeMap<String, i64>,
) -> Result<bool> {
    let (chain_ids, blocks): (Vec<String>, Vec<i64>) = block_bounds
        .iter()
        .map(|(chain_id, block)| (chain_id.clone(), *block))
        .unzip();
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_phase.name_surfaces surface
             JOIN unnest($2::text[], $3::bigint[]) AS bound(chain_id, block_number)
               ON bound.chain_id = surface.chain_id
             WHERE surface.logical_name_id = $1
               AND surface.block_number > bound.block_number
         )",
    )
    .bind(logical_name_id)
    .bind(&chain_ids)
    .bind(&blocks)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to check name surface {logical_name_id} at the bound"))
}
