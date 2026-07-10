use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;
use sqlx::Row;

use crate::reconciliation::ChainCoverageFrontiers;

use super::logging::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<()> {
    for chain in watched_chain_plan {
        let pre_sync_max_discovery_edge_id = max_discovery_edge_id(pool).await?;
        let mut admitted_edge_count = 0_usize;

        let summary = bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);
        admitted_edge_count += summary.admitted_edge_count;

        let summary = bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry resource/surface state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
        admitted_edge_count += summary.admitted_edge_count;

        let summary = bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);

        if admitted_edge_count > 0 {
            invalidate_coverage_frontier_for_admitted_edges(
                pool,
                &chain.chain,
                pre_sync_max_discovery_edge_id,
                coverage_frontiers,
            )
            .await?;
        }
    }

    Ok(())
}

async fn max_discovery_edge_id(pool: &sqlx::PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(discovery_edge_id) FROM discovery_edges")
        .fetch_one(pool)
        .await
        .context("failed to load the discovery edge id watermark")
        .map(|max| max.unwrap_or(0))
}

/// Discovery admission during a promotion crawl can create watched tuples
/// whose active windows start behind the already-verified coverage frontier
/// (admission is checkpoint-gated, so new edges always land in just-promoted
/// blocks). Rewind the frontier so the next verification covers the new
/// tuples: to just before the lowest newly inserted edge window when the
/// inserts are identifiable by the id watermark, or drop the memo entirely
/// when an admission reactivated existing rows (no new ids) or admitted an
/// unbounded window.
pub(crate) async fn invalidate_coverage_frontier_for_admitted_edges(
    pool: &sqlx::PgPool,
    chain: &str,
    pre_sync_max_discovery_edge_id: i64,
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS inserted_count,
            COUNT(*) FILTER (WHERE active_from_block_number IS NULL)::BIGINT AS unbounded_count,
            MIN(active_from_block_number) AS lowest_active_from_block
        FROM discovery_edges
        WHERE chain_id = $1
          AND discovery_edge_id > $2
        "#,
    )
    .bind(chain)
    .bind(pre_sync_max_discovery_edge_id)
    .fetch_one(pool)
    .await
    .context("failed to inspect newly admitted discovery edges")?;
    let inserted_count: i64 = row
        .try_get("inserted_count")
        .context("missing inserted_count")?;
    let unbounded_count: i64 = row
        .try_get("unbounded_count")
        .context("missing unbounded_count")?;
    let lowest_active_from_block: Option<i64> = row
        .try_get("lowest_active_from_block")
        .context("missing lowest_active_from_block")?;

    match lowest_active_from_block {
        Some(lowest) if inserted_count > 0 && unbounded_count == 0 => {
            coverage_frontiers.clamp_verified_through(chain, lowest - 1);
        }
        _ => coverage_frontiers.reset(chain),
    }
    Ok(())
}
