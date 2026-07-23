use anyhow::{Context, Result};
use bigname_adapters::StartupAdapterProgress;
use bigname_storage::{
    ExecutionOutcomeInvalidationProgress, ExecutionOutcomeInvalidationProgressFuture,
    invalidate_execution_outcomes_for_orphaned_blocks,
    invalidate_execution_outcomes_for_orphaned_blocks_with_progress, load_chain_lineage_block,
    mark_block_derived_normalized_events_range_orphaned, mark_chain_lineage_range_orphaned,
    mark_identity_rows_range_orphaned, mark_raw_block_facts_range_orphaned,
};
use tracing::info;

use super::{
    super::persistence::ensure_losing_branch_raw_blocks_exist_with_progress,
    progress::record_live_progress, stored_lineage::ChainCoverageFrontiers,
};

struct ExecutionInvalidationHeartbeat<'a>(&'a mut dyn StartupAdapterProgress);

impl ExecutionOutcomeInvalidationProgress for ExecutionInvalidationHeartbeat<'_> {
    fn record<'a>(
        &'a mut self,
        pool: &'a sqlx::PgPool,
    ) -> ExecutionOutcomeInvalidationProgressFuture<'a> {
        self.0.record(pool)
    }
}

#[allow(dead_code)]
pub(crate) async fn orphan_canonical_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<usize> {
    orphan_canonical_branch_with_progress(pool, chain, from_hash, stop_before_hash, &mut None).await
}

pub(super) async fn orphan_canonical_branch_with_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<usize> {
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        let block = load_chain_lineage_block(pool, chain, &block_hash)
            .await?
            .with_context(|| {
                format!(
                    "missing stored lineage row for chain {chain} block {block_hash} while orphaning the losing branch"
                )
            })?;
        cursor_hash = block.parent_hash;
        record_live_progress(pool, progress).await?;
    }

    let snapshots =
        mark_chain_lineage_range_orphaned(pool, chain, from_hash, stop_before_hash).await?;
    Ok(snapshots.len())
}

#[allow(dead_code)]
pub(crate) async fn orphan_reorg_losing_branch_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    current_canonical_hash: Option<&str>,
    stop_before_hash: Option<&str>,
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<()> {
    orphan_reorg_losing_branch_payloads_inner(
        pool,
        chain,
        current_canonical_hash,
        stop_before_hash,
        coverage_frontiers,
        &mut None,
    )
    .await
}

pub(super) async fn orphan_reorg_losing_branch_payloads_with_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    current_canonical_hash: Option<&str>,
    stop_before_hash: Option<&str>,
    coverage_frontiers: &ChainCoverageFrontiers,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    orphan_reorg_losing_branch_payloads_inner(
        pool,
        chain,
        current_canonical_hash,
        stop_before_hash,
        coverage_frontiers,
        progress,
    )
    .await
}

async fn orphan_reorg_losing_branch_payloads_inner(
    pool: &sqlx::PgPool,
    chain: &str,
    current_canonical_hash: Option<&str>,
    stop_before_hash: Option<&str>,
    coverage_frontiers: &ChainCoverageFrontiers,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(current_canonical_hash) = current_canonical_hash {
        ensure_losing_branch_raw_blocks_exist_with_progress(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
            progress,
        )
        .await?;
        let orphaned_raw_facts = mark_raw_block_facts_range_orphaned(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        if orphaned_raw_facts.code_hash_count > 0 {
            coverage_frontiers.invalidate_raw_code_baseline_frontier(chain);
        }
        record_live_progress(pool, progress).await?;

        let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        if orphaned_normalized_event_count > 0 {
            info!(
                service = "indexer",
                chain = %chain,
                orphaned_normalized_event_count,
                "block-derived normalized events orphaned for the losing branch"
            );
        }
        record_live_progress(pool, progress).await?;

        let orphaned_identity_counts = mark_identity_rows_range_orphaned(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        if orphaned_identity_counts.token_lineage_count > 0
            || orphaned_identity_counts.resource_count > 0
            || orphaned_identity_counts.name_surface_count > 0
            || orphaned_identity_counts.surface_binding_count > 0
        {
            info!(
                service = "indexer",
                chain = %chain,
                orphaned_token_lineage_count = orphaned_identity_counts.token_lineage_count,
                orphaned_resource_count = orphaned_identity_counts.resource_count,
                orphaned_name_surface_count = orphaned_identity_counts.name_surface_count,
                orphaned_surface_binding_count = orphaned_identity_counts.surface_binding_count,
                "identity rows orphaned for the losing branch"
            );
        }
        record_live_progress(pool, progress).await?;
    }

    let execution_invalidation_summary = match progress.as_deref_mut() {
        Some(progress) => {
            let mut heartbeat = ExecutionInvalidationHeartbeat(progress);
            invalidate_execution_outcomes_for_orphaned_blocks_with_progress(pool, &mut heartbeat)
                .await?
        }
        None => invalidate_execution_outcomes_for_orphaned_blocks(pool).await?,
    };
    if execution_invalidation_summary.deleted_outcome_count > 0 {
        info!(
            service = "indexer",
            chain = %chain,
            invalidated_execution_outcome_count =
                execution_invalidation_summary.deleted_outcome_count,
            "execution cache outcomes invalidated for orphaned block dependencies"
        );
    }

    Ok(())
}
