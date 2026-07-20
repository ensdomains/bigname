use anyhow::{Result, bail};
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, chain_lineage_contains_ancestor, load_chain_lineage_block,
    mark_chain_lineage_range_orphaned,
    upsert_chain_lineage_blocks_recanonicalizing_orphaned as upsert_recanonicalized_lineage_blocks,
    upsert_chain_lineage_blocks_without_snapshots,
    upsert_chain_lineage_blocks_without_snapshots_recanonicalizing_orphaned as upsert_recanonicalized_lineage_blocks_without_snapshots,
};
use tracing::info;

use crate::{
    backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
    provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot},
    resolver_profile_convergence::drain_resolver_profile_input_changes,
    runtime::IntakeChainTask,
};

use super::{
    lineage::{
        head_change_set, lineage_block_to_provider, provider_block_to_checkpoint_ref,
        provider_block_to_lineage_with_header_audit_mode,
    },
    persistence::persist_reconciled_raw_state,
    types::{
        CanonicalReconciliation, CanonicalReconciliationStatus, ChainReconciliationOutcome,
        HeaderAuditMode,
    },
};

#[path = "canonical/checkpoints.rs"]
mod checkpoints;
#[path = "canonical/cold_start.rs"]
mod cold_start;
#[path = "canonical/contiguous_gap.rs"]
mod contiguous_gap;
#[path = "canonical/ens_v2_coverage_recovery.rs"]
mod ens_v2_coverage_recovery;
#[path = "canonical/orphaning.rs"]
mod orphaning;
#[path = "canonical/poll.rs"]
mod poll;
#[path = "canonical/stored_lineage.rs"]
pub(crate) mod stored_lineage;

use checkpoints::{checkpoint_update_for_head, fill_checkpoint_ancestor_path};
use cold_start::ColdStartCheckpoint;
use contiguous_gap::reconcile_contiguous_checkpoint_gap;
pub(crate) use ens_v2_coverage_recovery::{
    EnsV2LiveCoverageRecoveryStatus, recover_ens_v2_live_coverage_requirement,
};
use orphaning::orphan_reorg_losing_branch_payloads;
pub(crate) use poll::{poll_provider_heads, poll_provider_heads_with_adapter_sync};
pub(crate) use stored_lineage::{ChainCoverageFrontiers, RawCodeBaselineFrontier};
use stored_lineage::{
    StoredLineagePromotion, reconcile_large_checkpoint_gap_from_stored_lineage,
    stored_lineage_promotion_anchors,
};

const MAX_PARENT_FETCH_DEPTH: usize = 131_072;
// Live polling fails closed before a large catch-up; hash-pinned backfill owns larger gaps.
const MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS: i64 = DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
#[allow(dead_code)]
pub(crate) async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_intake_chain_task_with_adapter_sync(
        pool,
        "test",
        task,
        provider,
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn reconcile_intake_chain_task_with_adapter_sync(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_head: Option<&ProviderBlock>,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let heads = provider.fetch_chain_heads().await?;
    reconcile_fetched_heads_with_gap_policy(
        pool,
        deployment_profile,
        task,
        provider,
        &heads,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        latched_bootstrap_finalized_head,
    )
    .await
}

#[allow(dead_code)]
pub(crate) async fn reconcile_fetched_heads(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_fetched_heads_with_gap_policy(
        pool,
        "test",
        task,
        provider,
        heads,
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn reconcile_fetched_heads_with_adapter_sync(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_fetched_heads_with_gap_policy(
        pool,
        "test",
        task,
        provider,
        heads,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn reconcile_fetched_heads_with_gap_policy(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
    coverage_frontiers: &ChainCoverageFrontiers,
    latched_bootstrap_finalized_head: Option<&ProviderBlock>,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let mut stored_promotion_anchors = stored_lineage_promotion_anchors(heads);
    if let Some(latched_head) = latched_bootstrap_finalized_head
        && !stored_promotion_anchors
            .iter()
            .any(|anchor| anchor.block_hash == latched_head.block_hash)
    {
        stored_promotion_anchors.push(latched_head.clone());
    }
    let canonical = reconcile_canonical_head(
        pool,
        provider,
        &task.chain,
        &task.checkpoint,
        &heads.canonical,
        header_audit_mode,
        &stored_promotion_anchors,
        coverage_frontiers,
    )
    .await?;
    let promotion_epoch = coverage_frontiers.take_promotion_epoch(
        &task.chain,
        canonical.status == CanonicalReconciliationStatus::StoredLineagePromoted,
    )?;
    let provider_head_change_set = head_change_set(task, heads, &canonical);
    if canonical.status == CanonicalReconciliationStatus::ReorgReconciled {
        orphan_reorg_losing_branch_payloads(
            pool,
            &task.chain,
            task.checkpoint.canonical_block_hash.as_deref(),
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
    }

    let fetched_checkpoint_ancestor_count = fill_checkpoint_ancestor_path(
        pool,
        provider,
        &task.chain,
        heads,
        &canonical,
        header_audit_mode,
    )
    .await?;
    if fetched_checkpoint_ancestor_count > 0 {
        info!(
            service = "indexer",
            chain = %task.chain,
            fetched_checkpoint_ancestor_count,
            "checkpoint ancestor path fetched for provider heads"
        );
    }

    let canonical_update = canonical.canonical.clone();
    let (safe_update, finalized_update) = if let Some(canonical_update) = &canonical_update {
        let safe_update = checkpoint_update_for_head(
            pool,
            &task.chain,
            "safe",
            task.checkpoint.safe_block_hash.as_deref(),
            task.checkpoint.safe_block_number,
            canonical_update,
            heads.safe.as_ref(),
        )
        .await?;
        let finalized_update = checkpoint_update_for_head(
            pool,
            &task.chain,
            "finalized",
            task.checkpoint.finalized_block_hash.as_deref(),
            task.checkpoint.finalized_block_number,
            canonical_update,
            heads.finalized.as_ref(),
        )
        .await?;

        (safe_update, finalized_update)
    } else {
        (None, None)
    };
    let accepted_heads = ProviderHeadSnapshot {
        canonical: heads.canonical.clone(),
        safe: safe_update
            .as_ref()
            .and_then(|_| heads.safe.as_ref().cloned()),
        finalized: finalized_update
            .as_ref()
            .and_then(|_| heads.finalized.as_ref().cloned()),
    };
    let head_change_set = head_change_set(task, &accepted_heads, &canonical);

    persist_reconciled_raw_state(
        pool,
        deployment_profile,
        task,
        provider,
        &accepted_heads,
        &canonical,
        head_change_set,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
        coverage_frontiers,
    )
    .await?;
    if adapter_sync_enabled {
        let profile_convergence = drain_resolver_profile_input_changes(pool).await?;
        profile_convergence
            .ensure_chain_completion_allowed(&task.chain, "chain checkpoint advancement")?;
    }
    let next_checkpoint = ChainCoverageFrontiers::advance_checkpoint_with_promotion_epoch(
        pool,
        &task.chain,
        promotion_epoch,
        canonical_update,
        safe_update,
        finalized_update,
    )
    .await?;
    if !head_change_set.canonical_head_changed
        && !head_change_set.safe_head_changed
        && !head_change_set.finalized_head_changed
        && !provider_head_change_set.safe_head_changed
        && !provider_head_change_set.finalized_head_changed
        && canonical.status == CanonicalReconciliationStatus::Unchanged
    {
        return Ok(None);
    }
    let mut next_task = task.clone();
    next_task.checkpoint = next_checkpoint.clone();
    Ok(Some((
        next_task,
        ChainReconciliationOutcome {
            chain: task.chain.clone(),
            canonical_status: canonical.status,
            canonical_head_changed: head_change_set.canonical_head_changed,
            safe_head_changed: head_change_set.safe_head_changed,
            finalized_head_changed: head_change_set.finalized_head_changed,
            fetched_parent_count: canonical.fetched_parent_count,
            orphaned_block_count: canonical.orphaned_block_count,
            canonical_block_number: next_checkpoint.canonical_block_number,
            safe_block_number: next_checkpoint.safe_block_number,
            finalized_block_number: next_checkpoint.finalized_block_number,
        },
    )))
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn reconcile_canonical_head(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
    header_audit_mode: HeaderAuditMode,
    stored_lineage_promotion_anchors: &[ProviderBlock],
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<CanonicalReconciliation> {
    let latest_hash = latest_head.block_hash.as_str();
    let cold_start =
        ColdStartCheckpoint::resolve(checkpoint, latest_head, stored_lineage_promotion_anchors);
    let current_canonical_hash = cold_start.canonical_hash;
    let current_canonical_number = cold_start.canonical_number;
    if cold_start.is_cold_start() {
        info!(
            service = "indexer",
            chain,
            cold_start_anchor_block_number = current_canonical_number,
            latest_block_number = latest_head.block_number,
            "reconciling live canonical tail above the latched bootstrap boundary"
        );
    }
    if let Some(initialized) = cold_start
        .initialize_unanchored_latest(pool, chain, latest_head, header_audit_mode)
        .await?
    {
        return Ok(initialized);
    }

    if current_canonical_hash == Some(latest_hash) {
        upsert_recanonicalized_lineage_blocks(
            pool,
            &[provider_block_to_lineage_with_header_audit_mode(
                chain,
                latest_head,
                CanonicalityState::Canonical,
                header_audit_mode,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Unchanged,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    if let (Some(current_canonical_hash), Some(current_canonical_number)) =
        (current_canonical_hash, current_canonical_number)
    {
        if let Some(mut reconciliation) = reconcile_contiguous_checkpoint_gap(
            pool,
            provider,
            chain,
            current_canonical_hash,
            current_canonical_number,
            latest_head,
            header_audit_mode,
        )
        .await?
        {
            reconciliation.status = cold_start.reconciliation_status(reconciliation.status);
            return Ok(reconciliation);
        }
        match reconcile_large_checkpoint_gap_from_stored_lineage(
            pool,
            provider,
            chain,
            current_canonical_hash,
            current_canonical_number,
            latest_head,
            stored_lineage_promotion_anchors,
            coverage_frontiers,
        )
        .await?
        {
            StoredLineagePromotion::Promoted(reconciliation) => return Ok(reconciliation),
            StoredLineagePromotion::Refused(reason) => {
                let live_gap_blocks = latest_head.block_number - current_canonical_number;
                if live_gap_blocks > MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
                    bail!("{reason}");
                }
            }
            StoredLineagePromotion::NotApplicable => {}
        }
        let live_gap_blocks = latest_head.block_number - current_canonical_number;
        if live_gap_blocks > MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
            bail!(
                "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; run bounded catch-up or hash-pinned backfill for the missing range"
            );
        }
    }

    let mut path = vec![latest_head.clone()];
    let mut cursor = latest_head.clone();
    let mut fetched_parent_count = 0usize;
    let mut common_ancestor_hash = None::<String>;
    let mut parent_fetch_limit_exhausted = true;
    let live_gap_blocks =
        current_canonical_number.map_or(0, |number| latest_head.block_number - number);

    for _ in 0..MAX_PARENT_FETCH_DEPTH {
        if cursor.parent_hash.as_deref() == current_canonical_hash {
            common_ancestor_hash = current_canonical_hash.map(ToOwned::to_owned);
            parent_fetch_limit_exhausted = false;
            break;
        }

        let Some(parent_hash) = cursor.parent_hash.clone() else {
            parent_fetch_limit_exhausted = false;
            break;
        };

        if let Some(stored_parent) = load_chain_lineage_block(pool, chain, &parent_hash).await? {
            let can_be_current_branch_ancestor = stored_parent.canonicality_state
                != CanonicalityState::Orphaned
                && current_canonical_number
                    .is_some_and(|number| stored_parent.block_number <= number);
            let is_current_branch_ancestor = if let (true, Some(head_hash)) =
                (can_be_current_branch_ancestor, current_canonical_hash)
            {
                chain_lineage_contains_ancestor(pool, chain, head_hash, &stored_parent.block_hash)
                    .await?
            } else {
                false
            };
            if can_be_current_branch_ancestor && is_current_branch_ancestor {
                common_ancestor_hash = Some(stored_parent.block_hash.clone());
                parent_fetch_limit_exhausted = false;
                break;
            }

            cursor = lineage_block_to_provider(&stored_parent);
            path.push(cursor.clone());
            continue;
        }

        // Over-bound live gaps may recover only through lineage already persisted by backfill.
        if live_gap_blocks > MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
            bail!(
                "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; run bounded catch-up or hash-pinned backfill for the missing range"
            );
        }

        let fetched_parent = provider.fetch_block_by_hash(&parent_hash).await?;
        fetched_parent_count += 1;
        if Some(fetched_parent.block_hash.as_str()) == current_canonical_hash {
            common_ancestor_hash = Some(fetched_parent.block_hash.clone());
            parent_fetch_limit_exhausted = false;
            break;
        }

        cursor = fetched_parent.clone();
        path.push(fetched_parent);
    }

    if common_ancestor_hash.is_none() {
        if parent_fetch_limit_exhausted {
            bail!(
                "canonical reorg walk for chain {chain} exceeded parent fetch limit {MAX_PARENT_FETCH_DEPTH}; run bounded catch-up or hash-pinned backfill to repair the ancestry path"
            );
        }
        let lineage_blocks = path
            .iter()
            .map(|block| {
                provider_block_to_lineage_with_header_audit_mode(
                    chain,
                    block,
                    CanonicalityState::Observed,
                    header_audit_mode,
                )
            })
            .collect::<Vec<_>>();
        upsert_chain_lineage_blocks_without_snapshots(pool, &lineage_blocks).await?;

        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::AwaitingAncestor,
            canonical: None,
            fetched_parent_count,
            orphaned_block_count: 0,
            reconciled_blocks: path,
            raw_orphan_stop_before_hash: None,
        });
    }

    let common_ancestor_hash = common_ancestor_hash.expect("checked above");
    let mut orphaned_block_count = 0usize;
    let status = if cold_start.is_cold_start() {
        CanonicalReconciliationStatus::Initialized
    } else if Some(common_ancestor_hash.as_str()) == current_canonical_hash {
        if path.len() == 1 {
            CanonicalReconciliationStatus::Appended
        } else {
            CanonicalReconciliationStatus::GapBackfilled
        }
    } else {
        orphaned_block_count = orphan_canonical_branch(
            pool,
            chain,
            current_canonical_hash.expect("current checkpoint must exist"),
            Some(common_ancestor_hash.as_str()),
        )
        .await?;
        CanonicalReconciliationStatus::ReorgReconciled
    };

    let lineage_blocks = path
        .iter()
        .rev()
        .map(|block| {
            provider_block_to_lineage_with_header_audit_mode(
                chain,
                block,
                CanonicalityState::Canonical,
                header_audit_mode,
            )
        })
        .collect::<Vec<_>>();
    upsert_recanonicalized_lineage_blocks_without_snapshots(pool, &lineage_blocks).await?;

    Ok(CanonicalReconciliation {
        status,
        canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
        fetched_parent_count,
        orphaned_block_count,
        reconciled_blocks: path,
        raw_orphan_stop_before_hash: (status == CanonicalReconciliationStatus::ReorgReconciled)
            .then_some(common_ancestor_hash),
    })
}

pub(crate) async fn orphan_canonical_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<usize> {
    let mut orphaned_block_count = 0usize;
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        let snapshots =
            mark_chain_lineage_range_orphaned(pool, chain, &block_hash, stop_before_hash).await?;
        orphaned_block_count += snapshots.len();
        cursor_hash = None;
    }

    Ok(orphaned_block_count)
}
