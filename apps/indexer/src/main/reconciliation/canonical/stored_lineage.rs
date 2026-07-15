use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ChainLineageBlock, CheckpointBlockRef, chain_lineage_contains_ancestor,
    load_chain_lineage_block, load_chain_lineage_canonical_child_path,
    load_highest_canonical_chain_lineage_block,
};

use crate::provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot};

use super::super::{
    lineage::lineage_block_to_provider,
    types::{CanonicalReconciliation, CanonicalReconciliationStatus},
};
use super::MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS;

#[path = "stored_lineage/admission_epoch_fence.rs"]
mod admission_epoch_fence;
#[path = "stored_lineage/coverage.rs"]
mod coverage;
#[path = "stored_lineage/topic_drift.rs"]
mod topic_drift;

use admission_epoch_fence::StoredLineageAdmissionEpochFence;
pub(crate) use coverage::ChainCoverageFrontiers;
use coverage::stored_path_has_required_raw_fact_coverage;
pub(crate) use topic_drift::ensure_required_topic_sets_undrifted_for_retention_generation;

const MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH: usize =
    (MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS as usize) * 4;

pub(super) enum StoredLineagePromotion {
    Promoted(CanonicalReconciliation),
    NotApplicable,
    Refused(String),
}

pub(super) fn stored_lineage_promotion_anchors(heads: &ProviderHeadSnapshot) -> Vec<ProviderBlock> {
    let mut anchors = heads.safe.iter().cloned().collect::<Vec<_>>();
    if let Some(finalized) = &heads.finalized
        && !anchors
            .iter()
            .any(|anchor| anchor.block_hash == finalized.block_hash)
    {
        anchors.push(finalized.clone());
    }
    anchors
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn reconcile_large_checkpoint_gap_from_stored_lineage(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    current_canonical_hash: &str,
    current_canonical_number: i64,
    latest_head: &ProviderBlock,
    stored_anchor_candidates: &[ProviderBlock],
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<StoredLineagePromotion> {
    if latest_head.block_number <= current_canonical_number {
        return Ok(StoredLineagePromotion::NotApplicable);
    }
    let live_gap_blocks = latest_head.block_number - current_canonical_number;
    if live_gap_blocks <= MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
        return Ok(StoredLineagePromotion::NotApplicable);
    }

    let Some((stored_anchor, provider_anchor_hash)) = select_stored_promotion_anchor(
        pool,
        provider,
        chain,
        current_canonical_number,
        stored_anchor_candidates,
    )
    .await?
    else {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; stored-lineage checkpoint promotion requires either the highest stored canonical block to match the provider's block at that height at or below the safe/finalized head, or a canonical/safe/finalized chain_lineage ancestor within {MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH} parent fetches of the provider safe/finalized head; run hash-pinned backfill through a stored safe/finalized ancestor, then retry live reconciliation"
        )));
    };
    let anchor_gap_blocks = stored_anchor.block_number - current_canonical_number;
    let batch_blocks = usize::try_from(anchor_gap_blocks.min(MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS))
        .expect("positive live gap batch size must fit in usize");
    let path = load_chain_lineage_canonical_child_path(
        pool,
        chain,
        current_canonical_hash,
        current_canonical_number,
        batch_blocks,
    )
    .await?;
    if path.len() != batch_blocks {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; stored lineage path from checkpoint {} is incomplete or has duplicate canonical children before the stored safe/finalized anchor {}; rerun hash-pinned backfill for the missing range and retry",
            current_canonical_number, stored_anchor.block_number
        )));
    }
    let admission_epoch = StoredLineageAdmissionEpochFence::read_epoch(pool, chain).await?;
    if let Err(reason) = stored_path_has_required_raw_fact_coverage(
        pool,
        chain,
        &path,
        coverage_frontiers,
        stored_anchor.block_number,
        admission_epoch,
    )
    .await
    {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; {reason}"
        )));
    }
    // Coverage is verified optimistically so historical scans do not block
    // discovery admission. A brief shared lock accepts the proof only if no
    // watched-surface mutation committed during verification.
    StoredLineageAdmissionEpochFence::acquire_for_epoch(pool, chain, admission_epoch)
        .await?
        .release()
        .await?;
    #[cfg(test)]
    admission_epoch_fence::pause_after_admission_epoch_verification_for_tests(chain).await;

    let target = path
        .last()
        .expect("non-empty stored lineage promotion path");
    let target_is_anchor = target.block_hash == stored_anchor.block_hash;
    let target_is_stored_anchor_ancestor = target_is_anchor
        || chain_lineage_contains_ancestor(
            pool,
            chain,
            &stored_anchor.block_hash,
            &target.block_hash,
        )
        .await?;
    if !target_is_stored_anchor_ancestor {
        // Separately backfilled segments may not retain every intervening
        // parent. Canonical markings alone are not proof that they share the
        // provider's current ancestry, so verify the disconnected target.
        let provider_targets = provider
            .fetch_block_hashes_by_numbers(&[target.block_number])
            .await?;
        let Some(provider_target) = provider_targets
            .iter()
            .find(|block| block.block_number == target.block_number)
        else {
            return Ok(StoredLineagePromotion::Refused(format!(
                "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; provider block at the promoted height {} was unavailable below stored safe/finalized anchor {} (provider anchor hash {provider_anchor_hash}); rerun hash-pinned backfill for the canonical range before retrying",
                target.block_number, stored_anchor.block_number
            )));
        };
        if !provider_target
            .block_hash
            .eq_ignore_ascii_case(&target.block_hash)
        {
            return Ok(StoredLineagePromotion::Refused(format!(
                "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; stored target block {} hash {} does not match provider block at the promoted height hash {} below stored safe/finalized anchor {} (provider anchor hash {provider_anchor_hash}); rerun hash-pinned backfill for the canonical range before retrying",
                target.block_number,
                target.block_hash,
                provider_target.block_hash,
                stored_anchor.block_number
            )));
        }
    }

    let canonical = CheckpointBlockRef {
        block_hash: target.block_hash.clone(),
        block_number: target.block_number,
    };
    let reconciled_blocks = path
        .into_iter()
        .rev()
        .map(|block| lineage_block_to_provider(&block))
        .collect::<Vec<_>>();

    coverage_frontiers.record_promotion_epoch(chain, admission_epoch);
    Ok(StoredLineagePromotion::Promoted(CanonicalReconciliation {
        status: CanonicalReconciliationStatus::StoredLineagePromoted,
        canonical: Some(canonical),
        fetched_parent_count: 0,
        orphaned_block_count: 0,
        reconciled_blocks,
        raw_orphan_stop_before_hash: None,
    }))
}

/// Two-strategy anchor search. Strategy 1 (primary, works for arbitrarily deep
/// gaps): take the highest stored canonical lineage row; if the provider's
/// block at that height has the same hash and the height is at or below the
/// provider's safe/finalized candidates, the stored frontier itself anchors
/// promotion — no parent walking and no provider-head proximity requirement.
/// Strategy 2 (near-tip): walk parents from the safe/finalized candidates
/// looking for a stored canonical row, for the case where the stored frontier
/// is close to or above the candidates. Provider RPC failures in either
/// strategy propagate as errors rather than being misreported as a missing
/// stored anchor.
async fn select_stored_promotion_anchor(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    current_canonical_number: i64,
    candidates: &[ProviderBlock],
) -> Result<Option<(ChainLineageBlock, String)>> {
    let max_candidate_height = candidates
        .iter()
        .map(|candidate| candidate.block_number)
        .max();
    if let Some(max_candidate_height) = max_candidate_height
        && let Some(stored_frontier) =
            load_highest_canonical_chain_lineage_block(pool, chain).await?
        && stored_frontier.block_number > current_canonical_number
        && stored_frontier.block_number <= max_candidate_height
    {
        let resolved = provider
            .fetch_block_hashes_by_numbers(&[stored_frontier.block_number])
            .await?;
        if let Some(provider_block) = resolved
            .iter()
            .find(|block| block.block_number == stored_frontier.block_number)
            && provider_block
                .block_hash
                .eq_ignore_ascii_case(&stored_frontier.block_hash)
        {
            let provider_anchor_hash = provider_block.block_hash.clone();
            return Ok(Some((stored_frontier, provider_anchor_hash)));
        }
        // Hash mismatch (stale fork tip) or unresolved height: fall back to
        // the parent walk, which can still find a lower stored ancestor.
    }

    for candidate in candidates {
        if candidate.block_number <= current_canonical_number {
            continue;
        }
        let mut cursor = candidate.clone();
        for parent_fetch_depth in 0..=MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH {
            if cursor.block_number <= current_canonical_number {
                break;
            }
            if let Some(stored) = load_chain_lineage_block(pool, chain, &cursor.block_hash).await?
                && stored_lineage_matches_provider_block(&stored, &cursor)
                && stored_anchor_is_canonical(stored.canonicality_state)
            {
                let provider_anchor_hash = candidate.block_hash.clone();
                return Ok(Some((stored, provider_anchor_hash)));
            }
            if parent_fetch_depth == MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH {
                break;
            }

            let Some(parent_hash) = cursor.parent_hash.clone() else {
                break;
            };
            let parent = provider.fetch_block_by_hash(&parent_hash).await?;
            if parent.block_hash != parent_hash || parent.block_number >= cursor.block_number {
                break;
            }
            cursor = parent;
        }
    }

    Ok(None)
}

fn stored_anchor_is_canonical(state: CanonicalityState) -> bool {
    matches!(
        state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

fn stored_lineage_matches_provider_block(
    stored: &ChainLineageBlock,
    provider: &ProviderBlock,
) -> bool {
    stored.block_hash == provider.block_hash
        && stored.parent_hash == provider.parent_hash
        && stored.block_number == provider.block_number
        && stored.block_timestamp.unix_timestamp() == provider.block_timestamp_unix_secs
        && optional_field_matches(&stored.logs_bloom, &provider.logs_bloom)
        && optional_field_matches(&stored.transactions_root, &provider.transactions_root)
        && optional_field_matches(&stored.receipts_root, &provider.receipts_root)
        && optional_field_matches(&stored.state_root, &provider.state_root)
}

fn optional_field_matches<T: Eq>(stored: &Option<T>, provider: &Option<T>) -> bool {
    matches!((stored, provider), (Some(stored), Some(provider)) if stored == provider)
        || stored.is_none()
}
