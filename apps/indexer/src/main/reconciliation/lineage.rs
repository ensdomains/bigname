use bigname_storage::{CanonicalityState, ChainLineageBlock, CheckpointBlockRef};

use crate::{
    provider::{ProviderBlock, ProviderHeadSnapshot},
    runtime::IntakeChainTask,
};

use super::types::{CanonicalReconciliation, HeadChangeSet};

pub(crate) fn provider_block_to_lineage(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}

pub(crate) fn lineage_block_to_provider(block: &ChainLineageBlock) -> ProviderBlock {
    ProviderBlock {
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp_unix_secs: block.block_timestamp.unix_timestamp(),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
    }
}

pub(crate) fn provider_block_to_checkpoint_ref(block: &ProviderBlock) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
    }
}

pub(crate) fn head_change_set(
    task: &IntakeChainTask,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> HeadChangeSet {
    let next_safe = heads.safe.as_ref().map(provider_block_to_checkpoint_ref);
    let next_finalized = heads
        .finalized
        .as_ref()
        .map(provider_block_to_checkpoint_ref);

    HeadChangeSet {
        canonical_head_changed: checkpoint_ref_changed(
            task.checkpoint.canonical_block_hash.as_deref(),
            task.checkpoint.canonical_block_number,
            canonical.canonical.as_ref(),
        ),
        safe_head_changed: checkpoint_ref_changed(
            task.checkpoint.safe_block_hash.as_deref(),
            task.checkpoint.safe_block_number,
            next_safe.as_ref(),
        ),
        finalized_head_changed: checkpoint_ref_changed(
            task.checkpoint.finalized_block_hash.as_deref(),
            task.checkpoint.finalized_block_number,
            next_finalized.as_ref(),
        ),
    }
}

pub(crate) fn checkpoint_ref_changed(
    current_hash: Option<&str>,
    current_number: Option<i64>,
    next: Option<&CheckpointBlockRef>,
) -> bool {
    let Some(next) = next else {
        return false;
    };

    current_hash != Some(next.block_hash.as_str()) || current_number != Some(next.block_number)
}
