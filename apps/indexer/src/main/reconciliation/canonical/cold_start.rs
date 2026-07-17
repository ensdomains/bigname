use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ChainCheckpoint,
    upsert_chain_lineage_blocks_recanonicalizing_orphaned as upsert_recanonicalized_lineage_blocks,
};

use crate::provider::ProviderBlock;

use super::super::{
    lineage::{provider_block_to_checkpoint_ref, provider_block_to_lineage_with_header_audit_mode},
    types::{CanonicalReconciliation, CanonicalReconciliationStatus, HeaderAuditMode},
};

pub(super) struct ColdStartCheckpoint<'a> {
    pub(super) canonical_hash: Option<&'a str>,
    pub(super) canonical_number: Option<i64>,
    is_cold_start: bool,
}

impl<'a> ColdStartCheckpoint<'a> {
    pub(super) fn resolve(
        checkpoint: &'a ChainCheckpoint,
        latest_head: &ProviderBlock,
        safe_and_finalized_heads: &'a [ProviderBlock],
    ) -> Self {
        let is_cold_start = checkpoint.canonical_block_hash.is_none();
        // Automatic bootstrap owns history only through its provider-finalized
        // boundary. A fresh checkpoint must reconcile every canonical block
        // above the lowest safe/finalized anchor before it publishes `latest`.
        let payload_anchor = if is_cold_start {
            safe_and_finalized_heads
                .iter()
                .filter(|anchor| anchor.block_number < latest_head.block_number)
                .min_by_key(|anchor| anchor.block_number)
        } else {
            None
        };
        Self {
            canonical_hash: checkpoint
                .canonical_block_hash
                .as_deref()
                .or_else(|| payload_anchor.map(|anchor| anchor.block_hash.as_str())),
            canonical_number: checkpoint
                .canonical_block_number
                .or_else(|| payload_anchor.map(|anchor| anchor.block_number)),
            is_cold_start,
        }
    }

    pub(super) async fn initialize_unanchored_latest(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        latest_head: &ProviderBlock,
        header_audit_mode: HeaderAuditMode,
    ) -> Result<Option<CanonicalReconciliation>> {
        if self.canonical_hash.is_some() {
            return Ok(None);
        }
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
        Ok(Some(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Initialized,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        }))
    }

    pub(super) fn reconciliation_status(
        &self,
        resumed_status: CanonicalReconciliationStatus,
    ) -> CanonicalReconciliationStatus {
        if self.is_cold_start {
            CanonicalReconciliationStatus::Initialized
        } else {
            resumed_status
        }
    }

    pub(super) fn is_cold_start(&self) -> bool {
        self.is_cold_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_uses_latched_bootstrap_finalized_head_when_finality_advances() {
        let checkpoint = ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: None,
            canonical_block_number: None,
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        };
        let latest = block("0x42", 42);
        let current_safe = block("0x40", 40);
        let current_finalized = block("0x39", 39);
        let latched_bootstrap_finalized = block("0x38", 38);

        let anchors = [current_safe, current_finalized, latched_bootstrap_finalized];
        let resolved = ColdStartCheckpoint::resolve(&checkpoint, &latest, &anchors);

        assert_eq!(resolved.canonical_hash, Some("0x38"));
        assert_eq!(resolved.canonical_number, Some(38));
        assert!(resolved.is_cold_start());
    }

    fn block(block_hash: &str, block_number: i64) -> ProviderBlock {
        ProviderBlock {
            block_hash: block_hash.to_owned(),
            parent_hash: Some(format!("0x{}", block_number - 1)),
            block_number,
            block_timestamp_unix_secs: block_number,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }
    }
}
