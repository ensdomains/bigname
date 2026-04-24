#[path = "backfill/failure_recording.rs"]
mod failure_recording;
#[path = "backfill/fetching.rs"]
mod fetching;
#[path = "backfill/range_resolution.rs"]
mod range_resolution;
#[path = "backfill/reservation_execution.rs"]
mod reservation_execution;
#[path = "backfill/selection.rs"]
mod selection;

use anyhow::{Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use sqlx::types::time::OffsetDateTime;

#[allow(unused_imports)]
pub(crate) use fetching::run_hash_pinned_backfill_range;
pub(crate) use reservation_execution::run_resumable_hash_pinned_backfill_job;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackfillBlockRange {
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
}

impl BackfillBlockRange {
    pub(crate) fn new(from_block: i64, to_block: i64) -> Result<Self> {
        if from_block < 0 {
            bail!("backfill from block cannot be negative: {from_block}");
        }
        if to_block < 0 {
            bail!("backfill to block cannot be negative: {to_block}");
        }
        if from_block > to_block {
            bail!("backfill range start {from_block} is after end {to_block}");
        }

        Ok(Self {
            from_block,
            to_block,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunConfig {
    pub(crate) deployment_profile: String,
    pub(crate) idempotency_key: String,
    pub(crate) range: BackfillBlockRange,
    pub(crate) lease_owner: String,
    pub(crate) lease_token: String,
    pub(crate) lease_expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunOutcome {
    pub(crate) backfill_job_id: i64,
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) idempotency_key: String,
    pub(crate) reserved_range_count: usize,
    pub(crate) completed_range_count: usize,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

impl BackfillJobRunOutcome {
    fn new(
        backfill_job_id: i64,
        source_plan: &WatchedSourceSelectorPlan,
        config: &BackfillJobRunConfig,
    ) -> Self {
        Self {
            backfill_job_id,
            chain: source_plan.watched_chain_plan.chain.clone(),
            from_block: config.range.from_block,
            to_block: config.range.to_block,
            idempotency_key: config.idempotency_key.clone(),
            reserved_range_count: 0,
            completed_range_count: 0,
            resolved_block_count: 0,
            raw_block_count: 0,
            raw_transaction_count: 0,
            raw_receipt_count: 0,
            raw_log_count: 0,
            raw_code_hash_count: 0,
        }
    }

    fn add_range_outcome(&mut self, outcome: &BackfillOutcome) {
        self.resolved_block_count += outcome.resolved_block_count;
        self.raw_block_count += outcome.raw_block_count;
        self.raw_transaction_count += outcome.raw_transaction_count;
        self.raw_receipt_count += outcome.raw_receipt_count;
        self.raw_log_count += outcome.raw_log_count;
        self.raw_code_hash_count += outcome.raw_code_hash_count;
    }
}
