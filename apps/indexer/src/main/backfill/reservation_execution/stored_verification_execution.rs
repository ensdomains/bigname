use anyhow::Result;
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{BackfillJob, BackfillRange};

use crate::backfill::{
    BackfillJobRunConfig, BackfillTopicPlan,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    stored_verification::{StoredVerificationPlan, finalize_stored_verification},
};

use super::run_with_backfill_lease_heartbeat;

#[expect(clippy::too_many_arguments)]
pub(in crate::backfill) async fn finalize_reserved_stored_verification(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    job: &BackfillJob,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    verification_plan: &StoredVerificationPlan,
    failure_reason: &'static str,
) -> Result<()> {
    let result = run_with_backfill_lease_heartbeat(
        pool,
        active_range,
        config,
        finalize_stored_verification(
            pool,
            job,
            source_plan,
            topic_plan,
            config.range,
            verification_plan,
        ),
    )
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) => Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: active_range,
            config,
            failure_reason,
            block_number: Some(config.range.to_block),
            attempted_range: Some(config.range),
            phase: "stored_verification_finalize",
            error,
        })
        .await),
    }
}
