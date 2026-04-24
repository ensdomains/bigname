use bigname_storage::{BackfillRange, fail_backfill_range};
use serde_json::json;
use tracing::error;

use super::{BackfillBlockRange, BackfillJobRunConfig};

const MAX_FAILURE_ERROR_CHARS: usize = 2048;

pub(super) struct ReservedRangeFailure<'a> {
    pub(super) pool: &'a sqlx::PgPool,
    pub(super) reserved_range: &'a BackfillRange,
    pub(super) config: &'a BackfillJobRunConfig,
    pub(super) failure_reason: &'a str,
    pub(super) block_number: Option<i64>,
    pub(super) attempted_range: Option<BackfillBlockRange>,
    pub(super) phase: &'a str,
    pub(super) error: anyhow::Error,
}

pub(super) async fn record_reserved_range_failure(
    failure: ReservedRangeFailure<'_>,
) -> anyhow::Error {
    let ReservedRangeFailure {
        pool,
        reserved_range,
        config,
        failure_reason,
        block_number,
        attempted_range,
        phase,
        error,
    } = failure;
    let failure_metadata = json!({
        "phase": phase,
        "block_number": block_number,
        "attempted_range_start_block_number": attempted_range.map(|range| range.from_block),
        "attempted_range_end_block_number": attempted_range.map(|range| range.to_block),
        "range_start_block_number": reserved_range.range_start_block_number,
        "range_end_block_number": reserved_range.range_end_block_number,
        "checkpoint_block_number": reserved_range.checkpoint_block_number,
        "idempotency_key": &config.idempotency_key,
        "error": truncate_failure_error(&format!("{error:#}")),
    });

    match fail_backfill_range(
        pool,
        reserved_range.backfill_range_id,
        &config.lease_token,
        failure_reason,
        failure_metadata,
    )
    .await
    {
        Ok(_) => error.context("recorded persisted backfill failure state"),
        Err(fail_error) => {
            error!(
                service = "indexer",
                command = "backfill",
                backfill_range_id = reserved_range.backfill_range_id,
                failure_record_error = %fail_error,
                "failed to record persisted backfill failure state"
            );
            error.context(format!(
                "failed to record persisted backfill failure state: {fail_error:#}"
            ))
        }
    }
}

fn truncate_failure_error(error: &str) -> String {
    let mut truncated = error
        .chars()
        .take(MAX_FAILURE_ERROR_CHARS)
        .collect::<String>();
    if error.chars().count() > MAX_FAILURE_ERROR_CHARS {
        truncated.push_str("...[truncated]");
    }
    truncated
}
