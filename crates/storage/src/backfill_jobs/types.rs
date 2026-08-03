use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

/// Persisted lifecycle state for historical backfill jobs and range checkpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillLifecycleStatus {
    Pending,
    Reserved,
    Running,
    Completed,
    Failed,
}

impl BackfillLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Reserved => "reserved",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "reserved" => Ok(Self::Reserved),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => bail!("unknown backfill lifecycle status {value}"),
        }
    }
}

/// Read-only snapshot of a historical backfill job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJob {
    pub backfill_job_id: i64,
    pub deployment_profile: String,
    pub chain_id: String,
    pub raw_log_retention_generation: i64,
    pub source_identity: Value,
    pub scan_mode: String,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub idempotency_key: String,
    pub status: BackfillLifecycleStatus,
    pub failure_reason: Option<String>,
    pub failure_metadata: Value,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

/// Read-only snapshot of a historical backfill range checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillRange {
    pub backfill_range_id: i64,
    pub backfill_job_id: i64,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub checkpoint_block_number: i64,
    pub status: BackfillLifecycleStatus,
    pub lease_token: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub attempt_count: i64,
    pub failure_reason: Option<String>,
    pub failure_metadata: Value,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

/// Historical job plus its child ranges, used by the surviving worker inspection command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJobRecord {
    pub job: BackfillJob,
    pub ranges: Vec<BackfillRange>,
}
