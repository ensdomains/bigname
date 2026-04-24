use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

/// Persisted lifecycle state for backfill jobs and range checkpoints.
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

/// Child range bounds for a bounded backfill job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillRangeSpec {
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
}

/// Immutable job creation contract. Empty `ranges` creates one range covering
/// the declared job bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJobCreate {
    pub deployment_profile: String,
    pub chain_id: String,
    pub source_identity: Value,
    pub scan_mode: String,
    pub range_start_block_number: i64,
    pub range_end_block_number: i64,
    pub idempotency_key: String,
    pub ranges: Vec<BackfillRangeSpec>,
}

/// Persisted backfill job snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJob {
    pub backfill_job_id: i64,
    pub deployment_profile: String,
    pub chain_id: String,
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

/// Persisted child range checkpoint snapshot.
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

/// Job plus child ranges returned by idempotent creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillJobRecord {
    pub job: BackfillJob,
    pub ranges: Vec<BackfillRange>,
}
