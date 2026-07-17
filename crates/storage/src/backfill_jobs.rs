mod complete;
mod coverage_facts;
mod create;
mod decode;
mod fail;
mod lease;
mod read;
mod sql;
mod topic_evidence;
mod types;
mod validate;

pub use complete::{
    complete_backfill_job, complete_backfill_range, complete_backfill_range_recording_coverage,
};
pub use coverage_facts::{
    BackfillCoverageFactDerivation, BackfillCoverageFactScope, BackfillCoverageFactWrite,
    load_backfill_coverage_fact_counts, write_backfill_coverage_facts,
};
pub use create::{
    create_backfill_job, create_generation_scoped_backfill_job,
    ensure_and_load_raw_log_retention_generation,
};
pub use fail::{fail_backfill_job, fail_backfill_range};
pub use lease::{advance_backfill_range, reserve_backfill_range};
pub use read::{
    load_backfill_job, load_backfill_ranges, load_completed_backfill_jobs_intersecting_range,
};
pub use topic_evidence::{
    BackfillTopicCoverageRequirement, BackfillTopicCoverageViolation,
    MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS, find_backfill_topic_coverage_violations,
    materialize_completed_backfill_topic_evidence,
};
pub use types::{
    BackfillJob, BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec,
};

#[cfg(test)]
mod tests;
