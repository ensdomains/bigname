mod complete;
mod coverage_facts;
mod create;
mod decode;
mod fail;
mod lease;
mod read;
mod sql;
mod types;
mod validate;

pub use complete::{
    complete_backfill_job, complete_backfill_range, complete_backfill_range_recording_coverage,
};
pub use coverage_facts::{
    BackfillCoverageFactDerivation, BackfillCoverageFactScope, BackfillCoverageFactWrite,
    load_backfill_coverage_fact_counts, write_backfill_coverage_facts,
};
pub use create::create_backfill_job;
pub use fail::{fail_backfill_job, fail_backfill_range};
pub use lease::{advance_backfill_range, reserve_backfill_range};
pub use read::{
    load_backfill_job, load_backfill_jobs_intersecting_range, load_backfill_ranges,
    load_completed_backfill_jobs_intersecting_range,
};
pub use types::{
    BackfillJob, BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec,
};

#[cfg(test)]
mod tests;
