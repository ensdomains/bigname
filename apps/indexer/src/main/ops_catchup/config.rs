use std::path::PathBuf;

use anyhow::{Result, bail};

use crate::{
    backfill::BackfillJobRunOutcome, cli::OpsCatchupArgs, deployment_profile_from_manifest_root,
};

pub(crate) const DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS: i64 = 32;
pub(crate) const DEFAULT_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS: u64 = 30;
pub(crate) const DEFAULT_OPS_CATCHUP_LEASE_DURATION_SECS: u64 = 300;

#[rustfmt::skip]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpsCatchupConfig { pub(crate) deployment_profile: String, pub(crate) manifests_root: PathBuf, pub(crate) chunk_blocks: i64, pub(crate) follow: bool, pub(crate) follow_iterations: Option<u64>, pub(crate) follow_poll_interval_secs: u64, pub(crate) lease_duration_secs: u64, pub(crate) capacity: CapacityGuardConfig }

#[rustfmt::skip]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapacityGuardConfig { pub(crate) postgres_max_bytes: Option<u64>, pub(crate) min_writable_free_disk_bytes: u64, pub(crate) writable_free_disk_path: PathBuf, pub(crate) estimated_bytes_per_block: u64 }

#[rustfmt::skip]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct OpsCatchupOutcome { pub(crate) follow_iteration_count: u64, pub(crate) active_chain_count: usize, pub(crate) provider_configured_chain_count: usize, pub(crate) missing_provider_chain_count: usize, pub(crate) no_finalized_head_chain_count: usize, pub(crate) skipped_unknown_start_target_count: usize, pub(crate) skipped_future_target_count: usize, pub(crate) planned_chunk_count: usize, pub(crate) reused_completed_chunk_count: usize, pub(crate) capacity_check_count: usize, pub(crate) drained_job_count: usize, pub(crate) reserved_range_count: usize, pub(crate) completed_range_count: usize }

impl OpsCatchupConfig {
    pub(crate) fn from_args(args: &OpsCatchupArgs) -> Result<Self> {
        if args.chunk_blocks <= 0 {
            bail!(
                "ops catch-up chunk blocks must be positive, got {}",
                args.chunk_blocks
            );
        }
        if args.follow_iterations == Some(0) {
            bail!("ops catch-up follow iterations must be positive when supplied");
        }
        if args.follow_poll_interval_secs == 0 {
            bail!("ops catch-up follow poll interval must be positive");
        }
        if args.lease_duration_secs == 0 {
            bail!("ops catch-up lease duration must be positive");
        }

        Ok(Self {
            deployment_profile: args
                .deployment_profile
                .clone()
                .unwrap_or_else(|| deployment_profile_from_manifest_root(&args.manifests_root)),
            manifests_root: args.manifests_root.clone(),
            chunk_blocks: args.chunk_blocks,
            follow: args.follow,
            follow_iterations: args.follow_iterations,
            follow_poll_interval_secs: args.follow_poll_interval_secs,
            lease_duration_secs: args.lease_duration_secs,
            capacity: CapacityGuardConfig {
                postgres_max_bytes: args.postgres_max_bytes,
                min_writable_free_disk_bytes: args.min_writable_free_disk_bytes,
                writable_free_disk_path: args.writable_free_disk_path.clone(),
                estimated_bytes_per_block: args.estimated_bytes_per_block,
            },
        })
    }
}

impl OpsCatchupOutcome {
    pub(super) fn add_job(&mut self, outcome: &BackfillJobRunOutcome) {
        self.drained_job_count += 1;
        self.reserved_range_count += outcome.reserved_range_count;
        self.completed_range_count += outcome.completed_range_count;
    }
}
