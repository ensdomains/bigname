use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use clap::Args;

use crate::{
    config::CapacityConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
};

#[derive(Clone, Debug, Args)]
pub(super) struct CapacityArgs {
    /// Always restore Interpret's prior state with the full-state loader instead of
    /// letting each chain choose the per-batch ENSv1 lookahead loader automatically.
    #[arg(long, env = "BIGNAME_INTERPRET_FORCE_FULL_STATE_LOADER")]
    interpret_force_full_state_loader: bool,
    /// Canonical blocks per Interpret batch; at least 1. Smaller batches use less memory.
    #[arg(
        long,
        env = "BIGNAME_INTERPRET_BLOCKS_PER_BATCH",
        default_value_t = bigname_interpret::DEFAULT_INTERPRET_BLOCKS_PER_BATCH
    )]
    interpret_blocks_per_batch: NonZeroU32,
    #[arg(long, env = "BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES")]
    interpreter_state_cache_entries: Option<usize>,
    #[arg(long, env = "BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES")]
    database_max_bytes: Option<u64>,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES",
        default_value_t = 0
    )]
    minimum_free_disk_bytes: u64,

    #[arg(long, env = "BIGNAME_PHASE_RUNNER_WRITABLE_PATH", default_value = ".")]
    writable_path: PathBuf,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_CAPACITY_POLL_MS",
        default_value_t = 5_000
    )]
    capacity_poll_ms: u64,
}

pub(super) fn resolve_capacity(args: CapacityArgs) -> RunnerResult<CapacityConfig> {
    if args.capacity_poll_ms == 0 {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "capacity poll interval must be positive",
        ));
    }
    Ok(CapacityConfig {
        interpret_blocks_per_batch: args.interpret_blocks_per_batch,
        interpret_force_full_state_loader: args.interpret_force_full_state_loader,
        database_max_bytes: args.database_max_bytes,
        minimum_free_disk_bytes: args.minimum_free_disk_bytes,
        writable_path: args.writable_path,
        poll_interval: Duration::from_millis(args.capacity_poll_ms),
        interpreter_state_cache_entries: args
            .interpreter_state_cache_entries
            .unwrap_or(bigname_interpret::DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES),
    })
}
