use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use clap::Args;

use crate::{
    config::CapacityConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
};

#[derive(Clone, Debug, Args)]
pub(super) struct CapacityArgs {
    /// Blocks per RPC Ingest window or redo window (1..=4096).
    #[arg(long, env = "BIGNAME_INGEST_BLOCKS_PER_BATCH", default_value_t = 256)]
    ingest_blocks_per_batch: u32,
    /// Maximum JSON-RPC calls in one request (1..=256); 1 sends standalone calls.
    #[arg(long, env = "BIGNAME_INGEST_RPC_BATCH_SIZE", default_value_t = 32)]
    ingest_rpc_batch_size: usize,
    /// Concurrent HTTP requests per Ingest/Live RPC provider (1..=32).
    #[arg(long, env = "BIGNAME_INGEST_RPC_MAX_IN_FLIGHT", default_value_t = 8)]
    ingest_rpc_max_in_flight: usize,
    /// What the RPC chain check compares on every RPC endpoint: full (chain id, and block 0
    /// where the chain pins a genesis) or chain-id-only, for local nodes run under a production
    /// chain's id.
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_RPC_CHAIN_CHECK",
        default_value = "full"
    )]
    rpc_chain_check: bigname_ingest::RpcChainCheck,
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
    /// Concurrent speculative Interpret preparations; 1 keeps serial execution.
    /// Results are validated and written in chain order. Requires the lookahead loader.
    #[arg(
        long,
        env = "BIGNAME_INTERPRET_SPECULATIVE_WORKERS",
        default_value_t = NonZeroU32::MIN
    )]
    interpret_speculative_workers: NonZeroU32,
    /// PostgreSQL statement timeout, in seconds, for the Interpret lookahead loader's reads.
    /// 0, the default, sets no timeout.
    #[arg(
        long,
        env = "BIGNAME_INTERPRET_LOOKAHEAD_STATEMENT_TIMEOUT_SECS",
        default_value_t = 0
    )]
    interpret_lookahead_statement_timeout_secs: u32,
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
    let ingest = bigname_ingest::IngestConfig::new(
        args.ingest_blocks_per_batch,
        args.ingest_rpc_batch_size,
        args.ingest_rpc_max_in_flight,
    )
    .map_err(|error| RunnerError::new(ErrorKind::Configuration, error.to_string()))?
    .with_rpc_chain_check(args.rpc_chain_check);
    Ok(CapacityConfig {
        ingest,
        interpret_blocks_per_batch: args.interpret_blocks_per_batch,
        interpret_speculative_workers: args.interpret_speculative_workers,
        interpret_force_full_state_loader: args.interpret_force_full_state_loader,
        interpret_lookahead_statement_timeout_secs: NonZeroU32::new(
            args.interpret_lookahead_statement_timeout_secs,
        ),
        database_max_bytes: args.database_max_bytes,
        minimum_free_disk_bytes: args.minimum_free_disk_bytes,
        writable_path: args.writable_path,
        poll_interval: Duration::from_millis(args.capacity_poll_ms),
        interpreter_state_cache_entries: args
            .interpreter_state_cache_entries
            .unwrap_or(bigname_interpret::DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES),
    })
}
