//! The Live flags of `run`.
use clap::Args;

/// The largest publication lag `run` accepts. `/v1/status` reads stale once the published block
/// is older than the network head by more than the API's `DEFAULT_MAX_LAG_SECS`, 60 seconds.
/// With 12-second blocks each lagged block adds 12 seconds on top of the head's own age of up
/// to one block: 3 blocks stay under 60 seconds, 4 do not on a late probe. The API's
/// `the_largest_live_publication_lag_stays_ready_on_a_late_probe` reads this constant and fails if
/// it outgrows `DEFAULT_MAX_LAG_SECS`.
pub const MAX_LIVE_PUBLICATION_LAG_BLOCKS: u32 = 3;

#[derive(Clone, Copy, Debug, Args)]
pub(super) struct LiveArgs {
    /// Blocks below the newest block Live has loaded that it publishes as the readable head
    /// (0..=3). A reorg no deeper than this replaces only blocks no phase has read. 0 publishes
    /// the newest loaded block.
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_LIVE_PUBLICATION_LAG_BLOCKS",
        default_value_t = 0,
        value_parser = clap::value_parser!(u32).range(0..=i64::from(MAX_LIVE_PUBLICATION_LAG_BLOCKS))
    )]
    pub(super) live_publication_lag_blocks: u32,
}
