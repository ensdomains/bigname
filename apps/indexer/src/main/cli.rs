use std::path::PathBuf;

use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

use crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_MAX_BLOCKS;
use crate::ops_catchup::{
    DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS, DEFAULT_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS,
    DEFAULT_OPS_CATCHUP_LEASE_DURATION_SECS,
};

#[derive(Parser, Debug)]
#[command(
    name = "bigname-indexer",
    about = "Bootstrap indexer process for bigname"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Run(RunArgs),
    Backfill(BackfillArgs),
    OpsCatchup(OpsCatchupArgs),
    Replay(ReplayArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    pub(crate) poll_interval_secs: u64,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_MAX_BLOCKS",
        default_value_t = DEFAULT_BOOTSTRAP_BACKFILL_MAX_BLOCKS
    )]
    pub(crate) bootstrap_backfill_max_blocks: i64,
}

#[derive(Args, Debug)]
pub(crate) struct BackfillArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long)]
    pub(crate) from_block: i64,
    #[arg(long)]
    pub(crate) to_block: i64,
    #[arg(long)]
    pub(crate) idempotency_key: String,
    #[arg(long)]
    pub(crate) deployment_profile: Option<String>,
    #[arg(long, conflicts_with = "watch_targets")]
    pub(crate) source_family: Option<String>,
    #[arg(
        long = "watch-target",
        value_name = "CONTRACT_INSTANCE_ID",
        conflicts_with = "source_family"
    )]
    pub(crate) watch_targets: Vec<sqlx::types::Uuid>,
    #[arg(long)]
    pub(crate) lease_owner: Option<String>,
    #[arg(long)]
    pub(crate) lease_token: Option<String>,
    #[arg(long, default_value_t = 300_u64)]
    pub(crate) lease_duration_secs: u64,
}

#[derive(Args, Debug)]
pub(crate) struct OpsCatchupArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_CHUNK_BLOCKS",
        default_value_t = DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS
    )]
    pub(crate) chunk_blocks: i64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW",
        default_value_t = false
    )]
    pub(crate) follow: bool,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW_ITERATIONS",
        requires = "follow"
    )]
    pub(crate) follow_iterations: Option<u64>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS",
        default_value_t = DEFAULT_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS
    )]
    pub(crate) follow_poll_interval_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_LEASE_DURATION_SECS",
        default_value_t = DEFAULT_OPS_CATCHUP_LEASE_DURATION_SECS
    )]
    pub(crate) lease_duration_secs: u64,
    #[arg(long, env = "BIGNAME_INDEXER_OPS_CATCHUP_POSTGRES_MAX_BYTES")]
    pub(crate) postgres_max_bytes: Option<u64>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_MIN_FREE_DISK_BYTES",
        default_value_t = 0_u64
    )]
    pub(crate) min_writable_free_disk_bytes: u64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_DISK_PATH",
        default_value = "."
    )]
    pub(crate) writable_free_disk_path: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_ESTIMATED_BYTES_PER_BLOCK",
        default_value_t = 0_u64
    )]
    pub(crate) estimated_bytes_per_block: u64,
}

#[derive(Args, Debug)]
pub(crate) struct ReplayArgs {
    #[command(subcommand)]
    pub(crate) command: ReplayCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ReplayCommand {
    NormalizedEvents(ReplayNormalizedEventsArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ReplayNormalizedEventsArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: String,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long, requires = "to_block", conflicts_with = "block_hashes")]
    pub(crate) from_block: Option<i64>,
    #[arg(long, requires = "from_block", conflicts_with = "block_hashes")]
    pub(crate) to_block: Option<i64>,
    #[arg(long = "block-hash", value_name = "BLOCK_HASH")]
    pub(crate) block_hashes: Vec<String>,
}
