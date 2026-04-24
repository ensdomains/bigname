use std::path::PathBuf;

use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

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
