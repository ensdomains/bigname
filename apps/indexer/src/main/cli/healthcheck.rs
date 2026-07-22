use std::path::PathBuf;

use bigname_storage::DatabaseConfig;
use clap::Args;

#[derive(Args, Debug)]
pub(crate) struct HealthcheckArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests/mainnet"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(long, env = "BIGNAME_HEARTBEAT_INSTANCE_ID")]
    pub(crate) heartbeat_instance_id: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_HEARTBEAT_MAX_AGE_SECS",
        default_value_t = 20_i64
    )]
    pub(crate) heartbeat_max_age_secs: i64,
}
