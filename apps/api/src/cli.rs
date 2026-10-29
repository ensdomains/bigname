use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, ensure};
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

use crate::ApiBoundsConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub(crate) enum RpcChainCheck {
    /// `eth_chainId` and the block 0 hash.
    Full,
    /// `eth_chainId` only, for local nodes that run under a production chain's id.
    ChainIdOnly,
}

const DEFAULT_RPC_CONNECT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_RPC_TOTAL_TIMEOUT_MS: u64 = 8_000;

#[derive(Parser, Debug)]
#[command(name = "bigname-api", about = "Read API process for bigname")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Serve(Box<ServeArgs>),
}

#[derive(Args, Debug)]
pub(crate) struct ServeArgs {
    #[arg(long, env = "BIGNAME_API_BIND_ADDR", default_value = "127.0.0.1:3000")]
    pub(crate) bind_addr: SocketAddr,
    #[arg(
        long,
        env = "BIGNAME_API_METRICS_BIND_ADDR",
        default_value = "127.0.0.1:9464"
    )]
    pub(crate) metrics_bind_addr: SocketAddr,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_API_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS",
        default_value_t = DEFAULT_RPC_CONNECT_TIMEOUT_MS
    )]
    pub(crate) rpc_connect_timeout_ms: u64,
    #[arg(
        long,
        env = "BIGNAME_API_RPC_TIMEOUT_MS",
        default_value_t = DEFAULT_RPC_TOTAL_TIMEOUT_MS
    )]
    pub(crate) rpc_timeout_ms: u64,
    /// What the startup RPC chain check compares; there is no mode that skips it.
    #[arg(
        long,
        env = "BIGNAME_API_RPC_CHAIN_CHECK",
        value_enum,
        default_value = "full"
    )]
    pub(crate) rpc_chain_check: RpcChainCheck,
    #[command(flatten)]
    pub(crate) bounds: ApiBoundsConfig,
    #[arg(
        long,
        env = "BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS",
        default_value_t = crate::state::DEFAULT_PHASE_HEARTBEAT_MAX_AGE_SECS
    )]
    pub(crate) phase_heartbeat_max_age_secs: i64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_TIMEOUT_MS",
        default_value_t = crate::v2::support::status_freshness::DEFAULT_PROVIDER_TIMEOUT_MS
    )]
    pub(crate) status_provider_timeout_ms: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_REFRESH_SECS",
        default_value_t = crate::v2::support::status_freshness::DEFAULT_PROVIDER_REFRESH_SECS
    )]
    pub(crate) status_provider_refresh_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_CACHE_TTL_SECS",
        default_value_t = crate::v2::support::status_freshness::DEFAULT_PROVIDER_CACHE_TTL_SECS
    )]
    pub(crate) status_provider_cache_ttl_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_MAX_BLOCK_LAG",
        default_value_t = crate::v2::support::status_freshness::DEFAULT_MAX_BLOCK_LAG
    )]
    pub(crate) status_max_block_lag: i64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_MAX_LAG_SECS",
        default_value_t = crate::v2::support::status_freshness::DEFAULT_MAX_LAG_SECS
    )]
    pub(crate) status_max_lag_secs: i64,
    /// How many blocks the family publication may trail the stored head and still be served.
    #[arg(
        long,
        env = "BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS",
        default_value_t = bigname_storage::PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
        value_parser = clap::value_parser!(i64).range(0..)
    )]
    pub(crate) publication_lag_tolerance_blocks: i64,
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}

impl ServeArgs {
    pub(crate) fn effective_lookup_chain_rpc_urls(&self) -> Result<bigname_lookup::ChainRpcUrls> {
        validate_rpc_timeouts(self.rpc_connect_timeout_ms, self.rpc_timeout_ms)?;
        bigname_lookup::ChainRpcUrls::from_entries(&self.chain_rpc_urls)?.with_http_timeouts(
            Duration::from_millis(self.rpc_connect_timeout_ms),
            Duration::from_millis(self.rpc_timeout_ms),
        )
    }
}

fn validate_rpc_timeouts(connect_timeout_ms: u64, total_timeout_ms: u64) -> Result<()> {
    ensure!(
        connect_timeout_ms > 0,
        "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS must be greater than zero"
    );
    ensure!(
        total_timeout_ms > 0,
        "BIGNAME_API_RPC_TIMEOUT_MS must be greater than zero"
    );
    ensure!(
        connect_timeout_ms < total_timeout_ms,
        "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS must be less than BIGNAME_API_RPC_TIMEOUT_MS"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn a_negative_publication_lag_tolerance_is_refused() {
        let error = super::Cli::try_parse_from([
            "bigname-api",
            "serve",
            "--publication-lag-tolerance-blocks=-1",
        ])
        .expect_err("a negative tolerance is refused");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(
            error
                .to_string()
                .contains("publication-lag-tolerance-blocks"),
            "{error}"
        );
    }
}
