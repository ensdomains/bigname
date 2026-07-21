use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, ensure};
use bigname_execution::ChainRpcUrls;
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

use crate::ApiBoundsConfig;

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
    Serve(ServeArgs),
    PrintOpenapi,
}

#[derive(Args, Debug)]
pub(crate) struct ServeArgs {
    #[arg(long, env = "BIGNAME_API_BIND_ADDR", default_value = "127.0.0.1:3000")]
    pub(crate) bind_addr: SocketAddr,
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
    #[command(flatten)]
    pub(crate) bounds: ApiBoundsConfig,
    #[arg(
        long,
        env = "BIGNAME_API_HEARTBEAT_MAX_AGE_SECS",
        default_value_t = 20_i64
    )]
    pub(crate) heartbeat_max_age_secs: i64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_TIMEOUT_MS",
        default_value_t = crate::status_freshness::DEFAULT_PROVIDER_TIMEOUT_MS
    )]
    pub(crate) status_provider_timeout_ms: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_REFRESH_SECS",
        default_value_t = crate::status_freshness::DEFAULT_PROVIDER_REFRESH_SECS
    )]
    pub(crate) status_provider_refresh_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_PROVIDER_CACHE_TTL_SECS",
        default_value_t = crate::status_freshness::DEFAULT_PROVIDER_CACHE_TTL_SECS
    )]
    pub(crate) status_provider_cache_ttl_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_MAX_BLOCK_LAG",
        default_value_t = crate::status_freshness::DEFAULT_MAX_BLOCK_LAG
    )]
    pub(crate) status_max_block_lag: i64,
    #[arg(
        long,
        env = "BIGNAME_API_STATUS_MAX_LAG_SECS",
        default_value_t = crate::status_freshness::DEFAULT_MAX_LAG_SECS
    )]
    pub(crate) status_max_lag_secs: i64,
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}

impl ServeArgs {
    pub(crate) fn effective_chain_rpc_urls(&self) -> Result<ChainRpcUrls> {
        ensure!(
            self.rpc_connect_timeout_ms > 0,
            "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            self.rpc_timeout_ms > 0,
            "BIGNAME_API_RPC_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            self.rpc_connect_timeout_ms <= self.rpc_timeout_ms,
            "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS must not exceed BIGNAME_API_RPC_TIMEOUT_MS"
        );
        ChainRpcUrls::from_entries(&self.chain_rpc_urls)?.with_http_timeouts(
            Duration::from_millis(self.rpc_connect_timeout_ms),
            Duration::from_millis(self.rpc_timeout_ms),
        )
    }
}
