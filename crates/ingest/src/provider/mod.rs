use std::{sync::Arc, time::Duration};

use anyhow::{Result, bail};
use reqwest::Url;

mod decode;
mod http_client;
mod request;
mod reth_db;
mod rpc;
mod types;

pub use types::{Block, BlockBundle, HeadSnapshot, Log, Receipt, ResolvedBlock, Transaction};

use http_client::RecoveringHttpClient;
use request::validate_endpoint;
use reth_db::RethDbProvider;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub enum ChainProvider {
    JsonRpc(JsonRpcProvider),
    RethDb(RethDbProvider),
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Url,
    client: RecoveringHttpClient,
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: validate_endpoint(endpoint)?,
            client: RecoveringHttpClient::new(CONNECT_TIMEOUT, REQUEST_TIMEOUT)?,
        })
    }
}

impl ChainProvider {
    pub fn new(chain_id: &str, kind: &str, endpoint: &str) -> Result<Self> {
        match normalized_kind(kind) {
            ProviderKind::Rpc => Ok(Self::JsonRpc(JsonRpcProvider::new(endpoint)?)),
            ProviderKind::Reth => Ok(Self::RethDb(RethDbProvider::new(chain_id, endpoint)?)),
            ProviderKind::Coinbase => bail!("Coinbase SQL is not a chain block provider"),
        }
    }

    pub async fn heads(&self) -> Result<HeadSnapshot> {
        match self {
            Self::JsonRpc(provider) => provider.heads().await,
            Self::RethDb(provider) => provider.heads().await,
        }
    }

    pub async fn resolve(&self, numbers: &[i64]) -> Result<Vec<ResolvedBlock>> {
        match self {
            Self::JsonRpc(provider) => provider.resolve(numbers).await,
            Self::RethDb(provider) => provider.resolve(numbers).await,
        }
    }

    pub async fn headers(&self, blocks: &[ResolvedBlock]) -> Result<Vec<Block>> {
        match self {
            Self::JsonRpc(provider) => provider.headers(blocks).await,
            Self::RethDb(provider) => provider.headers(blocks).await,
        }
    }

    pub async fn logs(
        &self,
        blocks: &[ResolvedBlock],
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Log>> {
        match self {
            Self::JsonRpc(provider) => provider.logs(blocks, addresses, topics).await,
            Self::RethDb(provider) => provider.logs(blocks, addresses, topics).await,
        }
    }

    pub async fn bundles(&self, blocks: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        match self {
            Self::JsonRpc(provider) => provider.bundles(blocks).await,
            Self::RethDb(provider) => provider.bundles(blocks).await,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderKind {
    Rpc,
    Reth,
    Coinbase,
}

pub fn normalized_kind(kind: &str) -> ProviderKind {
    match kind.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "reth" | "reth_db" => ProviderKind::Reth,
        "coinbase" | "coinbase_sql" | "cdp_sql" => ProviderKind::Coinbase,
        _ => ProviderKind::Rpc,
    }
}

pub fn is_retryable(error: &anyhow::Error) -> bool {
    request::retryable(error)
}

fn provider_error_text(error: &anyhow::Error) -> String {
    let mut rendered = format!("{error:#}");
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<reqwest::Error>()
            && let Some(url) = error.url()
        {
            let host = url.host_str().unwrap_or("<redacted-host>");
            rendered = rendered.replace(url.as_str(), &format!("{}://{host}", url.scheme()));
        }
    }
    rendered
}

pub type SharedProvider = Arc<ChainProvider>;

pub fn provider_error(context: &str, error: anyhow::Error) -> crate::IngestError {
    let kind = if is_retryable(&error) {
        crate::ErrorKind::Transient
    } else {
        crate::ErrorKind::DataIntegrity
    };
    crate::IngestError::with_source(kind, context, anyhow::anyhow!(provider_error_text(&error)))
}
