use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
pub use reth_db::RETH_DB_OPENED_STORAGE_CHILDREN;
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
    request_attempts: Arc<AtomicUsize>,
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: validate_endpoint(endpoint)?,
            client: RecoveringHttpClient::new(CONNECT_TIMEOUT, REQUEST_TIMEOUT)?,
            request_attempts: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(super) fn request_attempts(&self) -> usize {
        self.request_attempts.load(Ordering::Relaxed)
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

    /// Lowest block this source can still serve, when the source can report one.
    ///
    /// Only the datadir reader answers: an RPC endpoint owns its retention behind the
    /// wire, so a caller cannot read that boundary out of it.
    pub async fn earliest_available_block(&self) -> Result<Option<i64>> {
        match self {
            Self::JsonRpc(_) => Ok(None),
            Self::RethDb(provider) => provider.earliest_available_block().await.map(Some),
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

    pub(crate) async fn verification_blocks(
        &self,
        from_block: i64,
        to_block: i64,
    ) -> Result<Vec<ResolvedBlock>> {
        match self {
            Self::JsonRpc(_) => self.resolve(&[to_block]).await,
            Self::RethDb(_) => {
                self.resolve(&(from_block..=to_block).collect::<Vec<_>>())
                    .await
            }
        }
    }

    pub(crate) async fn verification_logs(
        &self,
        resolved: &[ResolvedBlock],
        from_block: i64,
        to_block: i64,
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Log>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .verification_logs(from_block, to_block, addresses, topics)
                    .await
            }
            Self::RethDb(provider) => {
                let blocks = resolved
                    .iter()
                    .filter(|block| (from_block..=to_block).contains(&block.number))
                    .cloned()
                    .collect::<Vec<_>>();
                provider.logs(&blocks, addresses, topics).await
            }
        }
    }

    pub(crate) fn verification_rpc_request_attempts(&self) -> usize {
        match self {
            Self::JsonRpc(provider) => provider.request_attempts(),
            Self::RethDb(_) => 0,
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
