use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use reqwest::Url;

mod bloom;
mod decode;
mod http_client;
mod request;
mod reth_db;
mod rpc;
mod types;

pub use bloom::bloom_contains;
pub use types::{
    Block, BlockBundle, HeadSnapshot, Log, Receipt, ResolvedBlock, Transaction, TransactionPayload,
};

use http_client::RecoveringHttpClient;
use request::validate_endpoint;
pub use reth_db::RETH_DB_OPENED_STORAGE_CHILDREN;
use reth_db::RethDbProvider;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

/// How many JSON-RPC batches or range log queries a provider keeps in flight.
///
/// Remote endpoints answer one round trip at a time; a window's hash lookups, header
/// lookups, range queries and per-transaction fetches are independent, so they overlap up
/// to this bound rather than running strictly in sequence.
pub const PROVIDER_PARALLELISM: usize = 8;

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

    pub async fn bundles(&self, blocks: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        match self {
            Self::JsonRpc(provider) => provider.bundles(blocks).await,
            Self::RethDb(provider) => provider.bundles(blocks).await,
        }
    }

    /// Whether ingest fetches the selected transactions one by one instead of whole blocks.
    ///
    /// Only the remote JSON-RPC provider pays for a block body it discards. The datadir
    /// reader already has the block in hand, so it keeps the bundle path.
    pub(crate) const fn fetches_transactions(&self) -> bool {
        matches!(self, Self::JsonRpc(_))
    }

    /// Range log lookup that does not re-resolve the blocks it touched.
    ///
    /// Returned logs are pinned to the hashes in `resolved`; the caller re-checks the
    /// whole window once more while loading [`Self::headers`].
    pub(crate) async fn range_logs(
        &self,
        resolved: &[ResolvedBlock],
        from: i64,
        to: i64,
        addresses: &[String],
        topics: &[String],
        topic1s: &[String],
    ) -> Result<Vec<Log>> {
        if from > to || topics.is_empty() {
            return Ok(Vec::new());
        }
        match self {
            Self::JsonRpc(provider) => {
                let logs = provider
                    .range_logs(from, to, addresses, topics, topic1s)
                    .await?;
                rpc::pin_logs_to_resolved(resolved, logs)
            }
            Self::RethDb(provider) => {
                let blocks = resolved
                    .iter()
                    .filter(|block| (from..=to).contains(&block.number))
                    .cloned()
                    .collect::<Vec<_>>();
                provider.logs(&blocks, addresses, topics, topic1s).await
            }
        }
    }

    /// Wide-range log lookup whose results are not yet pinned to any resolved window.
    ///
    /// Only the JSON-RPC provider prefetches: the datadir reader answers a window-sized
    /// query from local storage, so there is no round trip to amortise.
    pub(crate) async fn prefetch_range_logs(
        &self,
        from: i64,
        to: i64,
        addresses: &[String],
        topics: &[String],
        topic1s: &[String],
    ) -> Result<Option<Vec<Log>>> {
        match self {
            Self::JsonRpc(provider) => provider
                .range_logs(from, to, addresses, topics, topic1s)
                .await
                .map(Some),
            Self::RethDb(_) => Ok(None),
        }
    }

    pub(crate) async fn transaction_payloads(
        &self,
        hashes: &[String],
    ) -> Result<Vec<TransactionPayload>> {
        match self {
            Self::JsonRpc(provider) => provider.transaction_payloads(hashes).await,
            Self::RethDb(_) => {
                bail!("the Reth datadir provider fetches whole blocks, not single transactions")
            }
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
        topic1s: &[String],
    ) -> Result<Vec<Log>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .verification_logs(from_block, to_block, addresses, topics, topic1s)
                    .await
            }
            Self::RethDb(provider) => {
                let blocks = resolved
                    .iter()
                    .filter(|block| (from_block..=to_block).contains(&block.number))
                    .cloned()
                    .collect::<Vec<_>>();
                provider.logs(&blocks, addresses, topics, topic1s).await
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
