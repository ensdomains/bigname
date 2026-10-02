use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use reqwest::Url;
use tokio::sync::Semaphore;

use crate::IngestConfig;

mod bloom;
mod chain_check;
#[cfg(test)]
mod chain_check_tests;
mod counters;
mod decode;
mod http_client;
mod request;
mod reth_db;
mod rpc;
#[cfg(test)]
mod tuning_tests;
mod types;

pub use bloom::bloom_contains;
pub use chain_check::{
    ExpectedRpcChain, ObservedRpcChain, RPC_CHAIN_RECHECK_INTERVAL, RpcChainCheck, RpcChainMismatch,
};
pub use counters::{RpcCount, RpcCountKind, RpcCounters};
pub use types::{
    Block, BlockBundle, HeadSnapshot, Log, Receipt, ResolvedBlock, Transaction, TransactionPayload,
};

use chain_check::{ChainGuard, chain_mismatch_in};
use counters::SourceCounters;
use http_client::RecoveringHttpClient;
use request::validate_endpoint;
pub use reth_db::RETH_DB_OPENED_STORAGE_CHILDREN;
pub(crate) use reth_db::RethDbProvider;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

/// Existing range-query parallelism for the direct database reader.
pub const PROVIDER_PARALLELISM: usize = 8;

#[derive(Clone)]
pub enum ChainProvider {
    JsonRpc(JsonRpcProvider),
    RethDb(RethDbProvider),
}

/// Payload reads already selected by block density, with whole-block work left for fetching.
pub(crate) struct SelectedPayloads {
    pub transactions: Vec<TransactionPayload>,
    pub bundle_blocks: Vec<ResolvedBlock>,
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Url,
    client: RecoveringHttpClient,
    request_attempts: Arc<AtomicUsize>,
    counters: SourceCounters,
    config: IngestConfig,
    in_flight: Arc<Semaphore>,
    chain_guard: Option<Arc<ChainGuard>>,
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::with_config(endpoint, IngestConfig::default())
    }

    pub(super) fn with_config(endpoint: &str, config: IngestConfig) -> Result<Self> {
        Ok(Self {
            endpoint: validate_endpoint(endpoint)?,
            client: RecoveringHttpClient::new(CONNECT_TIMEOUT, REQUEST_TIMEOUT)?,
            request_attempts: Arc::new(AtomicUsize::new(0)),
            counters: SourceCounters::default(),
            config,
            in_flight: Arc::new(Semaphore::new(config.rpc_max_in_flight())),
            chain_guard: None,
        })
    }

    /// Checks the endpoint against `expected` before its first request, and again once the
    /// last check is older than [`RPC_CHAIN_RECHECK_INTERVAL`] or the HTTP client was rebuilt.
    #[must_use]
    pub fn with_chain_check(self, expected: ExpectedRpcChain) -> Self {
        self.with_chain_check_every(expected, RPC_CHAIN_RECHECK_INTERVAL)
    }

    fn with_chain_check_every(mut self, expected: ExpectedRpcChain, interval: Duration) -> Self {
        self.chain_guard = Some(Arc::new(ChainGuard::new(expected, interval)));
        self
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

    /// A provider for one configured source. An RPC endpoint is guarded by the RPC chain check
    /// when `config` carries a mode.
    pub(crate) fn with_config(
        chain_id: &str,
        source_key: &str,
        kind: &str,
        endpoint: &str,
        recorded_genesis: Option<&str>,
        config: IngestConfig,
    ) -> Result<Self> {
        match normalized_kind(kind) {
            ProviderKind::Rpc => {
                let provider = JsonRpcProvider::with_config(endpoint, config)?;
                let provider = match config.rpc_chain_check() {
                    Some(mode) => provider.with_chain_check(
                        ExpectedRpcChain::new(chain_id, source_key, mode)?
                            .with_recorded_genesis(recorded_genesis),
                    ),
                    None => provider,
                };
                Ok(Self::JsonRpc(provider))
            }
            _ => Self::new(chain_id, kind, endpoint),
        }
    }

    #[must_use]
    pub(crate) fn with_chain_check(self, expected: ExpectedRpcChain) -> Self {
        match self {
            Self::JsonRpc(provider) => Self::JsonRpc(provider.with_chain_check(expected)),
            Self::RethDb(provider) => Self::RethDb(provider),
        }
    }

    #[must_use]
    pub(crate) fn with_rpc_counters(self, counters: SourceCounters) -> Self {
        match self {
            Self::JsonRpc(provider) => Self::JsonRpc(JsonRpcProvider {
                counters,
                ..provider
            }),
            Self::RethDb(provider) => Self::RethDb(provider),
        }
    }

    pub(crate) fn query_parallelism(&self) -> usize {
        match self {
            Self::JsonRpc(provider) => provider.config.rpc_max_in_flight(),
            Self::RethDb(_) => PROVIDER_PARALLELISM,
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

    /// Returns sparse payloads and the dense blocks that should retain bundle assembly.
    pub(crate) async fn transaction_payloads(
        &self,
        resolved: &[ResolvedBlock],
        logs: &[Log],
    ) -> Result<SelectedPayloads> {
        match self {
            Self::JsonRpc(provider) => {
                let hashes = logs
                    .iter()
                    .map(|log| log.transaction_hash.clone())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                provider
                    .transaction_payloads(&hashes)
                    .await
                    .map(|transactions| SelectedPayloads {
                        transactions,
                        bundle_blocks: Vec::new(),
                    })
            }
            Self::RethDb(provider) => provider.transaction_payloads(resolved, logs).await,
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

/// Whether `endpoint` is a well-formed URL with a host for a transport other than HTTP(S), such
/// as a fixture placeholder. A malformed URL, or a bare `host:port` that parses with the host as
/// its scheme, is not: the JSON-RPC provider refuses it.
pub fn names_another_transport(endpoint: &str) -> bool {
    reqwest::Url::parse(endpoint)
        .is_ok_and(|url| url.has_host() && !matches!(url.scheme(), "http" | "https"))
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

/// Runs the RPC chain check once against `endpoint`. Transport failures are retried with the
/// provider's usual backoff before they are returned.
pub async fn verify_rpc_chain(
    endpoint: &str,
    expected: &ExpectedRpcChain,
) -> crate::Result<ObservedRpcChain> {
    let context = format!(
        "RPC chain check for chain {} source {}",
        expected.chain(),
        expected.source_key()
    );
    let provider = JsonRpcProvider::new(endpoint).map_err(|error| {
        crate::IngestError::with_source(crate::ErrorKind::Configuration, context.as_str(), error)
    })?;
    provider.verify_chain(expected).await.map_err(|error| {
        let redacted = redacted_text(&error, endpoint);
        match provider_error(&context, error) {
            error if error.rpc_chain_mismatch().is_some() => error,
            error => crate::IngestError::with_source(
                error.kind(),
                context.as_str(),
                anyhow::anyhow!(redacted),
            ),
        }
    })
}

/// Startup errors and retry warnings never carry the endpoint's credentials, path or query: a
/// provider's HTTP error body or JSON-RPC error message, which can echo any of them, is left out.
fn redacted_text(error: &anyhow::Error, endpoint: &str) -> String {
    let mut rendered = provider_error_text(error);
    for cause in error.chain() {
        if let Some(http) = cause.downcast_ref::<request::HttpStatusError>() {
            rendered = rendered.replace(&http.to_string(), &http.without_body());
        }
        if let Some(rpc) = cause.downcast_ref::<request::JsonRpcError>() {
            rendered = rendered.replace(&rpc.to_string(), &rpc.without_message());
        }
    }
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return rendered.replace(endpoint.trim(), "<redacted-endpoint>");
    };
    let mut secrets = [
        endpoint.trim(),
        url.as_str(),
        url.path(),
        url.query().unwrap_or_default(),
        url.username(),
        url.password().unwrap_or_default(),
    ]
    .into_iter()
    .filter(|secret| secret.len() > 1)
    .collect::<Vec<_>>();
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    for secret in secrets {
        rendered = rendered.replace(secret, "<redacted>");
    }
    rendered
}

pub type SharedProvider = Arc<ChainProvider>;

pub fn provider_error(context: &str, error: anyhow::Error) -> crate::IngestError {
    if let Some(mismatch) = chain_mismatch_in(&error) {
        return crate::IngestError::with_source(
            crate::ErrorKind::Configuration,
            context,
            mismatch.clone(),
        );
    }
    let kind = if is_retryable(&error) {
        crate::ErrorKind::Transient
    } else {
        crate::ErrorKind::DataIntegrity
    };
    crate::IngestError::with_source(kind, context, anyhow::anyhow!(provider_error_text(&error)))
}
