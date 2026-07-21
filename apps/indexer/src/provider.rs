use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::Url;
#[cfg(test)]
use serde_json::json;

#[cfg(test)]
use decode::parse_hex_i64;

mod block_transaction;
mod code;
mod decode;
mod error;
mod http_client;
mod logs_receipts;
mod ops;
mod payload_cache;
#[cfg(test)]
mod pool_recovery_tests;
mod proof;
mod request;
mod reth_db;
mod transaction_receipts;
mod types;

pub use reth_db::RethDbProvider;
#[allow(unused_imports)]
pub use types::ProviderBlockTag;
use types::ProviderHeadHashSnapshot;
pub use types::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockCodeHashProofRequest,
    ProviderBlockCodeHashProofs, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockLogRequest, ProviderBlockSelection,
    ProviderCodeHashProof, ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog,
    ProviderRawPayloadCacheMetadata, ProviderReceipt, ProviderResolvedBlock, ProviderTransaction,
    ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const DEFAULT_PROVIDER_BATCH_ITEM_LIMIT: usize = 32;
const MAX_PROVIDER_BATCH_ITEM_LIMIT: usize = 256;
const PROVIDER_BATCH_ITEM_LIMIT_ENV: &str = "BIGNAME_INDEXER_JSON_RPC_BATCH_ITEM_LIMIT";
const DEFAULT_PROVIDER_BATCH_REQUEST_CONCURRENCY: usize = 1;
const MAX_PROVIDER_BATCH_REQUEST_CONCURRENCY: usize = 16;
const PROVIDER_BATCH_REQUEST_CONCURRENCY_ENV: &str = "BIGNAME_INDEXER_JSON_RPC_BATCH_CONCURRENCY";
const PROVIDER_RECEIPT_FALLBACK_URLS_ENV: &str = "BIGNAME_INDEXER_CHAIN_RPC_RECEIPT_FALLBACK_URLS";
const MAX_TRANSACTION_RECEIPT_FALLBACK: usize = 128;
const JSON_RPC_PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const JSON_RPC_PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
pub(crate) const RAW_PAYLOAD_KIND_FULL_BLOCK: &str = "full_block";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_LOGS: &str = "block_logs";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_RECEIPTS: &str = "block_receipts";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_TYPE: &str = "application/json";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_ENCODING: &str = "identity";

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, ChainProvider>,
}

impl ProviderRegistry {
    #[cfg(test)]
    pub fn from_chain_rpc_urls(entries: &[String]) -> Result<Self> {
        Self::from_sources(entries, &[])
    }

    #[cfg(test)]
    pub fn from_sources(rpc_entries: &[String], reth_db_entries: &[String]) -> Result<Self> {
        Self::from_sources_with_code_fallbacks(rpc_entries, reth_db_entries, &[])
    }

    pub fn from_sources_with_code_fallbacks(
        rpc_entries: &[String],
        reth_db_entries: &[String],
        code_fallback_entries: &[String],
    ) -> Result<Self> {
        let mut providers = BTreeMap::new();
        let mut code_fallback_endpoints = parse_code_fallback_endpoints(code_fallback_entries)?;

        for entry in rpc_entries {
            let (chain, url) = parse_chain_source_entry(entry, "chain RPC", "<chain>=<url>")?;
            let code_fallback_endpoint = code_fallback_endpoints.remove(&chain);
            insert_provider(
                &mut providers,
                &chain,
                ChainProvider::JsonRpc(JsonRpcProvider::new_for_chain(
                    &chain,
                    &url,
                    code_fallback_endpoint,
                )?),
            )?;
        }

        for entry in reth_db_entries {
            let (chain, datadir) =
                parse_chain_source_entry(entry, "chain Reth DB", "<chain>=<datadir>")?;
            if providers.contains_key(&chain) {
                bail!("duplicate provider source configuration for {chain}");
            }
            let code_fallback_endpoint = code_fallback_endpoints.remove(&chain);
            let provider =
                RethDbProvider::new_with_code_fallback(&chain, &datadir, code_fallback_endpoint)?;
            providers.insert(chain, ChainProvider::RethDb(provider));
        }

        if let Some(chain) = code_fallback_endpoints.keys().next() {
            bail!(
                "code fallback provider configured for {chain} without a primary provider source"
            );
        }

        Ok(Self { providers })
    }

    pub fn provider_for(&self, chain: &str) -> Option<&ChainProvider> {
        self.providers.get(chain)
    }

    pub fn configured_chain_count(&self) -> usize {
        self.providers.len()
    }

    pub fn configured_chain_count_by_kind(&self, kind: ChainProviderKind) -> usize {
        self.providers
            .values()
            .filter(|provider| provider.kind() == kind)
            .count()
    }

    pub fn ensure_configured_chains_admitted<'a>(
        &self,
        admitted_chains: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let admitted_chains = admitted_chains.into_iter().collect::<BTreeSet<_>>();
        let invalid_chains = self
            .providers
            .keys()
            .filter(|chain| !admitted_chains.contains(chain.as_str()))
            .map(String::as_str)
            .collect::<Vec<_>>();
        if invalid_chains.is_empty() {
            return Ok(());
        }

        let admitted_chain_list = if admitted_chains.is_empty() {
            "<none>".to_owned()
        } else {
            admitted_chains
                .iter()
                .copied()
                .collect::<Vec<_>>()
                .join(", ")
        };
        bail!(
            "configured provider source chains outside selected/admitted runtime chain set: {}; admitted runtime chains: {admitted_chain_list}",
            invalid_chains.join(", ")
        );
    }
}

fn parse_chain_source_entry(
    entry: &str,
    source_label: &str,
    expected_shape: &str,
) -> Result<(String, String)> {
    let (chain, value) = entry.split_once('=').with_context(|| {
        format!("invalid {source_label} source entry {entry}; expected {expected_shape}")
    })?;
    let chain = chain.trim();
    let value = value.trim();
    if chain.is_empty() || value.is_empty() {
        bail!("invalid {source_label} source entry {entry}; expected non-empty {expected_shape}");
    }

    Ok((chain.to_owned(), value.to_owned()))
}

fn insert_provider(
    providers: &mut BTreeMap<String, ChainProvider>,
    chain: &str,
    provider: ChainProvider,
) -> Result<()> {
    if providers.contains_key(chain) {
        bail!("duplicate provider source configuration for {chain}");
    }

    providers.insert(chain.to_owned(), provider);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainProviderKind {
    JsonRpc,
    RethDb,
}

#[derive(Clone)]
pub enum ChainProvider {
    JsonRpc(JsonRpcProvider),
    RethDb(RethDbProvider),
}

impl ChainProvider {
    pub fn kind(&self) -> ChainProviderKind {
        match self {
            Self::JsonRpc(_) => ChainProviderKind::JsonRpc,
            Self::RethDb(_) => ChainProviderKind::RethDb,
        }
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait ChainProviderOps {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot>;

    async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>>;

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock>;

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>>;

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>>;

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>>;

    async fn fetch_block_bundle_by_hash(&self, block_hash: &str) -> Result<ProviderBlockBundle>;

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>>;

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>>;

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>>;

    // Sequential per-address variant retained for provider parity tests; the
    // live and baseline paths use the batched `_at_block_hashes` form.
    #[allow(dead_code)]
    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>>;

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>>;
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Url,
    client: http_client::RecoveringHttpClient,
    receipt_fallback_endpoint: Option<Url>,
    code_fallback_provider: Option<Arc<ConfiguredCodeFallback>>,
}

struct ConfiguredCodeFallback {
    chain: String,
    provider: JsonRpcProvider,
}

impl JsonRpcProvider {
    #[allow(dead_code)]
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::new_with_fallbacks(endpoint, None, None, None)
    }

    pub(crate) fn new_with_request_timeout(
        endpoint: &str,
        request_timeout: Duration,
    ) -> Result<Self> {
        Self::new_with_fallbacks_and_timeout(endpoint, None, None, None, request_timeout)
    }

    pub fn new_for_chain(
        chain: &str,
        endpoint: &str,
        code_fallback_endpoint: Option<Url>,
    ) -> Result<Self> {
        Self::new_with_fallbacks(
            endpoint,
            receipt_fallback_endpoint_for_chain(chain)?,
            code_fallback_endpoint,
            Some(chain.to_owned()),
        )
    }

    #[cfg(test)]
    fn new_with_receipt_fallback(
        endpoint: &str,
        receipt_fallback_endpoint: Option<Url>,
    ) -> Result<Self> {
        Self::new_with_fallbacks(endpoint, receipt_fallback_endpoint, None, None)
    }

    #[cfg(test)]
    pub(crate) fn new_with_code_fallback(
        chain: &str,
        endpoint: &str,
        code_fallback_endpoint: Url,
    ) -> Result<Self> {
        Self::new_with_fallbacks(
            endpoint,
            None,
            Some(code_fallback_endpoint),
            Some(chain.to_owned()),
        )
    }

    fn new_with_fallbacks(
        endpoint: &str,
        receipt_fallback_endpoint: Option<Url>,
        code_fallback_endpoint: Option<Url>,
        chain: Option<String>,
    ) -> Result<Self> {
        Self::new_with_fallbacks_and_timeout(
            endpoint,
            receipt_fallback_endpoint,
            code_fallback_endpoint,
            chain,
            JSON_RPC_PROVIDER_REQUEST_TIMEOUT,
        )
    }

    fn new_with_fallbacks_and_timeout(
        endpoint: &str,
        receipt_fallback_endpoint: Option<Url>,
        code_fallback_endpoint: Option<Url>,
        chain: Option<String>,
        request_timeout: Duration,
    ) -> Result<Self> {
        if request_timeout.is_zero() {
            bail!("JSON-RPC request timeout must be positive");
        }
        let endpoint = Url::parse(endpoint)
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("unsupported RPC endpoint scheme for {endpoint}; expected http:// or https://");
        }

        let client = http_client::RecoveringHttpClient::new(
            JSON_RPC_PROVIDER_CONNECT_TIMEOUT,
            request_timeout,
        )?;
        let code_fallback_provider = code_fallback_endpoint
            .map(|endpoint| -> Result<_> {
                Ok(Arc::new(ConfiguredCodeFallback {
                    chain: chain
                        .clone()
                        .context("code fallback RPC endpoint requires a chain")?,
                    provider: Self::new_code_fallback(endpoint, request_timeout)?,
                }))
            })
            .transpose()?;

        Ok(Self {
            endpoint,
            client,
            receipt_fallback_endpoint,
            code_fallback_provider,
        })
    }

    pub(super) fn new_code_fallback(endpoint: Url, request_timeout: Duration) -> Result<Self> {
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("unsupported code fallback RPC endpoint scheme; expected http:// or https://");
        }
        Ok(Self {
            endpoint,
            client: http_client::RecoveringHttpClient::new(
                JSON_RPC_PROVIDER_CONNECT_TIMEOUT,
                request_timeout,
            )?,
            receipt_fallback_endpoint: None,
            code_fallback_provider: None,
        })
    }
}

fn parse_code_fallback_endpoints(entries: &[String]) -> Result<BTreeMap<String, Url>> {
    let mut fallback_endpoints = BTreeMap::new();
    for entry in entries
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((entry_chain, value)) = entry.split_once('=') else {
            bail!("invalid chain RPC code fallback source entry; expected <chain>=<url>");
        };
        let entry_chain = entry_chain.trim();
        let value = value.trim();
        if entry_chain.is_empty() || value.is_empty() {
            bail!("invalid chain RPC code fallback source entry; expected non-empty <chain>=<url>");
        }
        if fallback_endpoints.contains_key(entry_chain) {
            bail!("duplicate code fallback provider source configuration for {entry_chain}");
        }
        let parsed = Url::parse(value).with_context(|| {
            format!("failed to parse code fallback RPC endpoint for {entry_chain}")
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!(
                "unsupported code fallback RPC endpoint scheme for {entry_chain}; expected http:// or https://"
            );
        }
        fallback_endpoints.insert(entry_chain.to_owned(), parsed);
    }
    Ok(fallback_endpoints)
}

fn receipt_fallback_endpoint_for_chain(chain: &str) -> Result<Option<Url>> {
    let Some(raw_entries) = std::env::var(PROVIDER_RECEIPT_FALLBACK_URLS_ENV).ok() else {
        return Ok(None);
    };
    let mut fallback_endpoint = None;
    for entry in raw_entries
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (entry_chain, value) =
            parse_chain_source_entry(entry, "chain RPC receipt fallback", "<chain>=<url>")?;
        if entry_chain != chain {
            continue;
        }
        if fallback_endpoint.is_some() {
            bail!("duplicate receipt fallback provider source configuration for {chain}");
        }
        let parsed = Url::parse(&value)
            .with_context(|| format!("failed to parse receipt fallback RPC endpoint {value}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!(
                "unsupported receipt fallback RPC endpoint scheme for {value}; expected http:// or https://"
            );
        }
        fallback_endpoint = Some(parsed);
    }

    Ok(fallback_endpoint)
}

pub(super) fn provider_batch_item_limit() -> usize {
    parse_provider_batch_item_limit(std::env::var(PROVIDER_BATCH_ITEM_LIMIT_ENV).ok().as_deref())
}

pub(super) fn provider_batch_request_concurrency() -> usize {
    parse_provider_batch_request_concurrency(
        std::env::var(PROVIDER_BATCH_REQUEST_CONCURRENCY_ENV)
            .ok()
            .as_deref(),
    )
}

fn parse_provider_batch_item_limit(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_PROVIDER_BATCH_ITEM_LIMIT))
        .unwrap_or(DEFAULT_PROVIDER_BATCH_ITEM_LIMIT)
}

fn parse_provider_batch_request_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_PROVIDER_BATCH_REQUEST_CONCURRENCY))
        .unwrap_or(DEFAULT_PROVIDER_BATCH_REQUEST_CONCURRENCY)
}

#[cfg(test)]
mod tests;
