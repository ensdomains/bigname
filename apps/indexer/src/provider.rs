use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http_body_util::Full;
use hyper::Uri;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
#[cfg(test)]
use serde_json::json;

#[cfg(test)]
use decode::parse_hex_i64;

mod block_transaction;
mod code;
mod decode;
mod logs_receipts;
mod payload_cache;
mod request;
mod types;

#[allow(unused_imports)]
pub use types::ProviderBlockTag;
use types::ProviderHeadHashSnapshot;
pub use types::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockLogRequest, ProviderBlockSelection,
    ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog, ProviderRawPayloadCacheMetadata,
    ProviderReceipt, ProviderResolvedBlock, ProviderTransaction,
};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const PROVIDER_BATCH_ITEM_LIMIT: usize = 32;
const MAX_TRANSACTION_RECEIPT_FALLBACK: usize = 128;
pub(crate) const RAW_PAYLOAD_KIND_FULL_BLOCK: &str = "full_block";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_LOGS: &str = "block_logs";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_RECEIPTS: &str = "block_receipts";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_TYPE: &str = "application/json";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_ENCODING: &str = "identity";

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, JsonRpcProvider>,
}

impl ProviderRegistry {
    pub fn from_chain_rpc_urls(entries: &[String]) -> Result<Self> {
        let mut providers = BTreeMap::new();

        for entry in entries {
            let (chain, url) = entry.split_once('=').with_context(|| {
                format!("invalid chain RPC entry {entry}; expected <chain>=<url>")
            })?;
            let chain = chain.trim();
            let url = url.trim();
            if chain.is_empty() || url.is_empty() {
                bail!("invalid chain RPC entry {entry}; expected non-empty <chain>=<url>");
            }
            if providers.contains_key(chain) {
                bail!("duplicate chain RPC configuration for {chain}");
            }

            providers.insert(chain.to_owned(), JsonRpcProvider::new(url)?);
        }

        Ok(Self { providers })
    }

    pub fn provider_for(&self, chain: &str) -> Option<&JsonRpcProvider> {
        self.providers.get(chain)
    }

    pub fn configured_chain_count(&self) -> usize {
        self.providers.len()
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
            "configured RPC provider chains outside selected/admitted runtime chain set: {}; admitted runtime chains: {admitted_chain_list}",
            invalid_chains.join(", ")
        );
    }
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Uri,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint = endpoint
            .parse::<Uri>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if endpoint.scheme_str() != Some("http") {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; bootstrap head fetch currently supports only http:// URLs"
            );
        }

        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);

        Ok(Self { endpoint, client })
    }
}

#[cfg(test)]
mod tests;
