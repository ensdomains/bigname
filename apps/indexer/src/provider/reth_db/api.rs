use std::{
    collections::BTreeMap,
    fmt,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, bail};
use reqwest::Url;
use tokio::task;

use super::{
    RethDbProvider, RethDbReader,
    convert::{normalized_contiguous_resolved_blocks, normalized_resolved_blocks},
    requested_block_number_from_pruned_state_error,
};
use crate::provider::{
    JSON_RPC_PROVIDER_REQUEST_TIMEOUT, JsonRpcProvider, ProviderBlock, ProviderBlockBundle,
    ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations, ProviderBlockSelection,
    ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog, ProviderResolvedBlock,
    ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
    code::fetch_code_observation_fallback, decode::normalize_hash,
};

impl RethDbProvider {
    pub fn new(chain: &str, datadir: &str) -> Result<Self> {
        Self::new_with_code_fallback(chain, datadir, None)
    }

    pub fn new_with_code_fallback(
        chain: &str,
        datadir: &str,
        code_fallback_endpoint: Option<Url>,
    ) -> Result<Self> {
        let chain = chain.trim();
        let datadir = datadir.trim();
        if chain.is_empty() {
            bail!("Reth DB provider chain cannot be empty");
        }
        if datadir.is_empty() {
            bail!("Reth DB provider datadir cannot be empty");
        }

        let code_fallback_provider = code_fallback_endpoint
            .map(|endpoint| {
                JsonRpcProvider::new_code_fallback(endpoint, JSON_RPC_PROVIDER_REQUEST_TIMEOUT)
                    .map(Arc::new)
            })
            .transpose()?;

        Ok(Self {
            reader: Arc::new(RethDbReader {
                chain: chain.to_owned(),
                datadir: PathBuf::from(datadir),
                factory: OnceLock::new(),
            }),
            code_fallback_provider,
        })
    }

    pub async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        self.blocking("fetch_chain_heads", |reader| {
            reader.fetch_chain_heads_sync()
        })
        .await
    }

    pub async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        let block_numbers = block_numbers.to_vec();
        self.blocking("fetch_block_hashes_by_numbers", move |reader| {
            reader.fetch_block_hashes_by_numbers_sync(&block_numbers)
        })
        .await
    }

    pub async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        let block_hash = block_hash.to_owned();
        self.blocking("fetch_block_by_hash", move |reader| {
            reader.fetch_block_by_hash_sync(&block_hash)
        })
        .await
    }

    pub async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking("fetch_block_headers_by_hashes", move |reader| {
            reader.fetch_block_headers_by_hashes_sync(&resolved_blocks)
        })
        .await
    }

    pub async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking("fetch_block_bundles_by_hashes", move |reader| {
            reader.fetch_block_bundles_by_hashes_sync(&resolved_blocks, true)
        })
        .await
    }

    pub async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking(
            "fetch_block_bundles_without_logs_by_hashes",
            move |reader| reader.fetch_block_bundles_by_hashes_sync(&resolved_blocks, false),
        )
        .await
    }

    pub async fn fetch_block_bundle_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<ProviderBlockBundle> {
        let block_hash = block_hash.to_owned();
        self.blocking("fetch_block_bundle_by_hash", move |reader| {
            reader.fetch_block_bundle_by_hash_sync(&block_hash, true)
        })
        .await
    }

    pub async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let resolved_blocks = normalized_contiguous_resolved_blocks(resolved_blocks)?;
        let addresses = addresses.to_vec();
        self.blocking("fetch_logs_by_block_range", move |reader| {
            reader.fetch_logs_by_block_range_sync(&resolved_blocks, &addresses)
        })
        .await
    }

    pub async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let resolved_blocks = normalized_contiguous_resolved_blocks(resolved_blocks)?;
        let topic0s = topic0s.to_vec();
        let addresses = addresses.to_vec();
        self.blocking(
            "fetch_logs_by_block_range_for_topic0s_and_addresses",
            move |reader| {
                reader.fetch_logs_by_block_range_for_topic0s_and_addresses_sync(
                    &resolved_blocks,
                    &topic0s,
                    &addresses,
                )
            },
        )
        .await
    }

    pub async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let requests = requests.to_vec();
        self.blocking("fetch_transaction_receipt_pairs_by_hashes", move |reader| {
            reader.fetch_transaction_receipt_pairs_by_hashes_sync(&requests)
        })
        .await
    }

    #[allow(dead_code, reason = "retained for provider parity with JSON-RPC")]
    pub async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let addresses = addresses.to_vec();
        self.blocking("fetch_code_observations_at_block", move |reader| {
            reader.fetch_code_observations_at_block_sync(&addresses, block)
        })
        .await
    }

    pub async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let requests = requests
            .iter()
            .map(|request| ProviderBlockCodeObservationRequest {
                block_number: request.block_number,
                block_hash: normalize_hash(&request.block_hash),
                addresses: request.addresses.clone(),
            })
            .collect::<Vec<_>>();
        let blocking_requests = requests.clone();
        let outcomes = self
            .blocking("fetch_code_observations_at_block_hashes", move |reader| {
                Ok(blocking_requests
                    .iter()
                    .map(|request| reader.fetch_code_observations_at_block_hash_sync(request))
                    .collect::<Vec<_>>())
            })
            .await?;
        let mut observations = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        let mut fallback_indexes = Vec::new();
        let mut fallback_requests = Vec::new();
        let mut primary_error = None;

        for (index, (request, outcome)) in requests.iter().zip(outcomes).enumerate() {
            match outcome {
                Ok(observation) => observations[index] = Some(observation),
                Err(error)
                    if requested_block_number_from_pruned_state_error(&error)
                        == Some(request.block_number) =>
                {
                    if self.code_fallback_provider.is_none() {
                        return Err(error);
                    }
                    primary_error.get_or_insert(error);
                    fallback_indexes.push(index);
                    fallback_requests.push(request.clone());
                }
                Err(error) => return Err(error),
            }
        }

        if !fallback_requests.is_empty() {
            let recovered = fetch_code_observation_fallback(
                &self.reader.chain,
                self.code_fallback_provider.as_deref(),
                &fallback_requests,
                primary_error.context("missing Reth DB code-observation fallback error")?,
            )
            .await?;
            for (index, observation) in fallback_indexes.into_iter().zip(recovered) {
                observations[index] = Some(observation);
            }
        }

        observations
            .into_iter()
            .map(|observation| {
                observation.context("Reth DB provider omitted code-observation block group")
            })
            .collect()
    }

    async fn blocking<T>(
        &self,
        operation: &'static str,
        work: impl FnOnce(Arc<RethDbReader>) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let reader = Arc::clone(&self.reader);
        task::spawn_blocking(move || work(reader))
            .await
            .with_context(|| format!("Reth DB provider task failed while running {operation}"))?
    }
}

impl fmt::Debug for RethDbProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RethDbProvider")
            .field("chain", &self.reader.chain)
            .field("datadir", &self.reader.datadir)
            .finish_non_exhaustive()
    }
}
