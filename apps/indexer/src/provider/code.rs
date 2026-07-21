use std::collections::BTreeMap;

use anyhow::{Context, Result};
use reqwest::Url;
use serde_json::Value;
use tracing::info;

use super::{
    JsonRpcProvider, ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations,
    ProviderBlockSelection, ProviderCodeObservation,
    decode::{address_hex_from_str, hash_hex_from_str, parse_hex_bytes},
    error::format_provider_error,
    provider_batch_item_limit,
    request::{JsonRpcBatchCall, requested_block_number_from_json_rpc_pruned_state_error},
};

#[derive(Clone)]
struct NormalizedCodeObservationRequest {
    block_number: i64,
    block_hash: String,
    block_parameter: Value,
    addresses: Vec<String>,
}

impl JsonRpcProvider {
    // Sequential per-address variant retained for provider parity tests; the
    // live and baseline paths use the batched `_at_block_hashes` form.
    #[allow(dead_code)]
    pub async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let block_parameter = block.json_rpc_parameter()?;
        let mut cached_observations: BTreeMap<String, ProviderCodeObservation> = BTreeMap::new();
        let mut observations = Vec::with_capacity(addresses.len());

        for address in addresses {
            let address = address_hex_from_str(address)?;
            if let Some(observation) = cached_observations.get(&address) {
                observations.push(observation.clone());
                continue;
            }

            let observation = ProviderCodeObservation {
                address: address.clone(),
                code: self
                    .fetch_code_for_address_at_block(&address, &block_parameter)
                    .await?,
            };
            cached_observations.insert(address, observation.clone());
            observations.push(observation);
        }

        Ok(observations)
    }

    pub async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let normalized_requests = normalize_code_observation_requests(requests)?;
        let primary_error = match self
            .fetch_normalized_code_observations(
                &normalized_requests,
                &self.endpoint,
                self.code_fallback_provider.is_some(),
            )
            .await
        {
            Ok(observations) => return Ok(observations),
            Err(error) => error,
        };
        let Some(first_pruned_block) =
            requested_block_number_from_json_rpc_pruned_state_error(&primary_error)
        else {
            return Err(primary_error);
        };
        if self.code_fallback_provider.is_none()
            || !normalized_requests
                .iter()
                .any(|request| request.block_number == first_pruned_block)
        {
            return Err(primary_error);
        }

        let mut observations = (0..normalized_requests.len())
            .map(|_| None)
            .collect::<Vec<_>>();
        let mut fallback_requests = Vec::new();
        let mut fallback_indexes = Vec::new();

        for (index, request) in normalized_requests.iter().enumerate() {
            if request.block_number == first_pruned_block {
                fallback_indexes.push(index);
                fallback_requests.push(request.to_provider_request());
                continue;
            }
            match self
                .fetch_normalized_code_observations(
                    std::slice::from_ref(request),
                    &self.endpoint,
                    true,
                )
                .await
            {
                Ok(mut fetched) => {
                    observations[index] = fetched.pop();
                }
                Err(error)
                    if requested_block_number_from_json_rpc_pruned_state_error(&error)
                        == Some(request.block_number) =>
                {
                    fallback_indexes.push(index);
                    fallback_requests.push(request.to_provider_request());
                }
                Err(error) => return Err(error),
            }
        }

        if !fallback_requests.is_empty() {
            let fallback = self
                .code_fallback_provider
                .as_deref()
                .context("missing configured code fallback provider")?;
            let recovered = fetch_code_observation_fallback(
                &fallback.chain,
                Some(&fallback.provider),
                &fallback_requests,
                primary_error,
            )
            .await?;
            for (index, observation) in fallback_indexes.into_iter().zip(recovered) {
                observations[index] = Some(observation);
            }
        }

        observations
            .into_iter()
            .map(|observation| observation.context("provider omitted code-observation block group"))
            .collect()
    }

    pub(super) async fn fetch_code_observations_at_block_hashes_primary(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let normalized_requests = normalize_code_observation_requests(requests)?;
        self.fetch_normalized_code_observations(&normalized_requests, &self.endpoint, false)
            .await
    }

    async fn fetch_normalized_code_observations(
        &self,
        normalized_requests: &[NormalizedCodeObservationRequest],
        endpoint: &Url,
        preserve_pruned_code_error: bool,
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let mut seen_call_keys = BTreeMap::<(String, String), ()>::new();
        let mut call_keys = Vec::<(String, String, Value)>::new();

        for request in normalized_requests {
            for address in &request.addresses {
                let key = (request.block_hash.clone(), address.clone());
                if seen_call_keys.insert(key.clone(), ()).is_none() {
                    call_keys.push((
                        request.block_hash.clone(),
                        address.clone(),
                        request.block_parameter.clone(),
                    ));
                }
            }
        }

        let mut code_by_key = BTreeMap::<(String, String), Vec<u8>>::new();
        for chunk in call_keys.chunks(provider_batch_item_limit()) {
            let calls = chunk
                .iter()
                .map(|(_, address, block_parameter)| JsonRpcBatchCall {
                    method: "eth_getCode",
                    params: vec![Value::String(address.clone()), block_parameter.clone()],
                })
                .collect::<Vec<_>>();
            let results = if preserve_pruned_code_error {
                self.fetch_json_rpc_batch_results_at_endpoint_preserving_pruned_code_error(
                    endpoint, calls,
                )
                .await?
            } else {
                self.fetch_json_rpc_batch_results_at_endpoint(endpoint, calls)
                    .await?
            };

            for ((block_hash, address, _), result) in chunk.iter().zip(results) {
                let code = result.with_context(|| {
                    format!(
                        "provider did not return code for address {address} at block hash {block_hash}"
                    )
                })?;
                let code = code
                    .as_str()
                    .context("expected code string in JSON-RPC result")?;
                code_by_key.insert(
                    (block_hash.clone(), address.clone()),
                    parse_hex_bytes(code)?,
                );
            }
        }

        let mut observations = Vec::with_capacity(normalized_requests.len());
        for request in normalized_requests {
            let mut block_observations = Vec::with_capacity(request.addresses.len());
            for address in &request.addresses {
                let code = code_by_key
                    .get(&(request.block_hash.clone(), address.clone()))
                    .with_context(|| {
                        format!(
                            "provider batch omitted code for address {address} at block hash {}",
                            request.block_hash
                        )
                    })?
                    .clone();
                block_observations.push(ProviderCodeObservation {
                    address: address.clone(),
                    code,
                });
            }

            observations.push(ProviderBlockCodeObservations {
                block_hash: request.block_hash.clone(),
                observations: block_observations,
            });
        }

        Ok(observations)
    }

    #[allow(dead_code)]
    async fn fetch_code_for_address_at_block(
        &self,
        address: &str,
        block_parameter: &Value,
    ) -> Result<Vec<u8>> {
        let code = self
            .fetch_json_rpc_result(
                "eth_getCode",
                vec![Value::String(address.to_owned()), block_parameter.clone()],
            )
            .await?
            .with_context(|| {
                format!(
                    "provider did not return code for address {address} at block {block_parameter}"
                )
            })?;
        let code = code
            .as_str()
            .context("expected code string in JSON-RPC result")?;

        parse_hex_bytes(code)
    }
}

impl NormalizedCodeObservationRequest {
    fn to_provider_request(&self) -> ProviderBlockCodeObservationRequest {
        ProviderBlockCodeObservationRequest {
            block_number: self.block_number,
            block_hash: self.block_hash.clone(),
            addresses: self.addresses.clone(),
        }
    }
}

fn normalize_code_observation_requests(
    requests: &[ProviderBlockCodeObservationRequest],
) -> Result<Vec<NormalizedCodeObservationRequest>> {
    requests
        .iter()
        .map(|request| {
            let block_hash =
                hash_hex_from_str(&request.block_hash, "provider code observation block hash")?;
            let block_parameter =
                ProviderBlockSelection::Hash(block_hash.clone()).json_rpc_parameter()?;
            let addresses = request
                .addresses
                .iter()
                .map(|address| address_hex_from_str(address))
                .collect::<Result<Vec<_>>>()?;
            Ok(NormalizedCodeObservationRequest {
                block_number: request.block_number,
                block_hash,
                block_parameter,
                addresses,
            })
        })
        .collect()
}

pub(super) async fn fetch_code_observation_fallback(
    chain: &str,
    fallback_provider: Option<&JsonRpcProvider>,
    requests: &[ProviderBlockCodeObservationRequest],
    primary_error: anyhow::Error,
) -> Result<Vec<ProviderBlockCodeObservations>> {
    let Some(fallback_provider) = fallback_provider else {
        return Err(primary_error);
    };
    let from_block = requests
        .iter()
        .map(|request| request.block_number)
        .min()
        .context("code fallback request batch has no first block")?;
    let to_block = requests
        .iter()
        .map(|request| request.block_number)
        .max()
        .context("code fallback request batch has no last block")?;
    let address_count = requests
        .iter()
        .map(|request| request.addresses.len())
        .sum::<usize>();
    info!(
        service = "indexer",
        component = "provider",
        chain,
        from_block,
        to_block,
        code_fallback_address_count = address_count,
        "reading historical code observations from fallback JSON-RPC provider"
    );

    match fallback_provider
        .fetch_code_observations_at_block_hashes_primary(requests)
        .await
    {
        Ok(observations) => Ok(observations),
        Err(fallback_error) => Err(primary_error).with_context(|| {
            format!(
                "code-observation fallback attempt failed: {}",
                format_provider_error(&fallback_error)
            )
        }),
    }
}
