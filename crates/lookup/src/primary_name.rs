use std::future::Future;

use serde_json::{Value, json};

use bigname_domain::normalization::normalize_name;

use crate::{
    ChainRpcUrls, ETHEREUM_MAINNET_CHAIN_ID, LookupError, LookupPosition, LookupRecordStatus,
    RecordSelector, Result,
    abi::{
        ResolutionResultAbi, decode_registry_resolver, decode_resolver_name, dns_encode_name,
        hex_to_bytes, namehash, registry_resolver_call, resolver_name_call,
    },
    call::{
        ExecutionBlock, RecordCallContext, execute_record_call,
        provider_unavailable_for_selected_block,
    },
    rpc::{JsonRpcCallResult, JsonRpcHttpClient},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnsPrimaryNameStatus {
    Success,
    NotFound,
    Mismatch,
    InvalidName,
    ExecutionFailed,
    /// The caller's gate declined forward verification for the reverse-claimed name, so no
    /// forward call was dispatched.
    ForwardRefused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsPrimaryNameLookup {
    pub position: LookupPosition,
    pub status: EnsPrimaryNameStatus,
    /// Verbatim value returned by the reverse resolver.
    pub name: Option<String>,
    pub normalized_name: Option<String>,
    pub reverse_resolver_address: Option<String>,
    pub forward_address: Option<String>,
    pub ccip_read: bool,
    pub failure_reason: Option<String>,
}

/// Manifest-selected entrypoints and a newest-processed-block anchor for ENS primary-name lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnsPrimaryNameRequest<'a> {
    pub normalized_address: &'a str,
    pub registry_address: &'a str,
    pub universal_resolver_address: &'a str,
    pub position: &'a LookupPosition,
    pub chain_rpc_urls: &'a ChainRpcUrls,
}

pub(crate) async fn lookup_ens_primary_name<F, Fut>(
    request: EnsPrimaryNameRequest<'_>,
    admit_forward: F,
) -> Result<EnsPrimaryNameLookup>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = bool>,
{
    if request.position.block_hash.trim().is_empty() {
        return Err(LookupError::configuration(
            "ENS primary-name lookup block hash must not be empty",
        ));
    }
    let reverse_node = reverse_node(request.normalized_address)?;
    let rpc = primary_name_rpc(request.chain_rpc_urls)?;
    let block_selector = hash_pinned_block_selector(&request.position.block_hash);
    let resolver_address = match registry_resolver(
        &rpc,
        request.registry_address,
        reverse_node,
        &block_selector,
    )
    .await
    {
        Ok(Some(address)) => address,
        Ok(None) => return Ok(not_found(request.position)),
        Err(error) => return primary_call_error(error, None, request.position),
    };
    let raw_name = match reverse_name(&rpc, &resolver_address, reverse_node, &block_selector).await
    {
        Ok(Some(name)) => name,
        Ok(None) => return Ok(not_found(request.position)),
        Err(error) => return primary_call_error(error, Some(&resolver_address), request.position),
    };
    let normalized_name = match normalized_reverse_claim(&raw_name) {
        ReverseClaimNormalization::Ready(name) => name,
        ReverseClaimNormalization::NotFound => return Ok(not_found(request.position)),
        ReverseClaimNormalization::Invalid {
            normalized_name,
            failure_reason,
        } => {
            return Ok(invalid_name(
                &raw_name,
                normalized_name,
                &resolver_address,
                failure_reason,
                request.position,
            ));
        }
    };

    // The reverse answer is settled here and the forward call has not gone out yet. This is the
    // only point at which a caller can refuse a name without paying for a resolver call it has
    // already decided not to trust -- and the forward call follows CCIP-read, so a refused
    // dispatch would reach external gateways.
    if !admit_forward(normalized_name.clone()).await {
        return Ok(forward_refused(
            &raw_name,
            &normalized_name,
            &resolver_address,
            request.position,
        ));
    }

    let record = RecordSelector::parse("addr:60")?;
    let dns_name = dns_encode_name(&normalized_name).map_err(|error| {
        LookupError::execution(format!(
            "failed to encode reverse ENS name {normalized_name}: {error:#}"
        ))
    })?;
    let node = namehash(&normalized_name).map_err(|error| {
        LookupError::execution(format!(
            "failed to hash reverse ENS name {normalized_name}: {error:#}"
        ))
    })?;
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: request.position.block_number,
        block_hash: request.position.block_hash.clone(),
    };
    let forward = execute_record_call(
        &RecordCallContext {
            dns_name: &dns_name,
            node,
            entrypoint_address: request.universal_resolver_address,
            block: &block,
            follow_ccip: true,
            result_abi: ResolutionResultAbi::EnsUniversalResolver,
            rpc: &rpc,
        },
        &record,
    )
    .await?;
    if forward.status == LookupRecordStatus::ExecutionFailed {
        return Ok(execution_failed(
            Some(&raw_name),
            Some(&normalized_name),
            Some(&resolver_address),
            forward
                .failure_reason
                .as_deref()
                .unwrap_or("resolver_call_failed"),
            forward.ccip_read,
            request.position,
        ));
    }
    let forward_address = forward
        .value
        .as_ref()
        .and_then(Value::as_str)
        .map(str::to_owned);
    let (status, failure_reason) = match forward_address.as_deref() {
        Some(value) if value.eq_ignore_ascii_case(request.normalized_address) => {
            (EnsPrimaryNameStatus::Success, None)
        }
        Some(_) => (
            EnsPrimaryNameStatus::Mismatch,
            Some("resolved_target_mismatch".to_owned()),
        ),
        None => (
            EnsPrimaryNameStatus::NotFound,
            forward.failure_reason.clone(),
        ),
    };
    Ok(EnsPrimaryNameLookup {
        position: request.position.clone(),
        status,
        name: Some(raw_name),
        normalized_name: Some(normalized_name),
        reverse_resolver_address: Some(resolver_address),
        forward_address,
        ccip_read: forward.ccip_read,
        failure_reason,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReverseClaimNormalization {
    Ready(String),
    NotFound,
    Invalid {
        normalized_name: Option<String>,
        failure_reason: &'static str,
    },
}

fn normalized_reverse_claim(raw_name: &str) -> ReverseClaimNormalization {
    if raw_name.trim().is_empty() {
        return ReverseClaimNormalization::NotFound;
    }
    let normalized = match normalize_name(raw_name) {
        Ok(normalized) => normalized.normalized_name,
        Err(_) => {
            return ReverseClaimNormalization::Invalid {
                normalized_name: None,
                failure_reason: "claim_name_not_normalizable",
            };
        }
    };
    if raw_name != normalized {
        return ReverseClaimNormalization::Invalid {
            normalized_name: Some(normalized),
            failure_reason: "claim_not_normalized",
        };
    }
    ReverseClaimNormalization::Ready(normalized)
}

fn invalid_name(
    raw_name: &str,
    normalized_name: Option<String>,
    resolver_address: &str,
    failure_reason: &str,
    position: &LookupPosition,
) -> EnsPrimaryNameLookup {
    EnsPrimaryNameLookup {
        position: position.clone(),
        status: EnsPrimaryNameStatus::InvalidName,
        name: Some(raw_name.to_owned()),
        normalized_name,
        reverse_resolver_address: Some(resolver_address.to_owned()),
        forward_address: None,
        ccip_read: false,
        failure_reason: Some(failure_reason.to_owned()),
    }
}

fn execution_failed(
    raw_name: Option<&str>,
    normalized_name: Option<&str>,
    resolver_address: Option<&str>,
    failure_reason: &str,
    ccip_read: bool,
    position: &LookupPosition,
) -> EnsPrimaryNameLookup {
    EnsPrimaryNameLookup {
        position: position.clone(),
        status: EnsPrimaryNameStatus::ExecutionFailed,
        name: raw_name.map(str::to_owned),
        normalized_name: normalized_name.map(str::to_owned),
        reverse_resolver_address: resolver_address.map(str::to_owned),
        forward_address: None,
        ccip_read,
        failure_reason: Some(failure_reason.to_owned()),
    }
}

fn forward_refused(
    raw_name: &str,
    normalized_name: &str,
    resolver_address: &str,
    position: &LookupPosition,
) -> EnsPrimaryNameLookup {
    EnsPrimaryNameLookup {
        position: position.clone(),
        status: EnsPrimaryNameStatus::ForwardRefused,
        name: Some(raw_name.to_owned()),
        normalized_name: Some(normalized_name.to_owned()),
        reverse_resolver_address: Some(resolver_address.to_owned()),
        forward_address: None,
        ccip_read: false,
        failure_reason: None,
    }
}

fn not_found(position: &LookupPosition) -> EnsPrimaryNameLookup {
    EnsPrimaryNameLookup {
        position: position.clone(),
        status: EnsPrimaryNameStatus::NotFound,
        name: None,
        normalized_name: None,
        reverse_resolver_address: None,
        forward_address: None,
        ccip_read: false,
        failure_reason: None,
    }
}

fn reverse_node(normalized_address: &str) -> Result<[u8; 32]> {
    let label = normalized_address.strip_prefix("0x").ok_or_else(|| {
        LookupError::configuration("ENS primary-name address must be 0x-prefixed")
    })?;
    if label.len() != 40 || !label.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(LookupError::configuration(
            "ENS primary-name address must contain 20 hexadecimal bytes",
        ));
    }
    namehash(&format!("{}.addr.reverse", label.to_ascii_lowercase())).map_err(|error| {
        LookupError::configuration(format!("failed to hash ENS reverse name: {error:#}"))
    })
}

fn primary_name_rpc(rpc_urls: &ChainRpcUrls) -> Result<JsonRpcHttpClient> {
    let endpoint = rpc_urls.url_for(ETHEREUM_MAINNET_CHAIN_ID).ok_or_else(|| {
        LookupError::configuration("ENS primary-name RPC provider is not configured")
    })?;
    JsonRpcHttpClient::new_for_rpc_urls(endpoint, rpc_urls).map_err(|error| {
        LookupError::configuration(format!(
            "ENS primary-name RPC provider is invalid: {error:#}"
        ))
    })
}

async fn registry_resolver(
    rpc: &JsonRpcHttpClient,
    registry_address: &str,
    node: [u8; 32],
    block_selector: &Value,
) -> PrimaryCallResult<Option<String>> {
    let call = registry_resolver_call(node);
    let bytes = eth_call(rpc, registry_address, call.calldata_hex(), block_selector).await?;
    decode_registry_resolver(&bytes)
        .map_err(|_| PrimaryCallError::InBand("resolver_return_data_malformed"))
}

async fn reverse_name(
    rpc: &JsonRpcHttpClient,
    resolver_address: &str,
    node: [u8; 32],
    block_selector: &Value,
) -> PrimaryCallResult<Option<String>> {
    let call = resolver_name_call(node);
    let bytes = eth_call(rpc, resolver_address, call.calldata_hex(), block_selector).await?;
    decode_resolver_name(&bytes)
        .map_err(|_| PrimaryCallError::InBand("resolver_return_data_malformed"))
}

async fn eth_call(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: String,
    block_selector: &Value,
) -> PrimaryCallResult<Vec<u8>> {
    let response = match rpc
        .call(
            "eth_call",
            vec![
                json!({ "to": to, "data": calldata }),
                block_selector.clone(),
            ],
        )
        .await
    {
        Ok(response) => response,
        Err(error) if rpc.is_configured_timeout(&error) => {
            return Err(PrimaryCallError::InBand("resolver_call_failed"));
        }
        Err(error) => {
            return Err(PrimaryCallError::Lookup(LookupError::transport(format!(
                "ENS primary-name RPC failed: {error:#}"
            ))));
        }
    };
    decode_call_response(response)
}

fn decode_call_response(response: JsonRpcCallResult) -> PrimaryCallResult<Vec<u8>> {
    let result = match response.result {
        Ok(result) => result,
        Err(error) if provider_unavailable_for_selected_block(&error) => {
            return Err(PrimaryCallError::Lookup(LookupError::stale(format!(
                "ENS primary-name provider could not serve selected block: {}",
                error.message
            ))));
        }
        Err(_) => return Err(PrimaryCallError::InBand("resolver_call_failed")),
    };
    let value = result
        .as_str()
        .ok_or(PrimaryCallError::InBand("resolver_return_data_malformed"))?;
    hex_to_bytes(value).map_err(|_| PrimaryCallError::InBand("resolver_return_data_malformed"))
}

enum PrimaryCallError {
    InBand(&'static str),
    Lookup(LookupError),
}

type PrimaryCallResult<T> = std::result::Result<T, PrimaryCallError>;

fn primary_call_error(
    error: PrimaryCallError,
    resolver_address: Option<&str>,
    position: &LookupPosition,
) -> Result<EnsPrimaryNameLookup> {
    match error {
        PrimaryCallError::InBand(reason) => Ok(execution_failed(
            None,
            None,
            resolver_address,
            reason,
            false,
            position,
        )),
        PrimaryCallError::Lookup(error) => Err(error),
    }
}

fn hash_pinned_block_selector(block_hash: &str) -> Value {
    json!({ "blockHash": block_hash, "requireCanonical": true })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_node_matches_legacy_execution_vector() -> Result<()> {
        assert_eq!(
            crate::abi::hex_string(&reverse_node("0x8e8db5ccef88cca9d624701db544989c996e3216")?),
            "0x658ecd2fe8aadf31c3ee6126e11967ff852cfd7592ef26c28e0b65c30e4e8628"
        );
        Ok(())
    }

    #[test]
    fn primary_name_normalization_gate_blocks_forward_execution() {
        assert_eq!(
            normalized_reverse_claim("ALICE.eth"),
            ReverseClaimNormalization::Invalid {
                normalized_name: Some("alice.eth".to_owned()),
                failure_reason: "claim_not_normalized",
            }
        );
        assert_eq!(
            normalized_reverse_claim("bad name.eth"),
            ReverseClaimNormalization::Invalid {
                normalized_name: None,
                failure_reason: "claim_name_not_normalizable",
            }
        );
        assert_eq!(
            normalized_reverse_claim("   "),
            ReverseClaimNormalization::NotFound
        );
        assert_eq!(
            normalized_reverse_claim("alice.eth"),
            ReverseClaimNormalization::Ready("alice.eth".to_owned())
        );
    }
}
