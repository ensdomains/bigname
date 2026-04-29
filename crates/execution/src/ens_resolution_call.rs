use anyhow::{Result, bail};
use bigname_storage::{
    CanonicalityState, NameCurrentRow, RawCallSnapshot, SupportedVerifiedResolutionRecordKey,
    parse_supported_verified_resolution_record_key,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::ens_resolution::{EnsResolutionRecord, ExecutionBlock};
use crate::ens_resolution_abi::{
    decode_selector_result, decode_universal_resolver_result, digest_json, hex_string,
    hex_to_bytes, resolver_calldata, selector_hex, universal_resolver_calldata,
};
use crate::rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient};
use crate::{ENS_UNIVERSAL_RESOLVER_ADDRESS, ETHEREUM_MAINNET_CHAIN_ID};

pub(super) struct SelectorCall {
    status: SelectorStatus,
    pub(super) request_hash: Option<String>,
    pub(super) response_hash: Option<String>,
    pub(super) raw_call_snapshot: Option<RawCallSnapshot>,
    pub(super) contract_call: Value,
    pub(super) universal_calldata: String,
    pub(super) resolver_selector: String,
}

enum SelectorStatus {
    Success { value: Value },
    NotFound { failure_reason: &'static str },
    ExecutionFailed { failure_reason: &'static str },
}

impl SelectorCall {
    pub(super) fn verified_query(&self, execution_trace_id: Uuid) -> Value {
        let mut query = match &self.status {
            SelectorStatus::Success { value } => json!({
                "status": "success",
                "value": value,
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string(),
                }
            }),
            SelectorStatus::NotFound { failure_reason } => json!({
                "status": "not_found",
                "failure_reason": failure_reason,
            }),
            SelectorStatus::ExecutionFailed { failure_reason } => json!({
                "status": "execution_failed",
                "failure_reason": failure_reason,
            }),
        };
        query["record_key"] = self
            .contract_call
            .get("record_key")
            .cloned()
            .unwrap_or(Value::Null);
        query
    }
}

pub(super) async fn execute_record_call(
    row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    dns_name: &[u8],
    node: [u8; 32],
    block: &ExecutionBlock,
    rpc: &JsonRpcHttpClient,
) -> Result<SelectorCall> {
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let resolver_data = resolver_calldata(&selector, &record.record_key, node)?;
    let universal_calldata = universal_resolver_calldata(dns_name, &resolver_data);
    let universal_calldata_hex = hex_string(&universal_calldata);
    let resolver_selector = selector_hex(first_selector(&resolver_data)?);
    let contract_call = json!({
        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
        "selector": "0x9061b923",
        "record_key": record.record_key,
        "resolver_selector": resolver_selector,
    });
    let call = json!({
        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
        "data": universal_calldata_hex,
    });
    let block_selector = json!({
        "blockHash": block.block_hash,
        "requireCanonical": true,
    });

    let result = rpc.call("eth_call", vec![call, block_selector]).await?;
    selector_call_from_rpc_result(
        row,
        record,
        &selector,
        result,
        block,
        contract_call,
        resolver_selector,
        universal_calldata_hex,
    )
}

fn selector_call_from_rpc_result(
    row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    selector: &SupportedVerifiedResolutionRecordKey,
    result: JsonRpcCallResult,
    block: &ExecutionBlock,
    contract_call: Value,
    resolver_selector: String,
    universal_calldata: String,
) -> Result<SelectorCall> {
    let request_hash = digest_json(&result.request_payload);
    let response_hash = digest_json(&result.response_payload);
    let raw_call_snapshot = RawCallSnapshot {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        request_hash: request_hash.clone(),
        request_payload: result.request_payload,
        response_hash: response_hash.clone(),
        response_payload: result.response_payload,
        canonicality_state: CanonicalityState::Canonical,
    };
    let status = match result.result {
        Ok(value) => decode_rpc_success(row, record, selector, value),
        Err(error) => Ok(decode_rpc_error(&error)),
    }?;

    Ok(SelectorCall {
        status,
        request_hash: Some(request_hash),
        response_hash: Some(response_hash),
        raw_call_snapshot: Some(raw_call_snapshot),
        contract_call,
        universal_calldata,
        resolver_selector,
    })
}

fn decode_rpc_success(
    _row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    selector: &SupportedVerifiedResolutionRecordKey,
    value: Value,
) -> Result<SelectorStatus> {
    let Some(hex) = value.as_str() else {
        return Ok(SelectorStatus::ExecutionFailed {
            failure_reason: "resolver_return_data_malformed",
        });
    };
    let return_data = hex_to_bytes(hex)?;
    let selector_return = decode_universal_resolver_result(&return_data)?;
    let decoded = match decode_selector_result(selector, &selector_return) {
        Ok(decoded) => decoded,
        Err(_) => {
            return Ok(SelectorStatus::ExecutionFailed {
                failure_reason: "resolver_return_data_malformed",
            });
        }
    };

    let Some(value) = decoded else {
        return Ok(SelectorStatus::NotFound {
            failure_reason: not_found_reason(selector),
        });
    };
    Ok(SelectorStatus::Success {
        value: selector_value(record, selector, value),
    })
}

fn decode_rpc_error(error: &JsonRpcCallError) -> SelectorStatus {
    let _ = error.code;
    let _ = &error.message;
    SelectorStatus::ExecutionFailed {
        failure_reason: "resolver_call_failed",
    }
}

fn selector_value(
    _record: &EnsResolutionRecord,
    selector: &SupportedVerifiedResolutionRecordKey,
    value: String,
) -> Value {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } => json!({
            "coin_type": coin_type,
            "value": value,
        }),
        SupportedVerifiedResolutionRecordKey::Text
        | SupportedVerifiedResolutionRecordKey::Avatar
        | SupportedVerifiedResolutionRecordKey::Contenthash => json!({
            "value": value,
        }),
    }
}

fn not_found_reason(selector: &SupportedVerifiedResolutionRecordKey) -> &'static str {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { .. } => "no_addr_record",
        SupportedVerifiedResolutionRecordKey::Text => "no_text_record",
        SupportedVerifiedResolutionRecordKey::Avatar => "no_avatar_record",
        SupportedVerifiedResolutionRecordKey::Contenthash => "no_contenthash_record",
    }
}

fn first_selector(data: &[u8]) -> Result<[u8; 4]> {
    if data.len() < 4 {
        bail!("resolver calldata is shorter than a selector");
    }
    let mut selector = [0_u8; 4];
    selector.copy_from_slice(&data[..4]);
    Ok(selector)
}
