use alloy_primitives::{Address, Bytes};
use alloy_sol_types::{SolError, SolValue, sol};
use anyhow::Context;
use serde_json::{Value, json};

use crate::{
    LookupError, LookupRecordResult, LookupRecordStatus, RecordSelector, Result,
    abi::{
        ResolutionResultAbi, decode_record_result, decode_resolution_result, hex_string,
        hex_to_bytes, resolver_record_call, universal_resolver_call,
    },
    ccip::{follow_ccip_read, rpc_error_contains_offchain_lookup},
    rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient},
};

sol! {
    error ResolverNotFound(bytes name);
}

pub(crate) struct RecordCallOutcome {
    pub result: LookupRecordResult,
    pub effective_resolver: Option<String>,
    pub resolver_not_found: bool,
}

enum ResolvedCallResult {
    Provider(JsonRpcCallResult),
    InternalFailure(&'static str),
    InternalUnsupported(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionBlock {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
}

pub(crate) struct RecordCallContext<'a> {
    pub dns_name: &'a [u8],
    pub node: [u8; 32],
    pub entrypoint_address: &'a str,
    pub block: &'a ExecutionBlock,
    pub follow_ccip: bool,
    pub result_abi: ResolutionResultAbi,
    pub resolver_not_found_is_not_found: bool,
    pub rpc: &'a JsonRpcHttpClient,
}

pub(crate) async fn execute_record_call(
    context: &RecordCallContext<'_>,
    record: &RecordSelector,
) -> Result<LookupRecordResult> {
    execute_record_call_with_resolver(context, record)
        .await
        .map(|outcome| outcome.result)
}

pub(crate) async fn execute_record_call_with_resolver(
    context: &RecordCallContext<'_>,
    record: &RecordSelector,
) -> Result<RecordCallOutcome> {
    let resolver_call = resolver_record_call(record, context.node).map_err(|error| {
        LookupError::unsupported(format!(
            "failed to build {} resolver call: {error:#}",
            record.record_key
        ))
    })?;
    let universal_call = universal_resolver_call(context.dns_name, resolver_call.calldata());
    let call = json!({
        "to": context.entrypoint_address,
        "data": universal_call.calldata_hex(),
    });
    let block_selector = json!({
        "blockHash": context.block.block_hash,
        "requireCanonical": true,
    });
    let initial = match context
        .rpc
        .call("eth_call", vec![call, block_selector.clone()])
        .await
    {
        Ok(result) => result,
        Err(error) if context.rpc.is_configured_timeout(&error) => {
            return Ok(outcome(failed(record, "resolver_call_failed", false), None));
        }
        Err(error) => {
            return Err(LookupError::transport(format!(
                "verified lookup RPC transport failed on {}: {error:#}",
                context.block.chain_id
            )));
        }
    };
    let (result, ccip_read) =
        resolve_ccip_if_supported(context, initial, &block_selector, record).await?;
    decode_resolved_call_result(
        record,
        context.result_abi,
        result,
        ccip_read,
        context.resolver_not_found_is_not_found,
        context.dns_name,
    )
}

async fn resolve_ccip_if_supported(
    context: &RecordCallContext<'_>,
    initial: JsonRpcCallResult,
    block_selector: &Value,
    record: &RecordSelector,
) -> Result<(ResolvedCallResult, bool)> {
    let Err(rpc_error) = &initial.result else {
        return Ok((ResolvedCallResult::Provider(initial), false));
    };
    let offchain_lookup = rpc_error_contains_offchain_lookup(rpc_error).map_err(|error| {
        LookupError::execution(format!(
            "failed to decode OffchainLookup response: {error:#}"
        ))
    })?;
    if !offchain_lookup {
        return Ok((ResolvedCallResult::Provider(initial), false));
    }
    if !context.follow_ccip {
        return Ok((
            ResolvedCallResult::InternalUnsupported("offchain_lookup_required"),
            true,
        ));
    }
    match follow_ccip_read(
        context.rpc,
        rpc_error,
        block_selector,
        context.entrypoint_address,
    )
    .await
    {
        Ok(Some(outcome)) => Ok((ResolvedCallResult::Provider(outcome.result), true)),
        Ok(None) => Ok((ResolvedCallResult::Provider(initial), false)),
        Err(error) if error.is_transport_failure() && error.is_configured_timeout() => Ok((
            ResolvedCallResult::InternalFailure("resolver_call_failed"),
            true,
        )),
        Err(error) if error.is_transport_failure() => Err(LookupError::transport(format!(
            "CCIP-Read transport failed for {}: {error}",
            record.record_key
        ))),
        Err(_error) => Ok((
            ResolvedCallResult::InternalFailure("ccip_read_failed"),
            true,
        )),
    }
}

fn decode_resolved_call_result(
    record: &RecordSelector,
    result_abi: ResolutionResultAbi,
    result: ResolvedCallResult,
    ccip_read: bool,
    resolver_not_found_is_not_found: bool,
    dns_name: &[u8],
) -> Result<RecordCallOutcome> {
    match result {
        ResolvedCallResult::Provider(result) => decode_call_result(
            record,
            result_abi,
            result,
            ccip_read,
            resolver_not_found_is_not_found,
            dns_name,
        ),
        ResolvedCallResult::InternalFailure(reason) => {
            Ok(outcome(failed(record, reason, ccip_read), None))
        }
        ResolvedCallResult::InternalUnsupported(reason) => {
            Ok(outcome(unsupported(record, reason, ccip_read), None))
        }
    }
}

fn decode_call_result(
    record: &RecordSelector,
    result_abi: ResolutionResultAbi,
    result: JsonRpcCallResult,
    ccip_read: bool,
    resolver_not_found_is_not_found: bool,
    dns_name: &[u8],
) -> Result<RecordCallOutcome> {
    match result.result {
        Ok(Value::String(hex)) => {
            let Ok((bytes, resolver)) =
                hex_to_bytes(&hex).and_then(|bytes| decode_resolution_output(result_abi, &bytes))
            else {
                return Ok(outcome(
                    failed(record, "resolver_return_data_malformed", ccip_read),
                    None,
                ));
            };
            match decode_record_result(record, &bytes) {
                Ok(Some(value)) => Ok(outcome(
                    success(record, canonical_value(record, value), ccip_read),
                    resolver,
                )),
                Ok(None) => Ok(outcome(
                    not_found(record, not_found_reason(record), ccip_read),
                    resolver,
                )),
                Err(_) => Ok(outcome(
                    failed(record, "resolver_return_data_malformed", ccip_read),
                    resolver,
                )),
            }
        }
        Ok(_) => Ok(outcome(
            failed(record, "resolver_return_data_malformed", ccip_read),
            None,
        )),
        Err(error)
            if resolver_not_found_is_not_found
                && result_abi == ResolutionResultAbi::EnsUniversalResolver
                && rpc_error_is_resolver_not_found(&error, dns_name) =>
        {
            Ok(resolver_not_found_outcome(not_found(
                record,
                "resolver_not_found",
                ccip_read,
            )))
        }
        Err(error) if provider_unavailable_for_selected_block(&error) => {
            Err(LookupError::stale(format!(
                "verified lookup RPC provider could not serve selected block: {}",
                error.message
            )))
        }
        Err(error) => Ok(outcome(
            failed(record, rpc_failure_reason(&error), ccip_read),
            None,
        )),
    }
}

fn decode_resolution_output(
    result_abi: ResolutionResultAbi,
    return_data: &[u8],
) -> anyhow::Result<(Vec<u8>, Option<String>)> {
    match result_abi {
        ResolutionResultAbi::EnsUniversalResolver => {
            let (result, resolver) = <(Bytes, Address)>::abi_decode_params_validate(return_data)
                .context("Universal Resolver return data is malformed")?;
            Ok((
                result.to_vec(),
                Some(hex_string(resolver.as_slice()).to_ascii_lowercase()),
            ))
        }
        ResolutionResultAbi::BasenamesL1Resolver => {
            decode_resolution_result(result_abi, return_data).map(|result| (result, None))
        }
    }
}

fn rpc_error_is_resolver_not_found(error: &JsonRpcCallError, dns_name: &[u8]) -> bool {
    let Some(data) = error.data.as_ref().and_then(rpc_error_hex_data) else {
        return false;
    };
    let Ok(bytes) = hex_to_bytes(data) else {
        return false;
    };
    let Ok(error) = ResolverNotFound::abi_decode_validate(&bytes) else {
        return false;
    };
    error.name.as_ref() == dns_name
}

fn rpc_error_hex_data(value: &Value) -> Option<&str> {
    match value {
        Value::String(text) if text.starts_with("0x") => Some(text),
        Value::Object(object) => object
            .get("data")
            .and_then(rpc_error_hex_data)
            .or_else(|| object.get("originalError").and_then(rpc_error_hex_data))
            .or_else(|| object.get("error").and_then(rpc_error_hex_data)),
        _ => None,
    }
}

fn outcome(result: LookupRecordResult, effective_resolver: Option<String>) -> RecordCallOutcome {
    RecordCallOutcome {
        result,
        effective_resolver,
        resolver_not_found: false,
    }
}

fn resolver_not_found_outcome(result: LookupRecordResult) -> RecordCallOutcome {
    RecordCallOutcome {
        result,
        effective_resolver: None,
        resolver_not_found: true,
    }
}

fn canonical_value(record: &RecordSelector, value: String) -> Value {
    if record.record_family == "addr" {
        Value::String(value.to_ascii_lowercase())
    } else {
        Value::String(value)
    }
}

fn success(record: &RecordSelector, value: Value, ccip_read: bool) -> LookupRecordResult {
    base_result(record, LookupRecordStatus::Success, Some(value), ccip_read)
}

fn not_found(record: &RecordSelector, reason: &str, ccip_read: bool) -> LookupRecordResult {
    let mut result = base_result(record, LookupRecordStatus::NotFound, None, ccip_read);
    result.failure_reason = Some(reason.to_owned());
    result
}

fn failed(record: &RecordSelector, reason: &str, ccip_read: bool) -> LookupRecordResult {
    let mut result = base_result(record, LookupRecordStatus::ExecutionFailed, None, ccip_read);
    result.failure_reason = Some(reason.to_owned());
    result
}

fn unsupported(record: &RecordSelector, reason: &str, ccip_read: bool) -> LookupRecordResult {
    let mut result = base_result(record, LookupRecordStatus::Unsupported, None, ccip_read);
    result.unsupported_reason = Some(reason.to_owned());
    result
}

fn base_result(
    record: &RecordSelector,
    status: LookupRecordStatus,
    value: Option<Value>,
    ccip_read: bool,
) -> LookupRecordResult {
    LookupRecordResult {
        record_key: record.record_key.clone(),
        record_family: record.record_family.clone(),
        selector_key: record.selector_key.clone(),
        status,
        value,
        failure_reason: None,
        unsupported_reason: None,
        ccip_read,
        ledger_action: crate::LedgerAction::None,
    }
}

fn not_found_reason(record: &RecordSelector) -> &'static str {
    match record.record_family.as_str() {
        "addr" => "no_addr_record",
        "text" => "no_text_record",
        "avatar" => "no_avatar_record",
        "contenthash" => "no_contenthash_record",
        _ => "record_not_found",
    }
}

fn rpc_failure_reason(error: &JsonRpcCallError) -> &'static str {
    let mut text = error.message.to_ascii_lowercase();
    if let Some(data) = &error.data {
        text.push(' ');
        text.push_str(&data.to_string().to_ascii_lowercase());
    }
    if text.contains("execution reverted") || text.contains("revert") {
        "resolver_call_reverted"
    } else {
        "resolver_call_failed"
    }
}

pub(crate) fn provider_unavailable_for_selected_block(error: &JsonRpcCallError) -> bool {
    let mut text = error.message.to_ascii_lowercase();
    if let Some(data) = &error.data {
        text.push(' ');
        text.push_str(&data.to_string().to_ascii_lowercase());
    }
    [
        "header not found",
        "block not found",
        "unknown block",
        "missing trie node",
        "state not available",
        "missing state",
        "historical state unavailable",
        "pruned",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

#[cfg(test)]
#[path = "call/tests.rs"]
mod tests;
