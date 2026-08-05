use serde_json::{Value, json};

use crate::{
    LookupError, LookupRecordResult, LookupRecordStatus, RecordSelector, Result,
    abi::{
        ResolutionResultAbi, decode_record_result, decode_resolution_result, hex_to_bytes,
        resolver_record_call, universal_resolver_call,
    },
    ccip::{follow_ccip_read, rpc_error_contains_offchain_lookup},
    rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient},
};

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
    pub rpc: &'a JsonRpcHttpClient,
}

pub(crate) async fn execute_record_call(
    context: &RecordCallContext<'_>,
    record: &RecordSelector,
) -> Result<LookupRecordResult> {
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
            return Ok(failed(record, "resolver_call_failed", false));
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
    decode_call_result(record, context.result_abi, result, ccip_read)
}

async fn resolve_ccip_if_supported(
    context: &RecordCallContext<'_>,
    initial: JsonRpcCallResult,
    block_selector: &Value,
    record: &RecordSelector,
) -> Result<(JsonRpcCallResult, bool)> {
    let Err(rpc_error) = &initial.result else {
        return Ok((initial, false));
    };
    let offchain_lookup = rpc_error_contains_offchain_lookup(rpc_error).map_err(|error| {
        LookupError::execution(format!(
            "failed to decode OffchainLookup response: {error:#}"
        ))
    })?;
    if !offchain_lookup {
        return Ok((initial, false));
    }
    if !context.follow_ccip {
        return Ok((
            synthetic_unsupported(initial, "offchain_lookup_required"),
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
        Ok(Some(outcome)) => Ok((outcome.result, true)),
        Ok(None) => Ok((initial, false)),
        Err(error) if error.is_transport_failure() && error.is_configured_timeout() => {
            Ok((synthetic_failure(initial, "resolver_call_failed"), true))
        }
        Err(error) if error.is_transport_failure() => Err(LookupError::transport(format!(
            "CCIP-Read transport failed for {}: {error}",
            record.record_key
        ))),
        Err(_error) => Ok((synthetic_failure(initial, "ccip_read_failed"), true)),
    }
}

fn decode_call_result(
    record: &RecordSelector,
    result_abi: ResolutionResultAbi,
    result: JsonRpcCallResult,
    ccip_read: bool,
) -> Result<LookupRecordResult> {
    match result.result {
        Ok(Value::String(hex)) => {
            let decoded = hex_to_bytes(&hex)
                .and_then(|bytes| decode_resolution_result(result_abi, &bytes))
                .and_then(|bytes| decode_record_result(record, &bytes));
            match decoded {
                Ok(Some(value)) => Ok(success(record, canonical_value(record, value), ccip_read)),
                Ok(None) => Ok(not_found(record, not_found_reason(record), ccip_read)),
                Err(_) => Ok(failed(record, "resolver_return_data_malformed", ccip_read)),
            }
        }
        Ok(_) => Ok(failed(record, "resolver_return_data_malformed", ccip_read)),
        Err(error) if error.code == Some(i64::MIN) => match error.message.as_str() {
            "offchain_lookup_required" => Ok(unsupported(record, &error.message, ccip_read)),
            reason => Ok(failed(record, reason, ccip_read)),
        },
        Err(error) if provider_unavailable_for_selected_block(&error) => {
            Err(LookupError::stale(format!(
                "verified lookup RPC provider could not serve selected block: {}",
                error.message
            )))
        }
        Err(error) => Ok(failed(record, rpc_failure_reason(&error), ccip_read)),
    }
}

fn canonical_value(record: &RecordSelector, value: String) -> Value {
    if record.record_family == "addr" {
        Value::String(value.to_ascii_lowercase())
    } else {
        Value::String(value)
    }
}

fn synthetic_failure(mut result: JsonRpcCallResult, reason: &str) -> JsonRpcCallResult {
    result.result = Err(JsonRpcCallError {
        code: Some(i64::MIN),
        message: reason.to_owned(),
        data: None,
    });
    result
}

fn synthetic_unsupported(mut result: JsonRpcCallResult, reason: &str) -> JsonRpcCallResult {
    result.result = Err(JsonRpcCallError {
        code: Some(i64::MIN),
        message: reason.to_owned(),
        data: Some(json!({ "classification": "unsupported" })),
    });
    result
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
