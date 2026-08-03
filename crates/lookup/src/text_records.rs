use std::str::FromStr;

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::{
    RecordSelector,
    abi::{decode_record_result, hex_string, hex_to_bytes, namehash, resolver_record_call},
    rpc::{ChainRpcUrls, JsonRpcHttpClient},
};

pub const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

mod abi {
    use super::*;

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }

        #[derive(Debug, PartialEq, Eq)]
        struct Result3 {
            bool success;
            bytes returnData;
        }

        function aggregate3(Call3[] calls) external payable returns (Result3[] returnData);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsTextRecordMulticallRequest {
    pub resolver_address: String,
    pub namehash: String,
    pub text_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsTextRecordMulticallBlock {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsTextRecordMulticallResult {
    Success { value: String },
    NotFound,
    Failed { message: String },
}

pub fn ens_namehash_hex(name: &str) -> Result<String> {
    namehash(name).map(|node| hex_string(&node))
}

pub async fn execute_ens_text_record_multicall(
    rpc_urls: &ChainRpcUrls,
    chain_id: &str,
    multicall3_address: &str,
    block: &EnsTextRecordMulticallBlock,
    requests: &[EnsTextRecordMulticallRequest],
) -> Result<Vec<EnsTextRecordMulticallResult>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if block.block_hash.trim().is_empty() {
        bail!("ENS text record Multicall3 block hash must not be empty");
    }

    let rpc_url = rpc_urls
        .url_for(chain_id)
        .with_context(|| format!("missing chain RPC URL for {chain_id}"))?;
    let rpc = JsonRpcHttpClient::new_for_rpc_urls(rpc_url, rpc_urls)?;
    let multicall3 = parse_address(multicall3_address, "multicall3")?;
    let (calls, call_indices, mut results) = multicall_calls_for_text_requests(requests);
    if calls.is_empty() {
        return finalize_text_multicall_results(results);
    }
    let call_count = calls.len();
    let calldata = abi::aggregate3Call { calls }.abi_encode();
    let call = json!({
        "to": format_address(multicall3),
        "data": hex_string(&calldata),
    });
    let block_selector = block_selector(block);
    let call_result = rpc
        .call("eth_call", vec![call, block_selector])
        .await
        .with_context(|| {
            format!(
                "failed to execute ENS text record Multicall3 batch on {chain_id} block {} ({}) with {} calls",
                block.block_number,
                block.block_hash,
                requests.len()
            )
        })?;
    let return_hex = match call_result.result {
        Ok(Value::String(value)) => value,
        Ok(other) => bail!("ENS text record Multicall3 eth_call returned non-string JSON {other}"),
        Err(error) => bail!(
            "ENS text record Multicall3 eth_call failed: {}",
            error.message
        ),
    };
    let return_data = hex_to_bytes(&return_hex)
        .context("ENS text record Multicall3 return data is not valid hex")?;
    let decoded_results = decode_multicall_results(&return_data)?;
    if decoded_results.len() != call_count {
        bail!(
            "ENS text record Multicall3 returned {} outcomes for {} calls",
            decoded_results.len(),
            call_count
        );
    }
    for (request_index, result) in call_indices.into_iter().zip(decoded_results) {
        results[request_index] = Some(result);
    }
    finalize_text_multicall_results(results)
}

fn multicall_calls_for_text_requests(
    requests: &[EnsTextRecordMulticallRequest],
) -> (
    Vec<abi::Call3>,
    Vec<usize>,
    Vec<Option<EnsTextRecordMulticallResult>>,
) {
    let mut calls = Vec::with_capacity(requests.len());
    let mut call_indices = Vec::with_capacity(requests.len());
    let mut results = vec![None; requests.len()];
    for (index, request) in requests.iter().enumerate() {
        match multicall_call_for_text_request(request) {
            Ok(call) => {
                calls.push(call);
                call_indices.push(index);
            }
            Err(error) => {
                results[index] = Some(EnsTextRecordMulticallResult::Failed {
                    message: format!("failed to build resolver text call: {error:#}"),
                });
            }
        }
    }
    (calls, call_indices, results)
}

fn finalize_text_multicall_results(
    results: Vec<Option<EnsTextRecordMulticallResult>>,
) -> Result<Vec<EnsTextRecordMulticallResult>> {
    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.with_context(|| format!("missing ENS text record Multicall3 result {index}"))
        })
        .collect()
}

fn multicall_call_for_text_request(request: &EnsTextRecordMulticallRequest) -> Result<abi::Call3> {
    let target = parse_address(&request.resolver_address, "resolver")?;
    let node = parse_namehash(&request.namehash)?;
    let selector = RecordSelector::exact_text(&request.text_key).map_err(anyhow::Error::from)?;
    let calldata = resolver_record_call(&selector, node)?;

    Ok(abi::Call3 {
        target,
        allowFailure: true,
        callData: Bytes::copy_from_slice(calldata.calldata()),
    })
}

fn decode_multicall_results(return_data: &[u8]) -> Result<Vec<EnsTextRecordMulticallResult>> {
    let decoded = abi::aggregate3Call::abi_decode_returns(return_data)
        .context("ENS text record Multicall3 return data is malformed")?;
    decoded
        .into_iter()
        .map(|result| {
            if !result.success {
                return Ok(EnsTextRecordMulticallResult::Failed {
                    message: "resolver text call returned failure from Multicall3".to_owned(),
                });
            }

            let selector = RecordSelector::exact_text("_").map_err(anyhow::Error::from)?;
            let value = match decode_record_result(&selector, result.returnData.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(EnsTextRecordMulticallResult::Failed {
                        message: format!("resolver text call return data is malformed: {error:#}"),
                    });
                }
            };
            Ok(match value {
                Some(value) => EnsTextRecordMulticallResult::Success { value },
                None => EnsTextRecordMulticallResult::NotFound,
            })
        })
        .collect()
}

fn parse_address(value: &str, context: &str) -> Result<Address> {
    Address::from_str(value).with_context(|| format!("failed to parse {context} address {value}"))
}

fn parse_namehash(value: &str) -> Result<[u8; 32]> {
    let bytes = hex_to_bytes(value)
        .with_context(|| format!("ENS text record Multicall3 namehash {value} is invalid"))?;
    <[u8; 32]>::try_from(bytes.as_slice()).with_context(|| {
        format!("ENS text record Multicall3 namehash {value} must contain exactly 32 bytes")
    })
}

fn format_address(address: Address) -> String {
    hex_string(address.as_slice())
}

fn block_selector(block: &EnsTextRecordMulticallBlock) -> Value {
    json!({
        "blockHash": block.block_hash,
        "requireCanonical": true,
    })
}

#[cfg(test)]
#[path = "text_records/tests.rs"]
mod tests;
