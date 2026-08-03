#[cfg(test)]
use alloy_primitives::Address;
use alloy_primitives::Bytes;
use alloy_sol_types::{SolError, SolValue, sol};
use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::{
    abi::{hex_string, hex_to_bytes},
    rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient},
};

mod gateway;

const MAX_CCIP_REDIRECTS: usize = 4;

mod contracts {
    use super::*;

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        error OffchainLookup(
            address sender,
            string[] urls,
            bytes callData,
            bytes4 callbackFunction,
            bytes extraData
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CcipReadOutcome {
    pub result: JsonRpcCallResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OffchainLookup {
    pub sender: String,
    pub urls: Vec<String>,
    pub call_data: Vec<u8>,
    pub callback_function: [u8; 4],
    pub extra_data: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct CcipReadError {
    message: String,
    transport_failure: bool,
    configured_timeout: bool,
}

impl CcipReadError {
    fn malformed(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
            transport_failure: false,
            configured_timeout: false,
        }
    }

    fn transport(message: impl Into<String>, configured_timeout: bool) -> Self {
        Self {
            message: message.into(),
            transport_failure: true,
            configured_timeout,
        }
    }

    pub const fn is_transport_failure(&self) -> bool {
        self.transport_failure
    }

    pub const fn is_configured_timeout(&self) -> bool {
        self.configured_timeout
    }
}

impl std::fmt::Display for CcipReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CcipReadError {}

pub(crate) async fn follow_ccip_read(
    rpc: &JsonRpcHttpClient,
    error: &JsonRpcCallError,
    block_selector: &Value,
    expected_sender: &str,
) -> std::result::Result<Option<CcipReadOutcome>, CcipReadError> {
    let Some(mut lookup) =
        offchain_lookup_from_rpc_error(error).map_err(CcipReadError::malformed)?
    else {
        return Ok(None);
    };
    let mut expected_sender = expected_sender.to_owned();
    for redirect_index in 0..MAX_CCIP_REDIRECTS {
        if !lookup.sender.eq_ignore_ascii_case(&expected_sender) {
            return Err(CcipReadError::malformed(format!(
                "CCIP-Read OffchainLookup sender {} does not match called contract {}",
                lookup.sender, expected_sender
            )));
        }
        let gateway_response = match gateway::fetch(&lookup).await {
            Ok(response) => response,
            Err(error) if error.is_transport_failure() => {
                return Err(CcipReadError::transport(
                    error.to_string(),
                    error.is_timeout(),
                ));
            }
            Err(error) => return Err(CcipReadError::malformed(error)),
        };
        let callback_calldata = callback_calldata(
            lookup.callback_function,
            &gateway_response,
            &lookup.extra_data,
        );
        let callback_sender = lookup.sender.clone();
        let call = json!({
            "to": lookup.sender,
            "data": hex_string(&callback_calldata),
        });
        let callback_result = rpc
            .call("eth_call", vec![call, block_selector.clone()])
            .await
            .map_err(|error| {
                CcipReadError::transport(
                    format!("failed to execute CCIP-Read callback eth_call: {error:#}"),
                    rpc.is_configured_timeout(&error),
                )
            })?;
        if let Err(error) = &callback_result.result
            && redirect_index + 1 < MAX_CCIP_REDIRECTS
            && let Some(next) =
                offchain_lookup_from_rpc_error(error).map_err(CcipReadError::malformed)?
        {
            expected_sender = callback_sender;
            lookup = next;
            continue;
        }
        return Ok(Some(CcipReadOutcome {
            result: callback_result,
        }));
    }
    Err(CcipReadError::malformed(
        "CCIP-Read exceeded maximum redirect depth",
    ))
}

pub(crate) fn rpc_error_contains_offchain_lookup(error: &JsonRpcCallError) -> Result<bool> {
    offchain_lookup_from_rpc_error(error).map(|lookup| lookup.is_some())
}

fn offchain_lookup_from_rpc_error(error: &JsonRpcCallError) -> Result<Option<OffchainLookup>> {
    let Some(data) = error.data.as_ref().and_then(rpc_error_hex_data) else {
        return Ok(None);
    };
    let bytes = hex_to_bytes(data)?;
    if !bytes.starts_with(&contracts::OffchainLookup::SELECTOR) {
        return Ok(None);
    }
    let decoded = contracts::OffchainLookup::abi_decode_validate(&bytes)
        .context("OffchainLookup data malformed")?;
    Ok(Some(OffchainLookup {
        sender: hex_string(decoded.sender.as_slice()),
        urls: decoded.urls,
        call_data: decoded.callData.to_vec(),
        callback_function: decoded.callbackFunction.into(),
        extra_data: decoded.extraData.to_vec(),
    }))
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

fn callback_calldata(callback_function: [u8; 4], response: &[u8], extra_data: &[u8]) -> Vec<u8> {
    let mut calldata = Vec::from(callback_function);
    calldata.extend_from_slice(
        &(
            Bytes::copy_from_slice(response),
            Bytes::copy_from_slice(extra_data),
        )
            .abi_encode_params(),
    );
    calldata
}

#[cfg(test)]
pub(crate) fn encode_offchain_lookup_for_test(
    sender: Address,
    urls: Vec<String>,
    call_data: Vec<u8>,
    callback_function: [u8; 4],
    extra_data: Vec<u8>,
) -> String {
    hex_string(
        &contracts::OffchainLookup {
            sender,
            urls,
            callData: Bytes::from(call_data),
            callbackFunction: callback_function.into(),
            extraData: Bytes::from(extra_data),
        }
        .abi_encode(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_rpc_error_shapes_decode_offchain_lookup() -> Result<()> {
        let encoded = encode_offchain_lookup_for_test(
            Address::repeat_byte(0x11),
            vec!["https://gateway.example/{data}".to_owned()],
            vec![0xab, 0xcd],
            [0x12, 0x34, 0x56, 0x78],
            vec![0xef],
        );
        for data in [
            json!(encoded.clone()),
            json!({ "data": encoded.clone() }),
            json!({ "originalError": { "data": encoded.clone() } }),
            json!({ "error": { "data": encoded.clone() } }),
        ] {
            let lookup = offchain_lookup_from_rpc_error(&JsonRpcCallError {
                code: Some(3),
                message: "execution reverted".to_owned(),
                data: Some(data),
            })?
            .expect("OffchainLookup must decode");
            assert_eq!(lookup.sender, "0x1111111111111111111111111111111111111111");
            assert_eq!(lookup.call_data, vec![0xab, 0xcd]);
            assert_eq!(lookup.callback_function, [0x12, 0x34, 0x56, 0x78]);
            assert_eq!(lookup.extra_data, vec![0xef]);
        }
        Ok(())
    }

    #[test]
    fn non_offchain_revert_is_ignored() -> Result<()> {
        assert!(
            offchain_lookup_from_rpc_error(&JsonRpcCallError {
                code: Some(3),
                message: "execution reverted".to_owned(),
                data: Some(json!("0x08c379a0")),
            })?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn portable_callback_calldata_vector_matches_legacy_execution() {
        assert_eq!(
            hex_string(&callback_calldata(
                [0x12, 0x34, 0x56, 0x78],
                &[0xab, 0xcd],
                &[0xef],
            )),
            concat!(
                "0x12345678",
                "0000000000000000000000000000000000000000000000000000000000000040",
                "0000000000000000000000000000000000000000000000000000000000000080",
                "0000000000000000000000000000000000000000000000000000000000000002",
                "abcd000000000000000000000000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000000000000000000000000001",
                "ef00000000000000000000000000000000000000000000000000000000000000",
            )
        );
    }
}
