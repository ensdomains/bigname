use std::time::Duration;

use bigname_storage::ENS_NAMESPACE;
use sqlx::types::time::OffsetDateTime;

use super::*;

#[tokio::test]
async fn rpc_transport_timeout_is_an_in_band_selector_failure() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, anyhow::Error>(())
    });
    let rpc = JsonRpcHttpClient::new_with_timeouts(
        &endpoint,
        Duration::from_millis(10),
        Duration::from_millis(25),
    )?;
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("addr:60", "addr", Some("60".to_owned()));
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };

    let selector_call = execute_record_call(
        &row,
        &record,
        b"\x05alice\x03eth\x00",
        [0_u8; 32],
        &block,
        &rpc,
        false,
    )
    .await?;
    server.abort();

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("execution_failed")
    );
    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_call_failed")
    );
    assert!(selector_call.raw_call_snapshot.is_none());
    assert!(selector_call.request_hash.is_none());
    assert!(selector_call.response_hash.is_none());
    Ok(())
}

#[test]
fn malformed_universal_resolver_success_is_selector_failure_for_latest_calls() -> Result<()> {
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };
    let selector_call = selector_call_from_rpc_result(SelectorCallRpcContext {
        row: &row,
        record: &record,
        selector: &selector,
        result: JsonRpcCallResult {
            request_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [
                    {
                        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "data": "0x9061b923"
                    },
                    "latest"
                ]
            }),
            response_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": "0x"
            }),
            result: Ok(json!("0x")),
        },
        block: &block,
        contract_call: json!({
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
            "selector": "0x9061b923",
            "record_key": "avatar",
            "resolver_selector": "0x59d1d43c"
        }),
        resolver_selector: "0x59d1d43c".to_owned(),
        universal_calldata: "0x9061b923".to_owned(),
        block_selector: json!("latest"),
        persist_raw_call_snapshot: false,
        ccip_summary: None,
        latency_ms: 7,
    })?;

    assert_eq!(selector_call.block_selector, json!("latest"));
    assert!(selector_call.request_hash.is_some());
    assert!(selector_call.response_hash.is_some());
    assert!(selector_call.raw_call_snapshot.is_none());

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("execution_failed")
    );
    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_return_data_malformed")
    );
    assert_eq!(
        verified_query.get("record_key").and_then(Value::as_str),
        Some("avatar")
    );
    Ok(())
}

#[test]
fn offchain_lookup_revert_is_selector_local_unsupported() -> Result<()> {
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };
    let selector_call = selector_call_from_rpc_result(SelectorCallRpcContext {
        row: &row,
        record: &record,
        selector: &selector,
        result: JsonRpcCallResult {
            request_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [
                    {
                        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "data": "0x9061b923"
                    },
                    {
                        "blockHash": "0xabc123",
                        "requireCanonical": true
                    }
                ]
            }),
            response_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": 3,
                    "message": "execution reverted",
                    "data": encoded_offchain_lookup_error(),
                }
            }),
            result: Err(JsonRpcCallError {
                code: Some(3),
                message: "execution reverted".to_owned(),
                data: Some(json!(encoded_offchain_lookup_error())),
            }),
        },
        block: &block,
        contract_call: json!({
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
            "selector": "0x9061b923",
            "record_key": "avatar",
            "resolver_selector": "0x59d1d43c"
        }),
        resolver_selector: "0x59d1d43c".to_owned(),
        universal_calldata: "0x9061b923".to_owned(),
        block_selector: json!({
            "blockHash": "0xabc123",
            "requireCanonical": true
        }),
        persist_raw_call_snapshot: true,
        ccip_summary: None,
        latency_ms: 7,
    })?;

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        verified_query
            .get("unsupported_reason")
            .and_then(Value::as_str),
        Some("offchain_lookup_required")
    );
    assert_eq!(
        verified_query.get("record_key").and_then(Value::as_str),
        Some("avatar")
    );
    Ok(())
}

#[test]
fn selected_block_unavailable_rpc_error_is_not_cacheable_selector_failure() {
    let error = JsonRpcCallError {
        code: Some(-32001),
        message: "header not found".to_owned(),
        data: None,
    };

    let Err(error) = decode_rpc_error(&error) else {
        panic!("selected-block provider unavailability must fail before persistence");
    };
    assert!(
        error
            .to_string()
            .contains("verified resolution RPC provider could not serve selected block"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn plain_execution_revert_uses_typed_failure_reason() -> Result<()> {
    let status = decode_rpc_error(&JsonRpcCallError {
        code: Some(3),
        message: "execution reverted".to_owned(),
        data: None,
    })?;
    let selector_call = SelectorCall {
        status,
        request_hash: None,
        response_hash: None,
        raw_call_snapshot: None,
        contract_call: json!({ "record_key": "avatar" }),
        universal_calldata: "0x9061b923".to_owned(),
        resolver_selector: "0x59d1d43c".to_owned(),
        block_selector: json!("latest"),
        ccip_summary: None,
        latency_ms: 7,
    };
    let verified_query = selector_call.verified_query(Uuid::from_u128(1));

    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_call_reverted")
    );
    Ok(())
}

fn test_name_current_row() -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: None,
        resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({}),
        provenance: json!({}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1)
            .expect("test timestamp must be valid"),
    }
}

fn encoded_offchain_lookup_error() -> String {
    use alloy_primitives::{Address, Bytes};
    use alloy_sol_types::{SolError, sol};

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

    crate::ens_resolution_abi::hex_string(
        &OffchainLookup {
            sender: Address::repeat_byte(0x11),
            urls: vec!["https://gateway.example/{data}".to_owned()],
            callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
            callbackFunction: alloy_primitives::FixedBytes::from(&[0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}
