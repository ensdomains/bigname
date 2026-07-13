use std::collections::HashMap;

use anyhow::Result;
use bigname_domain::normalization::normalize_name;
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};

use crate::evm_abi::{hex_string, keccak256_hex, u256_decimal};

use super::account_execution::{AccountExecution, unwrap_account_execution};
use super::calldata::{EntryPointCalldata, decode_entry_point_calldata, find_user_operation};
use super::decoding::{
    UserOperationObservation, decode_answer_updated_event, decode_user_operation_event,
};
use super::manifest_scope::GasSponsorshipManifestScope;
use super::raw_logs::GasSponsorshipRawLogRow;
use super::write_classifier::{NameWrite, classify_inner_calls};
use super::{
    DERIVATION_KIND_ENTRYPOINT_USER_OPERATION, EVENT_KIND_PRICE_FEED_ANSWER_UPDATED,
    EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED, EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED,
};

const ATTRIBUTION_ATTRIBUTED: &str = "attributed";
const ATTRIBUTION_UNATTRIBUTED: &str = "unattributed";
const ATTRIBUTION_INPUT_UNAVAILABLE: &str = "input_unavailable";
const ATTRIBUTION_UNSUPPORTED_BUNDLE_SHAPE: &str = "unsupported_bundle_shape";
const ATTRIBUTION_UNSUPPORTED_EXEC_MODE: &str = "unsupported_exec_mode";
const ATTRIBUTION_UNSUPPORTED_ACCOUNT_CALL: &str = "unsupported_account_call";
const ATTRIBUTION_CORRELATION_FAILED: &str = "correlation_failed";
const ATTRIBUTION_DECODE_FAILED: &str = "decode_failed";

/// One sponsored user operation resolved from its log and, when retained,
/// its transaction input.
pub(super) struct SponsoredOperation {
    pub(super) observation: UserOperationObservation,
    pub(super) attribution_status: &'static str,
    pub(super) writes: Vec<NameWrite>,
}

pub(super) fn resolve_sponsored_operation(
    raw_log: &GasSponsorshipRawLogRow,
    transaction_input: Option<&[u8]>,
) -> Result<SponsoredOperation> {
    let observation = decode_user_operation_event(&raw_log.topics, &raw_log.data)?;

    let (attribution_status, writes) = match transaction_input {
        None => (ATTRIBUTION_INPUT_UNAVAILABLE, Vec::new()),
        Some(input) => match decode_entry_point_calldata(input) {
            Err(_) => (ATTRIBUTION_DECODE_FAILED, Vec::new()),
            Ok(EntryPointCalldata::UnsupportedSelector { .. }) => {
                (ATTRIBUTION_UNSUPPORTED_BUNDLE_SHAPE, Vec::new())
            }
            Ok(EntryPointCalldata::HandleOps(operations)) => {
                match find_user_operation(
                    &operations,
                    &observation.sender,
                    observation.nonce,
                    &observation.paymaster,
                ) {
                    None => (ATTRIBUTION_CORRELATION_FAILED, Vec::new()),
                    Some(operation) => match unwrap_account_execution(&operation.call_data) {
                        Err(_) => (ATTRIBUTION_DECODE_FAILED, Vec::new()),
                        Ok(AccountExecution::UnsupportedCallType { .. }) => {
                            (ATTRIBUTION_UNSUPPORTED_EXEC_MODE, Vec::new())
                        }
                        Ok(AccountExecution::UnrecognizedSelector { .. }) => {
                            (ATTRIBUTION_UNSUPPORTED_ACCOUNT_CALL, Vec::new())
                        }
                        Ok(AccountExecution::Calls(inner_calls)) => {
                            let classified = classify_inner_calls(&inner_calls);
                            if classified.writes.is_empty() {
                                (ATTRIBUTION_UNATTRIBUTED, Vec::new())
                            } else {
                                (ATTRIBUTION_ATTRIBUTED, classified.writes)
                            }
                        }
                    },
                }
            }
        },
    };

    Ok(SponsoredOperation {
        observation,
        attribution_status,
        writes,
    })
}

pub(super) fn build_sponsored_user_operation_event(
    scope: &GasSponsorshipManifestScope,
    raw_log: &GasSponsorshipRawLogRow,
    operation: &SponsoredOperation,
) -> NormalizedEvent {
    let observation = &operation.observation;
    normalized_event(
        scope,
        raw_log,
        EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED,
        format!(
            "entrypoint_user_operation:{}:{}:{}:{}:{}:{}",
            scope.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED,
            observation.user_op_hash
        ),
        None,
        json!({
            "source_event": "UserOperationEvent",
            "user_op_hash": observation.user_op_hash,
            "sender": observation.sender,
            "paymaster": observation.paymaster,
            "nonce": u256_decimal(observation.nonce),
            "success": observation.success,
            "actual_gas_cost_wei": u256_decimal(observation.actual_gas_cost),
            "actual_gas_used": u256_decimal(observation.actual_gas_used),
            "attribution_status": operation.attribution_status,
            "attributed_node_count": operation.writes.len(),
        }),
    )
}

pub(super) fn build_sponsored_name_write_event(
    scope: &GasSponsorshipManifestScope,
    raw_log: &GasSponsorshipRawLogRow,
    operation: &SponsoredOperation,
    write: &NameWrite,
    surfaces_by_namehash: &HashMap<String, String>,
) -> NormalizedEvent {
    let observation = &operation.observation;
    let logical_name_id = resolve_write_logical_name_id(scope, write, surfaces_by_namehash);
    let write_identity_component = write.node.clone().unwrap_or_else(|| {
        format!(
            "unnormalized:{}",
            keccak256_hex(write.name.as_deref().unwrap_or_default().as_bytes())
        )
    });

    normalized_event(
        scope,
        raw_log,
        EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED,
        format!(
            "entrypoint_user_operation:{}:{}:{}:{}:{}:{}:{}:{}",
            scope.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED,
            observation.user_op_hash,
            write.write_kind.as_str(),
            write_identity_component
        ),
        logical_name_id,
        json!({
            "source_event": "UserOperationEvent",
            "user_op_hash": observation.user_op_hash,
            "success": observation.success,
            "write_kind": write.write_kind.as_str(),
            "node": write.node,
            "name": write.name,
            "target": write.target,
            "source_call": write.source_call,
            "attribution_source": "calldata",
        }),
    )
}

pub(super) fn build_price_feed_event(
    scope: &GasSponsorshipManifestScope,
    raw_log: &GasSponsorshipRawLogRow,
) -> Result<NormalizedEvent> {
    let observation = decode_answer_updated_event(&raw_log.topics, &raw_log.data)?;
    Ok(normalized_event(
        scope,
        raw_log,
        EVENT_KIND_PRICE_FEED_ANSWER_UPDATED,
        format!(
            "entrypoint_user_operation:{}:{}:{}:{}:{}:{}",
            scope.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            EVENT_KIND_PRICE_FEED_ANSWER_UPDATED,
            observation.round_id
        ),
        None,
        json!({
            "source_event": "AnswerUpdated",
            "pair": "ETH/USD",
            "answer_e8": observation.answer.to_string(),
            "round_id": u256_decimal(observation.round_id),
            "updated_at": u256_decimal(observation.updated_at),
        }),
    ))
}

fn resolve_write_logical_name_id(
    scope: &GasSponsorshipManifestScope,
    write: &NameWrite,
    surfaces_by_namehash: &HashMap<String, String>,
) -> Option<String> {
    if let Some(node) = &write.node
        && let Some(logical_name_id) = surfaces_by_namehash.get(&node.to_ascii_lowercase())
    {
        return Some(logical_name_id.clone());
    }
    // Primary claims carry the name; a valid claim identifies the surface
    // even before its forward facts are observed.
    write.name.as_deref().and_then(|name| {
        normalize_name(name)
            .ok()
            .map(|normalized| format!("{}:{}", scope.namespace, normalized.normalized_name))
    })
}

fn normalized_event(
    scope: &GasSponsorshipManifestScope,
    raw_log: &GasSponsorshipRawLogRow,
    event_kind: &str,
    event_identity: String,
    logical_name_id: Option<String>,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity,
        namespace: scope.namespace.clone(),
        logical_name_id,
        resource_id: None,
        event_kind: event_kind.to_owned(),
        source_family: super::SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1.to_owned(),
        manifest_version: scope.manifest_version,
        source_manifest_id: Some(scope.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": raw_log.chain_id,
            "block_hash": raw_log.block_hash,
            "block_number": raw_log.block_number,
            "transaction_hash": raw_log.transaction_hash,
            "transaction_index": raw_log.transaction_index,
            "log_index": raw_log.log_index,
            "emitting_address": raw_log.emitting_address,
            "topic0": raw_log.topics.first().cloned(),
            "data_hex": hex_string(&raw_log.data),
        }),
        derivation_kind: DERIVATION_KIND_ENTRYPOINT_USER_OPERATION.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    }
}
