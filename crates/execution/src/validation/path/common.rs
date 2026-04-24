use anyhow::{Context, Result, bail};
use bigname_storage::ExecutionTrace;
use serde_json::Value;

use crate::ETHEREUM_MAINNET_CHAIN_ID;
use crate::json_helpers::{required_array, required_object, required_string};
use crate::validation::RequestedChainPosition;

pub(crate) fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<Value> {
    trace
        .request_metadata
        .get(key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace.steps.iter().find_map(|step| {
                step.step_payload
                    .get(key)
                    .filter(|value| value.is_object())
                    .cloned()
            })
        })
}

pub(crate) fn ensure_single_ethereum_mainnet_position(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 1 {
        bail!(
            "{context} must include exactly one chain position, found {}",
            positions.len()
        );
    }
    let position = &positions[0];
    if position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        bail!(
            "{context} must target chain_id {}, found {}",
            ETHEREUM_MAINNET_CHAIN_ID,
            position.chain_id
        );
    }
    Ok(())
}

pub(crate) fn required_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let items = required_array(value, context)?;
    let mut positions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context}[{index}]"))?;
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .with_context(|| {
                format!("{context}[{index}] must include integer field block_number")
            })?;
        positions.push(RequestedChainPosition {
            chain_id: required_string(object, "chain_id", &format!("{context}[{index}]"))?
                .to_owned(),
            block_number,
            block_hash: required_string(object, "block_hash", &format!("{context}[{index}]"))?
                .to_owned(),
        });
    }
    Ok(positions)
}
