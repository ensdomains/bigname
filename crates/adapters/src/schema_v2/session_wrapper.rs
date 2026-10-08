//! Match wrapping completions to their mint frames within a complete raw transaction.
//! `_mint` emits before its receiver callback and `NameWrapped` after it. A nested wrap therefore
//! completes in stack order; an already-burned outer mint cannot restore its old token data.
//! Missing mint logs remain historical partial evidence: a completion with no matching frame
//! follows the existing NameWrapped interpretation. Positive same-transaction burn evidence
//! alone suppresses a matched completion; it never changes source admission.
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L257-L266 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
use std::collections::{BTreeMap, BTreeSet};

use crate::evm_abi::{decode_event_log, hex_string, u256_word_hex};
use crate::schema_v2::{
    RawLogInput,
    catalog::{Catalog, Selected},
    protocol::v1::wrapper::{NameWrapped, TransferBatch, TransferSingle},
};
use alloy_primitives::{Address, U256};

// Namespace, contract instance, block, transaction, node. Neither another wrapper deployment nor
// another transaction may complete this stack. The index is dropped with the prepared batch.
type Key = (String, uuid::Uuid, String, String, String);
type LogKey = (String, String, i64);

#[derive(Default)]
pub(super) struct WrapperCompletions {
    stale: BTreeSet<LogKey>,
}

fn key(selected: &Selected, raw: &RawLogInput, node: String) -> Key {
    (
        selected.source.namespace.clone(),
        selected.contract_instance_id,
        raw.block_hash.clone(),
        raw.transaction_hash.clone(),
        node,
    )
}

impl WrapperCompletions {
    pub(super) fn build(catalog: &Catalog, logs: &[RawLogInput]) -> anyhow::Result<Self> {
        let mut out = Self::default();
        let mut frames: BTreeMap<Key, Vec<bool>> = BTreeMap::new();
        for raw in logs {
            let Some(selected) = catalog.select(raw)? else {
                continue;
            };
            if selected.source.source_family != "ens_v1_wrapper_l1" {
                continue;
            }
            let mut transfer = |from: Address, to: Address, id: U256, value: U256| {
                if value != U256::from(1) {
                    return;
                }
                let node = key(&selected, raw, u256_word_hex(id));
                if from == Address::ZERO {
                    frames.entry(node).or_default().push(true);
                } else if to == Address::ZERO
                    && let Some(frame) = frames
                        .get_mut(&node)
                        .and_then(|stack| stack.iter_mut().rev().find(|live| **live))
                {
                    *frame = false;
                }
            };
            // Decoding errors belong to the normal dispatch, which handles declared-emitter
            // integrity failures and diagnostic skips without letting malformed bytes alter state.
            match selected.event.name.as_str() {
                "TransferSingle" => {
                    if let Ok(event) = decode_event_log::<TransferSingle>(
                        &raw.topics,
                        &raw.data,
                        "wrapper transfer",
                    ) {
                        transfer(event.from, event.to, event.id, event.value);
                    }
                }
                "TransferBatch" => {
                    if let Ok(event) =
                        decode_event_log::<TransferBatch>(&raw.topics, &raw.data, "wrapper batch")
                        && event.ids.len() == event.values.len()
                    {
                        for (id, value) in event.ids.into_iter().zip(event.values) {
                            transfer(event.from, event.to, id, value);
                        }
                    }
                }
                "NameWrapped" => {
                    if let Ok(event) = decode_event_log::<NameWrapped>(
                        &raw.topics,
                        &raw.data,
                        "wrapper completion",
                    ) {
                        let node = key(&selected, raw, hex_string(event.node));
                        if frames.get_mut(&node).and_then(Vec::pop) == Some(false) {
                            out.stale.insert((
                                raw.block_hash.clone(),
                                raw.transaction_hash.clone(),
                                raw.log_index,
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    pub(super) fn stale(&self, raw: &RawLogInput) -> bool {
        self.stale.contains(&(
            raw.block_hash.clone(),
            raw.transaction_hash.clone(),
            raw.log_index,
        ))
    }
}
