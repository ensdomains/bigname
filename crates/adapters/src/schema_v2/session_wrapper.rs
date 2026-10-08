//! Match complete-transaction wrapper mints to their later name/data evidence in stack order.
//! `_mint` emits before its receiver callback, while `_wrap` emits its original arguments after
//! it. Interpret the effective mint at its actual position; a completion must never replay it.
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L228-L278 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
use std::collections::{BTreeMap, BTreeSet};

use crate::evm_abi::{decode_event_log, hex_string, u256_word_hex};
use crate::schema_v2::{
    RawLogInput,
    catalog::{Catalog, Selected},
    common::{decode_dns_labels, namehash_raw},
    protocol::v1::wrapper::{NameWrapped, TransferSingle},
};
use alloy_primitives::{Address, U256};

// Neither another namespace, wrapper deployment, block, transaction nor node may complete a frame.
type Key = (String, uuid::Uuid, String, String, String);
type LogKey = (String, String, i64);

#[derive(Default)]
pub(super) struct WrapperCompletions {
    matched: BTreeSet<LogKey>,
    mints: BTreeMap<LogKey, RawLogInput>,
}

fn log_key(raw: &RawLogInput) -> LogKey {
    (
        raw.block_hash.clone(),
        raw.transaction_hash.clone(),
        raw.log_index,
    )
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
        let mut frames: BTreeMap<Key, Vec<(&RawLogInput, Address)>> = BTreeMap::new();
        for raw in logs {
            let Some(selected) = catalog.select(raw)? else {
                continue;
            };
            if selected.source.source_family != "ens_v1_wrapper_l1" {
                continue;
            }
            // Decode/validation failures remain with normal dispatch. In particular invalid
            // completion names cannot supply lookahead facts to an earlier mint.
            match selected.event.name.as_str() {
                "TransferSingle" => {
                    if let Ok(event) =
                        decode_event_log::<TransferSingle>(&raw.topics, &raw.data, "wrapper mint")
                        && event.value == U256::from(1)
                        && event.from == Address::ZERO
                        && event.to != Address::ZERO
                    {
                        frames
                            .entry(key(&selected, raw, u256_word_hex(event.id)))
                            .or_default()
                            .push((raw, event.to));
                    }
                }
                "NameWrapped" => {
                    if let Ok(event) = decode_event_log::<NameWrapped>(
                        &raw.topics,
                        &raw.data,
                        "wrapper completion",
                    ) && let Ok(labels) = decode_dns_labels(&event.name)
                        && namehash_raw(labels.iter().map(Vec::as_slice)) == hex_string(event.node)
                        && let Some((mint, receiver)) = frames
                            .get_mut(&key(&selected, raw, hex_string(event.node)))
                            .and_then(Vec::pop)
                        && receiver == event.owner
                    {
                        out.matched.insert(log_key(raw));
                        out.mints.insert(log_key(mint), raw.clone());
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    pub(super) fn matched(&self, raw: &RawLogInput) -> bool {
        self.matched.contains(&log_key(raw))
    }

    pub(super) fn mint_completion(&self, raw: &RawLogInput) -> Option<&RawLogInput> {
        self.mints.get(&log_key(raw))
    }
}
