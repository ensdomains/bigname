use std::collections::BTreeMap;

use alloy_primitives::B256;

use crate::schema_v2::{
    RawLogInput,
    catalog::{Catalog, Selected},
    migration::RegistrarContext,
    protocol::v1::{registrar_registration_namehash, registry_registration_setup_namehash},
};

/// (namespace, block hash, transaction hash, node)
type TransactionNodeKey = (String, String, String, String);

/// Same-transaction facts a log's interpretation reads from other logs of its transaction. A
/// batch holds whole blocks, so every transaction in it is complete.
#[derive(Default)]
pub(super) struct TransactionIndex {
    wrapper_completions: super::wrapper::WrapperCompletions,
    registry_setups: BTreeMap<TransactionNodeKey, Vec<(i64, String)>>,
    name_unwraps: BTreeMap<TransactionNodeKey, Vec<i64>>,
}

fn key(selected: &Selected, raw: &RawLogInput, node: String) -> TransactionNodeKey {
    (
        selected.source.namespace.clone(),
        raw.block_hash.clone(),
        raw.transaction_hash.clone(),
        node,
    )
}

impl TransactionIndex {
    pub(super) fn build(catalog: &Catalog, raw_logs: &[RawLogInput]) -> anyhow::Result<Self> {
        let mut index = Self {
            wrapper_completions: super::wrapper::WrapperCompletions::build(catalog, raw_logs)?,
            ..Self::default()
        };
        for raw in raw_logs {
            let Some(selected) = catalog.select(raw)? else {
                continue;
            };
            match selected.source.source_family.as_str() {
                "ens_v1_registry_l1" => {
                    if let Some((namehash, owner)) =
                        registry_registration_setup_namehash(&selected, raw)?
                    {
                        index
                            .registry_setups
                            .entry(key(&selected, raw, namehash))
                            .or_default()
                            .push((raw.log_index, owner));
                    }
                }
                "ens_v1_wrapper_l1" if selected.event.name == "NameUnwrapped" => {
                    // A malformed log is skipped here as the wrapper adapter skips it.
                    let Some(node) = raw.topics.get(1).and_then(|node| node.parse::<B256>().ok())
                    else {
                        continue;
                    };
                    index
                        .name_unwraps
                        .entry(key(&selected, raw, format!("{node:#x}")))
                        .or_default()
                        .push(raw.log_index);
                }
                _ => {}
            }
        }
        Ok(index)
    }

    pub(super) fn apply(
        &self,
        selected: &Selected,
        raw: &RawLogInput,
        context: &mut RegistrarContext,
    ) -> anyhow::Result<()> {
        context.stale_wrapper_completion = self.wrapper_completions.stale(raw);
        if let Some((namehash, owner)) = registrar_registration_namehash(selected, raw)? {
            context.transaction_has_registry_setup = self
                .registry_setups
                .get(&key(selected, raw, namehash))
                .is_some_and(|setups| {
                    setups
                        .iter()
                        .filter(|(log_index, _)| *log_index < raw.log_index)
                        .max_by_key(|(log_index, _)| *log_index)
                        .map(|(_, setup_owner)| setup_owner == &owner)
                        .unwrap_or_else(|| {
                            setups.iter().any(|(_, setup_owner)| setup_owner == &owner)
                        })
                });
        }
        if selected.source.source_family == "ens_v1_registry_l1"
            && let Some((namehash, _)) = registry_registration_setup_namehash(selected, raw)?
        {
            // `_unwrap` writes the registry just before it emits `NameUnwrapped`, so only the
            // last write for the node ahead of an unwrap is the NameWrapper's own
            // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f).
            let node = key(selected, raw, namehash);
            let writes = self.registry_setups.get(&node);
            context.wrapper_custody.unwrap_follows =
                self.name_unwraps.get(&node).is_some_and(|unwraps| {
                    unwraps.iter().any(|&unwrap| {
                        unwrap > raw.log_index
                            && !writes.is_some_and(|writes| {
                                writes
                                    .iter()
                                    .any(|(write, _)| (raw.log_index + 1..unwrap).contains(write))
                            })
                    })
                });
        }
        Ok(())
    }
}
