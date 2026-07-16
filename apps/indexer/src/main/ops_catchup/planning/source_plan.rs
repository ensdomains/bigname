use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
    WatchedTargetIdentity,
};

use super::CatchupChunk;

impl CatchupChunk {
    /// Build the exact watched-target-set plan from the covering arena already
    /// produced by catch-up planning. This avoids reloading and scanning the
    /// complete cross-chain watched view once per 32-block chunk.
    pub(in crate::ops_catchup) fn source_plan(
        &self,
        chain: &str,
    ) -> Result<WatchedSourceSelectorPlan> {
        let covered_targets = self.covered_targets();
        let requested_watched_targets = covered_targets
            .iter()
            .map(|target| WatchedTargetIdentity {
                contract_instance_id: target.contract_instance_id,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let addresses = covered_targets
            .iter()
            .map(|target| target.address.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let mut addresses_by_identity = BTreeMap::new();
        let mut selected_targets = BTreeSet::new();
        for target in covered_targets {
            let identity = (target.source_family.clone(), target.contract_instance_id);
            if let Some(existing_address) = addresses_by_identity.get(&identity) {
                if existing_address != &target.address {
                    bail!(
                        "source identity conflict for ops catch-up target {} in source family {}",
                        target.contract_instance_id,
                        target.source_family
                    );
                }
            } else {
                addresses_by_identity.insert(identity, target.address.clone());
            }
            selected_targets.insert(WatchedBackfillTarget {
                source_family: target.source_family.clone(),
                contract_instance_id: target.contract_instance_id,
                address: target.address.clone(),
                effective_from_block: self.range.from_block,
                effective_to_block: self.range.to_block,
            });
        }

        Ok(WatchedSourceSelectorPlan {
            chain: chain.to_owned(),
            selector_kind: WatchedSourceSelectorKind::WatchedTargetSet,
            source_family: None,
            requested_watched_targets,
            selected_targets: selected_targets.into_iter().collect(),
            watched_chain_plan: WatchedChainPlan {
                chain: chain.to_owned(),
                addresses,
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        })
    }
}
