use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::{
    WatchedChainPlan, WatchedContract, WatchedContractChainSummary, WatchedContractSource,
    WatchedContractSummary,
};

#[derive(Default)]
struct ChainPlanAccumulator {
    addresses: BTreeSet<String>,
    manifest_root_entry_count: usize,
    manifest_contract_entry_count: usize,
    discovery_edge_entry_count: usize,
}

pub fn summarize_watched_contracts(
    watched_contracts: &[WatchedContract],
) -> WatchedContractSummary {
    let mut unique_contracts = HashSet::new();
    let mut chains = BTreeMap::<String, WatchedContractChainSummary>::new();
    let mut manifest_root_count = 0;
    let mut manifest_contract_count = 0;
    let mut discovery_edge_count = 0;

    for watched_contract in watched_contracts {
        unique_contracts.insert((
            watched_contract.chain.clone(),
            watched_contract.address.clone(),
        ));

        let chain_summary = chains
            .entry(watched_contract.chain.clone())
            .or_insert_with(|| WatchedContractChainSummary {
                chain: watched_contract.chain.clone(),
                unique_contract_count: 0,
                manifest_root_count: 0,
                manifest_contract_count: 0,
                discovery_edge_count: 0,
            });

        match watched_contract.source {
            WatchedContractSource::ManifestRoot => {
                manifest_root_count += 1;
                chain_summary.manifest_root_count += 1;
            }
            WatchedContractSource::ManifestContract => {
                manifest_contract_count += 1;
                chain_summary.manifest_contract_count += 1;
            }
            WatchedContractSource::DiscoveryEdge => {
                discovery_edge_count += 1;
                chain_summary.discovery_edge_count += 1;
            }
        }
    }

    for chain_summary in chains.values_mut() {
        chain_summary.unique_contract_count = watched_contracts
            .iter()
            .filter(|contract| contract.chain == chain_summary.chain)
            .map(|contract| contract.address.as_str())
            .collect::<HashSet<_>>()
            .len();
    }

    WatchedContractSummary {
        unique_contract_count: unique_contracts.len(),
        source_entry_count: watched_contracts.len(),
        manifest_root_count,
        manifest_contract_count,
        discovery_edge_count,
        chains: chains.into_values().collect(),
    }
}

pub fn plan_watched_contracts(watched_contracts: &[WatchedContract]) -> Vec<WatchedChainPlan> {
    let mut plans = BTreeMap::<String, ChainPlanAccumulator>::new();

    for watched_contract in watched_contracts {
        let plan = plans.entry(watched_contract.chain.clone()).or_default();
        plan.addresses.insert(watched_contract.address.clone());

        match watched_contract.source {
            WatchedContractSource::ManifestRoot => plan.manifest_root_entry_count += 1,
            WatchedContractSource::ManifestContract => plan.manifest_contract_entry_count += 1,
            WatchedContractSource::DiscoveryEdge => plan.discovery_edge_entry_count += 1,
        }
    }

    plans
        .into_iter()
        .map(|(chain, accumulator)| WatchedChainPlan {
            chain,
            addresses: accumulator.addresses.into_iter().collect(),
            manifest_root_entry_count: accumulator.manifest_root_entry_count,
            manifest_contract_entry_count: accumulator.manifest_contract_entry_count,
            discovery_edge_entry_count: accumulator.discovery_edge_entry_count,
        })
        .collect()
}
