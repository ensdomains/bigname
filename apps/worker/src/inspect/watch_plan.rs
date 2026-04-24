use anyhow::Result;
use bigname_manifests::{
    WatchedChainPlan, WatchedContract, WatchedContractChainSummary, WatchedContractSource,
    WatchedContractSummary, load_watched_contracts, plan_watched_contracts,
    summarize_watched_contracts,
};
use serde_json::{Value, json};

use super::{InspectWatchPlanArgs, connect_read_only};

pub(in crate::inspect) async fn inspect_watch_plan(args: InspectWatchPlanArgs) -> Result<()> {
    let _emit_json = args.json;
    let pool = connect_read_only(&args.database).await?;
    let watched_contracts = load_watched_contracts(&pool).await?;
    let summary = summarize_watched_contracts(&watched_contracts);
    let watch_plan = plan_watched_contracts(&watched_contracts);

    println!(
        "{}",
        render_watch_plan_inspection(&watched_contracts, &summary, &watch_plan)
    );
    Ok(())
}

pub(in crate::inspect) fn render_watch_plan_inspection(
    watched_contracts: &[WatchedContract],
    summary: &WatchedContractSummary,
    watch_plan: &[WatchedChainPlan],
) -> Value {
    json!({
        "command": "inspect watch-plan",
        "read_only": true,
        "counts": {
            "unique_contracts": summary.unique_contract_count,
            "source_entries": summary.source_entry_count,
            "manifest_roots": summary.manifest_root_count,
            "manifest_contracts": summary.manifest_contract_count,
            "discovery_edges": summary.discovery_edge_count,
            "chains": summary.chains.len(),
        },
        "summary": {
            "unique_contract_count": summary.unique_contract_count,
            "source_entry_count": summary.source_entry_count,
            "manifest_root_count": summary.manifest_root_count,
            "manifest_contract_count": summary.manifest_contract_count,
            "discovery_edge_count": summary.discovery_edge_count,
            "chains": summary
                .chains
                .iter()
                .map(render_watched_contract_chain_summary)
                .collect::<Vec<_>>(),
        },
        "watch_plan": watch_plan
            .iter()
            .map(render_watched_chain_plan)
            .collect::<Vec<_>>(),
        "watched_contracts": watched_contracts
            .iter()
            .map(render_watched_contract)
            .collect::<Vec<_>>(),
    })
}

fn render_watched_contract_chain_summary(summary: &WatchedContractChainSummary) -> Value {
    json!({
        "chain": summary.chain.as_str(),
        "unique_contract_count": summary.unique_contract_count,
        "manifest_root_count": summary.manifest_root_count,
        "manifest_contract_count": summary.manifest_contract_count,
        "discovery_edge_count": summary.discovery_edge_count,
    })
}

fn render_watched_chain_plan(plan: &WatchedChainPlan) -> Value {
    json!({
        "chain": plan.chain.as_str(),
        "addresses": plan.addresses.clone(),
        "counts": {
            "unique_contracts": plan.addresses.len(),
            "source_entries": plan.manifest_root_entry_count
                + plan.manifest_contract_entry_count
                + plan.discovery_edge_entry_count,
            "manifest_roots": plan.manifest_root_entry_count,
            "manifest_contracts": plan.manifest_contract_entry_count,
            "discovery_edges": plan.discovery_edge_entry_count,
        },
    })
}

fn render_watched_contract(contract: &WatchedContract) -> Value {
    json!({
        "chain": contract.chain.as_str(),
        "source_family": contract.source_family.as_str(),
        "contract_instance_id": contract.contract_instance_id.to_string(),
        "address": contract.address.as_str(),
        "source": watched_contract_source_label(contract.source),
        "source_manifest_id": contract.source_manifest_id,
        "active_block_range": {
            "from_block_number": contract.active_from_block_number,
            "to_block_number": contract.active_to_block_number,
        },
    })
}

const fn watched_contract_source_label(source: WatchedContractSource) -> &'static str {
    match source {
        WatchedContractSource::ManifestRoot => "manifest_root",
        WatchedContractSource::ManifestContract => "manifest_contract",
        WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}
