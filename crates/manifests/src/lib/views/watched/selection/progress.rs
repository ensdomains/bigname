use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use uuid::Uuid;

use crate::{
    ManifestRuntimeProgress, WatchedBackfillTarget, WatchedChainPlan, WatchedContract,
    WatchedSourceSelector, WatchedSourceSelectorPlan, WatchedTargetIdentity,
    load_historical_watched_contracts_by_chain_with_progress,
};

pub async fn load_historical_watched_source_selector_plan_with_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    selector: WatchedSourceSelector,
    range_start_block_number: i64,
    range_end_block_number: i64,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<WatchedSourceSelectorPlan> {
    let watched_contracts =
        load_historical_watched_contracts_by_chain_with_progress(pool, chain, progress).await?;
    resolve_watched_source_selector_with_progress(
        pool,
        &watched_contracts,
        chain,
        selector,
        range_start_block_number,
        range_end_block_number,
        progress,
    )
    .await
}

async fn resolve_watched_source_selector_with_progress(
    pool: &sqlx::PgPool,
    watched_contracts: &[WatchedContract],
    chain: &str,
    selector: WatchedSourceSelector,
    range_start_block_number: i64,
    range_end_block_number: i64,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<WatchedSourceSelectorPlan> {
    if range_start_block_number < 0 {
        bail!("watched source selector range start must be non-negative");
    }
    if range_end_block_number < 0 {
        bail!("watched source selector range end must be non-negative");
    }
    if range_start_block_number > range_end_block_number {
        bail!(
            "watched source selector range start {range_start_block_number} is after end {range_end_block_number}"
        );
    }

    let selector_kind = selector.kind();
    let source_family = match &selector {
        WatchedSourceSelector::SourceFamily(source_family) => Some(source_family.clone()),
        _ => None,
    };
    let requested_watched_targets = normalize_requested_targets(pool, &selector, progress).await?;
    let requested_target_ids = requested_watched_targets
        .iter()
        .map(|target| target.contract_instance_id)
        .collect::<BTreeSet<_>>();

    let mut selected_contracts = Vec::new();
    for (index, watched_contract) in watched_contracts.iter().enumerate() {
        let selected = watched_contract.chain == chain
            && super::watched_contract_range_intersects(
                watched_contract,
                range_start_block_number,
                range_end_block_number,
            )
            && match &selector {
                WatchedSourceSelector::WholeActiveWatchedChain => true,
                WatchedSourceSelector::SourceFamily(source_family) => {
                    watched_contract.source_family == *source_family
                }
                WatchedSourceSelector::WatchedTargetSet(_) => {
                    requested_target_ids.contains(&watched_contract.contract_instance_id)
                }
            };
        if selected {
            selected_contracts.push(watched_contract.clone());
        }
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, watched_contracts.len()).await?;

    validate_selection(
        pool,
        chain,
        &selector,
        &requested_watched_targets,
        &selected_contracts,
        progress,
    )
    .await?;
    let selected_targets = selected_backfill_targets_with_progress(
        pool,
        &selected_contracts,
        range_start_block_number,
        range_end_block_number,
        progress,
    )
    .await?;
    let watched_chain_plan =
        super::plan_watched_contracts_with_progress(pool, &selected_contracts, progress)
            .await?
            .into_iter()
            .next()
            .unwrap_or_else(|| WatchedChainPlan {
                chain: chain.to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            });

    Ok(WatchedSourceSelectorPlan {
        chain: chain.to_owned(),
        selector_kind,
        source_family,
        requested_watched_targets,
        selected_targets,
        watched_chain_plan,
    })
}

async fn normalize_requested_targets(
    pool: &sqlx::PgPool,
    selector: &WatchedSourceSelector,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<WatchedTargetIdentity>> {
    let targets = match selector {
        WatchedSourceSelector::WatchedTargetSet(targets) => targets,
        _ => return Ok(Vec::new()),
    };
    let mut unique = BTreeSet::new();
    for (index, target) in targets.iter().enumerate() {
        unique.insert(target.clone());
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, targets.len()).await?;
    Ok(unique.into_iter().collect())
}

async fn validate_selection(
    pool: &sqlx::PgPool,
    chain: &str,
    selector: &WatchedSourceSelector,
    requested: &[WatchedTargetIdentity],
    selected: &[WatchedContract],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<()> {
    match selector {
        WatchedSourceSelector::WholeActiveWatchedChain if selected.is_empty() => bail!(
            "watched source selector whole_active_watched_chain found no active watched targets for chain {chain}"
        ),
        WatchedSourceSelector::SourceFamily(source_family) if selected.is_empty() => bail!(
            "watched source selector source_family {source_family} found no active watched targets for chain {chain}"
        ),
        WatchedSourceSelector::WatchedTargetSet(_) => {
            if requested.is_empty() {
                bail!("watched_target_set selector must include at least one contract_instance_id");
            }
            let selected_ids = selected
                .iter()
                .map(|contract| contract.contract_instance_id)
                .collect::<BTreeSet<_>>();
            for (index, requested_target) in requested.iter().enumerate() {
                if !selected_ids.contains(&requested_target.contract_instance_id) {
                    bail!(
                        "watched target {} is not active for chain {chain} in the selected range",
                        requested_target.contract_instance_id
                    );
                }
                record_every(pool, progress, index + 1).await?;
            }
            record_tail(pool, progress, requested.len()).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn selected_backfill_targets_with_progress(
    pool: &sqlx::PgPool,
    watched_contracts: &[WatchedContract],
    range_start_block_number: i64,
    range_end_block_number: i64,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<WatchedBackfillTarget>> {
    let mut addresses_by_identity = BTreeMap::<(String, Uuid), String>::new();
    let mut selected_targets = BTreeSet::new();
    for (index, watched_contract) in watched_contracts.iter().enumerate() {
        let Some((effective_from_block, effective_to_block)) =
            super::watched_contract_effective_range(
                watched_contract,
                range_start_block_number,
                range_end_block_number,
            )
        else {
            continue;
        };
        let target = WatchedBackfillTarget {
            source_family: watched_contract.source_family.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            address: watched_contract.address.clone(),
            effective_from_block,
            effective_to_block,
        };
        let identity = (target.source_family.clone(), target.contract_instance_id);
        if let Some(existing_address) = addresses_by_identity.get(&identity) {
            if existing_address != &target.address {
                bail!(
                    "source identity conflict for watched target {} in source family {}",
                    target.contract_instance_id,
                    target.source_family
                );
            }
        } else {
            addresses_by_identity.insert(identity, target.address.clone());
        }
        selected_targets.insert(target);
        record_every(pool, progress, index + 1).await?;
    }
    record_tail(pool, progress, watched_contracts.len()).await?;

    let mut result = Vec::with_capacity(selected_targets.len());
    for target in selected_targets {
        result.push(target);
        record_every(pool, progress, result.len()).await?;
    }
    record_tail(pool, progress, result.len()).await?;
    Ok(result)
}

async fn record_every(
    pool: &sqlx::PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
    completed: usize,
) -> Result<()> {
    if completed.is_multiple_of(super::super::WATCHED_PLAN_PROGRESS_ROWS) {
        progress.record(pool).await?;
    }
    Ok(())
}

async fn record_tail(
    pool: &sqlx::PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
    completed: usize,
) -> Result<()> {
    if completed > 0 && !completed.is_multiple_of(super::super::WATCHED_PLAN_PROGRESS_ROWS) {
        progress.record(pool).await?;
    }
    Ok(())
}
