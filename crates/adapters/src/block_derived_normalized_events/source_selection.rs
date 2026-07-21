use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{load_historical_watched_contracts_by_chain, load_watched_contracts};
use sqlx::PgPool;

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, source_rank, watched_contract_manifest_ids,
};

use super::types::{ActiveEmitter, RawLogSourceScopeTarget};

pub(super) fn normalized_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<RawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                RawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: address.to_ascii_lowercase(),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                }
            },
        )
        .collect()
}

#[derive(Clone, Debug, Default)]
pub(super) struct RawLogSourceScopeIndex {
    ranges_by_family_and_address: HashMap<String, HashMap<String, Vec<(i64, i64)>>>,
}

impl RawLogSourceScopeIndex {
    pub(super) fn new(targets: &[RawLogSourceScopeTarget]) -> Result<Self> {
        let mut ranges_by_family_and_address =
            HashMap::<String, HashMap<String, Vec<(i64, i64)>>>::new();
        for target in targets {
            if target.effective_from_block > target.effective_to_block {
                bail!(
                    "block-derived source scope for {}/{} has inverted interval {}..={}",
                    target.source_family,
                    target.address,
                    target.effective_from_block,
                    target.effective_to_block,
                );
            }
            ranges_by_family_and_address
                .entry(target.source_family.clone())
                .or_default()
                .entry(target.address.clone())
                .or_default()
                .push((target.effective_from_block, target.effective_to_block));
        }

        for ranges_by_address in ranges_by_family_and_address.values_mut() {
            for ranges in ranges_by_address.values_mut() {
                ranges.sort_unstable();
                let mut merged = Vec::<(i64, i64)>::with_capacity(ranges.len());
                for (from_block, to_block) in ranges.drain(..) {
                    match merged.last_mut() {
                        Some((_, merged_to)) if from_block <= merged_to.saturating_add(1) => {
                            *merged_to = (*merged_to).max(to_block);
                        }
                        _ => merged.push((from_block, to_block)),
                    }
                }
                *ranges = merged;
            }
        }

        Ok(Self {
            ranges_by_family_and_address,
        })
    }

    pub(super) fn contains(&self, source_family: &str, address: &str, block_number: i64) -> bool {
        let Some(ranges) = self
            .ranges_by_family_and_address
            .get(source_family)
            .and_then(|ranges_by_address| ranges_by_address.get(address))
        else {
            return false;
        };
        let insertion = ranges.partition_point(|(from_block, _)| *from_block <= block_number);
        insertion > 0 && ranges[insertion - 1].1 >= block_number
    }
}

pub(super) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    scoped_emitter_identities: Option<&HashSet<(String, String)>>,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = if scoped_emitter_identities.is_some() {
        load_historical_watched_contracts_by_chain(pool, chain)
            .await
            .context("failed to load historical watched contracts for scoped adapter emitter attribution")?
    } else {
        load_watched_contracts(pool)
            .await
            .context("failed to load watched contracts for adapter emitter attribution")?
    };
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .filter(|contract| {
            scoped_emitter_identities.is_none_or(|scope| {
                scope.contains(&(contract.source_family.clone(), contract.address.clone()))
            })
        })
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, "watched contracts").await?;

    let mut emitters = Vec::with_capacity(watched_contracts.len());
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            source_rank: source_rank(watched_contract.source),
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
        };
        emitters.push(candidate);
    }

    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
    });
    Ok(emitters)
}

pub(super) fn select_emitter_for_block<'a>(
    chain: &str,
    address: &str,
    block_number: i64,
    emitters: &'a [ActiveEmitter],
    source_scope: Option<&RawLogSourceScopeIndex>,
) -> Result<Option<&'a ActiveEmitter>> {
    let mut applicable = emitters.iter().filter(|emitter| {
        emitter_covers_block(emitter, block_number)
            && source_scope.is_none_or(|index| {
                index.contains(&emitter.source_family, &emitter.address, block_number)
            })
    });
    let Some(mut selected) = applicable.next() else {
        return Ok(None);
    };

    for candidate in applicable {
        if candidate.source_rank == selected.source_rank
            && !same_emitter_attribution(candidate, selected)
        {
            bail!(
                "ambiguous block-derived emitter attribution for {chain} address {address} at block {block_number}: {} manifest_id {} conflicts with {} manifest_id {} at equal source rank {}",
                selected.source_family,
                selected.source_manifest_id,
                candidate.source_family,
                candidate.source_manifest_id,
                selected.source_rank,
            );
        }
        if candidate_precedes(candidate, selected) {
            selected = candidate;
        }
    }

    Ok(Some(selected))
}

fn emitter_covers_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|from_block| block_number >= from_block)
        && emitter
            .active_to_block_number
            .is_none_or(|to_block| block_number <= to_block)
}

fn same_emitter_attribution(left: &ActiveEmitter, right: &ActiveEmitter) -> bool {
    left.source_manifest_id == right.source_manifest_id
        && left.namespace == right.namespace
        && left.source_family == right.source_family
        && left.manifest_version == right.manifest_version
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (
        candidate.source_rank,
        candidate.source_manifest_id,
        candidate.contract_instance_id,
    ) < (
        current.source_rank,
        current.source_manifest_id,
        current.contract_instance_id,
    )
}
