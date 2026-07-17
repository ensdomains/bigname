use anyhow::{Context, Result};
use bigname_domain::block_interval::InclusiveBlockInterval;
use sqlx::PgPool;

use crate::{
    ManifestBootstrapTarget, WatchedContract, WatchedContractSource,
    load_log_producing_source_families, normalize_address,
};

use super::{
    load_required_watched_tuples, sort_and_dedup_watched_contracts, watched_contracts_from_rows,
};

pub const ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES: &[&str] =
    &["ens_v2_root_l1", "ens_v2_registry_l1"];
const ENS_V2_DISCOVERY_BOOTSTRAP_ADDITIONAL_SOURCE_FAMILIES: &[&str] = &["ens_v2_resolver_l1"];

fn ens_v2_discovery_bootstrap_source_families() -> impl Iterator<Item = &'static str> {
    ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES
        .iter()
        .chain(ENS_V2_DISCOVERY_BOOTSTRAP_ADDITIONAL_SOURCE_FAMILIES)
        .copied()
}

/// Load finite provider-backfill targets for every known-start ENSv2 root,
/// registry, or resolver discovery edge which remains authoritative under the
/// active post-audit manifest corpus.
///
/// The active manifest ABI decides which mapped target families produce logs;
/// required watched tuples decide the exact authoritative intervals; and the
/// historical watched view supplies the stable discovered contract identity.
/// Deprecated manifests, migrations, event-silent target families, and rows
/// without a known start are therefore not promoted into automatic history.
pub async fn load_ens_v2_authoritative_discovery_bootstrap_targets(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<Vec<ManifestBootstrapTarget>> {
    if through_block < 0 {
        anyhow::bail!("discovery bootstrap target block must be non-negative");
    }

    let active_log_producing_families = load_log_producing_source_families(pool, chain).await?;
    let source_families = ens_v2_discovery_bootstrap_source_families()
        .filter(|source_family| {
            active_log_producing_families
                .iter()
                .any(|active| active.as_str() == *source_family)
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if source_families.is_empty() {
        return Ok(Vec::new());
    }
    let requirements =
        load_required_watched_tuples(pool, chain, 0, through_block, &source_families).await?;
    let historical = load_historical_watched_contracts_by_chain(pool, chain).await?;
    let mut targets = std::collections::BTreeSet::new();

    for contract in historical
        .iter()
        .filter(|contract| contract.source == WatchedContractSource::DiscoveryEdge)
    {
        let Some(active_from_block) = contract.active_from_block_number else {
            continue;
        };
        let address = normalize_address(&contract.address);
        for requirement in requirements.iter().filter(|requirement| {
            requirement.source_family == contract.source_family && requirement.address == address
        }) {
            let effective_from_block = active_from_block.max(requirement.required_from_block);
            let effective_to_block = contract
                .active_to_block_number
                .unwrap_or(through_block)
                .min(requirement.required_to_block);
            if effective_from_block > effective_to_block {
                continue;
            }
            targets.insert(ManifestBootstrapTarget {
                source_family: requirement.source_family.clone(),
                contract_instance_id: contract.contract_instance_id,
                address: address.clone(),
                effective_from_block,
                effective_to_block: Some(effective_to_block),
            });
        }
    }

    Ok(targets.into_iter().collect())
}

/// Load current manifest declarations plus every bounded manifest-address or
/// discovery interval retained under the active manifest corpus for
/// full-closure replay.
pub async fn load_historical_watched_contracts_by_chain(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<WatchedContract>> {
    let query = super::intervals::with_watched_intervals(&format!(
        r#"
        SELECT
            watched.chain,
            watched.source_family,
            watched.address,
            watched.contract_instance_id,
            watched.source,
            watched.source_manifest_id,
            watched.active_from_block_number,
            watched.active_to_block_number
        FROM watched_intervals watched
        WHERE {historical_predicate}
          AND watched.chain = $1
        ORDER BY 1, 2, 3, 5, 6, 4, 7, 8
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ));
    let rows = sqlx::query(&query)
        .bind(chain)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load historical watched contracts for chain {chain}")
        })?;

    let mut watched_contracts = watched_contracts_from_rows(rows)?;
    sort_and_dedup_watched_contracts(&mut watched_contracts);
    Ok(watched_contracts)
}

/// Build the finite, historically authoritative ENSv2 root/registry targets
/// needed to recover a retained-history proof through `through_block`.
///
/// Coverage authority comes from [`load_required_watched_tuples`]. Historical
/// watched rows are used only to recover stable contract-instance identities
/// for those exact family/address intervals. Rows without a known start are
/// deliberately omitted: automatic recovery must not invent block zero (or
/// any other historical start) for an unknown interval.
pub async fn load_ens_v2_retained_history_recovery_targets(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<Vec<ManifestBootstrapTarget>> {
    if through_block < 0 {
        anyhow::bail!("retained-history recovery target block must be non-negative");
    }

    let source_families = ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES
        .iter()
        .map(|source_family| (*source_family).to_owned())
        .collect::<Vec<_>>();
    let requirements =
        load_required_watched_tuples(pool, chain, 0, through_block, &source_families).await?;
    let historical = load_historical_watched_contracts_by_chain(pool, chain).await?;
    let mut targets = std::collections::BTreeSet::new();

    for requirement in requirements {
        let required_interval = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        )
        .with_context(|| {
            format!(
                "required retained-history tuple {}/{} has an inverted interval {}..={}",
                requirement.source_family,
                requirement.address,
                requirement.required_from_block,
                requirement.required_to_block
            )
        })?;
        let mut covered_intervals = Vec::new();
        for contract in historical.iter().filter(|contract| {
            contract.source_family == requirement.source_family
                && normalize_address(&contract.address) == requirement.address
        }) {
            let Some(active_from_block) = contract.active_from_block_number else {
                continue;
            };
            let effective_from_block = active_from_block.max(requirement.required_from_block);
            let effective_to_block = contract
                .active_to_block_number
                .unwrap_or(through_block)
                .min(requirement.required_to_block);
            if effective_from_block > effective_to_block {
                continue;
            }

            covered_intervals.push(
                InclusiveBlockInterval::new(effective_from_block, effective_to_block)
                    .expect("clamped historical watched interval must not be inverted"),
            );

            targets.insert(ManifestBootstrapTarget {
                source_family: requirement.source_family.clone(),
                contract_instance_id: contract.contract_instance_id,
                address: requirement.address.clone(),
                effective_from_block,
                effective_to_block: Some(effective_to_block),
            });
        }

        if !retained_requirement_is_covered(required_interval, covered_intervals) {
            anyhow::bail!(
                "required retained-history tuple {}/{} over {}..={} has no gap-free known-start historical contract identity",
                requirement.source_family,
                requirement.address,
                requirement.required_from_block,
                requirement.required_to_block
            );
        }
    }

    Ok(targets.into_iter().collect())
}

fn retained_requirement_is_covered(
    required: InclusiveBlockInterval,
    covered: impl IntoIterator<Item = InclusiveBlockInterval>,
) -> bool {
    required.is_covered_by(covered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_discovery_bootstrap_is_limited_to_ens_v2_log_families() {
        assert_eq!(
            ens_v2_discovery_bootstrap_source_families().collect::<Vec<_>>(),
            vec!["ens_v2_root_l1", "ens_v2_registry_l1", "ens_v2_resolver_l1",]
        );
        assert!(
            !ens_v2_discovery_bootstrap_source_families()
                .any(|family| family == "ens_v1_resolver_l1")
        );
        assert!(
            !ens_v2_discovery_bootstrap_source_families()
                .any(|family| family == "basenames_base_resolver")
        );
    }

    #[test]
    fn retained_requirement_uses_gap_free_union_through_terminal_block() {
        let interval = |from_block, through_block| {
            InclusiveBlockInterval::new(from_block, through_block)
                .expect("test interval must not be inverted")
        };
        let required = interval(i64::MAX - 3, i64::MAX);

        assert!(retained_requirement_is_covered(
            required,
            [
                interval(i64::MAX, i64::MAX),
                interval(i64::MAX - 3, i64::MAX - 1),
            ]
        ));
        assert!(!retained_requirement_is_covered(
            required,
            [
                interval(i64::MAX - 3, i64::MAX - 2),
                interval(i64::MAX, i64::MAX),
            ]
        ));
    }
}
