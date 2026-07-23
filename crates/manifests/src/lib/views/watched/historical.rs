use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_domain::block_interval::InclusiveBlockInterval;
use futures_util::TryStreamExt;
use sqlx::PgPool;

use crate::{
    ManifestBootstrapTarget, ManifestRuntimeProgress, WatchedContract, WatchedContractSource,
    load_log_producing_source_families, normalize_address,
};

use super::{sort_and_dedup_watched_contracts, watched_contracts_from_rows};

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
    load_ens_v2_authoritative_discovery_bootstrap_targets_inner(pool, chain, through_block, None)
        .await
}

pub async fn load_ens_v2_authoritative_discovery_bootstrap_targets_with_progress(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<ManifestBootstrapTarget>> {
    load_ens_v2_authoritative_discovery_bootstrap_targets_inner(
        pool,
        chain,
        through_block,
        Some(progress),
    )
    .await
}

async fn load_ens_v2_authoritative_discovery_bootstrap_targets_inner(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    mut progress: Option<&mut dyn ManifestRuntimeProgress>,
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
        .collect::<BTreeSet<_>>();
    if source_families.is_empty() {
        return Ok(Vec::new());
    }
    record_progress(pool, &mut progress).await?;
    let source_family_filter = source_families.iter().cloned().collect::<Vec<_>>();
    let historical =
        load_historical_with_optional_progress(pool, chain, &source_family_filter, &mut progress)
            .await?;
    let mut targets = BTreeSet::new();

    for (index, contract) in historical.iter().enumerate() {
        if (index + 1).is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
            record_progress(pool, &mut progress).await?;
        }
        if contract.source != WatchedContractSource::DiscoveryEdge
            || !source_families.contains(&contract.source_family)
        {
            continue;
        }
        let Some(active_from_block) = contract.active_from_block_number else {
            continue;
        };
        let address = normalize_address(&contract.address);
        let effective_from_block = active_from_block.max(0);
        let effective_to_block = contract
            .active_to_block_number
            .unwrap_or(through_block)
            .min(through_block);
        if effective_from_block <= effective_to_block {
            targets.insert(ManifestBootstrapTarget {
                source_family: contract.source_family.clone(),
                contract_instance_id: contract.contract_instance_id,
                address,
                effective_from_block,
                effective_to_block: Some(effective_to_block),
            });
        }
    }
    record_tail_progress(pool, &mut progress, historical.len()).await?;

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

pub async fn load_historical_watched_contracts_by_chain_with_progress(
    pool: &PgPool,
    chain: &str,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<WatchedContract>> {
    load_historical_watched_contracts_scoped_with_progress(pool, chain, &[], progress).await
}

/// Stream retained watch intervals for one chain and optional source-family
/// subset without a server-side union sort. This is the progress-aware route
/// for adapter preloads whose cardinality grows with admitted discovery.
pub async fn load_historical_watched_contracts_scoped_with_progress(
    pool: &PgPool,
    chain: &str,
    source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<WatchedContract>> {
    let query = super::intervals::with_streaming_watched_intervals(&format!(
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
          AND (
              cardinality($2::TEXT[]) = 0
              OR watched.source_family = ANY($2::TEXT[])
          )
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ));
    let mut rows = sqlx::query(&query)
        .bind(chain)
        .bind(source_families)
        .fetch(pool);
    let mut watched_contracts = BTreeSet::new();
    let mut streamed_row_count = 0usize;
    while let Some(row) = rows
        .try_next()
        .await
        .with_context(|| format!("failed to stream historical watched contracts for {chain}"))?
    {
        watched_contracts.insert(super::watched_contract_from_row(row)?);
        streamed_row_count += 1;
        if streamed_row_count.is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
            progress.record(pool).await?;
        }
    }
    if streamed_row_count > 0
        && !streamed_row_count.is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS)
    {
        progress.record(pool).await?;
    }

    let mut result = Vec::with_capacity(watched_contracts.len());
    for watched_contract in watched_contracts {
        result.push(watched_contract);
        if result
            .len()
            .is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS)
        {
            progress.record(pool).await?;
        }
    }
    if !result.is_empty()
        && !result
            .len()
            .is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS)
    {
        progress.record(pool).await?;
    }
    Ok(result)
}

/// Load the retained intervals for one exact watched target. This avoids a
/// whole-chain historical scan when live coverage recovery already knows the
/// family and address whose proof is missing.
pub async fn load_historical_watched_contracts_for_target(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    address: &str,
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
          AND watched.source_family = $2
          AND watched.address = $3
        ORDER BY 1, 2, 3, 5, 6, 4, 7, 8
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ));
    let rows = sqlx::query(&query)
        .bind(chain)
        .bind(source_family)
        .bind(normalize_address(address))
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load historical watched target {chain}/{source_family}/{address}")
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
    load_ens_v2_retained_history_recovery_targets_inner(pool, chain, through_block, None).await
}

pub async fn load_ens_v2_retained_history_recovery_targets_with_progress(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<ManifestBootstrapTarget>> {
    load_ens_v2_retained_history_recovery_targets_inner(pool, chain, through_block, Some(progress))
        .await
}

async fn load_ens_v2_retained_history_recovery_targets_inner(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    mut progress: Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<Vec<ManifestBootstrapTarget>> {
    if through_block < 0 {
        anyhow::bail!("retained-history recovery target block must be non-negative");
    }

    let source_families = ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES
        .iter()
        .map(|source_family| (*source_family).to_owned())
        .collect::<BTreeSet<_>>();
    let source_family_filter = source_families.iter().cloned().collect::<Vec<_>>();
    let historical =
        load_historical_with_optional_progress(pool, chain, &source_family_filter, &mut progress)
            .await?;
    let mut required = BTreeMap::<(String, String), Vec<InclusiveBlockInterval>>::new();
    let mut covered = BTreeMap::<(String, String), Vec<InclusiveBlockInterval>>::new();
    let mut targets = BTreeSet::new();

    for (index, contract) in historical.iter().enumerate() {
        if (index + 1).is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
            record_progress(pool, &mut progress).await?;
        }
        if !source_families.contains(&contract.source_family) {
            continue;
        }
        let address = normalize_address(&contract.address);
        let required_from_block = contract.active_from_block_number.unwrap_or(0).max(0);
        let required_to_block = contract
            .active_to_block_number
            .unwrap_or(through_block)
            .min(through_block);
        if required_from_block > required_to_block {
            continue;
        }
        let key = (contract.source_family.clone(), address.clone());
        required.entry(key.clone()).or_default().push(
            InclusiveBlockInterval::new(required_from_block, required_to_block)
                .expect("clamped required watched interval must not be inverted"),
        );
        if let Some(active_from_block) = contract.active_from_block_number {
            let effective_from_block = active_from_block.max(required_from_block);
            let interval = InclusiveBlockInterval::new(effective_from_block, required_to_block)
                .expect("known-start historical watched interval must not be inverted");
            covered.entry(key).or_default().push(interval);
            targets.insert(ManifestBootstrapTarget {
                source_family: contract.source_family.clone(),
                contract_instance_id: contract.contract_instance_id,
                address,
                effective_from_block,
                effective_to_block: Some(required_to_block),
            });
        }
    }
    record_tail_progress(pool, &mut progress, historical.len()).await?;

    for ((source_family, address), required_intervals) in required {
        let covered_intervals = covered.get(&(source_family.clone(), address.clone()));
        for required_interval in required_intervals {
            if !retained_requirement_is_covered(
                required_interval,
                covered_intervals.into_iter().flatten().copied(),
            ) {
                anyhow::bail!(
                    "required retained-history tuple {source_family}/{address} over {}..={} has no gap-free known-start historical contract identity",
                    required_interval.from_block(),
                    required_interval.through_block()
                );
            }
        }
        record_progress(pool, &mut progress).await?;
    }

    Ok(targets.into_iter().collect())
}

async fn load_historical_with_optional_progress(
    pool: &PgPool,
    chain: &str,
    source_families: &[String],
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<Vec<WatchedContract>> {
    match progress.as_deref_mut() {
        Some(progress) => {
            load_historical_watched_contracts_scoped_with_progress(
                pool,
                chain,
                source_families,
                progress,
            )
            .await
        }
        None => load_historical_watched_contracts_by_chain(pool, chain).await,
    }
}

async fn record_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

async fn record_tail_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
    row_count: usize,
) -> Result<()> {
    if row_count > 0 && !row_count.is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
        record_progress(pool, progress).await?;
    }
    Ok(())
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
