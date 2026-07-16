#[path = "watched/coverage.rs"]
mod coverage;
#[path = "watched/frontier.rs"]
mod frontier;
#[path = "watched/historical.rs"]
mod historical;
#[path = "watched/intervals.rs"]
mod intervals;
#[path = "watched/scoped.rs"]
mod scoped;
#[path = "watched/selection.rs"]
mod selection;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{WatchedContract, WatchedContractSource, normalize_address};

pub use coverage::{
    RequiredWatchedTuple, UncoveredWatchedTuple, find_uncovered_required_watched_tuples,
    find_uncovered_required_watched_tuples_for_retention_generation,
    find_uncovered_required_watched_tuples_for_retention_generation_in_transaction,
    find_uncovered_required_watched_tuples_in_transaction, find_uncovered_watched_tuples,
    load_required_watched_tuples, load_required_watched_tuples_in_transaction,
};
pub use frontier::{
    StoredLineageCoverageCandidateSummary, StoredLineageCoverageDeltaCursor,
    StoredLineageCoverageDeltaPage, load_earliest_known_watched_block,
    load_stored_lineage_coverage_candidate_delta_page,
    materialize_stored_lineage_coverage_candidate,
};
pub use historical::{
    ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES, load_ens_v2_authoritative_discovery_bootstrap_targets,
    load_ens_v2_retained_history_recovery_targets, load_historical_watched_contracts_by_chain,
};
pub use scoped::{
    load_watched_contracts_by_addresses, load_watched_contracts_by_source_family_and_addresses,
};
pub use selection::*;

#[derive(Clone, Copy)]
enum WatchedContractsFilter {
    All,
    Chain,
    SourceFamily,
}

impl WatchedContractsFilter {
    const fn predicate(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Chain => "AND watched.chain = $1",
            Self::SourceFamily => "AND watched.source_family = $1",
        }
    }
}

fn watched_contracts_sql(filter: WatchedContractsFilter) -> String {
    intervals::with_watched_intervals(&format!(
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
WHERE {current_predicate}
{filter_predicate}
ORDER BY 1, 2, 3, 5, 6, 4
"#,
        current_predicate = intervals::CURRENT_WATCHED_INTERVAL_PREDICATE,
        filter_predicate = filter.predicate(),
    ))
}

fn manifest_declared_watched_contracts_sql() -> String {
    intervals::with_watched_intervals(&format!(
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
WHERE {current_predicate}
  AND watched.source <> 'discovery_edge'
ORDER BY 1, 2, 3, 5, 6, 4
"#,
        current_predicate = intervals::CURRENT_WATCHED_INTERVAL_PREDICATE,
    ))
}

pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let query = watched_contracts_sql(WatchedContractsFilter::All);
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .context("failed to load watched contracts")?;

    watched_contracts_from_rows(rows)
}

pub async fn load_watched_contracts_by_chain(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<WatchedContract>> {
    let query = watched_contracts_sql(WatchedContractsFilter::Chain);
    let rows = sqlx::query(&query)
        .bind(chain)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load watched contracts for chain {chain}"))?;

    watched_contracts_from_rows(rows)
}

pub async fn load_watched_contracts_by_source_family(
    pool: &PgPool,
    source_family: &str,
) -> Result<Vec<WatchedContract>> {
    let query = watched_contracts_sql(WatchedContractsFilter::SourceFamily);
    let rows = sqlx::query(&query)
        .bind(source_family)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load watched contracts for source family {source_family}")
        })?;

    watched_contracts_from_rows(rows)
}

pub async fn load_manifest_declared_watched_contracts(
    pool: &PgPool,
) -> Result<Vec<WatchedContract>> {
    let query = manifest_declared_watched_contracts_sql();
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .context("failed to load manifest-declared watched contracts")?;

    watched_contracts_from_rows(rows)
}

fn watched_contracts_from_rows(rows: Vec<PgRow>) -> Result<Vec<WatchedContract>> {
    rows.into_iter()
        .map(|row| {
            let source = row
                .try_get::<String, _>("source")
                .context("failed to read watched contract source")?;
            Ok(WatchedContract {
                chain: row
                    .try_get("chain")
                    .context("failed to read watched contract chain")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read watched contract source_family")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read watched contract address")?,
                ),
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read watched contract_instance_id")?,
                source: WatchedContractSource::from_db_value(&source)?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read watched contract source_manifest_id")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read watched contract active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read watched contract active_to_block_number")?,
            })
        })
        .collect()
}

fn sort_and_dedup_watched_contracts(watched_contracts: &mut Vec<WatchedContract>) {
    watched_contracts.sort_by(|left, right| {
        (
            left.chain.as_str(),
            left.source_family.as_str(),
            left.address.as_str(),
            left.source,
            left.source_manifest_id,
            left.contract_instance_id,
            left.active_from_block_number,
            left.active_to_block_number,
        )
            .cmp(&(
                right.chain.as_str(),
                right.source_family.as_str(),
                right.address.as_str(),
                right.source,
                right.source_manifest_id,
                right.contract_instance_id,
                right.active_from_block_number,
                right.active_to_block_number,
            ))
    });
    watched_contracts.dedup_by(|left, right| {
        left.chain == right.chain
            && left.source_family == right.source_family
            && left.address == right.address
            && left.source == right.source
            && left.source_manifest_id == right.source_manifest_id
            && left.contract_instance_id == right.contract_instance_id
            && left.active_from_block_number == right.active_from_block_number
            && left.active_to_block_number == right.active_to_block_number
    });
}

#[cfg(test)]
mod query_tests {
    use super::{
        WatchedContractsFilter, manifest_declared_watched_contracts_sql, watched_contracts_sql,
    };

    #[test]
    fn watched_contract_queries_keep_their_filter_and_bind_shapes() {
        let all = watched_contracts_sql(WatchedContractsFilter::All);
        assert!(!all.contains("$1"));

        let chain = watched_contracts_sql(WatchedContractsFilter::Chain);
        assert_eq!(chain.matches("$1").count(), 1);
        assert!(chain.contains("AND watched.chain = $1"));

        let source_family = watched_contracts_sql(WatchedContractsFilter::SourceFamily);
        assert_eq!(source_family.matches("$1").count(), 1);
        assert!(source_family.contains("AND watched.source_family = $1"));

        let manifest_declared = manifest_declared_watched_contracts_sql();
        assert!(!manifest_declared.contains("$1"));
        assert!(manifest_declared.contains("watched.source <> 'discovery_edge'"));
    }
}
