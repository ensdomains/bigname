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

use std::{collections::BTreeSet, future::Future, pin::Pin};

use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};

use crate::{WatchedContract, WatchedContractSource, normalize_address};

const WATCHED_PLAN_PROGRESS_ROWS: usize = 10_000;

pub type ManifestRuntimeProgressFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait ManifestRuntimeProgress: Send {
    fn record<'a>(&'a mut self, pool: &'a PgPool) -> ManifestRuntimeProgressFuture<'a>;
}

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

fn streaming_watched_contracts_sql() -> String {
    intervals::with_streaming_watched_intervals(&format!(
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
"#,
        current_predicate = intervals::CURRENT_WATCHED_INTERVAL_PREDICATE,
    ))
}

fn resolver_profile_authority_target_cursor_sql() -> String {
    let targets = intervals::with_streaming_watched_intervals(&format!(
        r#"
SELECT watched.chain, watched.address
FROM watched_intervals watched
WHERE {current_predicate}
  AND watched.source_family IN (
      'ens_v1_resolver_l1',
      'basenames_base_resolver'
  )
"#,
        current_predicate = intervals::CURRENT_WATCHED_INTERVAL_PREDICATE,
    ));
    format!("DECLARE resolver_profile_authority_targets NO SCROLL CURSOR FOR\n{targets}")
}

/// One streaming server-side cursor over current addresses which can
/// contribute ENSv1 or Basenames
/// [resolver-profile](../../../../../docs/glossary.md) authority entries. The
/// caller deduplicates across pages so PostgreSQL need not sort the complete
/// multi-million-row watched surface before returning the first page.
pub struct ResolverProfileAuthorityTargetPages;

impl ResolverProfileAuthorityTargetPages {
    /// Declare the cursor on a caller-owned open transaction.
    pub async fn begin(connection: &mut PgConnection) -> Result<Self> {
        sqlx::query(&resolver_profile_authority_target_cursor_sql())
            .execute(connection)
            .await
            .context("failed to declare resolver-profile authority target cursor")?;
        Ok(Self)
    }

    pub async fn next_page(
        &mut self,
        connection: &mut PgConnection,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        ensure!(
            limit > 0,
            "resolver-profile authority target page limit must be positive"
        );
        let sql = format!("FETCH FORWARD {limit} FROM resolver_profile_authority_targets");
        sqlx::query_as::<_, (String, String)>(&sql)
            .fetch_all(connection)
            .await
            .context("failed to fetch resolver-profile authority target page")
    }

    pub async fn finish(self, connection: &mut PgConnection) -> Result<()> {
        sqlx::query("CLOSE resolver_profile_authority_targets")
            .execute(connection)
            .await
            .context("failed to finish resolver-profile authority target stream")
            .map(|_| ())
    }
}

pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let query = watched_contracts_sql(WatchedContractsFilter::All);
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .context("failed to load watched contracts")?;

    watched_contracts_from_rows(rows)
}

pub async fn load_watched_contracts_with_progress(
    pool: &PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<WatchedContract>> {
    let query = streaming_watched_contracts_sql();
    let mut rows = sqlx::query(&query).fetch(pool);
    let mut watched_contracts = BTreeSet::new();
    let mut streamed_row_count = 0usize;
    while let Some(row) = rows
        .try_next()
        .await
        .context("failed to stream watched contracts")?
    {
        watched_contracts.insert(watched_contract_from_row(row)?);
        streamed_row_count += 1;
        if streamed_row_count.is_multiple_of(WATCHED_PLAN_PROGRESS_ROWS) {
            progress.record(pool).await?;
        }
    }
    if streamed_row_count > 0 && !streamed_row_count.is_multiple_of(WATCHED_PLAN_PROGRESS_ROWS) {
        progress.record(pool).await?;
    }
    Ok(watched_contracts.into_iter().collect())
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
    rows.into_iter().map(watched_contract_from_row).collect()
}

fn watched_contract_from_row(row: PgRow) -> Result<WatchedContract> {
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
        WatchedContractsFilter, manifest_declared_watched_contracts_sql,
        resolver_profile_authority_target_cursor_sql, watched_contracts_sql,
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

        let resolver_targets = resolver_profile_authority_target_cursor_sql();
        assert!(resolver_targets.contains("DECLARE resolver_profile_authority_targets"));
        assert!(resolver_targets.contains("'ens_v1_resolver_l1'"));
        assert!(resolver_targets.contains("'basenames_base_resolver'"));
        assert!(resolver_targets.contains("SELECT watched.chain, watched.address"));
        assert!(resolver_targets.contains("UNION ALL"));
        assert!(!resolver_targets.contains("ORDER BY watched.chain, watched.address"));
    }
}
