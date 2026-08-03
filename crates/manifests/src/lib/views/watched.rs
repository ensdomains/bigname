#[path = "watched/intervals.rs"]
mod intervals;
#[path = "watched/selection.rs"]
mod selection;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{WatchedContract, WatchedContractSource, normalize_address};

pub use selection::*;

#[derive(Clone, Copy)]
enum WatchedContractsFilter {
    All,
    SourceFamily,
}

impl WatchedContractsFilter {
    const fn predicate(self) -> &'static str {
        match self {
            Self::All => "",
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

pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let query = watched_contracts_sql(WatchedContractsFilter::All);
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .context("failed to load watched contracts")?;

    watched_contracts_from_rows(rows)
}

pub(super) async fn load_watched_contracts_by_source_family(
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

#[cfg(test)]
mod query_tests {
    use super::{WatchedContractsFilter, watched_contracts_sql};

    #[test]
    fn watched_contract_queries_keep_their_filter_and_bind_shapes() {
        let all = watched_contracts_sql(WatchedContractsFilter::All);
        assert!(!all.contains("$1"));

        let source_family = watched_contracts_sql(WatchedContractsFilter::SourceFamily);
        assert_eq!(source_family.matches("$1").count(), 1);
        assert!(source_family.contains("AND watched.source_family = $1"));
    }
}
