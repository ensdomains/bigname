use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{WatchedContract, normalize_address};

pub async fn load_watched_contracts_by_source_family_and_addresses(
    pool: &PgPool,
    source_family: &str,
    targets: &[(String, String)],
) -> Result<Vec<WatchedContract>> {
    load_watched_contracts_by_addresses_scoped(pool, targets, Some(source_family)).await
}

pub async fn load_watched_contracts_by_addresses(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<Vec<WatchedContract>> {
    load_watched_contracts_by_addresses_scoped(pool, targets, None).await
}

async fn load_watched_contracts_by_addresses_scoped(
    pool: &PgPool,
    targets: &[(String, String)],
    source_family: Option<&str>,
) -> Result<Vec<WatchedContract>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let targets = targets
        .iter()
        .map(|(chain, address)| (chain.clone(), normalize_address(address)))
        .collect::<BTreeSet<_>>();
    let chains = targets
        .iter()
        .map(|(chain, _)| chain.clone())
        .collect::<Vec<_>>();
    let addresses = targets
        .iter()
        .map(|(_, address)| address.clone())
        .collect::<Vec<_>>();

    let query = super::intervals::with_watched_intervals(&format!(
        r#"
        , target_addresses AS (
            SELECT DISTINCT chain, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS target(chain, address)
        )
        SELECT
            watched.chain,
            watched.source_family,
            watched.address,
            watched.contract_instance_id,
            watched.source,
            watched.source_manifest_id,
            watched.active_from_block_number,
            watched.active_to_block_number
        FROM target_addresses target
        JOIN watched_intervals watched
          ON watched.chain = target.chain
         AND watched.address = target.address
        WHERE {current_predicate}
          AND ($3::TEXT IS NULL OR watched.source_family = $3)
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
        current_predicate = super::intervals::CURRENT_WATCHED_INTERVAL_PREDICATE,
    ));
    let rows = sqlx::query(&query)
        .bind(&chains)
        .bind(&addresses)
        .bind(source_family)
        .fetch_all(pool)
        .await
        .context("failed to load watched contracts for scoped addresses")?;

    let mut watched_contracts = super::watched_contracts_from_rows(rows)?;
    super::sort_and_dedup_watched_contracts(&mut watched_contracts);

    Ok(watched_contracts)
}
