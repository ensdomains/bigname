use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_domain::block_interval::{InclusiveBlockInterval, coalesce_inclusive_block_intervals};
use bigname_manifests::RequiredWatchedTuple;
use sqlx::{PgPool, Row};

use crate::{
    checkpoint_context::StartupAdapterProgress,
    ens_v2_registry::constants::DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
};

const WITNESS_PAGE_ROWS: i64 = 1_000;

type EventRequirements = BTreeMap<(String, String), Vec<InclusiveBlockInterval>>;
type AddressRequirements = BTreeMap<String, Vec<InclusiveBlockInterval>>;

pub(in crate::ens_v2_registry) async fn ensure_retained_semantic_witnesses(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    through_block: i64,
) -> Result<()> {
    ensure_retained_semantic_witnesses_inner(
        None,
        connection,
        chain,
        requirements,
        through_block,
        None,
    )
    .await
}

pub(in crate::ens_v2_registry) async fn ensure_retained_semantic_witnesses_with_progress(
    pool: &PgPool,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    through_block: i64,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<()> {
    ensure_retained_semantic_witnesses_inner(
        Some(pool),
        connection,
        chain,
        requirements,
        through_block,
        Some(progress),
    )
    .await
}

async fn ensure_retained_semantic_witnesses_inner(
    pool: Option<&PgPool>,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    through_block: i64,
    mut progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let (event_requirements, address_requirements) =
        requirement_indexes(pool, requirements, &mut progress).await?;
    verify_event_witnesses(
        pool,
        connection,
        chain,
        through_block,
        &event_requirements,
        &mut progress,
    )
    .await?;
    verify_discovery_witnesses(
        pool,
        connection,
        chain,
        through_block,
        &address_requirements,
        &mut progress,
    )
    .await
}

async fn requirement_indexes(
    pool: Option<&PgPool>,
    requirements: &[RequiredWatchedTuple],
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<(EventRequirements, AddressRequirements)> {
    let mut events = EventRequirements::new();
    let mut addresses = AddressRequirements::new();
    for (index, requirement) in requirements.iter().enumerate() {
        if let Some(interval) = InclusiveBlockInterval::new(
            requirement.required_from_block,
            requirement.required_to_block,
        ) {
            let address = requirement.address.to_ascii_lowercase();
            events
                .entry((requirement.source_family.clone(), address.clone()))
                .or_default()
                .push(interval);
            addresses.entry(address).or_default().push(interval);
        }
        if index + 1 < requirements.len() && (index + 1).is_multiple_of(WITNESS_PAGE_ROWS as usize)
        {
            record_progress(pool, progress).await?;
        }
    }
    for intervals in events.values_mut() {
        *intervals = coalesce_inclusive_block_intervals(intervals.iter().copied());
    }
    for intervals in addresses.values_mut() {
        *intervals = coalesce_inclusive_block_intervals(intervals.iter().copied());
    }
    if !requirements.is_empty() {
        record_progress(pool, progress).await?;
    }
    Ok((events, addresses))
}

async fn verify_event_witnesses(
    pool: Option<&PgPool>,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    through_block: i64,
    requirements: &EventRequirements,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut after_id = 0i64;
    loop {
        let ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT normalized_event_id
            FROM normalized_events
            WHERE normalized_event_id > $1
              AND chain_id = $2
              AND derivation_kind = $3
              AND raw_fact_ref ->> 'kind' = 'raw_log'
              AND block_number <= $4
              AND canonicality_state IN ('canonical', 'safe', 'finalized')
            ORDER BY normalized_event_id
            LIMIT $5
            "#,
        )
        .bind(after_id)
        .bind(chain)
        .bind(DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE)
        .bind(through_block)
        .bind(WITNESS_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .context("failed to page normalized-event identities for ENSv2 witness verification")?;
        let Some(last_id) = ids.last().copied() else {
            break;
        };
        let rows = sqlx::query(
            r#"
            SELECT
                event.source_family,
                lower(event.raw_fact_ref ->> 'emitting_address') AS emitting_address,
                event.block_number,
                EXISTS (
                    SELECT 1 FROM chain_lineage lineage
                    WHERE lineage.chain_id = event.chain_id
                      AND lineage.block_hash = event.block_hash
                      AND lineage.block_number = event.block_number
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ) AS readable_lineage,
                EXISTS (
                    SELECT 1
                    FROM raw_logs raw
                    JOIN chain_lineage lineage
                      ON lineage.chain_id = raw.chain_id
                     AND lineage.block_hash = raw.block_hash
                     AND lineage.block_number = raw.block_number
                     AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                    WHERE raw.chain_id = event.chain_id
                      AND raw.block_hash = event.block_hash
                      AND raw.block_number = event.block_number
                      AND raw.log_index = event.log_index
                      AND raw.canonicality_state IN ('canonical', 'safe', 'finalized')
                ) AS has_raw_witness
            FROM normalized_events event
            WHERE event.normalized_event_id = ANY($1::BIGINT[])
              AND event.chain_id = $2
              AND event.derivation_kind = $3
              AND event.raw_fact_ref ->> 'kind' = 'raw_log'
              AND event.block_number <= $4
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
            "#,
        )
        .bind(&ids)
        .bind(chain)
        .bind(DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE)
        .bind(through_block)
        .fetch_all(&mut *connection)
        .await
        .context("failed to verify an ENSv2 normalized-event witness page")?;
        for row in rows {
            let source_family: String = row.try_get("source_family")?;
            let address: String = row.try_get("emitting_address")?;
            let block_number: i64 = row.try_get("block_number")?;
            let required = requirements
                .get(&(source_family, address))
                .is_some_and(|intervals| contains_block(intervals, block_number));
            let readable_lineage: bool = row.try_get("readable_lineage")?;
            let has_raw_witness: bool = row.try_get("has_raw_witness")?;
            ensure!(
                !required || !readable_lineage || has_raw_witness,
                "ENSv2 retained history on {chain} is missing raw-log witnesses for materialized ENSv2 events or discovery through block {through_block}"
            );
        }
        after_id = last_id;
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn verify_discovery_witnesses(
    pool: Option<&PgPool>,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    through_block: i64,
    requirements: &AddressRequirements,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut after_id = 0i64;
    loop {
        let ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT discovery_edge_id
            FROM discovery_edges
            WHERE discovery_edge_id > $1
              AND chain_id = $2
              AND discovery_source LIKE 'ens_v2_registry_%'
              AND provenance ->> 'source' = 'raw_log'
              AND active_from_block_number <= $3
            ORDER BY discovery_edge_id
            LIMIT $4
            "#,
        )
        .bind(after_id)
        .bind(chain)
        .bind(through_block)
        .bind(WITNESS_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .context("failed to page discovery-edge identities for ENSv2 witness verification")?;
        let Some(last_id) = ids.last().copied() else {
            break;
        };
        let rows = sqlx::query(
            r#"
            SELECT
                lower(edge.provenance ->> 'from_address') AS from_address,
                edge.active_from_block_number,
                EXISTS (
                    SELECT 1 FROM chain_lineage lineage
                    WHERE lineage.chain_id = edge.chain_id
                      AND lineage.block_hash = edge.provenance ->> 'block_hash'
                      AND lineage.block_number = edge.active_from_block_number
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ) AS readable_lineage,
                EXISTS (
                    SELECT 1
                    FROM raw_logs raw
                    JOIN chain_lineage lineage
                      ON lineage.chain_id = raw.chain_id
                     AND lineage.block_hash = raw.block_hash
                     AND lineage.block_number = raw.block_number
                     AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                    WHERE raw.chain_id = edge.chain_id
                      AND raw.block_hash = edge.provenance ->> 'block_hash'
                      AND raw.block_number = edge.active_from_block_number
                      AND raw.log_index::TEXT = edge.provenance ->> 'log_index'
                      AND raw.canonicality_state IN ('canonical', 'safe', 'finalized')
                ) AS has_raw_witness
            FROM discovery_edges edge
            WHERE edge.discovery_edge_id = ANY($1::BIGINT[])
              AND edge.chain_id = $2
              AND edge.discovery_source LIKE 'ens_v2_registry_%'
              AND edge.provenance ->> 'source' = 'raw_log'
              AND edge.active_from_block_number <= $3
            "#,
        )
        .bind(&ids)
        .bind(chain)
        .bind(through_block)
        .fetch_all(&mut *connection)
        .await
        .context("failed to verify an ENSv2 discovery-edge witness page")?;
        for row in rows {
            let address: String = row.try_get("from_address")?;
            let block_number: i64 = row.try_get("active_from_block_number")?;
            let required = requirements
                .get(&address)
                .is_some_and(|intervals| contains_block(intervals, block_number));
            let readable_lineage: bool = row.try_get("readable_lineage")?;
            let has_raw_witness: bool = row.try_get("has_raw_witness")?;
            ensure!(
                !required || !readable_lineage || has_raw_witness,
                "ENSv2 retained history on {chain} is missing raw-log witnesses for materialized ENSv2 events or discovery through block {through_block}"
            );
        }
        after_id = last_id;
        record_progress(pool, progress).await?;
    }
    Ok(())
}

fn contains_block(intervals: &[InclusiveBlockInterval], block_number: i64) -> bool {
    intervals.iter().any(|interval| {
        block_number >= interval.from_block() && block_number <= interval.through_block()
    })
}

async fn record_progress(
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let (Some(pool), Some(progress)) = (pool, progress.as_deref_mut()) {
        progress.record(pool).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "witnesses/tests.rs"]
mod tests;
