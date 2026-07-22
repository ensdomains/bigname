use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::load_chain_checkpoint;

const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";

/// Result of one bounded route-local primary-name execution cleanup pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNameRouteCachePruneSummary {
    pub head_block_number: Option<i64>,
    pub cutoff_block_number: Option<i64>,
    pub deleted_outcome_count: u64,
    pub deleted_trace_count: u64,
}

/// Delete one bounded batch of route-local primary-name execution artifacts
/// whose selected checkpoint is outside the configured block window.
pub async fn prune_route_local_primary_name_execution(
    pool: &PgPool,
    retention_checkpoints: i64,
    batch_size: i64,
) -> Result<PrimaryNameRouteCachePruneSummary> {
    if retention_checkpoints <= 0 {
        bail!("primary-name route cache retention checkpoints must be positive");
    }
    if batch_size <= 0 {
        bail!("primary-name route cache prune batch size must be positive");
    }

    let head_block_number = load_chain_checkpoint(pool, ETHEREUM_MAINNET_CHAIN_ID)
        .await?
        .and_then(|checkpoint| checkpoint.canonical_block_number);
    let Some(head_block_number) = head_block_number else {
        return Ok(PrimaryNameRouteCachePruneSummary::default());
    };
    let cutoff_block_number = head_block_number.saturating_sub(retention_checkpoints);
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open primary-name route cache pruning transaction")?;
    let candidates = sqlx::query(
        r#"
        SELECT execution_cache_key, execution_trace_id
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND topology_version_boundary ->> 'boundary_kind' = 'selected_checkpoint'
          AND record_version_boundary = topology_version_boundary
          AND topology_version_boundary #>> '{chain_position,chain_id}' = 'ethereum-mainnet'
          AND jsonb_typeof(
                topology_version_boundary #> '{chain_position,block_number}'
              ) = 'number'
          AND (
                topology_version_boundary #>> '{chain_position,block_number}'
              )::numeric < $1
        ORDER BY
            (
                topology_version_boundary #>> '{chain_position,block_number}'
            )::numeric,
            execution_cache_key
        LIMIT $2
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(cutoff_block_number)
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await
    .context("failed to select stale route-local primary-name outcomes")?;

    let execution_cache_keys = candidates
        .iter()
        .map(|row| row.try_get::<String, _>("execution_cache_key"))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to decode stale primary-name execution cache keys")?;
    let mut execution_trace_ids = candidates
        .iter()
        .map(|row| row.try_get::<Uuid, _>("execution_trace_id"))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to decode stale primary-name execution trace IDs")?;
    execution_trace_ids.sort_unstable();
    execution_trace_ids.dedup();

    let mut deleted_outcome_count = 0;
    let mut deleted_trace_count = 0;
    if !execution_cache_keys.is_empty() {
        deleted_outcome_count =
            sqlx::query("DELETE FROM execution_cache_outcomes WHERE execution_cache_key = ANY($1)")
                .bind(&execution_cache_keys)
                .execute(&mut *transaction)
                .await
                .context("failed to delete stale route-local primary-name outcomes")?
                .rows_affected();
        deleted_trace_count = sqlx::query(
            r#"
            DELETE FROM execution_traces AS trace
            WHERE trace.execution_trace_id = ANY($1)
              AND NOT EXISTS (
                    SELECT 1
                    FROM execution_cache_outcomes AS outcome
                    WHERE outcome.execution_trace_id = trace.execution_trace_id
              )
            "#,
        )
        .bind(&execution_trace_ids)
        .execute(&mut *transaction)
        .await
        .context("failed to delete stale route-local primary-name traces")?
        .rows_affected();
    }

    // Same-identity executions can leave an older trace without an outcome
    // pointer. Bound that second storage family independently so a pass never
    // turns into an unbounded audit-table scan or delete.
    let orphan_trace_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT trace.execution_trace_id
        FROM execution_traces AS trace
        WHERE trace.request_type = 'verified_primary_name'
          AND trace.namespace = 'ens'
          AND trace.request_metadata ? 'route_local_claim'
          AND trace.request_metadata ->> 'coin_type' = '60'
          AND trace.request_metadata #>> '{cache_identity,topology_version_boundary,boundary_kind}' = 'selected_checkpoint'
          AND trace.request_metadata #> '{cache_identity,record_version_boundary}'
              = trace.request_metadata #> '{cache_identity,topology_version_boundary}'
          AND trace.request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,chain_id}' = 'ethereum-mainnet'
          AND jsonb_typeof(
                trace.request_metadata #> '{cache_identity,topology_version_boundary,chain_position,block_number}'
              ) = 'number'
          AND (
                trace.request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,block_number}'
              )::numeric < $1
          AND NOT EXISTS (
                SELECT 1
                FROM execution_cache_outcomes AS outcome
                WHERE outcome.execution_trace_id = trace.execution_trace_id
          )
        ORDER BY
            (
                trace.request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,block_number}'
            )::numeric,
            trace.execution_trace_id
        LIMIT $2
        FOR UPDATE OF trace SKIP LOCKED
        "#,
    )
    .bind(cutoff_block_number)
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await
    .context("failed to select orphaned route-local primary-name traces")?;
    if !orphan_trace_ids.is_empty() {
        deleted_trace_count +=
            sqlx::query("DELETE FROM execution_traces WHERE execution_trace_id = ANY($1)")
                .bind(&orphan_trace_ids)
                .execute(&mut *transaction)
                .await
                .context("failed to delete orphaned route-local primary-name traces")?
                .rows_affected();
    }

    transaction
        .commit()
        .await
        .context("failed to commit primary-name route cache pruning")?;
    Ok(PrimaryNameRouteCachePruneSummary {
        head_block_number: Some(head_block_number),
        cutoff_block_number: Some(cutoff_block_number),
        deleted_outcome_count,
        deleted_trace_count,
    })
}
