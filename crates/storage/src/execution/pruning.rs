use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{VERIFIED_PRIMARY_NAME_REQUEST_TYPE, load_chain_checkpoint, primary_name_fallback};

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

    let head_block_number = load_chain_checkpoint(pool, primary_name_fallback::CHAIN_ID)
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
    let outcome_candidates_sql = route_local_primary_name_outcome_candidates_sql();
    let candidates = sqlx::query(&outcome_candidates_sql)
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
    let orphan_trace_candidates_sql = route_local_primary_name_orphan_trace_candidates_sql();
    let orphan_trace_ids = sqlx::query_scalar::<_, Uuid>(&orphan_trace_candidates_sql)
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

fn route_local_primary_name_trace_domain_predicate(alias: &str) -> String {
    format!(
        r#"{alias}.request_type = '{VERIFIED_PRIMARY_NAME_REQUEST_TYPE}'
          AND {alias}.namespace = '{namespace}'
          AND {alias}.request_metadata ? 'route_local_claim'
          AND {alias}.request_metadata ->> 'coin_type' = '{coin_type}'
          AND {alias}.request_metadata #>> '{{cache_identity,topology_version_boundary,chain_position,chain_id}}' = '{chain_id}'"#,
        namespace = primary_name_fallback::NAMESPACE,
        coin_type = primary_name_fallback::COIN_TYPE,
        chain_id = primary_name_fallback::CHAIN_ID,
    )
}

pub(super) fn route_local_primary_name_outcome_candidates_sql() -> String {
    let trace_domain = route_local_primary_name_trace_domain_predicate("trace");
    format!(
        r#"
        SELECT outcome.execution_cache_key, outcome.execution_trace_id
        FROM execution_cache_outcomes AS outcome
        JOIN execution_traces AS trace
          ON trace.execution_trace_id = outcome.execution_trace_id
         AND trace.request_type = outcome.request_type
         AND trace.namespace = outcome.namespace
         AND trace.request_key = outcome.request_key
        WHERE outcome.request_type = '{VERIFIED_PRIMARY_NAME_REQUEST_TYPE}'
          AND outcome.namespace = '{namespace}'
          AND ({trace_domain})
          AND outcome.topology_version_boundary ->> 'boundary_kind' = 'selected_checkpoint'
          AND outcome.record_version_boundary = outcome.topology_version_boundary
          AND outcome.topology_version_boundary #>> '{{chain_position,chain_id}}' = '{chain_id}'
          AND jsonb_typeof(
                outcome.topology_version_boundary #> '{{chain_position,block_number}}'
              ) = 'number'
          AND (
                outcome.topology_version_boundary #>> '{{chain_position,block_number}}'
              )::numeric < $1
        ORDER BY
            (
                outcome.topology_version_boundary #>> '{{chain_position,block_number}}'
            )::numeric,
            outcome.execution_cache_key
        LIMIT $2
        FOR UPDATE OF outcome SKIP LOCKED
        "#,
        namespace = primary_name_fallback::NAMESPACE,
        chain_id = primary_name_fallback::CHAIN_ID,
    )
}

pub(super) fn route_local_primary_name_orphan_trace_candidates_sql() -> String {
    let trace_domain = route_local_primary_name_trace_domain_predicate("trace");
    format!(
        r#"
        SELECT trace.execution_trace_id
        FROM execution_traces AS trace
        WHERE ({trace_domain})
          AND trace.request_metadata #>> '{{cache_identity,topology_version_boundary,boundary_kind}}' = 'selected_checkpoint'
          AND trace.request_metadata #> '{{cache_identity,record_version_boundary}}'
              = trace.request_metadata #> '{{cache_identity,topology_version_boundary}}'
          AND jsonb_typeof(
                trace.request_metadata #> '{{cache_identity,topology_version_boundary,chain_position,block_number}}'
              ) = 'number'
          AND (
                trace.request_metadata #>> '{{cache_identity,topology_version_boundary,chain_position,block_number}}'
              )::numeric < $1
          AND NOT EXISTS (
                SELECT 1
                FROM execution_cache_outcomes AS outcome
                WHERE outcome.execution_trace_id = trace.execution_trace_id
          )
        ORDER BY
            (
                trace.request_metadata #>> '{{cache_identity,topology_version_boundary,chain_position,block_number}}'
            )::numeric,
            trace.execution_trace_id
        LIMIT $2
        FOR UPDATE OF trace SKIP LOCKED
        "#,
    )
}

#[cfg(test)]
mod domain_conformance_tests {
    use super::*;

    const DIVERGENCE_MESSAGE: &str = "primary-name fallback eligibility and pruning diverged; the frozen primary-name retention-index migrations must move too";

    #[test]
    fn fallback_eligibility_and_pruning_predicates_share_one_domain() {
        assert_eq!(
            (
                primary_name_fallback::NAMESPACE,
                primary_name_fallback::COIN_TYPE,
                primary_name_fallback::CHAIN_ID,
            ),
            ("ens", "60", "ethereum-mainnet"),
            "{DIVERGENCE_MESSAGE}"
        );
        assert!(
            primary_name_fallback::contains(
                primary_name_fallback::NAMESPACE,
                primary_name_fallback::COIN_TYPE,
            ),
            "{DIVERGENCE_MESSAGE}"
        );

        let shared_trace_domain = route_local_primary_name_trace_domain_predicate("trace");
        for pruning_sql in [
            route_local_primary_name_outcome_candidates_sql(),
            route_local_primary_name_orphan_trace_candidates_sql(),
        ] {
            assert!(
                pruning_sql.contains(&shared_trace_domain),
                "{DIVERGENCE_MESSAGE}:\n{pruning_sql}"
            );
        }
        assert!(
            shared_trace_domain.contains("trace.request_metadata ->> 'coin_type' = '60'"),
            "{DIVERGENCE_MESSAGE}"
        );
        assert!(
            shared_trace_domain.contains(
                "trace.request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,chain_id}' = 'ethereum-mainnet'"
            ),
            "{DIVERGENCE_MESSAGE}"
        );
    }
}
