use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

use super::{
    keying::execution_outcome_block_dependencies, types::ExecutionOutcomeInvalidationSummary,
};

const INVALIDATION_BATCH_SIZE: i64 = 500;

#[derive(Debug)]
struct Candidate {
    execution_cache_key: String,
    request_key: String,
    requested_chain_positions: Value,
    topology_version_boundary: Value,
    record_version_boundary: Value,
}

/// Remove cache eligibility for outcomes whose block dependencies are orphaned.
///
/// The caller owns the canonicality transaction. Durable traces and steps are
/// not removed, and a later canonical recovery does not recreate deleted cache
/// rows. A database without the retained public execution cache has nothing to
/// evict. Phase lineage lives in `bigname_phase`; the retained cache lives in
/// `public`, so both changes remain atomic in this transaction.
pub async fn invalidate_execution_outcomes_for_orphaned_blocks_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    if !execution_cache_exists(transaction).await? {
        return Ok(ExecutionOutcomeInvalidationSummary {
            deleted_outcome_count: 0,
        });
    }

    let mut deleted_outcome_count = 0;
    let mut last_seen_cache_key = None;
    loop {
        let outcomes = load_candidate_batch(transaction, last_seen_cache_key.as_deref()).await?;
        let Some(last_outcome) = outcomes.last() else {
            break;
        };
        last_seen_cache_key = Some(last_outcome.execution_cache_key.clone());

        let mut cache_keys = Vec::new();
        let mut parsed_dependencies = Vec::new();
        let mut candidate_dependencies = BTreeSet::new();
        for outcome in outcomes {
            let dependencies = match execution_outcome_block_dependencies(
                &outcome.request_key,
                &outcome.requested_chain_positions,
                &outcome.topology_version_boundary,
                &outcome.record_version_boundary,
            ) {
                Ok(dependencies) => dependencies,
                Err(_) => {
                    cache_keys.push(outcome.execution_cache_key);
                    continue;
                }
            };
            candidate_dependencies.extend(dependencies.iter().cloned());
            parsed_dependencies.push((outcome.execution_cache_key, dependencies));
        }

        let orphaned = load_orphaned_dependencies(transaction, &candidate_dependencies).await?;
        for (execution_cache_key, dependencies) in parsed_dependencies {
            if dependencies
                .iter()
                .any(|dependency| orphaned.contains(dependency))
            {
                cache_keys.push(execution_cache_key);
            }
        }
        deleted_outcome_count += delete_outcomes(transaction, &cache_keys).await?;
    }

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

async fn execution_cache_exists(transaction: &mut Transaction<'_, Postgres>) -> Result<bool> {
    sqlx::query_scalar("SELECT to_regclass('public.execution_cache_outcomes') IS NOT NULL")
        .fetch_one(&mut **transaction)
        .await
        .context("failed to check whether execution cache outcomes exist")
}

async fn load_candidate_batch(
    transaction: &mut Transaction<'_, Postgres>,
    after_execution_cache_key: Option<&str>,
) -> Result<Vec<Candidate>> {
    let rows = sqlx::query(
        r#"
        SELECT execution_cache_key, request_key, requested_chain_positions,
               topology_version_boundary, record_version_boundary
        FROM public.execution_cache_outcomes
        WHERE request_type IN ('verified_resolution', 'verified_primary_name')
          AND ($1::text IS NULL OR execution_cache_key > $1)
        ORDER BY execution_cache_key
        LIMIT $2
        "#,
    )
    .bind(after_execution_cache_key)
    .bind(INVALIDATION_BATCH_SIZE)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load execution outcomes for reorg invalidation")?;

    rows.into_iter().map(decode_candidate).collect()
}

async fn load_orphaned_dependencies(
    transaction: &mut Transaction<'_, Postgres>,
    dependencies: &BTreeSet<(String, String)>,
) -> Result<BTreeSet<(String, String)>> {
    if dependencies.is_empty() {
        return Ok(BTreeSet::new());
    }
    let chains = dependencies
        .iter()
        .map(|(chain, _)| chain.as_str())
        .collect::<Vec<_>>();
    let hashes = dependencies
        .iter()
        .map(|(_, hash)| hash.as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT lineage.chain_id, lineage.block_hash
        FROM UNNEST($1::text[], $2::text[]) dependency(chain_id, block_hash)
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = dependency.chain_id
         AND lineage.block_hash = dependency.block_hash
        WHERE lineage.canonicality_state = 'orphaned'::bigname_phase.canonicality_state
        "#,
    )
    .bind(chains)
    .bind(hashes)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to match execution dependencies against orphaned blocks")?;

    rows.into_iter()
        .map(|row| Ok((row.try_get("chain_id")?, row.try_get("block_hash")?)))
        .collect()
}

async fn delete_outcomes(
    transaction: &mut Transaction<'_, Postgres>,
    execution_cache_keys: &[String],
) -> Result<u64> {
    if execution_cache_keys.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "DELETE FROM public.execution_cache_outcomes WHERE execution_cache_key = ANY($1::text[])",
    )
    .bind(execution_cache_keys)
    .execute(&mut **transaction)
    .await
    .context("failed to delete orphan-dependent execution outcomes")?
    .rows_affected())
}

fn decode_candidate(row: PgRow) -> Result<Candidate> {
    Ok(Candidate {
        execution_cache_key: row.try_get("execution_cache_key")?,
        request_key: row.try_get("request_key")?,
        requested_chain_positions: row.try_get("requested_chain_positions")?,
        topology_version_boundary: row.try_get("topology_version_boundary")?,
        record_version_boundary: row.try_get("record_version_boundary")?,
    })
}
