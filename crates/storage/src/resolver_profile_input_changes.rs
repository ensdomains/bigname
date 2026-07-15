use anyhow::{Context, Result, ensure};
use sqlx::{Executor, PgPool, Postgres, Row};

use crate::evm_primitives::normalize_evm_address;

/// One coalesced resolver-profile input transition that still needs repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileInputChange {
    pub chain_id: String,
    pub contract_address: String,
    pub generation: i64,
    pub processed_generation: i64,
    pub previous_code_hash: Option<String>,
    pub current_code_hash: Option<String>,
    pub force_reconciliation: bool,
}

/// A resolver-profile target whose manifest/discovery admission changed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileReconciliationTarget {
    pub chain_id: String,
    pub contract_address: String,
}

/// Load the oldest coalesced resolver-profile transitions that remain dirty.
pub async fn load_pending_resolver_profile_input_changes(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ResolverProfileInputChange>> {
    ensure!(
        limit > 0,
        "resolver-profile input-change limit must be positive, got {limit}"
    );

    let rows = sqlx::query(
        r#"
        SELECT
            input.chain_id,
            input.contract_address,
            input.generation,
            input.processed_generation,
            input.previous_code_hash,
            latest.code_hash AS current_code_hash,
            input.force_reconciliation
        FROM resolver_profile_input_changes input
        LEFT JOIN LATERAL (
            SELECT lower(code_hash.code_hash) AS code_hash
            FROM raw_code_hashes code_hash
            WHERE code_hash.chain_id = input.chain_id
              AND code_hash.contract_address = input.contract_address
              AND code_hash.canonicality_state <> 'orphaned'::canonicality_state
            ORDER BY
                code_hash.block_number DESC,
                CASE code_hash.canonicality_state
                    WHEN 'finalized'::canonicality_state THEN 4
                    WHEN 'safe'::canonicality_state THEN 3
                    WHEN 'canonical'::canonicality_state THEN 2
                    WHEN 'observed'::canonicality_state THEN 1
                    ELSE 0
                END DESC,
                code_hash.raw_code_hash_id DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE input.processed_generation < input.generation
        ORDER BY input.last_changed_at, input.chain_id, input.contract_address
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("failed to load pending resolver-profile input changes")?;

    rows.into_iter()
        .map(|row| {
            Ok(ResolverProfileInputChange {
                chain_id: row.try_get("chain_id")?,
                contract_address: row.try_get("contract_address")?,
                generation: row.try_get("generation")?,
                processed_generation: row.try_get("processed_generation")?,
                previous_code_hash: row.try_get("previous_code_hash")?,
                current_code_hash: row.try_get("current_code_hash")?,
                force_reconciliation: row.try_get("force_reconciliation")?,
            })
        })
        .collect()
}

/// Force resolver-profile convergence after manifest or discovery admission
/// changes without a corresponding raw code-hash write.
///
/// Both audit hashes are the exact current latest non-orphaned observation.
/// The queue's force bit makes a clean same-hash target dirty, while ordinary
/// duplicate raw-code notifications remain suppressed by the storage trigger.
pub async fn enqueue_resolver_profile_reconciliations(
    pool: &PgPool,
    targets: &[ResolverProfileReconciliationTarget],
) -> Result<i64> {
    enqueue_resolver_profile_reconciliations_with_executor(pool, targets).await
}

pub(crate) async fn enqueue_resolver_profile_reconciliations_with_executor<'e, E>(
    executor: E,
    targets: &[ResolverProfileReconciliationTarget],
) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    if targets.is_empty() {
        return Ok(0);
    }

    let mut normalized_targets = targets
        .iter()
        .map(|target| {
            ensure!(
                !target.chain_id.trim().is_empty(),
                "resolver-profile reconciliation target has an empty chain"
            );
            ensure!(
                !target.contract_address.trim().is_empty(),
                "resolver-profile reconciliation target on {} has an empty address",
                target.chain_id
            );
            Ok(ResolverProfileReconciliationTarget {
                chain_id: target.chain_id.clone(),
                contract_address: normalize_evm_address(&target.contract_address),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    normalized_targets.sort_by(|left, right| {
        (&left.chain_id, &left.contract_address).cmp(&(&right.chain_id, &right.contract_address))
    });
    normalized_targets.dedup();

    let chain_ids = normalized_targets
        .iter()
        .map(|target| target.chain_id.clone())
        .collect::<Vec<_>>();
    let contract_addresses = normalized_targets
        .iter()
        .map(|target| target.contract_address.clone())
        .collect::<Vec<_>>();

    let recorded = sqlx::query_scalar::<_, i64>(
        r#"
        WITH targets AS (
            SELECT DISTINCT chain_id, contract_address
            FROM unnest($1::TEXT[], $2::TEXT[])
                AS input(chain_id, contract_address)
        ),
        changes AS (
            SELECT
                target.chain_id,
                target.contract_address,
                latest.code_hash
            FROM targets target
            LEFT JOIN LATERAL (
                SELECT lower(code_hash.code_hash) AS code_hash
                FROM raw_code_hashes code_hash
                WHERE code_hash.chain_id = target.chain_id
                  AND code_hash.contract_address = target.contract_address
                  AND code_hash.canonicality_state <> 'orphaned'::canonicality_state
                ORDER BY
                    code_hash.block_number DESC,
                    CASE code_hash.canonicality_state
                        WHEN 'finalized'::canonicality_state THEN 4
                        WHEN 'safe'::canonicality_state THEN 3
                        WHEN 'canonical'::canonicality_state THEN 2
                        WHEN 'observed'::canonicality_state THEN 1
                        ELSE 0
                    END DESC,
                    code_hash.raw_code_hash_id DESC
                LIMIT 1
            ) latest ON TRUE
        )
        SELECT public.record_resolver_profile_input_changes(
            COALESCE(
                jsonb_agg(jsonb_build_object(
                    'chain_id', chain_id,
                    'contract_address', contract_address,
                    'previous_code_hash', code_hash,
                    'current_code_hash', code_hash,
                    'force_reconciliation', TRUE
                )),
                '[]'::JSONB
            )
        )
        FROM changes
        "#,
    )
    .bind(&chain_ids)
    .bind(&contract_addresses)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to enqueue {} resolver-profile reconciliation targets",
            normalized_targets.len()
        )
    })?;

    // Keep this assertion close to the SQL function: explicit force work must
    // never be lost to the duplicate-current suppression used by raw-code
    // statement triggers.
    ensure!(
        recorded == i64::try_from(normalized_targets.len())?,
        "resolver-profile reconciliation enqueue recorded {recorded} of {} targets",
        normalized_targets.len()
    );
    Ok(recorded)
}

/// Acknowledge exactly the generation that was repaired.
///
/// Returns `false` when another input arrived after the caller loaded the row;
/// that newer generation remains dirty and must be repaired by a later drain.
pub async fn acknowledge_resolver_profile_input_change(
    pool: &PgPool,
    chain_id: &str,
    contract_address: &str,
    generation: i64,
) -> Result<bool> {
    ensure!(
        !chain_id.trim().is_empty(),
        "resolver-profile acknowledgement chain must not be empty"
    );
    ensure!(
        generation > 0,
        "resolver-profile acknowledgement generation must be positive, got {generation}"
    );
    let contract_address = normalize_evm_address(contract_address);

    let acknowledged = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE resolver_profile_input_changes
        SET
            processed_generation = generation,
            current_code_hash = (
                SELECT lower(code_hash.code_hash)
                FROM raw_code_hashes code_hash
                WHERE code_hash.chain_id = resolver_profile_input_changes.chain_id
                  AND code_hash.contract_address =
                      resolver_profile_input_changes.contract_address
                  AND code_hash.canonicality_state <>
                      'orphaned'::canonicality_state
                ORDER BY
                    code_hash.block_number DESC,
                    CASE code_hash.canonicality_state
                        WHEN 'finalized'::canonicality_state THEN 4
                        WHEN 'safe'::canonicality_state THEN 3
                        WHEN 'canonical'::canonicality_state THEN 2
                        WHEN 'observed'::canonicality_state THEN 1
                        ELSE 0
                    END DESC,
                    code_hash.raw_code_hash_id DESC
                LIMIT 1
            ),
            force_reconciliation = FALSE,
            processed_at = now()
        WHERE chain_id = $1
          AND contract_address = $2
          AND generation = $3
          AND processed_generation < generation
        RETURNING generation
        "#,
    )
    .bind(chain_id)
    .bind(&contract_address)
    .bind(generation)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to acknowledge resolver-profile input change for {chain_id}/{contract_address} generation {generation}"
        )
    })?;

    Ok(acknowledged.is_some())
}

#[cfg(test)]
mod tests;
