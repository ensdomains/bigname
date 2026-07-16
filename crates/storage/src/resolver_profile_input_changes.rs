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

const LATEST_NON_ORPHANED_CODE_HASH_RANKING_SQL: &str = r#"ORDER BY
                code_hash.block_number DESC,
                CASE code_hash.canonicality_state
                    WHEN 'finalized'::canonicality_state THEN 4
                    WHEN 'safe'::canonicality_state THEN 3
                    WHEN 'canonical'::canonicality_state THEN 2
                    WHEN 'observed'::canonicality_state THEN 1
                    ELSE 0
                END DESC,
                code_hash.raw_code_hash_id DESC
            LIMIT 1"#;

#[derive(Clone, Copy)]
enum CurrentCodeHashCaller {
    PendingInput,
    ReconciliationTarget,
    Acknowledgement,
}

impl CurrentCodeHashCaller {
    fn expressions(self) -> (&'static str, &'static str) {
        match self {
            Self::PendingInput => ("input.chain_id", "input.contract_address"),
            Self::ReconciliationTarget => ("target.chain_id", "target.contract_address"),
            Self::Acknowledgement => (
                "acknowledgement.chain_id",
                "acknowledgement.contract_address",
            ),
        }
    }
}

/// Build the current-state lookup from a closed set of trusted caller
/// expressions. Keeping the ranking here prevents queue load, force enqueue,
/// and acknowledgement from choosing different effective code observations.
fn latest_non_orphaned_code_hash_lateral(caller: CurrentCodeHashCaller) -> String {
    let (chain_id, contract_address) = caller.expressions();
    format!(
        r#"LEFT JOIN LATERAL (
            SELECT lower(code_hash.code_hash) AS code_hash
            FROM raw_code_hashes code_hash
            WHERE code_hash.chain_id = {chain_id}
              AND code_hash.contract_address = {contract_address}
              AND code_hash.canonicality_state <> 'orphaned'::canonicality_state
            {ranking}
        ) latest ON TRUE"#,
        ranking = LATEST_NON_ORPHANED_CODE_HASH_RANKING_SQL,
    )
}

fn load_pending_resolver_profile_input_changes_sql() -> String {
    format!(
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
        {latest_code_hash}
        WHERE input.processed_generation < input.generation
          AND NOT EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $3::TEXT[], $4::BIGINT[])
                  AS excluded(chain_id, contract_address, generation)
              WHERE excluded.chain_id = input.chain_id
                AND excluded.contract_address = input.contract_address
                AND excluded.generation = input.generation
          )
        ORDER BY input.last_changed_at, input.chain_id, input.contract_address
        LIMIT $1
        "#,
        latest_code_hash =
            latest_non_orphaned_code_hash_lateral(CurrentCodeHashCaller::PendingInput),
    )
}

/// Load the oldest coalesced resolver-profile transitions that remain dirty.
pub async fn load_pending_resolver_profile_input_changes(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ResolverProfileInputChange>> {
    load_pending_resolver_profile_input_changes_excluding(pool, limit, &[]).await
}

/// Load pending transitions while excluding exact generations already
/// attempted by the current bounded drain.
///
/// A concurrent generation increment no longer matches the exclusion and is
/// therefore returned for a fresh decision. Exclusion is process-local only:
/// it does not acknowledge or otherwise mutate durable queue work.
pub async fn load_pending_resolver_profile_input_changes_excluding(
    pool: &PgPool,
    limit: i64,
    excluded: &[ResolverProfileInputChange],
) -> Result<Vec<ResolverProfileInputChange>> {
    ensure!(
        limit > 0,
        "resolver-profile input-change limit must be positive, got {limit}"
    );

    let mut excluded = excluded
        .iter()
        .map(|input| {
            ensure!(
                !input.chain_id.trim().is_empty(),
                "excluded resolver-profile input-change chain must not be empty"
            );
            ensure!(
                input.generation > 0,
                "excluded resolver-profile input-change generation must be positive, got {}",
                input.generation
            );
            Ok((
                input.chain_id.clone(),
                normalize_evm_address(&input.contract_address),
                input.generation,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    excluded.sort();
    excluded.dedup();
    let excluded_chains = excluded
        .iter()
        .map(|(chain, _, _)| chain.clone())
        .collect::<Vec<_>>();
    let excluded_addresses = excluded
        .iter()
        .map(|(_, address, _)| address.clone())
        .collect::<Vec<_>>();
    let excluded_generations = excluded
        .iter()
        .map(|(_, _, generation)| *generation)
        .collect::<Vec<_>>();

    let sql = load_pending_resolver_profile_input_changes_sql();
    let rows = sqlx::query(&sql)
        .bind(limit)
        .bind(&excluded_chains)
        .bind(&excluded_addresses)
        .bind(&excluded_generations)
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

fn enqueue_resolver_profile_reconciliations_sql() -> String {
    format!(
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
            {latest_code_hash}
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
        latest_code_hash =
            latest_non_orphaned_code_hash_lateral(CurrentCodeHashCaller::ReconciliationTarget),
    )
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

    let sql = enqueue_resolver_profile_reconciliations_sql();
    let recorded = sqlx::query_scalar::<_, i64>(&sql)
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

fn acknowledge_resolver_profile_input_changes_sql() -> String {
    format!(
        r#"
        WITH acknowledgements AS (
            SELECT DISTINCT chain_id, contract_address, generation
            FROM unnest($1::TEXT[], $2::TEXT[], $3::BIGINT[])
                AS input(chain_id, contract_address, generation)
        ),
        current_inputs AS (
            SELECT
                acknowledgement.chain_id,
                acknowledgement.contract_address,
                acknowledgement.generation,
                latest.code_hash
            FROM acknowledgements acknowledgement
            {latest_code_hash}
        ),
        updated AS (
            UPDATE resolver_profile_input_changes input
            SET
                processed_generation = input.generation,
                current_code_hash = current_input.code_hash,
                force_reconciliation = FALSE,
                processed_at = now()
            FROM current_inputs current_input
            WHERE input.chain_id = current_input.chain_id
              AND input.contract_address = current_input.contract_address
              AND input.generation = current_input.generation
              AND input.processed_generation < input.generation
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM updated
        "#,
        latest_code_hash =
            latest_non_orphaned_code_hash_lateral(CurrentCodeHashCaller::Acknowledgement),
    )
}

/// Acknowledge a batch of exactly observed generations with one set-based
/// compare-and-set update.
///
/// The returned count excludes rows whose generation changed after loading;
/// those rows remain dirty for a later drain.
pub async fn acknowledge_resolver_profile_input_changes(
    pool: &PgPool,
    inputs: &[ResolverProfileInputChange],
) -> Result<usize> {
    if inputs.is_empty() {
        return Ok(0);
    }

    let mut acknowledgements = inputs
        .iter()
        .map(|input| {
            ensure!(
                !input.chain_id.trim().is_empty(),
                "resolver-profile acknowledgement chain must not be empty"
            );
            ensure!(
                input.generation > 0,
                "resolver-profile acknowledgement generation must be positive, got {}",
                input.generation
            );
            Ok((
                input.chain_id.clone(),
                normalize_evm_address(&input.contract_address),
                input.generation,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    acknowledgements.sort();
    acknowledgements.dedup();
    let chain_ids = acknowledgements
        .iter()
        .map(|(chain, _, _)| chain.clone())
        .collect::<Vec<_>>();
    let contract_addresses = acknowledgements
        .iter()
        .map(|(_, address, _)| address.clone())
        .collect::<Vec<_>>();
    let generations = acknowledgements
        .iter()
        .map(|(_, _, generation)| *generation)
        .collect::<Vec<_>>();

    let sql = acknowledge_resolver_profile_input_changes_sql();
    let acknowledged = sqlx::query_scalar::<_, i64>(&sql)
        .bind(&chain_ids)
        .bind(&contract_addresses)
        .bind(&generations)
        .fetch_one(pool)
        .await
        .context("failed to acknowledge resolver-profile input-change batch")?;

    usize::try_from(acknowledged)
        .context("acknowledged resolver-profile input-change count does not fit usize")
}

#[cfg(test)]
mod tests;
