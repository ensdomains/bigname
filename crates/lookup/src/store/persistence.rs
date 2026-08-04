use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    LedgerAction, LookupError, LookupRecordResult, RecordSelector, Result,
    error::database,
    store::{IndexedComparison, LookupSnapshot},
};

pub(crate) async fn persist_comparisons(
    pool: &PgPool,
    snapshot: &LookupSnapshot,
    results: &mut [LookupRecordResult],
) -> Result<()> {
    for result in results.iter_mut().filter(|result| result.ccip_read) {
        result.ledger_action = LedgerAction::SkippedCcip;
    }
    let mut transaction = pool
        .begin()
        .await
        .map_err(database("start divergence write"))?;
    lock_authoritative_head(&mut transaction, snapshot).await?;
    let Some(comparison) = snapshot.comparison.as_ref() else {
        transaction
            .commit()
            .await
            .map_err(database("commit lookup head revalidation"))?;
        return Ok(());
    };
    if !results
        .iter()
        .any(|result| !result.ccip_read && result.status.is_comparable())
    {
        transaction
            .commit()
            .await
            .map_err(database("commit lookup head revalidation"))?;
        return Ok(());
    }

    let locked_xmin: Option<String> = sqlx::query_scalar(
        r#"
        SELECT xmin::text
        FROM record_inventory_current
        WHERE resource_id = $1::uuid
          AND record_version_boundary_key = $2
          AND xmin::text = $3
        FOR SHARE
        "#,
    )
    .bind(&comparison.resource_id)
    .bind(&comparison.boundary_key)
    .bind(&comparison.row_xmin)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(database("lock compared record projection"))?;
    if locked_xmin.is_none() {
        return Err(LookupError::concurrent_state(
            "indexed record state changed while live lookup was running",
        ));
    }

    for result in results {
        persist_result(&mut transaction, snapshot, comparison, result).await?;
    }
    transaction.commit().await.map_err(divergence_write_error)?;
    Ok(())
}

async fn lock_authoritative_head(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &LookupSnapshot,
) -> Result<()> {
    let unchanged: Option<i64> = sqlx::query_scalar(
        "SELECT latest_block_number
         FROM chain_heads
         WHERE chain_id = $1
           AND latest_block_number = $2
           AND latest_block_hash = $3
         FOR SHARE",
    )
    .bind(&snapshot.authoritative_head.chain_id)
    .bind(snapshot.authoritative_head.block_number)
    .bind(&snapshot.authoritative_head.block_hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("revalidate lookup execution head"))?;
    if unchanged.is_none() {
        return Err(LookupError::concurrent_state(
            "canonical head changed while live lookup was running",
        ));
    }
    Ok(())
}

async fn persist_result(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &LookupSnapshot,
    comparison: &IndexedComparison,
    result: &mut LookupRecordResult,
) -> Result<()> {
    if result.ccip_read {
        return Ok(());
    }
    if !result.status.is_comparable() {
        return Ok(());
    }
    let selector = RecordSelector {
        record_key: result.record_key.clone(),
        record_family: result.record_family.clone(),
        selector_key: result.selector_key.clone(),
    };
    let indexed = snapshot.indexed_answer(&selector).ok_or_else(|| {
        LookupError::unsupported("live result has no exact indexed comparison target")
    })?;
    let live = result.comparison_value();
    let agrees = indexed == live;

    let status: String = sqlx::query_scalar(
        "SELECT write_resolution_divergence($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)",
    )
    .bind(&comparison.resource_id)
    .bind(&comparison.boundary_key)
    .bind(&comparison.row_xmin)
    .bind(&snapshot.logical_name_id)
    .bind(&snapshot.resolver_chain_id)
    .bind(&snapshot.resolver_address)
    .bind(&result.record_key)
    .bind(&snapshot.observed_positions)
    .bind(&indexed)
    .bind(&live)
    .fetch_one(&mut **transaction)
    .await
    .map_err(divergence_write_error)?;
    result.ledger_action = match (agrees, status.as_str()) {
        (true, "agreement") => LedgerAction::None,
        (true, "cleared") => LedgerAction::Cleared,
        (false, "written") => LedgerAction::Written,
        (_, "guard_rejected") => {
            return Err(LookupError::concurrent_state(
                "indexed or canonical state changed while live lookup was running",
            ));
        }
        _ => {
            return Err(LookupError::database(format!(
                "divergence writer returned unexpected status {status}"
            )));
        }
    };
    Ok(())
}

pub(crate) fn divergence_write_error(error: sqlx::Error) -> LookupError {
    if let sqlx::Error::Database(database_error) = &error {
        match database_error.code().as_deref() {
            Some("23503") => {
                return LookupError::concurrent_state(format!(
                    "canonical lookup state changed before divergence commit: {database_error}"
                ));
            }
            Some("23505")
                if database_error.constraint()
                    == Some("resolution_divergences_one_active_request_idx") =>
            {
                return LookupError::concurrent_state(format!(
                    "active lookup state changed before divergence commit: {database_error}"
                ));
            }
            Some("23514") => {
                return LookupError::database(format!(
                    "divergence ledger rejected invalid data: {database_error}"
                ));
            }
            _ => {}
        }
    }
    database("persist resolution divergence")(error)
}
