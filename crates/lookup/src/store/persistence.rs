use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    LedgerAction, LookupError, LookupPosition, LookupRecordResult, RecordSelector, Result,
    error::database,
    store::{EnsPrimaryNameAuthority, IndexedComparison, LookupSnapshot},
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
    let comparable = results
        .iter()
        .any(|result| !result.ccip_read && result.status.is_comparable());
    revalidate_lookup_state(
        &mut transaction,
        &snapshot.authoritative_position,
        &snapshot.revalidation_positions,
        &snapshot.execution_authority,
        snapshot.comparison.as_ref(),
    )
    .await?;
    let Some(comparison) = snapshot.comparison.as_ref().filter(|_| comparable) else {
        transaction
            .commit()
            .await
            .map_err(database("commit lookup head revalidation"))?;
        return Ok(());
    };

    for result in results {
        persist_result(&mut transaction, snapshot, comparison, result).await?;
    }
    transaction.commit().await.map_err(divergence_write_error)?;
    Ok(())
}

pub(crate) async fn revalidate_primary_name_position(
    pool: &PgPool,
    authority: &EnsPrimaryNameAuthority,
) -> Result<()> {
    let observed_positions = json!({ "ethereum": authority.position });
    let mut transaction = pool
        .begin()
        .await
        .map_err(database("start primary-name position revalidation"))?;
    revalidate_lookup_state(
        &mut transaction,
        &authority.position,
        &observed_positions,
        &authority.execution_authority,
        None,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(database("commit primary-name position revalidation"))
}

async fn revalidate_lookup_state(
    transaction: &mut Transaction<'_, Postgres>,
    authoritative_position: &LookupPosition,
    observed_positions: &Value,
    execution_authority: &Value,
    comparison: Option<&IndexedComparison>,
) -> Result<()> {
    let status: String = sqlx::query_scalar(
        "SELECT revalidate_resolution_lookup_state(
             $1, $2, $3, $4, $5, $6::uuid, $7, $8
         )",
    )
    .bind(&authoritative_position.chain_id)
    .bind(authoritative_position.block_number)
    .bind(&authoritative_position.block_hash)
    .bind(observed_positions)
    .bind(execution_authority)
    .bind(comparison.map(|comparison| comparison.resource_id.as_str()))
    .bind(comparison.map(|comparison| comparison.boundary_key.as_str()))
    .bind(comparison.map(|comparison| comparison.row_xmin.as_str()))
    .fetch_one(&mut **transaction)
    .await
    .map_err(lookup_state_error("revalidate lookup execution head"))?;
    match status.as_str() {
        "unchanged" => Ok(()),
        "head_changed" => Err(LookupError::concurrent_state(
            "canonical head changed while live lookup was running",
        )),
        "record_changed" => Err(LookupError::concurrent_state(
            "indexed record state changed while live lookup was running",
        )),
        "project_changed" => Err(LookupError::concurrent_state(
            "projected execution authority changed while live lookup was running",
        )),
        "name_changed" => Err(LookupError::concurrent_state(
            "projected name state changed while live lookup was running",
        )),
        "manifest_changed" => Err(LookupError::concurrent_state(
            "lookup manifest authority changed while live lookup was running",
        )),
        "position_changed" => Err(LookupError::concurrent_state(
            "canonical lookup position changed while live lookup was running",
        )),
        unexpected => Err(LookupError::database(format!(
            "lookup state guard returned unexpected status {unexpected}"
        ))),
    }
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
        "SELECT write_resolution_divergence(
             $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, false
         )",
    )
    .bind(&comparison.resource_id)
    .bind(&comparison.boundary_key)
    .bind(&comparison.row_xmin)
    .bind(&snapshot.authoritative_position.chain_id)
    .bind(snapshot.authoritative_position.block_number)
    .bind(&snapshot.authoritative_position.block_hash)
    .bind(&snapshot.execution_authority)
    .bind(&snapshot.logical_name_id)
    .bind(&snapshot.resolver_chain_id)
    .bind(&snapshot.resolver_address)
    .bind(&result.record_key)
    .bind(&snapshot.revalidation_positions)
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
            Some("40P01" | "40001") => {
                return LookupError::concurrent_state(format!(
                    "lookup state changed during divergence commit: {database_error}"
                ));
            }
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

fn lookup_state_error(context: &'static str) -> impl FnOnce(sqlx::Error) -> LookupError {
    move |error| {
        if let sqlx::Error::Database(database_error) = &error
            && matches!(database_error.code().as_deref(), Some("40P01" | "40001"))
        {
            return LookupError::concurrent_state(format!(
                "lookup state changed during revalidation: {database_error}"
            ));
        }
        database(context)(error)
    }
}
