use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    LedgerAction, LookupError, LookupPosition, LookupRecordResult, RecordSelector, Result,
    error::database,
    store::{EnsPrimaryNameAuthority, IndexedComparison, LookupRoute, LookupSnapshot},
};

pub(crate) async fn persist_comparisons(
    pool: &PgPool,
    snapshot: &LookupSnapshot,
    results: &mut [LookupRecordResult],
    write_divergences: bool,
) -> Result<()> {
    for result in results.iter_mut().filter(|result| result.ccip_read) {
        result.ledger_action = LedgerAction::SkippedCcip;
    }
    let mut transaction = revalidation_transaction(pool, write_divergences).await?;
    if snapshot.route == LookupRoute::EnsUniversalResolverDiscovery {
        revalidate_lookup_state(
            &mut transaction,
            &snapshot.head,
            &snapshot.revalidation_positions,
            &snapshot.execution_authority,
            None,
            write_divergences,
        )
        .await?;
        transaction.commit().await.map_err(lookup_state_error(
            "commit null-resolver lookup revalidation",
        ))?;
        return Ok(());
    }
    let comparable = results
        .iter()
        .any(|result| !result.ccip_read && result.status.is_comparable());
    revalidate_lookup_state(
        &mut transaction,
        &snapshot.head,
        &snapshot.revalidation_positions,
        &snapshot.execution_authority,
        snapshot.comparison.as_ref(),
        write_divergences,
    )
    .await?;
    let Some(comparison) = snapshot
        .comparison
        .as_ref()
        .filter(|_| comparable && write_divergences)
    else {
        transaction
            .commit()
            .await
            .map_err(lookup_state_error("commit lookup head revalidation"))?;
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
    lock_rows: bool,
) -> Result<()> {
    let observed_positions = json!({
        super::positions::chain_slot(&authority.position.chain_id)?: authority.position
    });
    let mut transaction = revalidation_transaction(pool, lock_rows).await?;
    revalidate_lookup_state(
        &mut transaction,
        &authority.head,
        &observed_positions,
        &authority.execution_authority,
        None,
        lock_rows,
    )
    .await?;
    transaction.commit().await.map_err(lookup_state_error(
        "commit primary-name position revalidation",
    ))
}

async fn revalidation_transaction(
    pool: &PgPool,
    lock_rows: bool,
) -> Result<Transaction<'_, Postgres>> {
    // Start after RPC, rather than keeping the pre-RPC snapshot: changes committed
    // during provider execution must be visible to the guard.
    let mut transaction = pool
        .begin()
        .await
        .map_err(database("start lookup revalidation"))?;
    if !lock_rows {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(lookup_state_error("set lookup revalidation isolation"))?;
    }
    Ok(transaction)
}

async fn revalidate_lookup_state(
    transaction: &mut Transaction<'_, Postgres>,
    head: &LookupPosition,
    observed_positions: &Value,
    execution_authority: &Value,
    comparison: Option<&IndexedComparison>,
    lock_rows: bool,
) -> Result<()> {
    // Retain the old entry point for writers so existing non-API EXECUTE grants
    // remain sufficient. Read-only callers use their fixed-mode entry point.
    let query = if lock_rows {
        "SELECT revalidate_resolution_lookup_state($1, $2, $3, $4, $5, $6::uuid, $7, $8)"
    } else {
        "SELECT revalidate_resolution_lookup_state_read_only($1, $2, $3, $4, $5, $6::uuid, $7, $8)"
    };
    let status: String = sqlx::query_scalar(query)
        .bind(&head.chain_id)
        .bind(head.block_number)
        .bind(&head.block_hash)
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
    .bind(&snapshot.head.chain_id)
    .bind(snapshot.head.block_number)
    .bind(&snapshot.head.block_hash)
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
            Some("25006") => {
                return LookupError::configuration(
                    "lookup database connection does not permit ledger writes",
                );
            }
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
        if let sqlx::Error::Database(database_error) = &error {
            match database_error.code().as_deref() {
                Some("25006") => {
                    return LookupError::configuration(
                        "lookup state guard attempted a write on a read-only database",
                    );
                }
                Some("40P01" | "40001") => {
                    return LookupError::concurrent_state(format!(
                        "lookup state changed during revalidation: {database_error}"
                    ));
                }
                _ => {}
            }
        }
        database(context)(error)
    }
}
