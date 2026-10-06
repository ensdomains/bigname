use crate::{InterpretError, Result};
use sqlx::{Postgres, Transaction};

pub(super) fn failure(error: anyhow::Error) -> InterpretError {
    InterpretError::data_integrity(format!("normalization search refresh failed: {error:#}"))
}

/// Read only the scope first; source-row FOR UPDATE reads happen after this label lock set.
pub(super) async fn lock(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from: i64,
    to: i64,
    labels: &[String],
) -> Result<Vec<String>> {
    let names = sqlx::query_scalar::<_, String>(
        "SELECT logical_name_id FROM name_surfaces
         WHERE chain_id=$1 AND block_number BETWEEN $2 AND $3 ORDER BY logical_name_id",
    )
    .bind(chain_id)
    .bind(from)
    .bind(to)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to read normalization search scope", error)
    })?;
    bigname_storage::identity_search::prepare(transaction, labels, &[], &names)
        .await
        .map_err(failure)?;
    Ok(names)
}
