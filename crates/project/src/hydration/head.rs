use sqlx::{PgPool, Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn load(pool: &PgPool, chain_id: &str) -> Result<Marker> {
    sqlx::query_as::<_, (i64, String)>(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to load hydration head", error))?
    .map(|(number, hash)| Marker { number, hash })
    .ok_or_else(|| ProjectError::data_integrity("canonical-head hydration requires chain heads"))
}

pub(super) async fn interpret_redo_pending(pool: &PgPool, chain_id: &str) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'interpret'
               AND redo_in_progress
         )",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ProjectError::database("failed to check hydration redo fence", error))
}

pub(super) async fn require_same(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    head: &Marker,
) -> Result<()> {
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT latest_block_number FROM chain_heads
         WHERE chain_id = $1 AND latest_block_number = $2 AND latest_block_hash = $3
         FOR SHARE",
    )
    .bind(chain_id)
    .bind(head.number)
    .bind(&head.hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to revalidate hydration head", error))?;
    if current.is_none() {
        return Err(ProjectError::transient(
            "canonical head changed during hydration; retry at the new head",
        ));
    }
    Ok(())
}
