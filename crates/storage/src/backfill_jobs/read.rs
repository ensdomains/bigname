use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    sql::{backfill_job_select_sql, backfill_range_select_sql},
    types::{BackfillJob, BackfillRange},
};

/// Load one historical backfill job by stable row identity.
pub async fn load_backfill_job(pool: &PgPool, backfill_job_id: i64) -> Result<Option<BackfillJob>> {
    let select_sql = backfill_job_select_sql("WHERE backfill_job_id = $1", "");
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to load backfill job {backfill_job_id}"))?;
    row.map(decode_backfill_job).transpose()
}

/// Load historical child ranges for one backfill job in declared range order.
pub async fn load_backfill_ranges(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>> {
    let select_sql = backfill_range_select_sql(
        "WHERE backfill_job_id = $1",
        "ORDER BY range_start_block_number, range_end_block_number",
    );
    let rows = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load ranges for backfill job {backfill_job_id}"))?;
    rows.into_iter().map(decode_backfill_range).collect()
}
