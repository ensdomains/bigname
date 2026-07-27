use anyhow::{Context, Result, ensure};
use sqlx::{Executor, PgConnection, PgPool, Postgres};

/// Reports whether a committed semantic raw-log mutation after `revision`
/// touched any block in the inclusive range. Unknown input, a checkpoint
/// revision below `block_revision_evidence_floor`, or an advanced revision
/// without gap-free per-block evidence returns `true` so boundary reuse fails
/// closed.
pub async fn raw_log_staging_block_range_changed_since(
    pool: &PgPool,
    chain: &str,
    revision: i64,
    from_block: i64,
    through_block: i64,
) -> Result<bool> {
    raw_log_staging_block_range_changed_since_with_executor(
        pool,
        chain,
        revision,
        from_block,
        through_block,
    )
    .await
}

pub(crate) async fn raw_log_staging_block_range_changed_since_from_connection(
    connection: &mut PgConnection,
    chain: &str,
    revision: i64,
    from_block: i64,
    through_block: i64,
) -> Result<bool> {
    raw_log_staging_block_range_changed_since_with_executor(
        &mut *connection,
        chain,
        revision,
        from_block,
        through_block,
    )
    .await
}

async fn raw_log_staging_block_range_changed_since_with_executor<'e, E>(
    executor: E,
    chain: &str,
    revision: i64,
    from_block: i64,
    through_block: i64,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    ensure!(
        revision >= 0,
        "raw-log staging revision must not be negative"
    );
    ensure!(
        from_block >= 0,
        "raw-log staging range start must not be negative"
    );
    ensure!(
        through_block >= from_block,
        "raw-log staging range end must not precede its start"
    );
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT CASE
            WHEN current_revision IS NULL OR current_revision < $2 THEN TRUE
            WHEN current_revision = $2 THEN FALSE
            WHEN $2 < block_revision_evidence_floor THEN TRUE
            WHEN (
                SELECT COUNT(DISTINCT revision)
                FROM raw_log_staging_block_revisions
                WHERE chain_id = $1
                  AND revision > $2
                  AND revision <= current_revision
            ) <> current_revision - $2 THEN TRUE
            ELSE EXISTS (
                SELECT 1
                FROM raw_log_staging_block_revisions
                WHERE chain_id = $1
                  AND revision > $2
                  AND revision <= current_revision
                  AND block_number BETWEEN $3 AND $4
            )
        END
        FROM (
            SELECT
                (
                    SELECT revision
                    FROM raw_log_staging_input_revisions
                    WHERE chain_id = $1
                ) AS current_revision,
                (
                    SELECT block_revision_evidence_floor
                    FROM raw_log_staging_input_revisions
                    WHERE chain_id = $1
                ) AS block_revision_evidence_floor
        ) AS current
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(from_block)
    .bind(through_block)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to inspect raw-log staging changes for {chain} after revision {revision} in {from_block}..={through_block}"
        )
    })
}

/// Returns the earliest block at or below `through_block` touched by a
/// semantic raw-log mutation after `revision`.
///
/// An unprovable legacy prefix or evidence gap returns `rewind_floor_block` so
/// cursor rewind fails closed without widening the replay before its stored
/// range start.
pub async fn earliest_raw_log_staging_block_changed_since(
    pool: &PgPool,
    chain: &str,
    revision: i64,
    through_block: i64,
    rewind_floor_block: i64,
) -> Result<Option<i64>> {
    ensure!(
        !chain.trim().is_empty(),
        "raw-log staging chain must not be empty"
    );
    ensure!(
        revision >= 0,
        "raw-log staging revision must not be negative"
    );
    ensure!(
        through_block >= 0,
        "raw-log staging changed-block boundary must not be negative"
    );
    ensure!(
        rewind_floor_block >= 0,
        "raw-log staging rewind floor must not be negative"
    );
    if through_block < rewind_floor_block {
        return Ok(None);
    }
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT CASE
            WHEN current_revision IS NULL OR current_revision <= $2 THEN NULL
            WHEN $2 < block_revision_evidence_floor THEN $4
            WHEN evidenced_revision_count <> current_revision - $2 THEN $4
            ELSE earliest_changed_block
        END
        FROM (
            SELECT
                current_revision,
                block_revision_evidence_floor,
                (
                    SELECT COUNT(DISTINCT evidence.revision)
                    FROM raw_log_staging_block_revisions evidence
                    WHERE evidence.chain_id = $1
                      AND evidence.revision > $2
                      AND evidence.revision <= current_revision
                ) AS evidenced_revision_count,
                (
                    SELECT MIN(evidence.block_number)
                    FROM raw_log_staging_block_revisions evidence
                    WHERE evidence.chain_id = $1
                      AND evidence.revision > $2
                      AND evidence.revision <= current_revision
                      AND evidence.block_number <= $3
                ) AS earliest_changed_block
            FROM (
                SELECT
                    (
                        SELECT revision
                        FROM raw_log_staging_input_revisions
                        WHERE chain_id = $1
                    ) AS current_revision,
                    (
                        SELECT block_revision_evidence_floor
                        FROM raw_log_staging_input_revisions
                        WHERE chain_id = $1
                    ) AS block_revision_evidence_floor
            ) revision_state
        ) current
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(through_block)
    .bind(rewind_floor_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load earliest raw-log staging change for {chain} after revision {revision} through block {through_block}"
        )
    })
}
