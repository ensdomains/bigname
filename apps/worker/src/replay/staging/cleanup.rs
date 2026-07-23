use super::*;

pub(crate) async fn cleanup_projection_checkpoint(pool: &PgPool, projection: &str) -> Result<()> {
    let mut transaction = pool.begin().await.with_context(|| {
        format!("failed to open {projection} staging checkpoint cleanup transaction")
    })?;
    lock_projection_checkpoint(&mut transaction, projection).await?;
    let Some(checkpoint) = load_checkpoint(&mut transaction, projection).await? else {
        transaction.commit().await?;
        return Ok(());
    };
    delete_checkpoint_and_stage_tables(&mut transaction, projection, &checkpoint).await?;
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit {projection} staging checkpoint cleanup"))?;
    Ok(())
}

pub(crate) async fn consume_completed_projection_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
    normalized_target_block: Option<i64>,
) -> Result<i64> {
    lock_projection_checkpoint(transaction, projection).await?;
    let checkpoint = load_checkpoint(transaction, projection)
        .await?
        .with_context(|| {
            format!("{projection} replay cannot be marked without its completed stage")
        })?;
    ensure!(
        checkpoint.replay_version == CURRENT_PROJECTION_REPLAY_VERSION,
        "{projection} staging replay version changed before completion marker"
    );
    ensure!(
        checkpoint.staging_schema_version == CURRENT_PROJECTION_STAGING_SCHEMA_VERSION,
        "{projection} staging schema version changed before completion marker"
    );
    ensure!(
        checkpoint.normalized_target_block == normalized_target_block,
        "{projection} staging target changed before completion marker"
    );
    ensure!(
        checkpoint.status == "staging_complete",
        "{projection} replay cannot be marked from an incomplete stage"
    );
    ensure_current_projection_full_replay_input_revision_in_transaction(
        transaction,
        checkpoint.full_replay_input_revision,
    )
    .await?;
    delete_checkpoint_and_stage_tables(transaction, projection, &checkpoint).await?;
    Ok(checkpoint.full_replay_input_revision)
}

async fn delete_checkpoint_and_stage_tables(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
    checkpoint: &StoredCheckpoint,
) -> Result<()> {
    drop_stage_tables(transaction, &checkpoint.stage_tables).await?;
    let deleted =
        sqlx::query("DELETE FROM current_projection_staging_checkpoints WHERE projection = $1")
            .bind(projection)
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to delete {projection} staging checkpoint"))?
            .rows_affected();
    ensure!(
        deleted == 1,
        "{projection} staging checkpoint changed during cleanup"
    );
    Ok(())
}
