use super::*;

pub(super) async fn materialize_candidate_source_pages(
    connection: &mut PgConnection,
    progress_pool: &PgPool,
    chain: &str,
    verified_from_block: i64,
    verified_through_block: i64,
    log_producing_source_families: &[String],
    source_table: &str,
    source_id_column: &str,
    watched_branch: &str,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<()> {
    let mut after_id = 0i64;
    loop {
        let source_ids = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT {source_id_column} FROM {source_table} WHERE {source_id_column} > $1 ORDER BY {source_id_column} LIMIT $2"
        ))
        .bind(after_id)
        .bind(COVERAGE_SOURCE_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("failed to page {source_table} for coverage candidate"))?;
        let Some(last_id) = source_ids.last().copied() else {
            break;
        };
        after_id = last_id;
        let query = super::super::intervals::with_streaming_watched_intervals(&format!(
            r#"
            INSERT INTO pg_temp.{candidate_table} (
                source_family,
                address,
                required_intervals
            )
            SELECT
                watched.source_family,
                LOWER(watched.address),
                range_agg(
                    int8range(
                        GREATEST(COALESCE(watched.active_from_block_number, $2), $2),
                        LEAST(COALESCE(watched.active_to_block_number, $3), $3) + 1,
                        '[)'
                    )
                )
            FROM {watched_branch} watched
            WHERE watched.source_row_id = ANY($5::BIGINT[])
              AND {historical_predicate}
              AND watched.chain = $1
              AND watched.source_family = ANY($4::TEXT[])
              AND COALESCE(watched.active_from_block_number, $2) <= $3
              AND COALESCE(watched.active_to_block_number, $3) >= $2
            GROUP BY watched.source_family, LOWER(watched.address)
            ON CONFLICT (source_family, address) DO UPDATE
            SET required_intervals = {candidate_table}.required_intervals
                                   + EXCLUDED.required_intervals
            "#,
            candidate_table = STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE,
            historical_predicate = super::super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
        ));
        sqlx::query(&query)
            .bind(chain)
            .bind(verified_from_block)
            .bind(verified_through_block)
            .bind(log_producing_source_families)
            .bind(&source_ids)
            .execute(&mut *connection)
            .await
            .with_context(|| format!("failed to materialize {watched_branch} coverage page"))?;
        progress.record(progress_pool).await?;
    }
    Ok(())
}
