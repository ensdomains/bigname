use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;

use super::{AuthoritySnapshot, RollupState};
use crate::full_closure_coverage::eligible_facts::eligible_facts_cte;

const DIRTY_KEYS_TABLE: &str = "full_closure_coverage_dirty_keys";

pub(super) async fn synchronize_deltas(
    connection: &mut PgConnection,
    chain: &str,
    state: &RollupState,
    authority: AuthoritySnapshot,
    topics: &Value,
) -> Result<(u64, u64)> {
    create_dirty_keys(connection).await?;
    mark_journal_rebuilds(connection, chain, state, authority).await?;
    mark_raw_revision_rebuilds(connection, chain, state, authority).await?;
    let rebuilt_key_count =
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM {DIRTY_KEYS_TABLE}"))
            .fetch_one(&mut *connection)
            .await
            .context("failed to count dirty full-closure coverage keys")?;
    rebuild_dirty_keys(connection, chain, authority, topics).await?;
    let appended_fact_count = append_new_facts(connection, chain, state, authority, topics).await?;
    Ok((
        appended_fact_count,
        u64::try_from(rebuilt_key_count)
            .context("dirty full-closure coverage key count must not be negative")?,
    ))
}

async fn create_dirty_keys(connection: &mut PgConnection) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TEMP TABLE {DIRTY_KEYS_TABLE} (
            source_family TEXT NOT NULL,
            scope TEXT NOT NULL,
            address TEXT,
            CONSTRAINT full_closure_coverage_dirty_keys_tuple_key
                UNIQUE NULLS NOT DISTINCT (source_family, scope, address)
        ) ON COMMIT DROP
        "#
    ))
    .execute(connection)
    .await
    .context("failed to create dirty full-closure coverage key table")?;
    Ok(())
}

async fn mark_journal_rebuilds(
    connection: &mut PgConnection,
    chain: &str,
    state: &RollupState,
    authority: AuthoritySnapshot,
) -> Result<()> {
    sqlx::query(&format!(
        r#"
        INSERT INTO {DIRTY_KEYS_TABLE} (source_family, scope, address)
        SELECT DISTINCT source_family, scope, address
        FROM full_closure_coverage_input_changes
        WHERE chain_id = $1
          AND revision > $2
          AND revision <= $3
          AND change_kind = 'rebuild'
        ON CONFLICT ON CONSTRAINT full_closure_coverage_dirty_keys_tuple_key
        DO NOTHING
        "#
    ))
    .bind(chain)
    .bind(state.coverage_input_revision)
    .bind(authority.coverage_input_revision)
    .execute(connection)
    .await
    .with_context(|| format!("failed to collect changed full-closure coverage keys for {chain}"))?;
    Ok(())
}

async fn mark_raw_revision_rebuilds(
    connection: &mut PgConnection,
    chain: &str,
    state: &RollupState,
    authority: AuthoritySnapshot,
) -> Result<()> {
    if state.raw_log_input_revision == authority.raw_log_input_revision {
        return Ok(());
    }
    sqlx::query(&format!(
        r#"
        INSERT INTO {DIRTY_KEYS_TABLE} (source_family, scope, address)
        SELECT DISTINCT fact.source_family, fact.scope, fact.address
        FROM raw_log_staging_block_revisions changed
        JOIN backfill_jobs job
          ON job.chain_id = changed.chain_id
         AND job.status = 'completed'::backfill_lifecycle_status
         AND job.raw_log_retention_generation = $4
         AND job.stored_verification_raw_log_input_revision IS NOT NULL
         AND job.stored_verification_raw_log_input_revision < changed.revision
        JOIN backfill_coverage_facts fact
          ON fact.backfill_job_id = job.backfill_job_id
         AND fact.chain_id = job.chain_id
         AND changed.block_number BETWEEN
             fact.covered_from_block AND fact.covered_to_block
        WHERE changed.chain_id = $1
          AND changed.revision > $2
          AND changed.revision <= $3
        ON CONFLICT ON CONSTRAINT full_closure_coverage_dirty_keys_tuple_key
        DO NOTHING
        "#
    ))
    .bind(chain)
    .bind(state.raw_log_input_revision)
    .bind(authority.raw_log_input_revision)
    .bind(authority.raw_log_retention_generation)
    .execute(connection)
    .await
    .with_context(|| {
        format!("failed to collect raw-invalidated full-closure coverage keys for {chain}")
    })?;
    Ok(())
}

async fn rebuild_dirty_keys(
    connection: &mut PgConnection,
    chain: &str,
    authority: AuthoritySnapshot,
    topics: &Value,
) -> Result<()> {
    sqlx::query(&format!(
        r#"
        DELETE FROM full_closure_coverage_rollups rollup
        USING {DIRTY_KEYS_TABLE} dirty
        WHERE rollup.chain_id = $1
          AND rollup.source_family = dirty.source_family
          AND rollup.scope = dirty.scope
          AND rollup.address IS NOT DISTINCT FROM dirty.address
        "#
    ))
    .bind(chain)
    .execute(&mut *connection)
    .await
    .with_context(|| format!("failed to clear dirty full-closure coverage keys for {chain}"))?;
    let eligible_facts_cte = eligible_facts_cte(&format!(
        r#"
        EXISTS (
            SELECT 1
            FROM {DIRTY_KEYS_TABLE} dirty
            WHERE dirty.source_family = fact.source_family
              AND dirty.scope = fact.scope
              AND dirty.address IS NOT DISTINCT FROM fact.address
        )
        "#
    ));
    let query = format!(
        r#"
        {eligible_facts_cte}
        INSERT INTO full_closure_coverage_rollups (
            chain_id,
            raw_log_retention_generation,
            source_family,
            scope,
            address,
            covered_blocks
        )
        SELECT
            $1,
            $2,
            fact.source_family,
            fact.scope,
            fact.address,
            range_agg(int8range(fact.covered_from_block, fact.covered_to_block, '[]'))
        FROM eligible_coverage_facts fact
        JOIN {DIRTY_KEYS_TABLE} dirty
          ON dirty.source_family = fact.source_family
         AND dirty.scope = fact.scope
         AND dirty.address IS NOT DISTINCT FROM fact.address
        GROUP BY fact.source_family, fact.scope, fact.address
        "#
    );
    sqlx::query(&query)
        .bind(chain)
        .bind(authority.raw_log_retention_generation)
        .bind(authority.raw_log_input_revision)
        .bind(authority.block_revision_evidence_floor)
        .bind(topics)
        .execute(connection)
        .await
        .with_context(|| {
            format!("failed to rebuild dirty full-closure coverage keys for {chain}")
        })?;
    Ok(())
}

async fn append_new_facts(
    connection: &mut PgConnection,
    chain: &str,
    state: &RollupState,
    authority: AuthoritySnapshot,
    topics: &Value,
) -> Result<u64> {
    let appended_fact_count = sqlx::query_scalar::<_, i64>(&format!(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM full_closure_coverage_input_changes changed
        WHERE changed.chain_id = $1
          AND changed.revision > $2
          AND changed.revision <= $3
          AND changed.change_kind = 'append'
          AND NOT EXISTS (
              SELECT 1
              FROM {DIRTY_KEYS_TABLE} dirty
              WHERE dirty.source_family = changed.source_family
                AND dirty.scope = changed.scope
                AND dirty.address IS NOT DISTINCT FROM changed.address
          )
        "#
    ))
    .bind(chain)
    .bind(state.coverage_input_revision)
    .bind(authority.coverage_input_revision)
    .fetch_one(&mut *connection)
    .await
    .with_context(|| format!("failed to count appended full-closure coverage facts for {chain}"))?;
    let eligible_facts_cte = eligible_facts_cte(
        r#"
        EXISTS (
            SELECT 1
            FROM full_closure_coverage_input_changes selected_change
            WHERE selected_change.chain_id = $1
              AND selected_change.revision > $6
              AND selected_change.revision <= $7
              AND selected_change.change_kind = 'append'
              AND selected_change.backfill_coverage_fact_id
                  = fact.backfill_coverage_fact_id
        )
        "#,
    );
    let query = format!(
        r#"
        {eligible_facts_cte},
        additions AS (
            SELECT
                fact.source_family,
                fact.scope,
                fact.address,
                range_agg(
                    int8range(fact.covered_from_block, fact.covered_to_block, '[]')
                ) AS covered_blocks
            FROM full_closure_coverage_input_changes changed
            JOIN eligible_coverage_facts fact
              ON fact.backfill_coverage_fact_id = changed.backfill_coverage_fact_id
            WHERE changed.chain_id = $1
              AND changed.revision > $6
              AND changed.revision <= $7
              AND changed.change_kind = 'append'
              AND NOT EXISTS (
                  SELECT 1
                  FROM {DIRTY_KEYS_TABLE} dirty
                  WHERE dirty.source_family = changed.source_family
                    AND dirty.scope = changed.scope
                    AND dirty.address IS NOT DISTINCT FROM changed.address
              )
            GROUP BY fact.source_family, fact.scope, fact.address
        )
        INSERT INTO full_closure_coverage_rollups (
            chain_id,
            raw_log_retention_generation,
            source_family,
            scope,
            address,
            covered_blocks
        )
        SELECT
            $1,
            $2,
            source_family,
            scope,
            address,
            covered_blocks
        FROM additions
        ON CONFLICT ON CONSTRAINT full_closure_coverage_rollups_tuple_key
        DO UPDATE SET covered_blocks =
            full_closure_coverage_rollups.covered_blocks
            + EXCLUDED.covered_blocks
        "#
    );
    sqlx::query(&query)
        .bind(chain)
        .bind(authority.raw_log_retention_generation)
        .bind(authority.raw_log_input_revision)
        .bind(authority.block_revision_evidence_floor)
        .bind(topics)
        .bind(state.coverage_input_revision)
        .bind(authority.coverage_input_revision)
        .execute(connection)
        .await
        .with_context(|| format!("failed to append full-closure coverage facts for {chain}"))?;
    u64::try_from(appended_fact_count)
        .context("appended full-closure coverage fact count must not be negative")
}
