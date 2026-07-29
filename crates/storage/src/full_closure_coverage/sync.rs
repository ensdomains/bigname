use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, Row};

use super::{
    FULL_CLOSURE_COVERAGE_PROOF_FORMAT_VERSION, FullClosureCoverageSynchronization,
    eligible_facts::eligible_facts_cte,
};

#[path = "sync/delta.rs"]
mod delta;

use delta::synchronize_deltas;

#[derive(Clone, Debug)]
pub(super) struct RollupState {
    proof_format_version: String,
    coverage_input_revision: i64,
    raw_log_input_revision: i64,
    raw_log_retention_generation: i64,
    discovery_admission_epoch: i64,
    topic0s_by_family: Value,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AuthoritySnapshot {
    coverage_input_revision: i64,
    raw_log_input_revision: i64,
    raw_log_retention_generation: i64,
    block_revision_evidence_floor: i64,
    discovery_admission_epoch: i64,
}

pub(super) async fn synchronize(
    connection: &mut PgConnection,
    chain: &str,
    expected_retention_generation: i64,
    expected_discovery_admission_epoch: i64,
    topic0s_by_family: &BTreeMap<String, Vec<String>>,
) -> Result<FullClosureCoverageSynchronization> {
    // TRUNCATE acquires ACCESS EXCLUSIVE before its invalidation trigger takes
    // the advisory key. Take the compatible table lock first so proof and
    // truncation share table -> advisory lock order. Ordinary fact writes are
    // not blocked by ACCESS SHARE.
    sqlx::query("LOCK TABLE backfill_coverage_facts IN ACCESS SHARE MODE")
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to fence full-closure fact truncation for {chain}"))?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('full_closure_coverage:' || $1, 0))",
    )
    .bind(chain)
    .execute(&mut *connection)
    .await
    .with_context(|| format!("failed to lock full-closure coverage state for {chain}"))?;
    sqlx::query(
        r#"
        INSERT INTO full_closure_coverage_input_revisions (chain_id, revision)
        VALUES ($1, 0)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(&mut *connection)
    .await
    .with_context(|| format!("failed to ensure full-closure coverage revision for {chain}"))?;

    let authority = load_authority(connection, chain).await?;
    ensure!(
        authority.raw_log_retention_generation == expected_retention_generation,
        "raw-log retention generation changed before full-closure coverage synchronization for {chain}: expected {expected_retention_generation}, observed {}",
        authority.raw_log_retention_generation
    );
    ensure!(
        authority.discovery_admission_epoch == expected_discovery_admission_epoch,
        "discovery admission epoch changed before full-closure coverage synchronization for {chain}: expected {expected_discovery_admission_epoch}, observed {}",
        authority.discovery_admission_epoch
    );
    ensure_raw_revision_evidence_is_complete(connection, chain, &authority).await?;

    let topics = serde_json::to_value(topic0s_by_family)
        .context("failed to encode full-closure coverage topic sets")?;
    let state = load_state(connection, chain).await?;
    let journal_complete =
        coverage_journal_is_complete(connection, chain, state.as_ref(), authority).await?;
    // These are the complete cold-rebuild triggers. Coverage-fact inserts and
    // fact/job retirement are commit-ordered in the coverage journal and use
    // the delta path below. A retention-generation change, admission-epoch
    // change (and therefore a watched-set change), current-topic change,
    // proof-format change, revision regression, journal gap, or raw revision
    // evidence-floor advance invalidates the saved aggregate wholesale.
    let full_rebuild = state.as_ref().is_none_or(|state| {
        state.proof_format_version != FULL_CLOSURE_COVERAGE_PROOF_FORMAT_VERSION
            || state.raw_log_retention_generation != authority.raw_log_retention_generation
            || state.discovery_admission_epoch != authority.discovery_admission_epoch
            || state.topic0s_by_family != topics
            || state.coverage_input_revision > authority.coverage_input_revision
            || state.raw_log_input_revision > authority.raw_log_input_revision
            || state.raw_log_input_revision < authority.block_revision_evidence_floor
            || !journal_complete
    });

    let (appended_fact_count, rebuilt_key_count) = if full_rebuild {
        let rebuilt = rebuild_all(connection, chain, authority, &topics).await?;
        (0, rebuilt)
    } else {
        synchronize_deltas(
            connection,
            chain,
            state
                .as_ref()
                .expect("non-rebuild synchronization has state"),
            authority,
            &topics,
        )
        .await?
    };
    store_state(connection, chain, authority, &topics).await?;
    sqlx::query(
        r#"
        DELETE FROM full_closure_coverage_input_changes
        WHERE chain_id = $1
          AND revision <= $2
        "#,
    )
    .bind(chain)
    .bind(authority.coverage_input_revision)
    .execute(&mut *connection)
    .await
    .with_context(|| {
        format!("failed to prune applied full-closure coverage changes for {chain}")
    })?;

    Ok(FullClosureCoverageSynchronization {
        full_rebuild,
        appended_fact_count,
        rebuilt_key_count,
        coverage_input_revision: authority.coverage_input_revision,
        raw_log_input_revision: authority.raw_log_input_revision,
    })
}

async fn load_authority(connection: &mut PgConnection, chain: &str) -> Result<AuthoritySnapshot> {
    let row = sqlx::query(
        r#"
        SELECT
            coverage.revision AS coverage_input_revision,
            retained.revision AS raw_log_input_revision,
            retained.retention_generation AS raw_log_retention_generation,
            retained.block_revision_evidence_floor,
            COALESCE(admission.epoch, 0) AS discovery_admission_epoch
        FROM full_closure_coverage_input_revisions coverage
        JOIN raw_log_staging_input_revisions retained
          ON retained.chain_id = coverage.chain_id
        LEFT JOIN discovery_admission_epochs admission
          ON admission.chain_id = coverage.chain_id
        WHERE coverage.chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| format!("failed to load full-closure coverage authority for {chain}"))?
    .with_context(|| format!("missing raw-log retention authority for chain {chain}"))?;
    Ok(AuthoritySnapshot {
        coverage_input_revision: row.try_get("coverage_input_revision")?,
        raw_log_input_revision: row.try_get("raw_log_input_revision")?,
        raw_log_retention_generation: row.try_get("raw_log_retention_generation")?,
        block_revision_evidence_floor: row.try_get("block_revision_evidence_floor")?,
        discovery_admission_epoch: row.try_get("discovery_admission_epoch")?,
    })
}

async fn load_state(connection: &mut PgConnection, chain: &str) -> Result<Option<RollupState>> {
    sqlx::query(
        r#"
        SELECT
            proof_format_version,
            coverage_input_revision,
            raw_log_input_revision,
            raw_log_retention_generation,
            discovery_admission_epoch,
            topic0s_by_family
        FROM full_closure_coverage_rollup_states
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(connection)
    .await
    .with_context(|| format!("failed to load full-closure coverage state for {chain}"))?
    .map(|row| {
        Ok(RollupState {
            proof_format_version: row.try_get("proof_format_version")?,
            coverage_input_revision: row.try_get("coverage_input_revision")?,
            raw_log_input_revision: row.try_get("raw_log_input_revision")?,
            raw_log_retention_generation: row.try_get("raw_log_retention_generation")?,
            discovery_admission_epoch: row.try_get("discovery_admission_epoch")?,
            topic0s_by_family: row.try_get("topic0s_by_family")?,
        })
    })
    .transpose()
}

async fn ensure_raw_revision_evidence_is_complete(
    connection: &mut PgConnection,
    chain: &str,
    authority: &AuthoritySnapshot,
) -> Result<()> {
    let state_revision = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT raw_log_input_revision
        FROM full_closure_coverage_rollup_states
        WHERE chain_id = $1
          AND raw_log_retention_generation = $2
          AND raw_log_input_revision >= $3
          AND raw_log_input_revision <= $4
        "#,
    )
    .bind(chain)
    .bind(authority.raw_log_retention_generation)
    .bind(authority.block_revision_evidence_floor)
    .bind(authority.raw_log_input_revision)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| format!("failed to load raw revision base for {chain}"))?;
    let evidence_base = if let Some(state_revision) = state_revision {
        state_revision
    } else {
        // A retention-generation change has no per-block witness for its
        // TRUNCATE revision. Old-generation aggregate state is unusable, so a
        // cold rebuild needs evidence only after the oldest current-generation
        // stored verification that can contribute a fact. Facts without
        // stored verification do not depend on raw-log revision evidence.
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(job.stored_verification_raw_log_input_revision)::BIGINT
            FROM backfill_coverage_facts fact
            JOIN backfill_jobs job
              ON job.backfill_job_id = fact.backfill_job_id
             AND job.chain_id = fact.chain_id
            WHERE fact.chain_id = $1
              AND job.status = 'completed'::backfill_lifecycle_status
              AND job.raw_log_retention_generation = $2
              AND job.stored_verification_raw_log_input_revision >= $3
              AND job.stored_verification_raw_log_input_revision <= $4
              AND fact.covered_from_block >= job.range_start_block_number
              AND fact.covered_to_block <= job.range_end_block_number
            "#,
        )
        .bind(chain)
        .bind(authority.raw_log_retention_generation)
        .bind(authority.block_revision_evidence_floor)
        .bind(authority.raw_log_input_revision)
        .fetch_one(&mut *connection)
        .await
        .with_context(|| {
            format!("failed to load current-generation raw revision base for {chain}")
        })?
        .unwrap_or(authority.raw_log_input_revision)
    };
    let evidence_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT revision)::BIGINT
        FROM raw_log_staging_block_revisions
        WHERE chain_id = $1
          AND revision > $2
          AND revision <= $3
        "#,
    )
    .bind(chain)
    .bind(evidence_base)
    .bind(authority.raw_log_input_revision)
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to validate raw revision evidence for {chain}"))?;
    let expected = authority.raw_log_input_revision - evidence_base;
    ensure!(
        evidence_count == expected,
        "raw-log staging revision advanced for {chain} from {evidence_base} to {} without per-block evidence for every intervening revision: expected {expected}, found {evidence_count}",
        authority.raw_log_input_revision
    );
    Ok(())
}

async fn coverage_journal_is_complete(
    connection: &mut PgConnection,
    chain: &str,
    state: Option<&RollupState>,
    authority: AuthoritySnapshot,
) -> Result<bool> {
    let from_revision = state
        .map(|state| state.coverage_input_revision)
        .unwrap_or(0);
    if from_revision > authority.coverage_input_revision {
        return Ok(false);
    }
    let observed = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT revision)::BIGINT
        FROM full_closure_coverage_input_changes
        WHERE chain_id = $1
          AND revision > $2
          AND revision <= $3
        "#,
    )
    .bind(chain)
    .bind(from_revision)
    .bind(authority.coverage_input_revision)
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to validate full-closure coverage journal for {chain}"))?;
    Ok(observed == authority.coverage_input_revision - from_revision)
}

async fn rebuild_all(
    connection: &mut PgConnection,
    chain: &str,
    authority: AuthoritySnapshot,
    topics: &Value,
) -> Result<u64> {
    sqlx::query("DELETE FROM full_closure_coverage_rollups WHERE chain_id = $1")
        .bind(chain)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to clear full-closure coverage rollups for {chain}"))?;
    let eligible_facts_cte = eligible_facts_cte("TRUE");
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
            source_family,
            scope,
            address,
            range_agg(int8range(covered_from_block, covered_to_block, '[]'))
        FROM eligible_coverage_facts
        GROUP BY source_family, scope, address
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
        .with_context(|| format!("failed to rebuild full-closure coverage rollups for {chain}"))
        .map(|result| result.rows_affected())
}

async fn store_state(
    connection: &mut PgConnection,
    chain: &str,
    authority: AuthoritySnapshot,
    topics: &Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO full_closure_coverage_rollup_states (
            chain_id,
            proof_format_version,
            coverage_input_revision,
            raw_log_input_revision,
            raw_log_retention_generation,
            discovery_admission_epoch,
            topic0s_by_family,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (chain_id) DO UPDATE
        SET proof_format_version = EXCLUDED.proof_format_version,
            coverage_input_revision = EXCLUDED.coverage_input_revision,
            raw_log_input_revision = EXCLUDED.raw_log_input_revision,
            raw_log_retention_generation = EXCLUDED.raw_log_retention_generation,
            discovery_admission_epoch = EXCLUDED.discovery_admission_epoch,
            topic0s_by_family = EXCLUDED.topic0s_by_family,
            updated_at = now()
        "#,
    )
    .bind(chain)
    .bind(FULL_CLOSURE_COVERAGE_PROOF_FORMAT_VERSION)
    .bind(authority.coverage_input_revision)
    .bind(authority.raw_log_input_revision)
    .bind(authority.raw_log_retention_generation)
    .bind(authority.discovery_admission_epoch)
    .bind(topics)
    .execute(connection)
    .await
    .with_context(|| format!("failed to store full-closure coverage state for {chain}"))?;
    Ok(())
}
