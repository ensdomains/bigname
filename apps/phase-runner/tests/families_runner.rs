#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    project_phase::FamilySettings,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

const CHAIN: &str = "families-runner";

// The family loop runs after the batch's progress is recorded, in its own transactions. A
// failure there must leave the Project batch, its recorded progress and the served tables as
// they would have been without it; the next batch catches the families up.
#[tokio::test]
async fn a_failing_family_loop_leaves_project_progress_recorded() -> Result<()> {
    let scratch = ScratchDatabase::create("families_runner_failure").await?;
    seed_lineage(scratch.pool(), CHAIN, 3).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    seed_completed_extent(scratch.pool(), 3).await?;
    sqlx::query(
        "CREATE FUNCTION refuse_family_marker() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected family failure'; END $$",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER refuse_family_marker BEFORE INSERT OR UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION refuse_family_marker()",
    )
    .execute(scratch.pool())
    .await?;

    redo_project(&scratch).await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false)
    );
    assert_eq!(marker(&scratch).await?, None, "the family loop was refused");

    sqlx::query("DROP TRIGGER refuse_family_marker ON project_family_marker")
        .execute(scratch.pool())
        .await?;
    redo_project(&scratch).await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false)
    );
    assert_eq!(marker(&scratch).await?, Some(3), "the next batch caught up");
    scratch.cleanup().await
}

// A schema whose family tables were never migrated: every family run fails and is counted as a
// skip, and the Project batch and its progress commit as they would without the families.
#[tokio::test]
async fn a_missing_family_migration_leaves_project_progress_recorded() -> Result<()> {
    let scratch = ready("families_runner_missing").await?;
    sqlx::query("DROP TABLE project_family_marker")
        .execute(scratch.pool())
        .await?;
    redo_project(&scratch).await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false)
    );
    scratch.cleanup().await
}

// The input token read before the progress write is bounded. When it does not finish in time the
// families skip the batch, the progress write is not held up, and the next batch catches up.
#[tokio::test]
async fn a_late_input_token_skips_the_families_and_the_next_batch_catches_up() -> Result<()> {
    let scratch = ready("families_runner_token").await?;
    redo_project_with(
        &scratch,
        FamilySettings {
            token_budget: Duration::ZERO,
            ..FamilySettings::default()
        },
    )
    .await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false)
    );
    assert_eq!(marker(&scratch).await?, None, "the families skipped the batch");

    redo_project(&scratch).await?;
    assert_eq!(marker(&scratch).await?, Some(3), "the next batch caught up");
    scratch.cleanup().await
}

async fn ready(prefix: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    seed_lineage(scratch.pool(), CHAIN, 3).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    seed_completed_extent(scratch.pool(), 3).await?;
    Ok(scratch)
}

async fn redo_project(scratch: &ScratchDatabase) -> Result<()> {
    redo_project_with(scratch, FamilySettings::default()).await
}

async fn redo_project_with(scratch: &ScratchDatabase, families: FamilySettings) -> Result<()> {
    PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families)),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "families-runner",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?
    .redo(
        &chain_config()?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, 3)?,
        CancellationToken::new(),
    )
    .await?;
    Ok(())
}

async fn project_state(scratch: &ScratchDatabase) -> Result<(String, Option<i64>, bool)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

async fn marker(scratch: &ScratchDatabase) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(scratch.pool())
    .await?
    .flatten())
}

async fn seed_completed_extent(pool: &sqlx::PgPool, head: i64) -> Result<()> {
    let hash = format!("{CHAIN}-block-{head}");
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2, current_block_hash = $3,
             target_block_number = $2, target_block_hash = $3,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0, $2, $3, $3, $4)",
    )
    .bind(CHAIN)
    .bind(head + 1)
    .bind(head)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn chain_config() -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        CHAIN,
        vec![SourceConfig::new(
            CHAIN,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}
