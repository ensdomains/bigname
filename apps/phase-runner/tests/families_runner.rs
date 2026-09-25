#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    phase::{
        AfterProgressFuture, BlockRange, CompletedPhaseFuture, LoopbackPhase, Phase,
        PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseSet, RunMode,
    },
    project_phase::FamilySettings,
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
    assert_eq!(
        marker(&scratch).await?,
        None,
        "the families skipped the batch"
    );

    redo_project(&scratch).await?;
    assert_eq!(marker(&scratch).await?, Some(3), "the next batch caught up");
    scratch.cleanup().await
}

// A one-shot redo whose family rebuild needs more blocks than one run's budget: nothing calls the
// family loop after the command returns, so the command finishes the rebuild before it does.
#[tokio::test]
async fn a_one_shot_redo_finishes_a_family_rebuild_longer_than_one_budget() -> Result<()> {
    let scratch = ready_through("families_runner_budget", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    redo_project_through(
        &scratch,
        FamilySettings {
            max_blocks_per_run: 10,
            finish_each_batch: true,
            ..FamilySettings::default()
        },
        30,
    )
    .await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(
        marker(&scratch).await?,
        Some(30),
        "the family rebuild finished before the command returned"
    );
    scratch.cleanup().await
}

// A one-shot redo whose family runs stop short of the served marker, here on a failing block,
// reports it: the served redo is recorded, the command fails, and rerunning it repairs the
// families.
#[tokio::test]
async fn a_one_shot_redo_whose_families_stop_short_fails_and_a_rerun_repairs_them() -> Result<()> {
    let scratch = ready_through("families_runner_short", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    sqlx::query(
        "CREATE FUNCTION refuse_block_15() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.current_block_number = 15 THEN RAISE EXCEPTION 'injected family failure'; END IF;
             RETURN NEW;
         END $$",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER refuse_block_15 BEFORE INSERT OR UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION refuse_block_15()",
    )
    .execute(scratch.pool())
    .await?;
    let families = FamilySettings {
        max_blocks_per_run: 10,
        finish_each_batch: true,
        ..FamilySettings::default()
    };
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    let error = redo_with_phase(&scratch, project, 30)
        .await
        .expect_err("the families stopped short of the served marker");
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(marker(&scratch).await?, Some(14), "block 15 was refused");
    let message = error.to_string();
    assert!(message.contains("family repair incomplete"), "{message}");
    assert!(
        message.contains("families at block 14 (") && message.contains("served marker block 30 ("),
        "{message}"
    );

    sqlx::query("DROP TRIGGER refuse_block_15 ON project_family_marker")
        .execute(scratch.pool())
        .await?;
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
    scratch.cleanup().await
}

// A stop that arrives while the final served batch is being recorded abandons the family run
// before it starts. The redo still records the served batch, and the command fails with the
// families reported short; an uncancelled rerun repairs them.
#[tokio::test]
async fn a_stop_during_the_final_served_batch_fails_the_one_shot_redo_and_a_rerun_repairs_it()
-> Result<()> {
    let scratch = ready_through("families_runner_stop", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    let families = FamilySettings {
        max_blocks_per_run: 10,
        finish_each_batch: true,
        ..FamilySettings::default()
    };
    let stop = CancellationToken::new();
    let project = Arc::new(StopAfterFinalBatch {
        inner: ProjectPhase::new(scratch.pool().clone()).with_family_settings(families),
        stop: stop.clone(),
    });
    let error = redo_with_phase_and_stop(&scratch, project, 30, stop)
        .await
        .expect_err("the stop abandoned the family run");
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false),
        "the served batch is recorded"
    );
    assert_eq!(
        marker(&scratch).await?,
        None,
        "the family run never started"
    );
    let message = error.to_string();
    assert!(message.contains("family repair incomplete"), "{message}");
    assert!(
        message.contains("served marker block 30 (")
            && message.contains("the family marker is unavailable"),
        "{message}"
    );

    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
    scratch.cleanup().await
}

/// Project, with a stop raised as soon as its final batch returns, so the stop is pending while
/// the runner records that batch and wins the select against the family run.
struct StopAfterFinalBatch {
    inner: ProjectPhase,
    stop: CancellationToken,
}

impl Phase for StopAfterFinalBatch {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn preflight(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        mode: &RunMode,
    ) -> phase_runner::error::RunnerResult<()> {
        self.inner.preflight(chain_id, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await;
            if matches!(outcome, Ok(PhaseBatchOutcome::Complete(_))) {
                self.stop.cancel();
            }
            outcome
        })
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn after_redo(&self, chain_id: &str) -> phase_runner::error::RunnerResult<()> {
        self.inner.after_redo(chain_id)
    }

    fn revalidates_completed(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> phase_runner::error::RunnerResult<bool> {
        self.inner.revalidates_completed(chain_id, sources)
    }

    fn revalidate_completed(&self, context: PhaseContext) -> CompletedPhaseFuture<'_> {
        self.inner.revalidate_completed(context)
    }
}

/// One event per block 1 to 30, so a family rebuild has thirty blocks of work.
async fn seed_thirty_blocks_of_work(scratch: &ScratchDatabase) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         SELECT 'budget:' || block, 'ens', 'PreimageObserved', 'budget_probe', 1, $1, block,
                $1 || '-block-' || block, 'ens_v2_registry_resource_surface', 'canonical', '{}'
         FROM generate_series(1, 30) block",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    Ok(())
}

async fn ready(prefix: &str) -> Result<ScratchDatabase> {
    ready_through(prefix, 3).await
}

async fn ready_through(prefix: &str, head: i64) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    seed_lineage(scratch.pool(), CHAIN, head).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    seed_completed_extent(scratch.pool(), head).await?;
    Ok(scratch)
}

async fn redo_project(scratch: &ScratchDatabase) -> Result<()> {
    redo_project_with(scratch, FamilySettings::default()).await
}

async fn redo_project_with(scratch: &ScratchDatabase, families: FamilySettings) -> Result<()> {
    redo_project_through(scratch, families, 3).await
}

async fn redo_project_through(
    scratch: &ScratchDatabase,
    families: FamilySettings,
    head: i64,
) -> Result<()> {
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(scratch, project, head).await
}

async fn redo_with_phase(
    scratch: &ScratchDatabase,
    project: Arc<ProjectPhase>,
    head: i64,
) -> Result<()> {
    redo_with_phase_and_stop(scratch, project, head, CancellationToken::new()).await
}

async fn redo_with_phase_and_stop(
    scratch: &ScratchDatabase,
    project: Arc<dyn Phase>,
    head: i64,
    stop: CancellationToken,
) -> Result<()> {
    PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            project,
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
        BlockRange::new(0, head)?,
        stop,
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
