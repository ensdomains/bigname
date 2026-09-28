//! The Project batch with the publication switch on (TYR-36 step 7b): the owned key family loop is
//! the batch, the served engine and hydrator do not run, and the batch's progress is the family
//! marker. Every served table here carries a statement trigger that refuses any write, so a batch
//! that reached the served engine fails, even one that would change no row.
#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use bigname_storage::publication_source::with_serve_from_families;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::ErrorKind,
    heads::{BlockMarker, HeadMarkers},
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseName, PhaseResume,
        PhaseSet, RunMode,
    },
    project_phase::{FamilySettings, ProjectPhase},
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

const CHAIN: &str = "families-runner-switch";
const HEAD: i64 = 30;

/// The tables the served Project batch publishes; step 7c drops them.
const SERVED_TABLES: &[&str] = &[
    "name_current",
    "children_current",
    "record_inventory_current",
    "resolver_current",
    "permissions_current",
    "permissions_current_resource_summary",
    "account_permission_state_current",
    "address_names_current",
    "address_records_current",
    "primary_names_current",
];

// A normal batch with the switch on: the families rebuild to the head in one run, the batch
// completes with the family marker as its progress, and no statement reached a served table.
// With the switch off the same batch runs the served engine, which the triggers refuse.
#[tokio::test]
async fn a_project_batch_under_the_switch_writes_no_served_row_and_advances_the_marker_to_the_head()
-> Result<()> {
    let scratch = ready("families_switch_batch").await?;
    refuse_served_writes(&scratch).await?;
    let head = head_marker(&scratch, HEAD).await?;
    let project = ProjectPhase::new(scratch.pool().clone());

    let served = with_serve_from_families(false, project.run_batch(context(&head, None))).await;
    let refused = served
        .expect_err("with the switch off the served engine runs")
        .to_string();
    ensure!(refused.contains("served table written"), "{refused}");
    ensure!(marker(&scratch).await?.is_none(), "no family run followed");

    let outcome = with_serve_from_families(true, project.run_batch(context(&head, None))).await?;
    let PhaseBatchOutcome::Complete(progress) = outcome else {
        anyhow::bail!("the family batch did not complete: {outcome:?}");
    };
    ensure!(
        progress.current.as_ref() == Some(&head) && progress.target.as_ref() == Some(&head),
        "the progress is the family marker at the head: {progress:?}"
    );
    ensure!(
        marker_row(&scratch).await? == (Some(HEAD), Some(head.hash.clone()), "live".to_owned()),
        "the family marker is live at the head"
    );
    ensure!(
        !project.has_after_progress_work(CHAIN),
        "nothing follows the recorded progress"
    );

    // The next batch follows from the recorded marker in normal mode.
    let next = head_marker(&scratch, HEAD).await?;
    let outcome =
        with_serve_from_families(true, project.run_batch(context(&next, Some(&head)))).await?;
    ensure!(
        matches!(outcome, PhaseBatchOutcome::Complete(_)),
        "{outcome:?}"
    );
    scratch.cleanup().await
}

// A family rebuild longer than one run's budget: each batch spends its budget and answers
// Continue with the marker it reached, and the next batch resumes the rebuild rather than
// starting it again, until the families reach the head.
#[tokio::test]
async fn a_budgeted_family_batch_continues_the_rebuild_from_its_marker() -> Result<()> {
    let scratch = ready("families_switch_budget").await?;
    refuse_served_writes(&scratch).await?;
    let head = head_marker(&scratch, HEAD).await?;
    let project = ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
        max_blocks_per_run: 10,
        ..FamilySettings::default()
    });
    let mut resume: Option<BlockMarker> = None;
    let mut continued = 0;
    loop {
        let outcome =
            with_serve_from_families(true, project.run_batch(context(&head, resume.as_ref())))
                .await?;
        let progress = outcome.progress().clone();
        ensure!(progress.target.as_ref() == Some(&head), "{progress:?}");
        resume = progress.current.clone();
        match outcome {
            PhaseBatchOutcome::Continue(_) => {
                continued += 1;
                ensure!(continued < 10, "the rebuild never finished");
                ensure!(
                    marker(&scratch).await? == resume.as_ref().map(|marker| marker.number),
                    "the progress is the family marker"
                );
            }
            PhaseBatchOutcome::Complete(_) => break,
            PhaseBatchOutcome::Idle(_) => anyhow::bail!("a family batch is never idle"),
        }
    }
    ensure!(
        continued >= 2,
        "the rebuild spanned several budgets: {continued}"
    );
    ensure!(resume.as_ref() == Some(&head));
    ensure!(marker(&scratch).await? == Some(HEAD));
    scratch.cleanup().await
}

// The one-shot redo under the switch, through the runner, with a budget shorter than the redo:
// the redo's batches continue until the families reach the Project row's block, the redo
// completes there, and no served table was written.
#[tokio::test]
async fn a_one_shot_redo_under_the_switch_replays_the_families_and_writes_no_served_row()
-> Result<()> {
    let scratch = ready("families_switch_redo").await?;
    refuse_served_writes(&scratch).await?;
    let settings = FamilySettings {
        max_blocks_per_run: 10,
        retry_family_failures: false,
        ..FamilySettings::default()
    };
    with_serve_from_families(true, redo(&scratch, settings)).await?;
    ensure!(
        project_state(&scratch).await? == ("completed".into(), Some(HEAD), false),
        "the redo completed at the head"
    );
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(
        repair_completed(&scratch).await?,
        "the repair record completed"
    );
    // A second redo undoes and replays from the live families.
    with_serve_from_families(true, redo(&scratch, settings)).await?;
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(repair_completed(&scratch).await?);
    scratch.cleanup().await
}

// Switching back off after the switch ran Project: the first switch-on batch records the block
// the served tables stood at, a switch-off normal run refuses the chain with the redo that
// replays the gap, and that redo deletes the record so the next run starts.
#[tokio::test]
async fn switching_off_refuses_served_tables_the_switch_left_behind_until_a_redo_replays_them()
-> Result<()> {
    const SERVED: i64 = 20;
    let scratch = ready("families_switch_served_stop").await?;
    let head = head_marker(&scratch, HEAD).await?;
    let served = head_marker(&scratch, SERVED).await?;
    set_project_block(&scratch, &served).await?;
    let project = ProjectPhase::new(scratch.pool().clone());
    with_serve_from_families(true, project.run_batch(context(&head, None))).await?;
    // The runner records the family batch's progress on the Project row.
    set_project_block(&scratch, &head).await?;
    ensure!(served_stop(&scratch).await? == Some(Some(SERVED)));

    let restarted = ProjectPhase::new(scratch.pool().clone());
    let refused = with_serve_from_families(false, restarted.run_batch(context(&head, Some(&head))))
        .await
        .expect_err("the served tables are behind the Project row");
    ensure!(
        refused.kind() == ErrorKind::Configuration && !refused.is_retryable(),
        "{refused:?}"
    );
    ensure!(
        refused.to_string().contains(&format!(
            "--phase project --from-block {} --to-block {HEAD}",
            SERVED + 1
        )),
        "the error names the redo: {refused}"
    );
    ensure!(served_stop(&scratch).await? == Some(Some(SERVED)));

    with_serve_from_families(
        false,
        redo(
            &scratch,
            FamilySettings {
                retry_family_failures: false,
                ..FamilySettings::default()
            },
        ),
    )
    .await?;
    ensure!(
        served_stop(&scratch).await?.is_none(),
        "the covering redo deleted the record"
    );

    // A record at the Project row's block skipped nothing: a normal run deletes it and runs.
    sqlx::query("INSERT INTO project_served_stop (chain_id, block_number) VALUES ($1, $2)")
        .bind(CHAIN)
        .bind(HEAD)
        .execute(scratch.pool())
        .await?;
    let restarted = ProjectPhase::new(scratch.pool().clone());
    with_serve_from_families(false, restarted.run_batch(context(&head, Some(&head)))).await?;
    ensure!(served_stop(&scratch).await?.is_none());
    scratch.cleanup().await
}

async fn set_project_block(scratch: &ScratchDatabase, block: &BlockMarker) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_number = $2, current_block_hash = $3
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .bind(block.number)
    .bind(&block.hash)
    .execute(scratch.pool())
    .await?;
    Ok(())
}

async fn served_stop(scratch: &ScratchDatabase) -> Result<Option<Option<i64>>> {
    Ok(
        sqlx::query_scalar("SELECT block_number FROM project_served_stop WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_optional(scratch.pool())
            .await?,
    )
}

async fn refuse_served_writes(scratch: &ScratchDatabase) -> Result<()> {
    sqlx::query(
        "CREATE FUNCTION refuse_served_write() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'served table written: %', TG_TABLE_NAME; END $$",
    )
    .execute(scratch.pool())
    .await?;
    for table in SERVED_TABLES {
        sqlx::query(&format!(
            "CREATE TRIGGER refuse_served_write BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
             ON {table} FOR EACH STATEMENT EXECUTE FUNCTION refuse_served_write()"
        ))
        .execute(scratch.pool())
        .await?;
    }
    Ok(())
}

async fn redo(scratch: &ScratchDatabase, settings: FamilySettings) -> Result<()> {
    let project: Arc<dyn Phase> =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(settings));
    // A batch that reached the served engine is refused by the triggers and retried forever as
    // a transient failure, so the redo is bounded.
    let stop = CancellationToken::new();
    let deadline = {
        let stop = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            stop.cancel();
        })
    };
    let result = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            project,
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "families-runner-switch",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?
    .redo(
        &chain_config()?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, HEAD)?,
        stop.clone(),
    )
    .await;
    deadline.abort();
    ensure!(
        !stop.is_cancelled(),
        "the redo did not finish within two minutes"
    );
    result?;
    Ok(())
}

fn context(head: &BlockMarker, resume: Option<&BlockMarker>) -> PhaseContext {
    PhaseContext {
        chain_id: CHAIN.to_owned(),
        phase: PhaseName::Project,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Vec::new().into(),
        available_heads: Some(HeadMarkers {
            latest: head.clone(),
            safe: None,
            finalized: None,
        }),
        live_handoff: None,
        resume: PhaseResume {
            current: resume.cloned(),
            ..PhaseResume::default()
        },
    }
}

async fn head_marker(scratch: &ScratchDatabase, number: i64) -> Result<BlockMarker> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(number)
    .fetch_one(scratch.pool())
    .await?;
    Ok(BlockMarker::new(number, hash)?)
}

async fn repair_completed(scratch: &ScratchDatabase) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project_repair_record record
             JOIN chain_phase_state project
               ON project.chain_id = record.chain_id AND project.phase_name = 'project'
             JOIN project_family_marker marker ON marker.chain_id = record.chain_id
             WHERE record.chain_id = $1 AND record.state = 'complete'
               AND record.attempt = project.redo_attempt_generation
               AND record.completed_sequence = marker.sequence
               AND marker.current_block_number = project.current_block_number
               AND marker.current_block_hash = project.current_block_hash)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

async fn marker_row(scratch: &ScratchDatabase) -> Result<(Option<i64>, Option<String>, String)> {
    Ok(sqlx::query_as(
        "SELECT current_block_number, current_block_hash, state
         FROM project_family_marker WHERE chain_id = $1",
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

async fn project_state(scratch: &ScratchDatabase) -> Result<(String, Option<i64>, bool)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

/// Lineage, the other phases complete through the head, Project at the head, and one event per
/// block so a family rebuild has thirty blocks of work.
async fn ready(prefix: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    seed_lineage(scratch.pool(), CHAIN, HEAD).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    let hash = format!("{CHAIN}-block-{HEAD}");
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
    .bind(HEAD)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0, $2, $3, $3, $4)",
    )
    .bind(CHAIN)
    .bind(HEAD + 1)
    .bind(HEAD)
    .bind(hash)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         SELECT 'budget:' || block, 'ens', 'PreimageObserved', 'budget_probe', 1, $1, block,
                $1 || '-block-' || block, 'ens_v2_registry_resource_surface', 'canonical', '{}'
         FROM generate_series(1, $2) block",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .execute(scratch.pool())
    .await?;
    Ok(scratch)
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
