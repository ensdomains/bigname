//! Phase-state behaviour under the split content hashes (`docs/storage.md` § Interpretation
//! replay): Ingest, Verify and Interpret rows record the interpreter content hash, Project and Live
//! rows the projection content hash. A projection-only change needs one full-range Project redo
//! and leaves Interpret alone. An interpreter change runs the full-range Interpret redo, whose
//! stamped Project redo adopts the projection content hash.
#[allow(dead_code)]
mod support;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use phase_runner::{
    INTERPRETER_CONTENT_HASH, PROJECTION_CONTENT_HASH,
    capacity::{CapacityFuture, CapacityGuard, CapacityMeasurement, CapacityProbe},
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseFuture, PhaseName, PhaseSet,
        RedoAttemptFence, RunMode,
    },
    runner::{PhaseRunner, RedoPhase},
    state::{PhaseStore, StartDisposition},
};
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

const PRIOR_INTERPRETER: &str = "keccak256:prior-interpreter";
const PRIOR_PROJECTION: &str = "keccak256:prior-projection";
const PRIOR_COMBINED: &str = "keccak256:prior-combined";

// Row 1: Interpret records this binary's interpreter content hash, Project another projection
// content hash. Interpret keeps running; Project needs one full-range Project redo, which the
// runner never starts by itself, and that redo adopts the projection content hash without an
// Interpret redo or a watch-set attestation.
#[tokio::test]
async fn projection_only_rotation_redoes_project_alone() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_projection_only").await?;
    let chain_id = "projection-only";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());

    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 5).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "Interpret is current, so it runs"
    );
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 9).await?;
    let interpret_before = phase_row(scratch.pool(), chain_id, PhaseName::Interpret).await?;

    let error = normal_start_error(&store, chain_id, PhaseName::Project).await?;
    assert_project_redo_named(&error, chain_id, "project", PRIOR_PROJECTION);
    let runner = runner(&scratch, PhaseSet::loopback(), "projection-only-runner")?;
    let partial = redo(&runner, chain_id, PhaseName::Project, 0, 1)
        .await
        .expect_err("a partial Project redo cannot adopt the projection content hash");
    assert_redo_range_refused(&partial, chain_id, 0, 1);

    redo(&runner, chain_id, PhaseName::Project, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    assert_eq!(
        phase_row(scratch.pool(), chain_id, PhaseName::Interpret).await?,
        interpret_before,
        "the Project redo leaves the Interpret row alone"
    );
    assert_eq!(attestations(scratch.pool(), chain_id).await?, 0);
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Project, 5).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Project, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    scratch.cleanup().await
}

// Row 2: both rows record another binary's hashes. As before the split, the full-range Interpret
// redo starts the rotation and its stamped Project redo adopts the projection content hash.
#[tokio::test]
async fn interpreter_rotation_unchanged() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_interpreter").await?;
    let chain_id = "interpreter-rotation";
    seed(
        scratch.pool(),
        chain_id,
        PRIOR_INTERPRETER,
        PRIOR_PROJECTION,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let error = normal_start_error(&store, chain_id, PhaseName::Project).await?;
    assert_interpreter_rotation_named(&error, PRIOR_INTERPRETER);

    let runner = runner(
        &scratch,
        PhaseSet::loopback(),
        "interpreter-rotation-runner",
    )?;
    let project = redo(&runner, chain_id, PhaseName::Project, 0, 9)
        .await
        .expect_err("a Project redo waits for the Interpret redo");
    assert_interpreter_rotation_named(&project, PRIOR_INTERPRETER);
    redo(&runner, chain_id, PhaseName::Interpret, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    scratch.cleanup().await
}

// Row 3: a release that changes both sides, from rows that record one combined hash, is
// discharged by one Interpret redo and the Project redo it stamps.
#[tokio::test]
async fn both_rotations_discharged_by_one_pair() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_both").await?;
    let chain_id = "both-rotations";
    seed(scratch.pool(), chain_id, PRIOR_COMBINED, PRIOR_COMBINED).await?;
    let runner = runner(&scratch, PhaseSet::loopback(), "both-rotations-runner")?;
    redo(&runner, chain_id, PhaseName::Interpret, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Project, 5).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Project, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "no second redo is owed"
    );
    scratch.cleanup().await
}

// Row 4: an Interpret redo interrupted under this binary's interpreter content hash while Project
// records another projection content hash. Normal Interpret and a Project redo stay refused. The
// resumed Interpret redo keeps its epoch, and its completion stamps the full Project redo.
#[tokio::test]
async fn projection_rotation_during_interrupted_interpret_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_interrupted_interpret").await?;
    let chain_id = "interrupted-interpret";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    leave_redo_active(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        INTERPRETER_CONTENT_HASH,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let normal = store
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("an interrupted Interpret redo refuses a normal start");
    assert_eq!(normal.kind(), ErrorKind::InvalidTransition, "{normal}");

    let runner = runner(
        &scratch,
        PhaseSet::loopback(),
        "interrupted-interpret-runner",
    )?;
    let project = redo(&runner, chain_id, PhaseName::Project, 0, 9)
        .await
        .expect_err("a Project redo waits for the interrupted Interpret redo");
    assert_eq!(project.kind(), ErrorKind::InvalidTransition, "{project}");
    assert!(
        project
            .to_string()
            .contains("while phase interpret is running"),
        "{project}"
    );

    redo(&runner, chain_id, PhaseName::Interpret, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    scratch.cleanup().await
}

// Row 5: a Project redo interrupted under another projection content hash while Interpret is
// current. Its saved progress belongs to the other projection content hash, so the new Project
// redo must cover the full range and rebuilds from the first block.
#[tokio::test]
async fn interrupted_project_redo_of_old_projection_restarts() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_interrupted_project").await?;
    let chain_id = "interrupted-project";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    leave_redo_active(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        PRIOR_PROJECTION,
    )
    .await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let phases = PhaseSet::new(PhaseName::ALL.map(|name| {
        Arc::new(RecordingPhase {
            inner: LoopbackPhase::new(name),
            attempts: Arc::clone(&attempts),
        }) as Arc<dyn Phase>
    }))?;
    let runner = runner(&scratch, phases, "interrupted-project-runner")?;

    let partial = redo(&runner, chain_id, PhaseName::Project, 3, 9)
        .await
        .expect_err("a Project redo short of the full range is refused");
    assert_redo_range_refused(&partial, chain_id, 3, 9);

    redo(&runner, chain_id, PhaseName::Project, 0, 9).await?;
    let attempts = attempts.lock().expect("attempts lock").clone();
    let (fence, resumed_from) = attempts
        .iter()
        .find(|(phase, _, _)| *phase == PhaseName::Project)
        .map(|(_, fence, current)| (*fence, *current))
        .expect("the Project redo ran a batch");
    let fence = fence.expect("a Project redo batch carries its attempt fence");
    assert!(!fence.resumes_interrupted, "{fence:?}");
    assert_eq!(fence.execution_range, BlockRange::new(0, 9)?);
    assert_ne!(
        resumed_from,
        Some(5),
        "the cursor of the other projection's redo is not resumed"
    );
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    scratch.cleanup().await
}

// Row 6: the same interrupted Project redo plus a scoped Interpret redo. No Interpret redo
// supersedes a Project redo while Interpret records this binary's hash, so the Interpret redo is
// refused and names that Project redo, with the interrupted redo's range, to finish first. A
// Project redo short of the full range is then refused with the full range printed (row 5).
#[tokio::test]
async fn scoped_interpret_redo_waits_for_the_stale_project_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_scoped_interpret").await?;
    let chain_id = "scoped-interpret";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    leave_redo_active(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        PRIOR_PROJECTION,
    )
    .await?;
    let before = phase_row(scratch.pool(), chain_id, PhaseName::Project).await?;
    let runner = runner(&scratch, PhaseSet::loopback(), "scoped-interpret-runner")?;

    let error = redo(&runner, chain_id, PhaseName::Interpret, 2, 4)
        .await
        .expect_err("the stale Project redo finishes first");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition, "{error}");
    let message = error.to_string();
    assert!(
        message.contains("while phase project is running"),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "its redo began under projection content hash {PRIOR_PROJECTION}, so finish that \
             Project redo first"
        )),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "phase-runner redo --chain {chain_id} --phase project --from-block 0 --to-block 9"
        )),
        "{message}"
    );
    assert_eq!(
        phase_row(scratch.pool(), chain_id, PhaseName::Project).await?,
        before
    );
    scratch.cleanup().await
}

// While Interpret records another interpreter content hash, a start blocked by a Project redo of
// another projection content hash gets no hint to finish that Project redo: the Interpret redo
// that starts the rotation supersedes it.
#[tokio::test]
async fn a_stale_interpret_row_gets_no_project_redo_hint() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_stale_interpret_hint").await?;
    let chain_id = "stale-interpret-hint";
    seed(
        scratch.pool(),
        chain_id,
        PRIOR_INTERPRETER,
        PRIOR_PROJECTION,
    )
    .await?;
    leave_redo_active(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        PRIOR_PROJECTION,
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 5).await?;
    let error = PhaseStore::new(scratch.pool().clone())
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("a running Project redo blocks a normal Interpret start");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition, "{error}");
    let message = error.to_string();
    assert!(
        message.contains("while phase project is running"),
        "{message}"
    );
    assert!(
        !message.contains("finish that Project redo first"),
        "{message}"
    );
    scratch.cleanup().await
}

// Row 8: a manifest-authority marker on both rows while the projection content hash changed too.
// A Project redo before the attested Interpret redo is refused. That Interpret redo and the one
// Project redo it stamps discharge both.
#[tokio::test]
async fn marker_and_projection_rotation_one_project_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_marker").await?;
    let chain_id = "marker-rotation";
    let generation = "marker-rotation-generation";
    let marker = format!("manifest-authority:{}:{generation}", "b".repeat(64));
    seed(scratch.pool(), chain_id, &marker, &marker).await?;
    let project = redo(
        &runner(&scratch, PhaseSet::loopback(), "marker-project-runner")?,
        chain_id,
        PhaseName::Project,
        0,
        9,
    )
    .await
    .expect_err("the Project redo waits for the attested Interpret redo");
    assert_interpreter_rotation_named(&project, &marker);
    let attested = runner(&scratch, PhaseSet::loopback(), "marker-rotation-runner")?
        .with_watch_set_coverage_attestation(chain_id, generation);
    redo(&attested, chain_id, PhaseName::Interpret, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    assert_eq!(attestations(scratch.pool(), chain_id).await?, 1);
    scratch.cleanup().await
}

// Row 9: the Basenames marker manifest sync writes on the Base Project row alone. Interpret is
// current, so one full-range Project redo adopts the projection content hash with no Interpret
// redo.
#[tokio::test]
async fn base_marker_and_projection_rotation() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_base_marker").await?;
    let chain_id = "base-marker";
    let marker = format!("manifest-authority:{}:base-generation", "c".repeat(64));
    seed(scratch.pool(), chain_id, INTERPRETER_CONTENT_HASH, &marker).await?;
    let interpret_before = phase_row(scratch.pool(), chain_id, PhaseName::Interpret).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let error = normal_start_error(&store, chain_id, PhaseName::Project).await?;
    assert_project_redo_named(&error, chain_id, "project", &marker);

    let runner = runner(&scratch, PhaseSet::loopback(), "base-marker-runner")?;
    redo(&runner, chain_id, PhaseName::Project, 0, 9).await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    assert_eq!(
        phase_row(scratch.pool(), chain_id, PhaseName::Interpret).await?,
        interpret_before
    );
    scratch.cleanup().await
}

// Row 10a: Live writes projected rows, so under a projection-only change it is refused and names
// the Project redo.
#[tokio::test]
async fn live_start_projection_only() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_live_projection").await?;
    let chain_id = "live-projection-only";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let error = normal_start_error(&store, chain_id, PhaseName::Live).await?;
    assert_project_redo_named(&error, chain_id, "live", PRIOR_PROJECTION);
    scratch.cleanup().await
}

// Row 10b: under an interpreter change Live is refused as before the split, naming the Interpret
// redo.
#[tokio::test]
async fn live_start_interpreter_rotation() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_live_interpreter").await?;
    let chain_id = "live-interpreter";
    seed(
        scratch.pool(),
        chain_id,
        PRIOR_INTERPRETER,
        PRIOR_PROJECTION,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let error = normal_start_error(&store, chain_id, PhaseName::Live).await?;
    assert_interpreter_rotation_named(&error, PRIOR_INTERPRETER);
    scratch.cleanup().await
}

// Row 10c: with Interpret on the interpreter content hash and Project on the projection content
// hash, Live starts.
#[tokio::test]
async fn live_start_current() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_live_current").await?;
    let chain_id = "live-current";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Live, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    scratch.cleanup().await
}

// Row 11: rolling back to a binary that differs only in its projection content hash leaves
// Interpret current; Project needs only the full-range Project redo.
#[tokio::test]
async fn rollback_projection_only() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_rollback").await?;
    let chain_id = "rollback-projection";
    let newer_projection = "keccak256:newer-release-projection";
    seed(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        newer_projection,
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 5).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 9).await?;
    let error = normal_start_error(&store, chain_id, PhaseName::Project).await?;
    assert_project_redo_named(&error, chain_id, "project", newer_projection);
    redo(
        &runner(&scratch, PhaseSet::loopback(), "rollback-projection-runner")?,
        chain_id,
        PhaseName::Project,
        0,
        9,
    )
    .await?;
    assert_hashes(
        scratch.pool(),
        chain_id,
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    scratch.cleanup().await
}

// Row 12: a database restored from a snapshot another binary derived. Equal hashes need no redo,
// an equal interpreter content hash needs only the Project redo, and another interpreter content
// hash needs the full Interpret redo.
#[tokio::test]
async fn seed_restore_classification() -> Result<()> {
    let scratch = ScratchDatabase::create("content_hash_split_seed_restore").await?;
    let store = PhaseStore::new(scratch.pool().clone());

    seed(
        scratch.pool(),
        "seed-equal",
        INTERPRETER_CONTENT_HASH,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    set_phase_extent(scratch.pool(), "seed-equal", PhaseName::Project, 5).await?;
    assert_eq!(
        store
            .start_phase("seed-equal", PhaseName::Project, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );

    seed(
        scratch.pool(),
        "seed-projection",
        INTERPRETER_CONTENT_HASH,
        PRIOR_PROJECTION,
    )
    .await?;
    let error = normal_start_error(&store, "seed-projection", PhaseName::Project).await?;
    assert_project_redo_named(&error, "seed-projection", "project", PRIOR_PROJECTION);

    seed(
        scratch.pool(),
        "seed-interpreter",
        PRIOR_INTERPRETER,
        PROJECTION_CONTENT_HASH,
    )
    .await?;
    let error = normal_start_error(&store, "seed-interpreter", PhaseName::Project).await?;
    assert_interpreter_rotation_named(&error, PRIOR_INTERPRETER);
    scratch.cleanup().await
}

/// The refusal a stale Project row gives: it names the full range and the Project redo command,
/// in the words release tooling reads to rerun with the printed range.
fn assert_project_redo_named(error: &RunnerError, chain_id: &str, phase: &str, recorded: &str) {
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch, "{error}");
    let message = error.to_string();
    for expected in [
        format!("refusing derived writes for chain {chain_id} phase {phase}"),
        format!(
            "binary projection content hash {PROJECTION_CONTENT_HASH} differs from recorded \
             {recorded} on phase project"
        ),
        "redo the full range 0..=9 of phase project alone".to_owned(),
        format!("phase-runner redo --chain {chain_id} --phase project --from-block 0 --to-block 9"),
    ] {
        assert!(message.contains(&expected), "{expected:?} not in {message}");
    }
}

/// The refusal of a Project redo short of the full range, in the exact words release tooling
/// parses to rerun with the printed range.
fn assert_redo_range_refused(error: &RunnerError, chain_id: &str, from: i64, to: i64) {
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch, "{error}");
    let expected = format!(
        "cannot adopt a new interpretation-input hash for chain {chain_id} phase project with \
         range {from}..={to}; redo the full range 0..=9"
    );
    assert!(
        error.to_string().contains(&expected),
        "{expected:?} not in {error}"
    );
}

fn assert_interpreter_rotation_named(error: &RunnerError, recorded: &str) {
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch, "{error}");
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "binary interpretation-input hash {INTERPRETER_CONTENT_HASH} differs from recorded \
             {recorded} on phase interpret; start the interpreter content-hash rotation"
        )),
        "{message}"
    );
}

async fn normal_start_error(
    store: &PhaseStore,
    chain_id: &str,
    phase: PhaseName,
) -> Result<RunnerError> {
    if phase == PhaseName::Project {
        set_phase_extent(store.pool(), chain_id, PhaseName::Project, 5).await?;
    }
    let error = store
        .start_phase(chain_id, phase, &RunMode::Normal)
        .await
        .expect_err("the start is refused");
    if phase == PhaseName::Project {
        set_phase_extent(store.pool(), chain_id, PhaseName::Project, 9).await?;
    }
    Ok(error)
}

async fn redo(
    runner: &PhaseRunner,
    chain_id: &str,
    phase: PhaseName,
    from: i64,
    to: i64,
) -> RunnerResult<()> {
    runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(phase),
            BlockRange::new(from, to)?,
            CancellationToken::new(),
        )
        .await
}

/// Ingest completed and Interpret and Project completed through block 9 under the
/// given hashes, with the canonical head and Ingest handoff at block 9.
async fn seed(pool: &sqlx::PgPool, chain_id: &str, interpret: &str, project: &str) -> Result<()> {
    PhaseStore::new(pool.clone())
        .initialize_chain(chain_id)
        .await?;
    mark_completed(pool, chain_id, PhaseName::Ingest, None).await?;
    mark_completed(pool, chain_id, PhaseName::Interpret, Some(interpret)).await?;
    mark_completed(pool, chain_id, PhaseName::Project, Some(project)).await?;
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        set_phase_extent(pool, chain_id, phase, 9).await?;
    }
    sqlx::query(
        "UPDATE chain_phase_state
         SET live_handoff_block_number = 9, live_handoff_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-block-9"))
    .execute(pool)
    .await?;
    seed_lineage(pool, chain_id, 9).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number, target_block_number,
             last_processed_block_number, last_processed_block_hash
         )
         VALUES ($1, 'source', 'test', 'ethereum_head', 0, 10, 9, 9, $2)",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-block-9"))
    .execute(pool)
    .await?;
    let head = BlockMarker::new(9, format!("{chain_id}-block-9"))?;
    publish_heads(
        pool,
        chain_id,
        &HeadMarkers {
            latest: head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    Ok(())
}

/// The row as a redo leaves it when its binary stops mid-way without recording the stop.
async fn leave_redo_active(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    hash: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_attempt_generation = 74,
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0, redo_to_block_number = 9,
             redo_current_block_number = 5, redo_current_block_hash = $4,
             redo_target_block_number = 9, redo_target_block_hash = $5,
             input_content_hash = $3,
             last_error = CASE WHEN $2 = 'project'
                 THEN 'required downstream redo active: interpret redo completed'
             END,
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(hash)
    .bind(format!("{chain_id}-block-5"))
    .bind(format!("{chain_id}-block-9"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_hashes(
    pool: &sqlx::PgPool,
    chain_id: &str,
    interpret: &str,
    project: &str,
) -> Result<()> {
    let rows: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, phase_status::text, redo_in_progress, input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            (
                "interpret".to_owned(),
                "completed".to_owned(),
                false,
                Some(interpret.to_owned())
            ),
            (
                "project".to_owned(),
                "completed".to_owned(),
                false,
                Some(project.to_owned())
            ),
        ]
    );
    Ok(())
}

type PhaseRow = (String, Option<i64>, Option<String>, bool, Option<String>);

async fn phase_row(pool: &sqlx::PgPool, chain_id: &str, phase: PhaseName) -> Result<PhaseRow> {
    Ok(sqlx::query_as(
        "SELECT phase_status::text, current_block_number, input_content_hash, redo_in_progress,
                last_error
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_one(pool)
    .await?)
}

async fn attestations(pool: &sqlx::PgPool, chain_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM manifest_authority_attestations WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?)
}

async fn mark_completed(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    content_hash: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', input_content_hash = $3,
             current_block_number = COALESCE(current_block_number, 100),
             current_block_hash = COALESCE(current_block_hash, 'test-extent-block-100'),
             target_block_number = COALESCE(target_block_number, 100),
             target_block_hash = COALESCE(target_block_hash, 'test-extent-block-100'),
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(content_hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_phase_extent(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    through: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = $3, current_block_hash = $4,
             target_block_number = $3, target_block_hash = $4
         WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(through)
    .bind(format!("{chain_id}-block-{through}"))
    .execute(pool)
    .await?;
    Ok(())
}

fn runner(scratch: &ScratchDatabase, phases: PhaseSet, instance_id: &str) -> Result<PhaseRunner> {
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::new(CapacityConfig::default(), Arc::new(AlwaysAvailable)),
        instance_id,
        TimingConfig {
            initial_backoff: std::time::Duration::from_millis(1),
            maximum_backoff: std::time::Duration::from_millis(4),
            live_poll_interval: std::time::Duration::from_millis(1),
        },
    )?)
}

fn chain(chain_id: &str) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "test",
            SeedBasis::EthereumHead,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

struct AlwaysAvailable;

impl CapacityProbe for AlwaysAvailable {
    fn measure<'a>(
        &'a self,
        _pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async {
            Ok(CapacityMeasurement {
                database_size_bytes: 0,
                free_disk_bytes: u64::MAX,
            })
        })
    }
}

type Attempt = (PhaseName, Option<RedoAttemptFence>, Option<i64>);

/// A loopback phase that records each batch's attempt fence and resume cursor.
struct RecordingPhase {
    inner: LoopbackPhase,
    attempts: Arc<Mutex<Vec<Attempt>>>,
}

impl Phase for RecordingPhase {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        self.attempts.lock().expect("attempts lock").push((
            context.phase,
            context.redo_attempt,
            context.resume.current.as_ref().map(|marker| marker.number),
        ));
        self.inner.run_batch(context)
    }
}
