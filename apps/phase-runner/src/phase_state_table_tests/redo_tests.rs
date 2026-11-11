//! `docs/phase-state.md` § Transitions, an explicit redo's life: begin (`redo_state.rs:211`),
//! progress (`state_persistence.rs:168`), failure (`redo_failure.rs:20`), the widened completion
//! (`redo_completion.rs:137`), finish (`redo_state.rs:494`) and the supersede of a Project redo
//! (`redo_supersede.rs:97`).
use anyhow::{Result, bail, ensure};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    ACTIVE, EARLIER_FAILURE, PENDING, RANGE, Row, Scratch, State, completed, expect_state,
    expect_unchanged, failed, idle, marker, operator_redo, paused, read, redo, refusal,
    required_active, required_pending, running, seed, unchanged_except, with_redo_progress, writes,
};
use crate::{
    error::RunnerError,
    phase::{
        BlockRange, PhaseName, PhaseProgress, RedoAttemptFence, RunMode, SourceProgress,
        VerificationLevel,
    },
    redo_completion::CompletionCoverage,
    redo_state::{RedoOutcome, RedoSession},
    state::PhaseStore,
    transitions::{lock_chain_phase_state, row_for},
};

/// A range inside the cursor (60) of the running, paused and failed rows.
const LOW: BlockRange = BlockRange { from: 10, to: 50 };
const INTERRUPTED: &str = "phase was interrupted before redo; resume the normal phase";

/// The other rows a Verify redo needs: Project completed is its prerequisite.
fn verify_chain(chain: &str, verify: Row) -> Vec<Row> {
    vec![
        completed(chain, PhaseName::Ingest),
        completed(chain, PhaseName::Interpret),
        completed(chain, PhaseName::Project),
        verify,
    ]
}

async fn begin(
    store: &PhaseStore,
    chain: &str,
    phase: PhaseName,
    range: BlockRange,
) -> crate::error::RunnerResult<RedoSession> {
    store
        .begin_redo(
            chain,
            phase,
            &RunMode::Redo(range),
            &[],
            None,
            "phase-state-table",
            false,
        )
        .await
}

fn verify_completion() -> PhaseProgress {
    PhaseProgress {
        current: Some(marker(100)),
        target: Some(marker(100)),
        verification_level: Some(VerificationLevel::QuickSynced),
        ..PhaseProgress::default()
    }
}

fn range(row: &Value) -> (Value, Value) {
    (
        row["redo_from_block_number"].clone(),
        row["redo_to_block_number"].clone(),
    )
}

async fn fingerprint(pool: &PgPool, chain: &str) -> Result<String> {
    let sql = format!("SELECT {}", crate::redo_manifest_authority::FINGERPRINT_SQL);
    Ok(sqlx::query_scalar::<_, String>(&sql)
        .bind(chain)
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
async fn redo_begin_saves_or_keeps_the_lifecycle() -> Result<()> {
    let scratch = Scratch::create("phase_state_redo_begin").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let verify = PhaseName::Verify;

    // From a normal row the begin saves the lifecycle, clears progress and starts now.
    for (chain, row, range_begun) in [
        (
            "begin-completed",
            completed("begin-completed", verify),
            RANGE,
        ),
        ("begin-failed", failed("begin-failed", verify), LOW),
        ("begin-running", running("begin-running", verify), LOW),
        ("begin-paused", paused("begin-paused", verify), LOW),
    ] {
        seed(pool, chain, &verify_chain(chain, row)).await?;
        let before = read(pool, chain, verify).await?;
        begin(&store, chain, verify, range_begun).await?;
        let after = read(pool, chain, verify).await?;
        expect_state(&after, State::OperatorRedo)?;
        ensure!(after["phase_status"] == json!("running"));
        ensure!(
            after["redo_previous_phase_status"] == before["phase_status"],
            "{chain}"
        );
        ensure!(
            after["redo_previous_last_error"] == before["last_error"],
            "{chain}"
        );
        ensure!(
            after["redo_previous_started_at"] == before["started_at"],
            "{chain}"
        );
        ensure!(
            after["redo_previous_finished_at"] == before["finished_at"],
            "{chain}"
        );
        ensure!(after["last_error"].is_null() && after["started_at"] != before["started_at"]);
        ensure!(range(&after) == (json!(range_begun.from), json!(range_begun.to)));
        ensure!(after["redo_attempt_generation"] == json!(1));
        unchanged_except(&before, &after, writes::REDO_BEGIN)?;
    }

    // The same range resumes: progress, start time and the saved lifecycle stay.
    let chain = "begin-resume";
    seed(
        pool,
        chain,
        &verify_chain(chain, with_redo_progress(operator_redo(chain, verify))),
    )
    .await?;
    let before = read(pool, chain, verify).await?;
    begin(&store, chain, verify, RANGE).await?;
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["redo_current_block_number"] == json!(40));
    ensure!(after["started_at"] == before["started_at"]);
    ensure!(after["redo_attempt_generation"] == json!(2));
    unchanged_except(&before, &after, writes::REDO_BEGIN)?;

    // A covering range restarts the redo but keeps the saved lifecycle.
    let chain = "begin-cover";
    seed(
        pool,
        chain,
        &verify_chain(chain, with_redo_progress(operator_redo(chain, verify))),
    )
    .await?;
    let before = read(pool, chain, verify).await?;
    begin(&store, chain, verify, BlockRange { from: 10, to: 95 }).await?;
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["redo_current_block_number"].is_null());
    ensure!(after["started_at"] != before["started_at"]);
    ensure!(after["redo_previous_phase_status"] == before["redo_previous_phase_status"]);
    ensure!(range(&after) == (json!(10), json!(95)));
    unchanged_except(&before, &after, writes::REDO_BEGIN)?;

    // The operator begin over a required stamp takes it over: pending becomes active, active
    // stays active.
    for (chain, row, owner) in [
        (
            "begin-pending",
            required_pending("begin-pending", verify),
            ACTIVE,
        ),
        (
            "begin-active",
            required_active("begin-active", verify),
            ACTIVE,
        ),
    ] {
        seed(pool, chain, &verify_chain(chain, row)).await?;
        let before = read(pool, chain, verify).await?;
        begin(&store, chain, verify, RANGE).await?;
        let after = read(pool, chain, verify).await?;
        expect_state(&after, State::RequiredActive)?;
        ensure!(after["last_error"] == json!(owner));
        ensure!(after["started_at"] == before["started_at"], "{chain}");
        unchanged_except(&before, &after, writes::REDO_BEGIN)?;
    }

    // Refused before the statement: a range that does not cover the interrupted one, and a row
    // with no cursor.
    for (chain, row, range_begun) in [
        (
            "begin-narrow",
            operator_redo("begin-narrow", verify),
            BlockRange { from: 20, to: 90 },
        ),
        ("begin-idle", idle("begin-idle", verify), RANGE),
    ] {
        seed(pool, chain, &verify_chain(chain, row)).await?;
        let before = read(pool, chain, verify).await?;
        refusal(begin(&store, chain, verify, range_begun).await, chain)?;
        expect_unchanged(&before, &read(pool, chain, verify).await?)?;
    }

    // An Ingest begin binds the manifest fingerprint and stamps Interpret and Verify.
    let ingest = PhaseName::Ingest;
    let chain = "begin-ingest";
    seed(pool, chain, &verify_chain(chain, completed(chain, verify))).await?;
    let before = read(pool, chain, ingest).await?;
    begin(&store, chain, ingest, RANGE).await?;
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["redo_manifest_authority_fingerprint"] == json!(fingerprint(pool, chain).await?));
    unchanged_except(&before, &after, writes::REDO_BEGIN)?;
    for phase in [PhaseName::Interpret, verify] {
        let dependent = read(pool, chain, phase).await?;
        expect_state(&dependent, State::RequiredPending)?;
        ensure!(
            dependent["last_error"]
                == json!("required downstream redo: ingest redo may change the raw-fact extent")
        );
    }

    // Over a required Ingest stamp, which binds no fingerprint, the begin restarts it.
    let chain = "begin-ingest-pending";
    seed(
        pool,
        chain,
        &verify_chain(chain, completed(chain, verify))
            .into_iter()
            .map(|row| {
                if row.get("phase_name") == json!("ingest") {
                    with_redo_progress(required_pending(chain, ingest))
                } else {
                    row
                }
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    let before = read(pool, chain, ingest).await?;
    begin(&store, chain, ingest, RANGE).await?;
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::RequiredActive)?;
    ensure!(after["redo_current_block_number"].is_null());
    ensure!(!after["redo_manifest_authority_fingerprint"].is_null());
    ensure!(after["started_at"] != before["started_at"]);
    unchanged_except(&before, &after, writes::REDO_BEGIN)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_finish_restores_the_saved_lifecycle() -> Result<()> {
    let scratch = Scratch::create("phase_state_redo_finish").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let verify = PhaseName::Verify;

    // Completed comes back completed and finishes now; failed comes back with its error and
    // finish time; a run the redo interrupted comes back failed.
    for (chain, row, range_begun, status, error) in [
        (
            "finish-completed",
            completed("finish-completed", verify),
            RANGE,
            "completed",
            Value::Null,
        ),
        (
            "finish-failed",
            failed("finish-failed", verify),
            LOW,
            "failed",
            json!(EARLIER_FAILURE),
        ),
        (
            "finish-running",
            running("finish-running", verify),
            LOW,
            "failed",
            json!(INTERRUPTED),
        ),
    ] {
        seed(pool, chain, &verify_chain(chain, row)).await?;
        let normal = read(pool, chain, verify).await?;
        let session = begin(&store, chain, verify, range_begun).await?;
        let begun = read(pool, chain, verify).await?;
        let mut connection = pool.acquire().await?;
        store
            .finish_redo(
                &mut connection,
                chain,
                verify,
                session,
                RedoOutcome::Completed(&verify_completion()),
            )
            .await?;
        drop(connection);
        let after = read(pool, chain, verify).await?;
        ensure!(!state_is_redo(&after), "{chain}: {after}");
        ensure!(after["phase_status"] == json!(status), "{chain}: {after}");
        ensure!(after["last_error"] == error, "{chain}: {after}");
        ensure!(
            after["started_at"] == normal["started_at"],
            "{chain}: {after}"
        );
        ensure!(after["verification_level"] == json!("quick_synced"));
        ensure!(after["redo_attempt_generation"] == json!(1));
        if status == "failed" && error == json!(EARLIER_FAILURE) {
            ensure!(
                after["finished_at"] == normal["finished_at"],
                "{chain}: {after}"
            );
        } else {
            ensure!(
                after["finished_at"] != normal["finished_at"],
                "{chain}: {after}"
            );
        }
        unchanged_except(&begun, &after, writes::REDO_FINISH)?;
    }

    // A stamp inside the range after the last progress write: overtaken. The redo stays in
    // progress with the refusal as its error.
    let chain = "finish-overtaken";
    seed(pool, chain, &verify_chain(chain, completed(chain, verify))).await?;
    let session = begin(&store, chain, verify, RANGE).await?;
    let mut transaction = pool.begin().await?;
    crate::redo_stamp::stamp_required_in_transaction(
        &mut transaction,
        chain,
        verify,
        BlockRange { from: 20, to: 30 },
        "test overtake",
    )
    .await?;
    transaction.commit().await?;
    let mut connection = pool.acquire().await?;
    let result = store
        .finish_redo(
            &mut connection,
            chain,
            verify,
            session,
            RedoOutcome::Completed(&verify_completion()),
        )
        .await;
    drop(connection);
    let error = refusal(result, chain)?;
    ensure!(error.to_string().contains("was overtaken"), "{error}");
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(
        after["last_error"]
            .as_str()
            .is_some_and(|text| text.contains("was overtaken"))
    );

    // A stamp that widens the range: the finish keeps the redo and clears its progress.
    let chain = "finish-widened";
    seed(pool, chain, &verify_chain(chain, completed(chain, verify))).await?;
    let session = begin(&store, chain, verify, RANGE).await?;
    let mut transaction = pool.begin().await?;
    crate::redo_stamp::stamp_required_in_transaction(
        &mut transaction,
        chain,
        verify,
        BlockRange {
            from: 50,
            to: i64::MAX,
        },
        "test widen",
    )
    .await?;
    transaction.commit().await?;
    let mut connection = pool.acquire().await?;
    store
        .finish_redo(
            &mut connection,
            chain,
            verify,
            session,
            RedoOutcome::Completed(&verify_completion()),
        )
        .await?;
    drop(connection);
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(range(&after) == (json!(RANGE.from), json!(100)));

    // A failed attempt records its error and stays in progress.
    let chain = "finish-failed-attempt";
    seed(pool, chain, &verify_chain(chain, completed(chain, verify))).await?;
    let session = begin(&store, chain, verify, RANGE).await?;
    let begun = read(pool, chain, verify).await?;
    let mut connection = pool.acquire().await?;
    store
        .finish_redo(
            &mut connection,
            chain,
            verify,
            session,
            RedoOutcome::Failed(&RunnerError::transient("rpc timeout")),
        )
        .await?;
    drop(connection);
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["last_error"] == json!("rpc timeout"));
    unchanged_except(&begun, &after, writes::REDO_FAILURE)?;
    scratch.cleanup().await
}

fn state_is_redo(row: &Value) -> bool {
    row["redo_in_progress"].as_bool() == Some(true)
}

#[tokio::test]
async fn redo_failure_keeps_the_marker_and_returns_required_work_to_pending() -> Result<()> {
    let scratch = Scratch::create("phase_state_redo_failure").await?;
    let pool = scratch.pool();
    let interpret = PhaseName::Interpret;
    let attempt = RunnerError::transient("rpc timeout");
    let suffixed = format!("{PENDING}; last attempt failed: rpc timeout");

    for (chain, row, error, expected) in [
        (
            "failure-operator",
            with_redo_progress(operator_redo("failure-operator", interpret)),
            "rpc timeout".to_owned(),
            State::OperatorRedo,
        ),
        (
            "failure-active",
            required_active("failure-active", interpret),
            suffixed.clone(),
            State::RequiredPending,
        ),
        (
            "failure-pending",
            required_pending("failure-pending", interpret),
            suffixed.clone(),
            State::RequiredPending,
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, interpret).await?;
        let mut connection = pool.acquire().await?;
        crate::redo_failure::record(&mut connection, chain, interpret, &attempt).await?;
        drop(connection);
        let after = read(pool, chain, interpret).await?;
        expect_state(&after, expected)?;
        ensure!(after["last_error"] == json!(error), "{chain}: {after}");
        ensure!(after["redo_current_block_number"] == before["redo_current_block_number"]);
        unchanged_except(&before, &after, writes::REDO_FAILURE)?;
    }

    // A boundary divergence keeps the marker and clears what it loaded.
    let chain = "failure-boundary";
    seed(
        pool,
        chain,
        &[
            with_redo_progress(operator_redo(chain, PhaseName::Ingest)).set(
                "redo_source_boundary_markers",
                json!({"rpc": {"number": 40, "hash": "h40"}}),
            ),
        ],
    )
    .await?;
    let before = read(pool, chain, PhaseName::Ingest).await?;
    let divergence = RunnerError::transient(format!(
        "{}test",
        bigname_ingest::REDO_BOUNDARY_DIVERGENCE_PREFIX
    ));
    let mut connection = pool.acquire().await?;
    crate::redo_failure::record(&mut connection, chain, PhaseName::Ingest, &divergence).await?;
    drop(connection);
    let after = read(pool, chain, PhaseName::Ingest).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["redo_current_block_number"].is_null());
    ensure!(after["redo_source_boundary_markers"].is_null());
    unchanged_except(&before, &after, writes::REDO_FAILURE)?;

    let chain = "failure-normal";
    seed(pool, chain, &[completed(chain, interpret)]).await?;
    let before = read(pool, chain, interpret).await?;
    let mut connection = pool.acquire().await?;
    let result = crate::redo_failure::record(&mut connection, chain, interpret, &attempt).await;
    drop(connection);
    refusal(result, chain)?;
    expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn widened_completion_clears_progress_and_returns_required_work_to_pending() -> Result<()> {
    let scratch = Scratch::create("phase_state_widened").await?;
    let pool = scratch.pool();
    let interpret = PhaseName::Interpret;

    for (chain, row, error) in [
        (
            "widened-operator",
            with_redo_progress(operator_redo("widened-operator", interpret)),
            Value::Null,
        ),
        (
            "widened-active",
            with_redo_progress(required_active("widened-active", interpret)),
            json!(format!(
                "{PENDING}; range widened; rerun the full persisted range"
            )),
        ),
    ] {
        seed(pool, chain, &[row.set("redo_to_block_number", json!(100))]).await?;
        let before = read(pool, chain, interpret).await?;
        let mut transaction = pool.begin().await?;
        let coverage = crate::redo_completion::lock_completion_coverage(
            &mut transaction,
            chain,
            interpret,
            RANGE,
            1,
            false,
        )
        .await?;
        transaction.commit().await?;
        match coverage {
            CompletionCoverage::Widened(persisted) => {
                ensure!(persisted == BlockRange { from: 10, to: 100 });
            }
            _ => bail!("{chain}: expected a widened completion"),
        }
        let after = read(pool, chain, interpret).await?;
        ensure!(after["last_error"] == error, "{chain}: {after}");
        ensure!(after["redo_current_block_number"].is_null());
        unchanged_except(&before, &after, writes::WIDENED)?;
    }

    // The exact range writes nothing; a narrower one or no marker refuses.
    let chain = "widened-exact";
    seed(pool, chain, &[operator_redo(chain, interpret)]).await?;
    let before = read(pool, chain, interpret).await?;
    let mut transaction = pool.begin().await?;
    let coverage = crate::redo_completion::lock_completion_coverage(
        &mut transaction,
        chain,
        interpret,
        RANGE,
        1,
        false,
    )
    .await?;
    transaction.commit().await?;
    ensure!(matches!(coverage, CompletionCoverage::Exact));
    expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    for (chain, row) in [
        (
            "widened-narrower",
            operator_redo("widened-narrower", interpret).set("redo_from_block_number", json!(20)),
        ),
        ("widened-normal", completed("widened-normal", interpret)),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, interpret).await?;
        let mut transaction = pool.begin().await?;
        let result = crate::redo_completion::lock_completion_coverage(
            &mut transaction,
            chain,
            interpret,
            RANGE,
            1,
            false,
        )
        .await;
        transaction.rollback().await?;
        if result.is_ok() {
            bail!("{chain}: expected a refusal");
        }
        expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn supersede_restores_a_project_redo_like_a_finish() -> Result<()> {
    let scratch = Scratch::create("phase_state_supersede").await?;
    let pool = scratch.pool();
    let project = PhaseName::Project;

    // The restore keeps the saved finish time where a finish of a completed row writes now
    // (`redo_state.rs:526`), and keeps the cursor and content hash a finish rewrites.
    for (chain, row, status, error) in [
        (
            "supersede-completed",
            completed("supersede-completed", project),
            "completed",
            Value::Null,
        ),
        (
            "supersede-running",
            running("supersede-running", project),
            "failed",
            json!(INTERRUPTED),
        ),
    ] {
        let row = with_redo_progress(redo(row, "redo", None))
            .set("redo_attempt_generation", json!(3))
            .set("input_content_hash", json!("old-projection-hash"));
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, project).await?;
        let mut transaction = pool.begin().await?;
        let rows = lock_chain_phase_state(&mut transaction, chain).await?;
        crate::redo_supersede::supersede(&mut transaction, chain, row_for(&rows, project)?).await?;
        transaction.commit().await?;
        let after = read(pool, chain, project).await?;
        ensure!(!state_is_redo(&after), "{chain}: {after}");
        ensure!(after["phase_status"] == json!(status) && after["last_error"] == error);
        ensure!(after["started_at"] == before["redo_previous_started_at"]);
        if status == "completed" {
            ensure!(after["finished_at"] == before["redo_previous_finished_at"]);
        }
        ensure!(after["redo_attempt_generation"] == json!(3));
        unchanged_except(&before, &after, writes::SUPERSEDE)?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_progress_is_fenced_to_the_attempt_and_owner() -> Result<()> {
    let scratch = Scratch::create("phase_state_redo_progress").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let fence = |generation| RedoAttemptFence {
        generation,
        execution_range: RANGE,
        resumes_interrupted: false,
    };
    let progress = PhaseProgress {
        current: Some(marker(40)),
        target: Some(marker(90)),
        ..PhaseProgress::default()
    };

    let chain = "progress-verify";
    seed(pool, chain, &[operator_redo(chain, PhaseName::Verify)]).await?;
    let before = read(pool, chain, PhaseName::Verify).await?;
    store
        .record_progress(
            chain,
            PhaseName::Verify,
            &RunMode::Redo(RANGE),
            Some(fence(1)),
            &progress,
        )
        .await?;
    let after = read(pool, chain, PhaseName::Verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["redo_current_block_number"] == json!(40));
    unchanged_except(&before, &after, writes::REDO_PROGRESS)?;
    let before = after;
    let result = store
        .record_progress(
            chain,
            PhaseName::Verify,
            &RunMode::Redo(RANGE),
            Some(fence(0)),
            &progress,
        )
        .await;
    refusal(result, "progress-verify stale generation")?;
    expect_unchanged(&before, &read(pool, chain, PhaseName::Verify).await?)?;

    // Ingest progress also needs the manifest fingerprint and a runner that owns a required redo.
    let ingest = PhaseName::Ingest;
    let ingest_progress = PhaseProgress {
        source_progress: vec![SourceProgress {
            source_key: "rpc".to_owned(),
            current: None,
            target: None,
            redo_loaded_boundary: Some(marker(40)),
        }],
        ..progress.clone()
    };
    for (chain, build, accepted) in [
        (
            "progress-ingest-active",
            required_active as fn(&str, PhaseName) -> Row,
            true,
        ),
        ("progress-ingest-pending", required_pending, false),
        ("progress-ingest-operator", operator_redo, true),
    ] {
        let bound = fingerprint(pool, chain).await?;
        seed(
            pool,
            chain,
            &[build(chain, ingest).set("redo_manifest_authority_fingerprint", json!(bound))],
        )
        .await?;
        let before = read(pool, chain, ingest).await?;
        let result = store
            .record_progress(
                chain,
                ingest,
                &RunMode::Redo(RANGE),
                Some(fence(1)),
                &ingest_progress,
            )
            .await;
        let after = read(pool, chain, ingest).await?;
        if accepted {
            result?;
            ensure!(
                after["redo_source_boundary_markers"]
                    == json!({"rpc": {"number": 40, "hash": "h40"}}),
                "{chain}: {after}"
            );
            unchanged_except(&before, &after, writes::REDO_PROGRESS)?;
        } else {
            refusal(result, chain)?;
            expect_unchanged(&before, &after)?;
        }
    }

    // A redo that binds no fingerprint records no Ingest progress.
    let chain = "progress-ingest-unbound";
    seed(pool, chain, &[operator_redo(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    let result = store
        .record_progress(
            chain,
            ingest,
            &RunMode::Redo(RANGE),
            Some(fence(1)),
            &ingest_progress,
        )
        .await;
    refusal(result, chain)?;
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    scratch.cleanup().await
}

/// Canonical lineage through `through` and one ingest source from block 10, which an Interpret
/// redo's raw-data check requires (`redo_presence.rs:88-224`).
async fn seed_interpret_inputs(pool: &PgPool, chain: &str, through: i64) -> Result<()> {
    for number in 0..=through {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                                        block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(chain)
        .bind(format!("h{number}"))
        .bind((number > 0).then(|| format!("h{}", number - 1)))
        .bind(number)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO ingest_cursors (chain_id, source_key, source_kind, seed_basis,
                                     start_block_number, next_block_number)
         VALUES ($1, 'source', 'test', 'ethereum_head', 10, $2)",
    )
    .bind(chain)
    .bind(through + 1)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn an_interpret_redo_runs_to_its_cursor_and_a_recompute_runs_its_request() -> Result<()> {
    let scratch = Scratch::create("phase_state_interpret_range").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let interpret = PhaseName::Interpret;
    let request = BlockRange { from: 10, to: 90 };

    for (chain, mode, recorded) in [
        (
            "interpret-redo-range",
            RunMode::Redo(request),
            BlockRange { from: 10, to: 100 },
        ),
        (
            "interpret-recompute-range",
            RunMode::RecomputeFlags(request),
            request,
        ),
    ] {
        seed(
            pool,
            chain,
            &[
                completed(chain, PhaseName::Ingest),
                completed(chain, interpret),
                completed(chain, PhaseName::Project),
            ],
        )
        .await?;
        seed_interpret_inputs(pool, chain, 100).await?;
        let source = crate::config::SourceConfig::new(
            chain,
            "source",
            "test",
            crate::config::SeedBasis::EthereumHead,
            10,
            "http://source.invalid",
        )?;
        let before = read(pool, chain, interpret).await?;
        store
            .begin_redo(
                chain,
                interpret,
                &mode,
                std::slice::from_ref(&source),
                None,
                "phase-state-table",
                false,
            )
            .await?;
        let after = read(pool, chain, interpret).await?;
        ensure!(
            range(&after) == (json!(recorded.from), json!(recorded.to)),
            "{chain}: {after}"
        );
        unchanged_except(&before, &after, writes::REDO_BEGIN)?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn an_ingest_redo_finish_restores_the_handoff_from_before_the_redo() -> Result<()> {
    let scratch = Scratch::create("phase_state_ingest_handoff").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let ingest = PhaseName::Ingest;
    let chain = "ingest-handoff";
    seed(
        pool,
        chain,
        &verify_chain(chain, completed(chain, PhaseName::Verify)),
    )
    .await?;
    let normal = read(pool, chain, ingest).await?;
    ensure!(normal["live_handoff_block_number"] == json!(100));
    let session = begin(&store, chain, ingest, RANGE).await?;
    let begun = read(pool, chain, ingest).await?;
    let mut connection = pool.acquire().await?;
    store
        .finish_redo(
            &mut connection,
            chain,
            ingest,
            session,
            RedoOutcome::Completed(&PhaseProgress {
                current: Some(marker(90)),
                target: Some(marker(90)),
                live_handoff: Some(marker(80)),
                ..PhaseProgress::default()
            }),
        )
        .await?;
    drop(connection);
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::Completed)?;
    ensure!(after["live_handoff_block_number"] == normal["live_handoff_block_number"]);
    ensure!(after["live_handoff_block_hash"] == normal["live_handoff_block_hash"]);
    ensure!(after["current_block_number"] == normal["current_block_number"]);
    unchanged_except(&begun, &after, writes::REDO_FINISH)?;
    scratch.cleanup().await
}
