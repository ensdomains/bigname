//! `docs/phase-state.md` § Transitions, the normal lifecycle: `state.rs`, `state_persistence.rs`,
//! `state_settlement.rs` and `completed_phase_recovery.rs`.
use anyhow::{Result, ensure};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;

use super::{
    EARLIER_FAILURE, PENDING, Scratch, State, completed, expect_check_refusal, expect_state,
    expect_unchanged, failed, idle, marker, operator_redo, paused, read, redo, refusal,
    retained_completion, running, seed, unchanged_except, writes,
};
use crate::{
    error::{COMPLETED_VALIDATION_FAILURE_PREFIX, ErrorKind},
    phase::{PhaseName, PhaseProgress, RunMode},
    state::StartDisposition,
};

const LIVE_MISMATCH_PREFIX: &str =
    "live phase stopped because verify reported a verification mismatch: ";

fn ingest_completion() -> PhaseProgress {
    PhaseProgress {
        current: Some(marker(120)),
        target: Some(marker(120)),
        live_handoff: Some(marker(120)),
        ..PhaseProgress::default()
    }
}

#[tokio::test]
async fn initialize_inserts_idle_rows_and_keeps_existing_ones() -> Result<()> {
    let scratch = Scratch::create("phase_state_initialize").await?;
    let (pool, store) = (scratch.pool(), scratch.store());

    store.initialize_chain("init-fresh").await?;
    for phase in PhaseName::ALL {
        let row = read(pool, "init-fresh", phase).await?;
        expect_state(&row, State::Idle)?;
        ensure!(row["started_at"].is_null() && row["current_block_number"].is_null());
    }

    let chain = "init-existing";
    seed(pool, chain, &[completed(chain, PhaseName::Ingest)]).await?;
    let before = read(pool, chain, PhaseName::Ingest).await?;
    store.initialize_chain(chain).await?;
    expect_unchanged(&before, &read(pool, chain, PhaseName::Ingest).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn start_phase_moves_each_from_state() -> Result<()> {
    let scratch = Scratch::create("phase_state_start").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let ingest = PhaseName::Ingest;
    let normal = RunMode::Normal;

    // Idle: a fresh start clears the cursor and the Ingest handoff.
    let chain = "start-idle";
    seed(pool, chain, &[]).await?;
    let before = read(pool, chain, ingest).await?;
    ensure!(store.start_phase(chain, ingest, &normal).await? == StartDisposition::Started);
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::Running)?;
    ensure!(after["current_block_number"].is_null() && !after["started_at"].is_null());
    ensure!(after["input_content_hash"] == json!(crate::INTERPRETER_CONTENT_HASH));
    unchanged_except(&before, &after, writes::START)?;

    // Running, paused and failed resume at their cursor.
    for (chain, row) in [
        ("start-running", running("start-running", ingest)),
        ("start-paused", paused("start-paused", ingest)),
        ("start-failed", failed("start-failed", ingest)),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        ensure!(store.start_phase(chain, ingest, &normal).await? == StartDisposition::Started);
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, State::Running)?;
        ensure!(
            after["current_block_number"] == json!(60),
            "{chain}: {after}"
        );
        ensure!(after["last_error"].is_null() && after["started_at"] != before["started_at"]);
        unchanged_except(&before, &after, writes::START)?;
    }

    // A complete Ingest completion is not restarted; the statement does not run.
    let chain = "start-completed";
    seed(pool, chain, &[completed(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    ensure!(store.start_phase(chain, ingest, &normal).await? == StartDisposition::AlreadyCompleted);
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;

    // An Ingest completion without its handoff restarts (`state.rs:143`).
    let chain = "start-completed-incomplete";
    seed(
        pool,
        chain,
        &[completed(chain, ingest)
            .set("live_handoff_block_number", json!(null))
            .set("live_handoff_block_hash", json!(null))],
    )
    .await?;
    let before = read(pool, chain, ingest).await?;
    ensure!(store.start_phase(chain, ingest, &normal).await? == StartDisposition::Started);
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::Running)?;
    ensure!(after["current_block_number"] == json!(100));
    unchanged_except(&before, &after, writes::START)?;

    // A retained completion is recovered without the statement (`state.rs:168`).
    let chain = "start-retained";
    seed(pool, chain, &[retained_completion(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    ensure!(
        store.start_phase(chain, ingest, &normal).await? == StartDisposition::RecoveringCompleted
    );
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;

    // A redo marker refuses a normal start (`transitions.rs:166`).
    let chain = "start-operator-redo";
    seed(pool, chain, &[operator_redo(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    let error = refusal(store.start_phase(chain, ingest, &normal).await, chain)?;
    ensure!(error.kind() == ErrorKind::InvalidTransition, "{error}");
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;

    // A settled Verify completion restarts and carries the settled marker into the run.
    let chain = "start-settled-verify";
    seed(
        pool,
        chain,
        &[
            completed(chain, PhaseName::Project),
            completed(chain, PhaseName::Verify)
                .set("settled_while_unconfigured", json!(true))
                .set("verification_level", json!(null)),
        ],
    )
    .await?;
    let before = read(pool, chain, PhaseName::Verify).await?;
    ensure!(
        store.start_phase(chain, PhaseName::Verify, &normal).await? == StartDisposition::Started
    );
    let after = read(pool, chain, PhaseName::Verify).await?;
    expect_state(&after, State::Running)?;
    ensure!(
        after["settled_while_unconfigured"] == json!(true),
        "{after}"
    );
    unchanged_except(&before, &after, writes::START)?;

    // Live always restarts a completion.
    let chain = "start-live-completed";
    seed(
        pool,
        chain,
        &[
            completed(chain, PhaseName::Interpret),
            completed(chain, PhaseName::Project),
            completed(chain, PhaseName::Live),
        ],
    )
    .await?;
    let before = read(pool, chain, PhaseName::Live).await?;
    ensure!(store.start_phase(chain, PhaseName::Live, &normal).await? == StartDisposition::Started);
    let after = read(pool, chain, PhaseName::Live).await?;
    expect_state(&after, State::Running)?;
    unchanged_except(&before, &after, writes::START)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn pause_and_resume_change_only_the_status() -> Result<()> {
    let scratch = Scratch::create("phase_state_pause").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let ingest = PhaseName::Ingest;

    let chain = "pause-running";
    seed(pool, chain, &[running(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    store.pause_phase(chain, ingest).await?;
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::Paused)?;
    unchanged_except(&before, &after, writes::STATUS)?;

    let chain = "resume-paused";
    seed(pool, chain, &[paused(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    store.resume_phase(chain, ingest).await?;
    let after = read(pool, chain, ingest).await?;
    expect_state(&after, State::Running)?;
    unchanged_except(&before, &after, writes::STATUS)?;

    // A redo pauses for capacity like a normal run.
    let chain = "pause-operator-redo";
    seed(pool, chain, &[operator_redo(chain, PhaseName::Interpret)]).await?;
    let before = read(pool, chain, PhaseName::Interpret).await?;
    store.pause_phase(chain, PhaseName::Interpret).await?;
    let after = read(pool, chain, PhaseName::Interpret).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(after["phase_status"] == json!("paused"));
    unchanged_except(&before, &after, writes::STATUS)?;

    // `can_transition_to` refuses these before the statement runs.
    for (chain, row, pause) in [
        ("pause-idle", idle("pause-idle", ingest), true),
        (
            "pause-completed",
            completed("pause-completed", ingest),
            true,
        ),
        (
            "resume-completed",
            completed("resume-completed", ingest),
            false,
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        let result = if pause {
            store.pause_phase(chain, ingest).await
        } else {
            store.resume_phase(chain, ingest).await
        };
        let error = refusal(result, chain)?;
        ensure!(
            error.kind() == ErrorKind::InvalidTransition,
            "{chain}: {error}"
        );
        expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    }

    // Pinned finding: `can_transition_to` allows failed -> running, and the statement writes only
    // the status, so the CHECKs refuse the running row that keeps its error and finish time.
    let chain = "resume-failed";
    seed(pool, chain, &[failed(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    let error = refusal(store.resume_phase(chain, ingest).await, chain)?;
    expect_check_refusal(&error.to_string(), "chain_phase_state_check7")?;
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn completion_and_failure_record_the_end_of_a_run() -> Result<()> {
    let scratch = Scratch::create("phase_state_complete").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let ingest = PhaseName::Ingest;

    for (chain, row) in [
        ("complete-running", running("complete-running", ingest)),
        ("complete-paused", paused("complete-paused", ingest)),
        (
            "complete-settled",
            running("complete-settled", ingest).set("settled_while_unconfigured", json!(true)),
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        let mut connection = pool.acquire().await?;
        store
            .complete_phase_on_connection(&mut connection, chain, ingest, &ingest_completion())
            .await?;
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, State::Completed)?;
        ensure!(
            after["live_handoff_block_number"] == json!(120) && !after["finished_at"].is_null()
        );
        ensure!(
            after["settled_while_unconfigured"].is_null(),
            "{chain}: {after}"
        );
        unchanged_except(&before, &after, writes::COMPLETE)?;
    }

    for (chain, row) in [
        ("fail-running", running("fail-running", ingest)),
        ("fail-paused", paused("fail-paused", ingest)),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        store.fail_phase(chain, ingest, "rpc timeout").await?;
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, State::Failed)?;
        ensure!(after["last_error"] == json!("rpc timeout"));
        unchanged_except(&before, &after, writes::FAIL)?;
    }

    // `can_transition_to` refuses these before the statement runs.
    let chain = "complete-failed";
    seed(pool, chain, &[failed(chain, ingest)]).await?;
    let before = read(pool, chain, ingest).await?;
    let mut connection = pool.acquire().await?;
    let result = store
        .complete_phase_on_connection(&mut connection, chain, ingest, &ingest_completion())
        .await;
    drop(connection);
    let error = refusal(result, chain)?;
    ensure!(error.kind() == ErrorKind::InvalidTransition, "{error}");
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    for (chain, row) in [
        ("fail-idle", idle("fail-idle", ingest)),
        ("fail-completed", completed("fail-completed", ingest)),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        let error = refusal(store.fail_phase(chain, ingest, "x").await, chain)?;
        ensure!(
            error.kind() == ErrorKind::InvalidTransition,
            "{chain}: {error}"
        );
        expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    }

    // Pinned finding (TYR-511): a required redo stamped on a running row leaves the normal run's
    // writer a row it can neither complete nor fail. `can_transition_to` allows both; the CHECKs
    // refuse a completed or failed row with a redo marker.
    let chain = "complete-stamped";
    seed(
        pool,
        chain,
        &[redo(running(chain, ingest), "redo", Some(PENDING))],
    )
    .await?;
    let before = read(pool, chain, ingest).await?;
    let mut connection = pool.acquire().await?;
    let result = store
        .complete_phase_on_connection(&mut connection, chain, ingest, &ingest_completion())
        .await;
    drop(connection);
    let error = refusal(result, chain)?;
    expect_check_refusal(&error.to_string(), "chain_phase_state_check4")?;
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    let error = refusal(store.fail_phase(chain, ingest, "rpc timeout").await, chain)?;
    expect_check_refusal(&error.to_string(), "chain_phase_state_check4")?;
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_progress_has_no_status_guard() -> Result<()> {
    let scratch = Scratch::create("phase_state_progress").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let ingest = PhaseName::Ingest;
    let progress = PhaseProgress {
        current: Some(marker(70)),
        target: Some(marker(100)),
        ..PhaseProgress::default()
    };

    // Running is the from-state the callers guarantee. The other two are pinned findings: the
    // statement has no status or redo guard, so it writes the cursor of an idle row and of a row
    // a required redo was stamped on (TYR-511).
    for (chain, row, expected) in [
        (
            "progress-running",
            running("progress-running", ingest),
            State::Running,
        ),
        ("progress-idle", idle("progress-idle", ingest), State::Idle),
        (
            "progress-stamped",
            redo(running("progress-stamped", ingest), "redo", Some(PENDING)),
            State::RequiredPending,
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, ingest).await?;
        store
            .record_progress(chain, ingest, &RunMode::Normal, None, &progress)
            .await?;
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, expected)?;
        ensure!(
            after["current_block_number"] == json!(70),
            "{chain}: {after}"
        );
        unchanged_except(&before, &after, writes::PROGRESS)?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn live_verification_mismatch_fails_live_from_any_status() -> Result<()> {
    let scratch = Scratch::create("phase_state_live_mismatch").await?;
    let pool = scratch.pool();
    let live = PhaseName::Live;

    // `can_transition_to` has no idle -> failed, completed -> failed or failed -> failed; this
    // statement does not consult it.
    for (chain, row) in [
        ("mismatch-idle", idle("mismatch-idle", live)),
        ("mismatch-running", running("mismatch-running", live)),
        ("mismatch-paused", paused("mismatch-paused", live)),
        ("mismatch-completed", completed("mismatch-completed", live)),
        ("mismatch-failed", failed("mismatch-failed", live)),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, live).await?;
        ensure!(
            crate::state_persistence::record_live_verification_mismatch(pool, chain, "hash")
                .await?
        );
        let after = read(pool, chain, live).await?;
        expect_state(&after, State::Failed)?;
        ensure!(after["last_error"] == json!(format!("{LIVE_MISMATCH_PREFIX}hash")));
        ensure!(!after["started_at"].is_null(), "{chain}: {after}");
        unchanged_except(&before, &after, writes::LIVE_MISMATCH)?;
    }

    // The CHECKs accept a Live redo marker; the statement's guard leaves such a row alone.
    let chain = "mismatch-live-redo";
    seed(pool, chain, &[operator_redo(chain, live)]).await?;
    let before = read(pool, chain, live).await?;
    ensure!(!crate::state_persistence::record_live_verification_mismatch(pool, chain, "x").await?);
    expect_unchanged(&before, &read(pool, chain, live).await?)?;
    scratch.cleanup().await
}

async fn updated_at(scratch: &Scratch, chain: &str, phase: PhaseName) -> Result<OffsetDateTime> {
    Ok(sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT updated_at FROM chain_phase_state WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain)
    .bind(phase.as_str())
    .fetch_one(scratch.pool())
    .await?)
}

#[tokio::test]
async fn settlement_completes_an_active_normal_row() -> Result<()> {
    let scratch = Scratch::create("phase_state_settle").await?;
    let (pool, store) = (scratch.pool(), scratch.store());

    // A stopped Live run completes (`complete_stopped_live`).
    let chain = "settle-live";
    seed(pool, chain, &[running(chain, PhaseName::Live)]).await?;
    let before = read(pool, chain, PhaseName::Live).await?;
    let mut connection = pool.acquire().await?;
    ensure!(
        store
            .complete_stopped_phase(&mut connection, chain, PhaseName::Live)
            .await?
    );
    drop(connection);
    let after = read(pool, chain, PhaseName::Live).await?;
    expect_state(&after, State::Completed)?;
    unchanged_except(&before, &after, writes::SETTLE)?;

    // An unconfigured chain's row completes whatever its cursor, marked settled; Ingest loses its
    // handoff, which makes the next start restart it.
    for (chain, row) in [
        (
            "settle-unconfigured-ingest",
            running("settle-unconfigured-ingest", PhaseName::Ingest)
                .set("live_handoff_block_number", json!(60))
                .set("live_handoff_block_hash", json!("h60")),
        ),
        (
            "settle-unconfigured-verify",
            running("settle-unconfigured-verify", PhaseName::Verify),
        ),
        (
            "settle-unconfigured-paused",
            paused("settle-unconfigured-paused", PhaseName::Interpret),
        ),
    ] {
        seed(pool, chain, std::slice::from_ref(&row)).await?;
        let phase: PhaseName = row.get("phase_name").as_str().unwrap_or_default().parse()?;
        let before = read(pool, chain, phase).await?;
        let observed = updated_at(&scratch, chain, phase).await?;
        let mut connection = pool.acquire().await?;
        ensure!(
            store
                .complete_unconfigured_phase(&mut connection, chain, phase, observed)
                .await?,
            "{chain}"
        );
        drop(connection);
        let after = read(pool, chain, phase).await?;
        expect_state(&after, State::CompletedUnconfigured)?;
        ensure!(after["live_handoff_block_number"].is_null() && after["last_error"].is_null());
        unchanged_except(&before, &after, writes::SETTLE)?;
    }

    // The guard leaves a redo, a completed row and a row changed since it was observed.
    for (chain, row, phase) in [
        (
            "settle-redo",
            operator_redo("settle-redo", PhaseName::Interpret),
            PhaseName::Interpret,
        ),
        (
            "settle-completed",
            completed("settle-completed", PhaseName::Interpret),
            PhaseName::Interpret,
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, phase).await?;
        let mut connection = pool.acquire().await?;
        ensure!(
            !store
                .complete_stopped_phase(&mut connection, chain, phase)
                .await?
        );
        drop(connection);
        expect_unchanged(&before, &read(pool, chain, phase).await?)?;
    }
    let chain = "settle-moved";
    seed(pool, chain, &[running(chain, PhaseName::Ingest)]).await?;
    let before = read(pool, chain, PhaseName::Ingest).await?;
    let observed: OffsetDateTime = sqlx::query_scalar(
        "SELECT updated_at + interval '1 second' FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    let mut connection = pool.acquire().await?;
    ensure!(
        !store
            .complete_unconfigured_phase(&mut connection, chain, PhaseName::Ingest, observed)
            .await?
    );
    drop(connection);
    expect_unchanged(&before, &read(pool, chain, PhaseName::Ingest).await?)?;

    // The settled marker is cleared only on a completed row.
    let verify = PhaseName::Verify;
    let chain = "clear-completed";
    seed(
        pool,
        chain,
        &[completed(chain, verify).set("settled_while_unconfigured", json!(true))],
    )
    .await?;
    let before = read(pool, chain, verify).await?;
    let mut connection = pool.acquire().await?;
    store
        .clear_unconfigured_settlement(&mut connection, chain, verify)
        .await?;
    drop(connection);
    let after = read(pool, chain, verify).await?;
    expect_state(&after, State::Completed)?;
    unchanged_except(&before, &after, writes::CLEAR_SETTLEMENT)?;
    let chain = "clear-running";
    seed(
        pool,
        chain,
        &[running(chain, verify).set("settled_while_unconfigured", json!(true))],
    )
    .await?;
    let before = read(pool, chain, verify).await?;
    let mut connection = pool.acquire().await?;
    store
        .clear_unconfigured_settlement(&mut connection, chain, verify)
        .await?;
    drop(connection);
    expect_unchanged(&before, &read(pool, chain, verify).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_validation_fails_and_revalidation_completes() -> Result<()> {
    let scratch = Scratch::create("phase_state_revalidate").await?;
    let (pool, store) = (scratch.pool(), scratch.store());
    let message = format!("{COMPLETED_VALIDATION_FAILURE_PREFIX}source changed");

    // completed -> failed, which `can_transition_to` has no arm for.
    for phase in [PhaseName::Ingest, PhaseName::Verify] {
        let chain = format!("validation-{phase}");
        seed(pool, &chain, &[completed(&chain, phase)]).await?;
        let before = read(pool, &chain, phase).await?;
        store
            .fail_completed_validation(&chain, phase, &message)
            .await?;
        let after = read(pool, &chain, phase).await?;
        expect_state(&after, State::RetainedCompletion)?;
        unchanged_except(&before, &after, writes::FAIL)?;
    }
    for (chain, row) in [
        (
            "validation-running",
            running("validation-running", PhaseName::Ingest),
        ),
        (
            "validation-interpret",
            completed("validation-interpret", PhaseName::Interpret),
        ),
    ] {
        seed(pool, chain, std::slice::from_ref(&row)).await?;
        let phase: PhaseName = row.get("phase_name").as_str().unwrap_or_default().parse()?;
        let before = read(pool, chain, phase).await?;
        refusal(
            store
                .fail_completed_validation(chain, phase, &message)
                .await,
            chain,
        )?;
        expect_unchanged(&before, &read(pool, chain, phase).await?)?;
    }

    // failed -> running -> completed in one transaction. Verify's retained completion needs no
    // validation prefix, only its extent and level (`completed_phase_recovery.rs:55`).
    for (chain, row) in [
        (
            "revalidated-ingest",
            retained_completion("revalidated-ingest", PhaseName::Ingest),
        ),
        (
            "revalidated-verify-settled",
            retained_completion("revalidated-verify-settled", PhaseName::Verify)
                .set("settled_while_unconfigured", json!(true)),
        ),
        (
            "revalidated-verify-ordinary-failure",
            retained_completion("revalidated-verify-ordinary-failure", PhaseName::Verify)
                .set("last_error", json!(EARLIER_FAILURE)),
        ),
    ] {
        seed(pool, chain, std::slice::from_ref(&row)).await?;
        let phase: PhaseName = row.get("phase_name").as_str().unwrap_or_default().parse()?;
        let before = read(pool, chain, phase).await?;
        let mut connection = pool.acquire().await?;
        store
            .complete_revalidated_phase(&mut connection, chain, phase)
            .await?;
        drop(connection);
        let after = read(pool, chain, phase).await?;
        expect_state(&after, State::Completed)?;
        ensure!(after["last_error"].is_null() && after["started_at"] != before["started_at"]);
        unchanged_except(&before, &after, writes::REVALIDATED)?;
    }
    for (chain, row) in [
        (
            "revalidated-ingest-ordinary-failure",
            retained_completion("revalidated-ingest-ordinary-failure", PhaseName::Ingest)
                .set("last_error", json!(EARLIER_FAILURE)),
        ),
        (
            "revalidated-completed",
            completed("revalidated-completed", PhaseName::Ingest),
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let before = read(pool, chain, PhaseName::Ingest).await?;
        let mut connection = pool.acquire().await?;
        let result = store
            .complete_revalidated_phase(&mut connection, chain, PhaseName::Ingest)
            .await;
        drop(connection);
        refusal(result, chain)?;
        expect_unchanged(&before, &read(pool, chain, PhaseName::Ingest).await?)?;
    }
    scratch.cleanup().await
}
