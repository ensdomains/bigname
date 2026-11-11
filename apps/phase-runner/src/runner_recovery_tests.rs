//! `docs/phase-state.md` § Transitions, start-up recovery: `resolve_stopped_phase`
//! (`runner_recovery.rs:88, :114`), the settler that runs while the phase's advisory lock is held.
use anyhow::{Result, ensure};
use serde_json::{Value, json};

use crate::{
    phase::PhaseName,
    phase_state_table_tests::{
        ACTIVE, PENDING, Row, Scratch, State, completed, expect_state, expect_unchanged,
        operator_redo, paused, read, required_active, required_pending, running, seed,
        unchanged_except, writes,
    },
};

const STOPPED: &str =
    "phase stopped before completion; its advisory lock was free at runner restart";

fn caught_up(row: Row) -> Row {
    row.set("current_block_number", json!(100))
        .set("current_block_hash", json!("h100"))
}

async fn resolve(scratch: &Scratch, chain: &str, phase: PhaseName) -> Result<(Value, Value)> {
    let before = read(scratch.pool(), chain, phase).await?;
    let mut connection = scratch.pool().acquire().await?;
    super::resolve_stopped_phase(&mut connection, chain, phase).await?;
    drop(connection);
    Ok((before, read(scratch.pool(), chain, phase).await?))
}

#[tokio::test]
async fn recovery_settles_stopped_runs_and_returns_active_ingest_work_to_pending() -> Result<()> {
    let scratch = Scratch::create("phase_state_recovery").await?;
    let pool = scratch.pool();
    let ingest = PhaseName::Ingest;

    // Ingest: only an active required redo moves, back to pending with its range and progress.
    for (chain, row, error) in [
        (
            "recovery-ingest-active",
            required_active("recovery-ingest-active", ingest),
            PENDING.to_owned(),
        ),
        (
            "recovery-ingest-active-failed",
            required_active("recovery-ingest-active-failed", ingest).set(
                "last_error",
                json!(format!("{ACTIVE}; last attempt failed: rpc timeout")),
            ),
            format!("{PENDING}; last attempt failed: rpc timeout"),
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let (before, after) = resolve(&scratch, chain, ingest).await?;
        expect_state(&after, State::RequiredPending)?;
        ensure!(after["last_error"] == json!(error), "{chain}: {after}");
        unchanged_except(&before, &after, writes::RECOVERY_INGEST)?;
    }
    // A normal Ingest run is never settled here: the next start resumes it.
    for (chain, row) in [
        (
            "recovery-ingest-pending",
            required_pending("recovery-ingest-pending", ingest),
        ),
        (
            "recovery-ingest-running",
            running("recovery-ingest-running", ingest),
        ),
        (
            "recovery-ingest-operator",
            operator_redo("recovery-ingest-operator", ingest),
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let (before, after) = resolve(&scratch, chain, ingest).await?;
        expect_unchanged(&before, &after)?;
    }

    // The other finite phases: completed when the cursor reached the target, else failed.
    // Verify is never completed here; a final checkpoint becomes a retained completion.
    let verify = PhaseName::Verify;
    for (chain, row, expected, error) in [
        (
            "recovery-interpret-caught-up",
            caught_up(running(
                "recovery-interpret-caught-up",
                PhaseName::Interpret,
            )),
            State::Completed,
            Value::Null,
        ),
        (
            "recovery-interpret-behind",
            running("recovery-interpret-behind", PhaseName::Interpret),
            State::Failed,
            json!(STOPPED),
        ),
        (
            "recovery-project-paused",
            caught_up(paused("recovery-project-paused", PhaseName::Project)),
            State::Completed,
            Value::Null,
        ),
        (
            "recovery-verify-checkpoint",
            caught_up(running("recovery-verify-checkpoint", verify))
                .set("verification_level", json!("quick_synced")),
            State::RetainedCompletion,
            json!(format!(
                "{}runner stopped after Verify saved its final checkpoint; revalidate retained \
                 verification before completion",
                crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX
            )),
        ),
        (
            "recovery-verify-no-level",
            caught_up(running("recovery-verify-no-level", verify)),
            State::Failed,
            json!(STOPPED),
        ),
    ] {
        let phase: PhaseName = row.get("phase_name").as_str().unwrap_or_default().parse()?;
        seed(pool, chain, &[row]).await?;
        let (before, after) = resolve(&scratch, chain, phase).await?;
        expect_state(&after, expected)?;
        ensure!(after["last_error"] == error, "{chain}: {after}");
        unchanged_except(&before, &after, writes::RECOVERY)?;
    }
    // A redo, required or not, and a finished row stay as they are.
    for (chain, row) in [
        (
            "recovery-interpret-redo",
            operator_redo("recovery-interpret-redo", PhaseName::Interpret),
        ),
        (
            "recovery-interpret-active",
            required_active("recovery-interpret-active", PhaseName::Interpret),
        ),
        (
            "recovery-completed",
            completed("recovery-completed", PhaseName::Interpret),
        ),
    ] {
        seed(pool, chain, &[row]).await?;
        let (before, after) = resolve(&scratch, chain, PhaseName::Interpret).await?;
        expect_unchanged(&before, &after)?;
    }
    scratch.cleanup().await
}
