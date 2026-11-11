//! `docs/phase-state.md` § States and § Combinations no statement writes, against the CHECKs of
//! `crates/storage/schema/baseline/10_phase_state.sql`.
use anyhow::{Context, Result, bail, ensure};
use serde_json::json;

use super::{
    ACTIVE, PENDING, Row, Scratch, State, T0, T1, completed, expect_state, failed, idle, insert,
    operator_redo, paused, read, redo, required_active, required_pending, retained_completion,
    running, with_redo_progress,
};
use crate::phase::PhaseName;

const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FINITE: [PhaseName; 4] = [
    PhaseName::Ingest,
    PhaseName::Interpret,
    PhaseName::Project,
    PhaseName::Verify,
];

struct Case {
    chain: String,
    phase: PhaseName,
    row: Row,
}

fn case(label: &str, phase: PhaseName, build: fn(&str, PhaseName) -> Row) -> Case {
    let chain = format!("{label}-{phase}");
    let row = build(&chain, phase);
    Case { chain, phase, row }
}

fn named_states() -> Vec<(Case, State)> {
    let mut cases = Vec::new();
    for phase in PhaseName::ALL {
        cases.push((case("idle", phase, idle), State::Idle));
        cases.push((case("running", phase, running), State::Running));
        cases.push((case("completed", phase, completed), State::Completed));
        cases.push((case("failed", phase, failed), State::Failed));
    }
    for phase in FINITE {
        cases.push((case("paused", phase, paused), State::Paused));
        cases.push((
            case("settled", phase, |chain, phase| {
                completed(chain, phase).set("settled_while_unconfigured", json!(true))
            }),
            State::CompletedUnconfigured,
        ));
        cases.push((
            case("operator-redo", phase, operator_redo),
            State::OperatorRedo,
        ));
        cases.push((
            case("operator-redo-paused", phase, |chain, phase| {
                operator_redo(chain, phase).set("phase_status", json!("paused"))
            }),
            State::OperatorRedo,
        ));
        cases.push((
            case("operator-redo-progress", phase, |chain, phase| {
                with_redo_progress(operator_redo(chain, phase))
            }),
            State::OperatorRedo,
        ));
        cases.push((
            case("redo-over-running", phase, |chain, phase| {
                redo(running(chain, phase), "redo", None)
            }),
            State::OperatorRedo,
        ));
        cases.push((
            case("redo-over-paused", phase, |chain, phase| {
                redo(paused(chain, phase), "redo", None)
            }),
            State::OperatorRedo,
        ));
        cases.push((
            case("redo-over-failed", phase, |chain, phase| {
                redo(failed(chain, phase), "redo", None)
            }),
            State::OperatorRedo,
        ));
        cases.push((
            case("operator-redo-failed-attempt", phase, |chain, phase| {
                operator_redo(chain, phase).set("last_error", json!("rpc timeout"))
            }),
            State::OperatorRedo,
        ));
    }
    for phase in FINITE {
        cases.push((
            case("required-pending", phase, required_pending),
            State::RequiredPending,
        ));
        cases.push((
            case("required-pending-after-failure", phase, |chain, phase| {
                required_pending(chain, phase).set(
                    "last_error",
                    json!(format!("{PENDING}; last attempt failed: rpc timeout")),
                )
            }),
            State::RequiredPending,
        ));
        cases.push((
            case("required-active", phase, required_active),
            State::RequiredActive,
        ));
        cases.push((
            case("required-active-paused", phase, |chain, phase| {
                required_active(chain, phase).set("phase_status", json!("paused"))
            }),
            State::RequiredActive,
        ));
        cases.push((
            case("required-over-running", phase, |chain, phase| {
                redo(running(chain, phase), "redo", Some(PENDING))
            }),
            State::RequiredPending,
        ));
    }
    for phase in [PhaseName::Ingest, PhaseName::Verify] {
        cases.push((
            case("retained", phase, retained_completion),
            State::RetainedCompletion,
        ));
    }
    cases.extend([
        (
            case("live-mismatch", PhaseName::Live, |chain, phase| {
                failed(chain, phase).set(
                    "last_error",
                    json!("live phase stopped because verify reported a verification mismatch: x"),
                )
            }),
            State::Failed,
        ),
        (
            case(
                "interrupted-before-redo",
                PhaseName::Interpret,
                |chain, phase| {
                    failed(chain, phase).set(
                        "last_error",
                        json!("phase was interrupted before redo; resume the normal phase"),
                    )
                },
            ),
            State::Failed,
        ),
        (
            case(
                "stopped-before-completion",
                PhaseName::Project,
                |chain, phase| {
                    failed(chain, phase).set(
                        "last_error",
                        json!(
                            "phase stopped before completion; its advisory lock was free at runner \
                         restart"
                        ),
                    )
                },
            ),
            State::Failed,
        ),
        (
            case("ingest-redo-evidence", PhaseName::Ingest, |chain, phase| {
                required_active(chain, phase)
                    .set("redo_manifest_authority_fingerprint", json!(FINGERPRINT))
                    .set(
                        "redo_source_boundary_markers",
                        json!({"rpc": {"number": 40, "hash": "h40"}}),
                    )
            }),
            State::RequiredActive,
        ),
        (
            case("recompute", PhaseName::Interpret, |chain, phase| {
                redo(completed(chain, phase), "recompute_flags", None)
            }),
            State::RecomputeRedo,
        ),
        (
            case(
                "project-request-inside",
                PhaseName::Project,
                |chain, phase| {
                    operator_redo(chain, phase)
                        .set("redo_requested_from_block_number", json!(30))
                        .set("redo_requested_to_block_number", json!(60))
                },
            ),
            State::OperatorRedo,
        ),
        (
            case(
                "settled-carried-running",
                PhaseName::Verify,
                |chain, phase| running(chain, phase).set("settled_while_unconfigured", json!(true)),
            ),
            State::Running,
        ),
        (
            case(
                "settled-carried-failed",
                PhaseName::Verify,
                |chain, phase| failed(chain, phase).set("settled_while_unconfigured", json!(true)),
            ),
            State::Failed,
        ),
        (
            case(
                "ingest-completed-no-handoff",
                PhaseName::Ingest,
                |chain, phase| {
                    completed(chain, phase)
                        .set("live_handoff_block_number", json!(null))
                        .set("live_handoff_block_hash", json!(null))
                },
            ),
            State::Completed,
        ),
        (
            case(
                "completed-no-cursor",
                PhaseName::Interpret,
                |chain, phase| {
                    completed(chain, phase)
                        .set("current_block_number", json!(null))
                        .set("current_block_hash", json!(null))
                        .set("target_block_number", json!(null))
                        .set("target_block_hash", json!(null))
                },
            ),
            State::Completed,
        ),
        (
            case("stale-hash", PhaseName::Interpret, |chain, phase| {
                completed(chain, phase).set("input_content_hash", json!("manifest-authority:x:1"))
            }),
            State::Completed,
        ),
    ]);
    cases
}

/// Combinations the CHECKs refuse, with the constraint PostgreSQL names. It tests a table's CHECKs
/// in name order, so the name is the first refusing CHECK by name.
fn refused() -> Vec<(Case, &'static str)> {
    vec![
        (
            case("redo-on-completed", PhaseName::Interpret, |chain, phase| {
                operator_redo(chain, phase).set("phase_status", json!("completed"))
            }),
            "chain_phase_state_check4",
        ),
        (
            case("redo-on-failed", PhaseName::Interpret, |chain, phase| {
                operator_redo(chain, phase)
                    .set("phase_status", json!("failed"))
                    .set("finished_at", json!(T1))
                    .set("last_error", json!("x"))
            }),
            "chain_phase_state_check4",
        ),
        (
            case("redo-on-idle", PhaseName::Interpret, |chain, phase| {
                operator_redo(chain, phase)
                    .set("phase_status", json!("idle"))
                    .set("started_at", json!(null))
            }),
            "chain_phase_state_check4",
        ),
        (
            case(
                "redo-columns-without-marker",
                PhaseName::Interpret,
                |chain, phase| completed(chain, phase).set("redo_mode", json!("redo")),
            ),
            "chain_phase_state_check4",
        ),
        (
            case(
                "saved-status-without-marker",
                PhaseName::Interpret,
                |chain, phase| {
                    completed(chain, phase).set("redo_previous_phase_status", json!("completed"))
                },
            ),
            "chain_phase_state_check4",
        ),
        (
            case(
                "recompute-on-project",
                PhaseName::Project,
                |chain, phase| redo(completed(chain, phase), "recompute_flags", None),
            ),
            "chain_phase_state_check4",
        ),
        (
            case("recompute-on-ingest", PhaseName::Ingest, |chain, phase| {
                redo(completed(chain, phase), "recompute_flags", None)
            }),
            "chain_phase_state_check4",
        ),
        (
            case(
                "saved-failure-without-error",
                PhaseName::Interpret,
                |chain, phase| {
                    redo(failed(chain, phase), "redo", None)
                        .set("redo_previous_last_error", json!(null))
                },
            ),
            "chain_phase_state_check4",
        ),
        (
            case(
                "redo-cursor-above-target",
                PhaseName::Interpret,
                |chain, phase| {
                    with_redo_progress(operator_redo(chain, phase))
                        .set("redo_current_block_number", json!(80))
                        .set("redo_current_block_hash", json!("h80"))
                        .set("redo_target_block_number", json!(70))
                        .set("redo_target_block_hash", json!("h70"))
                },
            ),
            "chain_phase_state_check4",
        ),
        (
            case(
                "redo-cursor-outside-range",
                PhaseName::Interpret,
                |chain, phase| {
                    with_redo_progress(operator_redo(chain, phase))
                        .set("redo_current_block_number", json!(5))
                        .set("redo_current_block_hash", json!("h5"))
                },
            ),
            "chain_phase_state_check4",
        ),
        (
            case(
                "request-on-interpret",
                PhaseName::Interpret,
                |chain, phase| {
                    operator_redo(chain, phase)
                        .set("redo_requested_from_block_number", json!(10))
                        .set("redo_requested_to_block_number", json!(90))
                },
            ),
            "chain_phase_state_project_redo_request_check",
        ),
        (
            case(
                "request-outside-execution",
                PhaseName::Project,
                |chain, phase| {
                    operator_redo(chain, phase).set("redo_requested_to_block_number", json!(95))
                },
            ),
            "chain_phase_state_project_redo_request_check",
        ),
        (
            case(
                "request-without-marker",
                PhaseName::Project,
                |chain, phase| {
                    completed(chain, phase)
                        .set("redo_requested_from_block_number", json!(10))
                        .set("redo_requested_to_block_number", json!(90))
                },
            ),
            "chain_phase_state_project_redo_request_check",
        ),
        (
            case(
                "boundaries-on-interpret",
                PhaseName::Interpret,
                |chain, phase| {
                    operator_redo(chain, phase).set(
                        "redo_source_boundary_markers",
                        json!({"rpc": {"number": 40, "hash": "h40"}}),
                    )
                },
            ),
            "chain_phase_state_ingest_redo_source_boundaries_check",
        ),
        (
            case(
                "fingerprint-on-verify",
                PhaseName::Verify,
                |chain, phase| {
                    operator_redo(chain, phase)
                        .set("redo_manifest_authority_fingerprint", json!(FINGERPRINT))
                },
            ),
            "chain_phase_state_ingest_redo_manifest_authority_check",
        ),
        (
            case(
                "fingerprint-without-marker",
                PhaseName::Ingest,
                |chain, phase| {
                    completed(chain, phase)
                        .set("redo_manifest_authority_fingerprint", json!(FINGERPRINT))
                },
            ),
            "chain_phase_state_ingest_redo_manifest_authority_check",
        ),
        (
            case("failed-without-error", PhaseName::Ingest, |chain, phase| {
                failed(chain, phase).set("last_error", json!(null))
            }),
            "chain_phase_state_check8",
        ),
        (
            case("running-with-error", PhaseName::Ingest, |chain, phase| {
                running(chain, phase).set("last_error", json!("x"))
            }),
            "chain_phase_state_check8",
        ),
        (
            case("completed-with-error", PhaseName::Ingest, |chain, phase| {
                completed(chain, phase).set("last_error", json!("x"))
            }),
            "chain_phase_state_check8",
        ),
        (
            case(
                "completed-without-finish",
                PhaseName::Ingest,
                |chain, phase| completed(chain, phase).set("finished_at", json!(null)),
            ),
            "chain_phase_state_check7",
        ),
        (
            case("finish-before-start", PhaseName::Ingest, |chain, phase| {
                completed(chain, phase)
                    .set("started_at", json!(T1))
                    .set("finished_at", json!(T0))
            }),
            "chain_phase_state_check7",
        ),
        (
            case("idle-with-start", PhaseName::Ingest, |chain, phase| {
                idle(chain, phase).set("started_at", json!(T0))
            }),
            "chain_phase_state_check7",
        ),
        (
            case("redo-with-finish", PhaseName::Interpret, |chain, phase| {
                operator_redo(chain, phase).set("finished_at", json!(T1))
            }),
            "chain_phase_state_check7",
        ),
        (
            case("level-outside-verify", PhaseName::Ingest, |chain, phase| {
                completed(chain, phase).set("verification_level", json!("quick_synced"))
            }),
            "chain_phase_state_verification_phase_check",
        ),
        (
            case(
                "handoff-outside-ingest",
                PhaseName::Interpret,
                |chain, phase| {
                    completed(chain, phase)
                        .set("live_handoff_block_number", json!(100))
                        .set("live_handoff_block_hash", json!("h100"))
                },
            ),
            "chain_phase_state_check6",
        ),
        (
            case("settled-false", PhaseName::Verify, |chain, phase| {
                completed(chain, phase).set("settled_while_unconfigured", json!(false))
            }),
            "chain_phase_state_unconfigured_settlement_check",
        ),
        (
            case("half-cursor", PhaseName::Ingest, |chain, phase| {
                running(chain, phase).set("current_block_hash", json!(null))
            }),
            "chain_phase_state_check",
        ),
        (
            case("unknown-status", PhaseName::Ingest, |chain, phase| {
                idle(chain, phase).set("phase_status", json!("stopped"))
            }),
            // The status CHECK refuses it too; check7 sorts first.
            "chain_phase_state_check7",
        ),
    ]
}

/// Combinations no statement writes, by the trace in `docs/phase-state.md`, that the CHECKs accept.
fn accepted_unreachable() -> Vec<Case> {
    vec![
        case(
            "redo-saved-from-idle",
            PhaseName::Interpret,
            |chain, phase| redo(idle(chain, phase), "redo", None),
        ),
        case("live-redo", PhaseName::Live, operator_redo),
        case(
            "required-text-on-normal-failure",
            PhaseName::Interpret,
            |chain, phase| failed(chain, phase).set("last_error", json!(PENDING)),
        ),
        case(
            "active-text-on-normal-failure",
            PhaseName::Interpret,
            |chain, phase| failed(chain, phase).set("last_error", json!(ACTIVE)),
        ),
        case(
            "recompute-owned-as-required",
            PhaseName::Interpret,
            |chain, phase| redo(completed(chain, phase), "recompute_flags", Some(PENDING)),
        ),
        case("redo-generation-zero", PhaseName::Verify, |chain, phase| {
            operator_redo(chain, phase).set("redo_attempt_generation", json!(0))
        }),
        case("idle-with-cursor", PhaseName::Interpret, |chain, phase| {
            idle(chain, phase)
                .set("current_block_number", json!(100))
                .set("current_block_hash", json!("h100"))
        }),
        case("idle-settled", PhaseName::Verify, |chain, phase| {
            idle(chain, phase).set("settled_while_unconfigured", json!(true))
        }),
        case("idle-with-level", PhaseName::Verify, |chain, phase| {
            idle(chain, phase).set("verification_level", json!("quick_synced"))
        }),
    ]
}

#[tokio::test]
async fn named_states_pass_the_checks() -> Result<()> {
    let scratch = Scratch::create("phase_state_named").await?;
    for (case, expected) in named_states() {
        insert(scratch.pool(), &case.row)
            .await
            .with_context(|| format!("the CHECKs refused named state {}", case.chain))?;
        let row = read(scratch.pool(), &case.chain, case.phase).await?;
        expect_state(&row, expected).with_context(|| case.chain.clone())?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn unreachable_combinations_meet_the_checks() -> Result<()> {
    let scratch = Scratch::create("phase_state_unreachable").await?;
    for (case, constraint) in refused() {
        let error = match insert(scratch.pool(), &case.row).await {
            Ok(()) => bail!(
                "the CHECKs accepted {}, which should meet {constraint}",
                case.chain
            ),
            Err(error) => error,
        };
        let database = error
            .as_database_error()
            .with_context(|| format!("{} failed outside the database: {error}", case.chain))?;
        ensure!(
            database.code().as_deref() == Some("23514")
                && database.constraint() == Some(constraint),
            "{} expected a refusal by {constraint}, got {database}",
            case.chain
        );
    }
    // Pinned: the CHECKs accept these, and no statement writes them.
    for case in accepted_unreachable() {
        insert(scratch.pool(), &case.row)
            .await
            .with_context(|| format!("the CHECKs refused {}", case.chain))?;
    }
    scratch.cleanup().await
}
