use std::io::{self, Write};

use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    transitions::PhaseStateRow,
};

pub(crate) const PROJECT_REFRESH_REASON: &str = "recompute-flags scoped projection refresh";
pub(crate) const PROJECT_REFRESH_STAGED_REASON: &str =
    "recompute-flags project refresh complete; interpret flags pending";

pub(crate) fn owns_project_refresh(message: &str) -> bool {
    message.contains(PROJECT_REFRESH_REASON)
}

pub(crate) fn is_staged_project_refresh(message: &str) -> bool {
    message == PROJECT_REFRESH_STAGED_REASON
}

pub(crate) fn reject_separate_project_run(
    chain_id: &str,
    phase: PhaseName,
    active: &PhaseStateRow,
) -> RunnerResult<()> {
    if phase != PhaseName::Project
        || !active.redo_in_progress
        || !active
            .last_error
            .as_deref()
            .is_some_and(is_staged_project_refresh)
    {
        return Ok(());
    }
    let (Some(from), Some(to)) = (active.redo_from_block_number, active.redo_to_block_number)
    else {
        return Err(RunnerError::data_integrity(
            "staged recompute-flags Project refresh is missing its persisted range",
        ));
    };
    Err(RunnerError::new(
        ErrorKind::InvalidTransition,
        format!(
            "cannot run Project separately for chain {chain_id}: recompute-flags is pending \
             after its scoped projection refresh; rerun `phase-runner redo --chain {chain_id} \
             --phase recompute-flags --from-block {from} --to-block {to}`"
        ),
    ))
}

pub(crate) async fn finalize_metadata(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<bigname_interpret::RecomputeSummary> {
    bigname_interpret::finalize_recompute_flags(transaction, chain_id, range.from, range.to)
        .await
        .map_err(runner_interpret_error)
}

pub(crate) async fn stamp_transitions_and_load_ranges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    summary: Option<bigname_interpret::RecomputeSummary>,
) -> RunnerResult<Vec<(String, i64, i64)>> {
    let Some(summary) = summary else {
        return Ok(Vec::new());
    };
    let Some(from) = summary.earliest_transition_block() else {
        return Ok(Vec::new());
    };
    let transition_range = BlockRange { from, to: i64::MAX };
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        crate::redo_stamp::stamp_required_in_transaction(
            transaction,
            chain_id,
            phase,
            transition_range,
            "recompute-flags found a visibility-class transition",
        )
        .await?;
    }
    sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1
           AND phase_name IN ('interpret', 'project')
           AND redo_in_progress
         ORDER BY phase_name",
    )
    .bind(chain_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load recompute-flags redo report for chain {chain_id}"),
            error,
        )
    })
}

pub(crate) async fn clear_staged_project_refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> RunnerResult<bool> {
    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = CASE
                 WHEN redo_previous_phase_status IN ('running', 'paused') THEN 'failed'
                 ELSE redo_previous_phase_status
             END,
             settled_while_unconfigured = CASE
                 WHEN redo_previous_phase_status = 'completed'
                  AND current_block_number IS NOT NULL
                  AND current_block_number = target_block_number
                  AND current_block_hash IS NOT NULL
                  AND current_block_hash = target_block_hash
                  AND input_content_hash IS NOT NULL
                 THEN NULL
                 ELSE settled_while_unconfigured
             END,
             last_error = CASE
                 WHEN redo_previous_phase_status IN ('running', 'paused')
                     THEN 'phase was interrupted before redo; resume the normal phase'
                 ELSE redo_previous_last_error
             END,
             started_at = redo_previous_started_at,
             finished_at = CASE
                 WHEN redo_previous_phase_status IN ('running', 'paused') THEN now()
                 ELSE redo_previous_finished_at
             END,
             redo_in_progress = false,
             redo_mode = NULL,
             redo_previous_phase_status = NULL,
             redo_previous_last_error = NULL,
             redo_previous_started_at = NULL,
             redo_previous_finished_at = NULL,
             redo_from_block_number = NULL,
             redo_to_block_number = NULL,
             redo_current_block_number = NULL,
             redo_current_block_hash = NULL,
             redo_target_block_number = NULL,
             redo_target_block_hash = NULL,
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = NULL,
             updated_at = now()
         WHERE chain_id = $1
           AND phase_name = 'project'
           AND redo_in_progress
           AND redo_mode = 'redo'
           AND last_error = $2",
    )
    .bind(chain_id)
    .bind(PROJECT_REFRESH_STAGED_REASON)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to clear staged recompute-flags project refresh for chain {chain_id}"),
            error,
        )
    })?;
    Ok(result.rows_affected() == 1)
}

pub(crate) struct ProjectRefreshCompletion<'a> {
    pub previous: &'a PhaseStateRow,
    pub verification_level: Option<&'a str>,
    pub current_hash: Option<&'a str>,
    pub target_hash: Option<&'a str>,
    pub content_hash: Option<&'a str>,
}

pub(crate) async fn stage_project_refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    completion: ProjectRefreshCompletion<'_>,
) -> RunnerResult<()> {
    let previous = completion.previous;
    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET verification_level = $3,
             current_block_number = $4,
             current_block_hash = $5,
             target_block_number = $6,
             target_block_hash = $7,
             input_content_hash = $8,
             live_handoff_block_number = $9,
             live_handoff_block_hash = $10,
             redo_current_block_number = NULL,
             redo_current_block_hash = NULL,
             redo_target_block_number = NULL,
             redo_target_block_hash = NULL,
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = NULL,
             last_error = $11,
             updated_at = now()
         WHERE chain_id = $1
           AND phase_name = $2
           AND redo_in_progress
           AND redo_mode = 'redo'",
    )
    .bind(chain_id)
    .bind(PhaseName::Project.as_str())
    .bind(completion.verification_level)
    .bind(previous.current_block_number)
    .bind(completion.current_hash)
    .bind(previous.target_block_number)
    .bind(completion.target_hash)
    .bind(completion.content_hash)
    .bind(previous.live_handoff_block_number)
    .bind(previous.live_handoff_block_hash.as_deref())
    .bind(PROJECT_REFRESH_STAGED_REASON)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to stage completed recompute-flags project refresh for chain {chain_id}"
            ),
            error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::data_integrity(format!(
            "completed recompute-flags project refresh lost its redo marker for chain {chain_id}"
        )));
    }
    Ok(())
}

pub(crate) fn report(
    chain_id: &str,
    summary: Option<bigname_interpret::RecomputeSummary>,
    stamped_ranges: &[(String, i64, i64)],
) {
    let Some(summary) = summary else {
        return;
    };
    let stdout = io::stdout();
    if let Err(error) = write_operator_report(stdout.lock(), chain_id, summary, stamped_ranges) {
        tracing::error!(
            chain_id,
            error = %error,
            "failed to write recompute-flags operator report"
        );
    }
    tracing::info!(
        chain_id,
        same_class_names = summary.same_class_names,
        shadow_to_active_names = summary.shadow_to_active_names,
        shadow_to_active_from_block = ?summary.shadow_to_active_from_block,
        active_to_shadow_names = summary.active_to_shadow_names,
        active_to_shadow_from_block = ?summary.active_to_shadow_from_block,
        ?stamped_ranges,
        "recompute-flags completed and reported ordinary redo coverage"
    );
}

fn write_operator_report(
    mut writer: impl Write,
    chain_id: &str,
    summary: bigname_interpret::RecomputeSummary,
    stamped_ranges: &[(String, i64, i64)],
) -> io::Result<()> {
    let stamped_ranges = stamped_ranges
        .iter()
        .map(|(phase, from, to)| {
            json!({
                "phase": phase,
                "from_block": from,
                "to_block": to,
            })
        })
        .collect::<Vec<_>>();
    let report = json!({
        "event": "recompute_flags_completed",
        "chain_id": chain_id,
        "same_class_names": summary.same_class_names,
        "shadow_to_active_names": summary.shadow_to_active_names,
        "shadow_to_active_from_block": summary.shadow_to_active_from_block,
        "active_to_shadow_names": summary.active_to_shadow_names,
        "active_to_shadow_from_block": summary.active_to_shadow_from_block,
        "stamped_redo_ranges": stamped_ranges,
    });
    writeln!(writer, "{report}")
}

fn runner_interpret_error(error: bigname_interpret::InterpretError) -> RunnerError {
    let kind = match error.kind() {
        bigname_interpret::ErrorKind::Transient => ErrorKind::Transient,
        bigname_interpret::ErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        bigname_interpret::ErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_report_is_json_and_includes_stamped_ranges() {
        let mut output = Vec::new();
        write_operator_report(
            &mut output,
            "base-mainnet",
            bigname_interpret::RecomputeSummary {
                same_class_names: 8,
                shadow_to_active_names: 2,
                active_to_shadow_names: 1,
                shadow_to_active_from_block: Some(11),
                active_to_shadow_from_block: Some(17),
            },
            &[("interpret".into(), 11, 23), ("project".into(), 11, 23)],
        )
        .expect("memory-backed operator report must serialize");

        let value: serde_json::Value =
            serde_json::from_slice(&output).expect("operator report must be one JSON value");
        assert_eq!(
            value,
            json!({
                "event": "recompute_flags_completed",
                "chain_id": "base-mainnet",
                "same_class_names": 8,
                "shadow_to_active_names": 2,
                "shadow_to_active_from_block": 11,
                "active_to_shadow_names": 1,
                "active_to_shadow_from_block": 17,
                "stamped_redo_ranges": [
                    {"phase": "interpret", "from_block": 11, "to_block": 23},
                    {"phase": "project", "from_block": 11, "to_block": 23},
                ],
            })
        );
    }
}
