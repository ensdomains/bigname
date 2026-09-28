use std::io::{self, Write};

use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
};

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
