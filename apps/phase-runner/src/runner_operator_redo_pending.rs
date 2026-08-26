use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
};

use super::PhaseRunner;

type PendingRedoRow = (
    String,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

impl PhaseRunner {
    pub(super) async fn require_no_pending_redo_for_all(
        &self,
        chain_id: &str,
        allowed_project_stamp: Option<BlockRange>,
        allowed_verify_stamp: Option<BlockRange>,
        recovery_all_range: Option<BlockRange>,
    ) -> RunnerResult<()> {
        let pending: Vec<PendingRedoRow> = sqlx::query_as(
            "SELECT phase_name, redo_mode, redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1
               AND redo_in_progress
             ORDER BY array_position(
                 ARRAY['ingest','interpret','project','verify','live'], phase_name
             )",
        )
        .bind(chain_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to inspect pending redo for chain {chain_id}"),
                error,
            )
        })?;
        let mut recovery = Vec::new();
        for (phase, mode, from, to, last_error) in pending {
            let persisted_range = from.zip(to).and_then(|(from, to)| {
                (from >= 0 && to >= from).then_some(BlockRange { from, to })
            });
            let allowed = ((allowed_project_stamp.is_some()
                && phase == PhaseName::Project.as_str()
                && persisted_range == allowed_project_stamp)
                || (allowed_verify_stamp.is_some()
                    && phase == PhaseName::Verify.as_str()
                    && persisted_range == allowed_verify_stamp))
                && mode.as_deref() == Some("redo")
                && last_error
                    .as_deref()
                    .is_some_and(crate::redo_stamp::owns_required_redo);
            if allowed {
                continue;
            }
            let Some(range) = persisted_range else {
                return Err(RunnerError::data_integrity(format!(
                    "cannot redo all phases for chain {chain_id}: the pending {phase} redo has an \
                     invalid persisted range"
                )));
            };
            let phase_argument = if mode.as_deref() == Some("recompute_flags")
                || last_error
                    .as_deref()
                    .is_some_and(crate::redo_recompute::is_staged_project_refresh)
            {
                "recompute-flags".to_owned()
            } else {
                phase.clone()
            };
            recovery.push((phase, phase_argument, range));
            if recovery_all_range.is_none() {
                break;
            }
        }
        let Some((first_phase, _, first_range)) = recovery.first() else {
            return Ok(());
        };
        let commands = recovery
            .iter()
            .map(|(_, phase, range)| {
                format!(
                    "rerun `phase-runner redo --chain {chain_id} --phase {phase} --from-block {} \
                     --to-block {}`",
                    range.from, range.to
                )
            })
            .collect::<Vec<_>>()
            .join(", then ");
        let rerun_range = recovery_all_range.unwrap_or(*first_range);
        Err(RunnerError::data_integrity(format!(
            "cannot redo all phases for chain {chain_id}: a pending {first_phase} redo must be \
             completed first; {commands}, then rerun `phase-runner redo --chain {chain_id} \
             --phase all --from-block {} --to-block {}`",
            rerun_range.from, rerun_range.to
        )))
    }
}
