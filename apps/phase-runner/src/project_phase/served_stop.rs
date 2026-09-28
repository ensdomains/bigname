//! Where the served tables stopped when the [publication switch](bigname_storage::publication_source)
//! ran Project (TYR-36 step 7b). With the switch on the served engine does not run and the Project
//! row follows the family marker, so switching back off would resume the served engine past blocks
//! it never applied while the API presented its tables as current at the Project row.
//!
//! The first switch-on batch on a chain records the Project row's block in `project_served_stop`.
//! With the switch off, a normal run refuses a chain whose record is short of the Project row and
//! names the Project redo that replays the gap; that redo deletes the record once it commits. The
//! API refuses to start on the same condition. Step 7c removes the switch and this record.
use std::collections::BTreeSet;

use super::ProjectPhase;
use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseContext, RunMode},
};

/// A chain's recorded stop: the block the served tables last applied, or none when Project had
/// applied no block when the switch first ran it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ServedStop(Option<i64>);

impl ServedStop {
    /// The first block a redo must replay: the one after the stop, or the chain's first source
    /// block when the served tables never applied one.
    fn gap_from(self, context: &PhaseContext) -> i64 {
        self.0.map_or_else(
            || {
                context
                    .sources
                    .iter()
                    .map(|source| source.start_block_number)
                    .min()
                    .unwrap_or(0)
            },
            |block| block.saturating_add(1),
        )
    }
}

impl ProjectPhase {
    fn served_stop_settled(&self, chain_id: &str) -> bool {
        self.served_stop_settled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&settled_key(chain_id))
    }

    fn settle_served_stop(&self, chain_id: &str) {
        self.served_stop_settled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(settled_key(chain_id));
    }

    /// With the switch on, before a family batch can move the Project row: record the block the
    /// served tables stand at, unless an earlier switch-on batch already did. Once per process.
    pub(super) async fn record_served_stop(&self, chain_id: &str) -> RunnerResult<()> {
        if self.served_stop_settled(chain_id) {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO project_served_stop (chain_id, block_number)
             SELECT $1, (SELECT current_block_number FROM chain_phase_state
                         WHERE chain_id = $1 AND phase_name = 'project')
             ON CONFLICT (chain_id) DO NOTHING",
        )
        .bind(chain_id)
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to record where the served tables of chain {chain_id} stop"),
                error,
            )
        })?;
        self.settle_served_stop(chain_id);
        Ok(())
    }

    /// With the switch off, before the served engine runs. A normal run refuses a chain whose
    /// served tables stopped short of the Project row; one that is not short deletes the record.
    /// A redo runs and returns the record for [`Self::clear_served_stop`].
    pub(super) async fn check_served_stop(
        &self,
        context: &PhaseContext,
    ) -> RunnerResult<Option<ServedStop>> {
        let chain_id = context.chain_id.as_str();
        if self.served_stop_settled(chain_id) {
            return Ok(None);
        }
        let stop: Option<Option<i64>> =
            sqlx::query_scalar("SELECT block_number FROM project_served_stop WHERE chain_id = $1")
                .bind(chain_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        format!("failed to load where the served tables of chain {chain_id} stop"),
                        error,
                    )
                })?;
        let Some(stop) = stop.map(ServedStop) else {
            self.settle_served_stop(chain_id);
            return Ok(None);
        };
        match context.mode {
            RunMode::Normal => {
                let current = context.resume.current.as_ref().map(|marker| marker.number);
                // No Project block means the served engine rebuilds from scratch.
                if current.is_none() || stop.0 >= current {
                    self.delete_served_stop(chain_id, stop).await?;
                    self.settle_served_stop(chain_id);
                    return Ok(None);
                }
                let current = current.expect("checked above");
                let stopped = stop.0.map_or_else(
                    || "before any block".to_owned(),
                    |block| format!("at {block}"),
                );
                Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!(
                        "chain {chain_id}: the publication switch ran Project to block {current}, \
                         but the served tables stopped {stopped}. With the switch off, replay \
                         them first with `phase-runner redo --chain {chain_id} --phase project \
                         --from-block {} --to-block {current}`, or turn the switch back on \
                         (docs/deployment.md, Publication switch)",
                        stop.gap_from(context)
                    ),
                )
                .not_retried())
            }
            RunMode::Redo(_) => Ok(Some(stop)),
            RunMode::RecomputeFlags(_) => Ok(None),
        }
    }

    /// After a served redo committed: a redo from the gap's first block through the recorded
    /// head replayed every block the served tables missed, so it deletes the record.
    pub(super) async fn clear_served_stop(
        &self,
        context: &PhaseContext,
        stop: ServedStop,
        range: BlockRange,
        head: i64,
    ) -> RunnerResult<()> {
        if range.from <= stop.gap_from(context) && range.to >= head {
            self.delete_served_stop(&context.chain_id, stop).await?;
        }
        Ok(())
    }

    async fn delete_served_stop(&self, chain_id: &str, stop: ServedStop) -> RunnerResult<()> {
        sqlx::query(
            "DELETE FROM project_served_stop
             WHERE chain_id = $1 AND block_number IS NOT DISTINCT FROM $2",
        )
        .bind(chain_id)
        .bind(stop.0)
        .execute(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to clear where the served tables of chain {chain_id} stop"),
                error,
            )
        })?;
        Ok(())
    }
}

/// Chains whose record the process has settled, by switch setting: recorded under the switch,
/// or checked and cleared without it. The switch is read once at startup, so a process never
/// needs to look again; the setting is part of the key only for tests that scope the switch.
pub(super) type Settled = BTreeSet<(bool, String)>;

fn settled_key(chain_id: &str) -> (bool, String) {
    (
        bigname_storage::publication_source::serve_from_families(),
        chain_id.to_owned(),
    )
}
