//! The repair transitions of a family run that are not a block or an undo: a rebuild's reset
//! and population, opening an undo-then-replay, and the record transitions with no block of
//! their own. Each is one transaction fenced on the marker generation and the repair record.
use super::{Run, at_block};
use crate::{
    ProjectError, Result,
    families::{
        FamilyOutcome, block,
        input::{self, InputToken, Revision},
        marker::{self, FamilyMarker},
        repair::{self, NewRepair, Reason, Record},
        tables,
    },
};

impl Run<'_> {
    /// Clear the families and open a rebuild in one transaction, then populate.
    pub(super) async fn rebuild(
        &mut self,
        reason: Reason,
        attempt: i64,
        family: &FamilyMarker,
        planned: Option<&Record>,
        token: &InputToken,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        if token.revision().is_none() {
            return Err(block::revision_error(self.chain_id, 0, None, &(None, None)));
        }
        let (reset, revision) = self.reset(reason, attempt, family, planned).await?;
        outcome.reset = true;
        self.populate(reset, attempt, &revision, outcome).await
    }

    /// Resume a rebuild the record says is under way; start it again when its input revision
    /// changed or its last block left the readable lineage.
    pub(super) async fn resume_rebuild(
        &mut self,
        record: &Record,
        family: &FamilyMarker,
        token: &InputToken,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let revision = token.revision();
        let restart = revision.is_none()
            || record.prefix_revision != revision
            || !family.bootstrap
            || !self.readable(family.current.as_ref()).await?;
        if restart {
            return self
                .rebuild(
                    Reason::ContentHashRebuild,
                    record.attempt,
                    family,
                    Some(record),
                    token,
                    outcome,
                )
                .await;
        }
        let revision = revision.expect("checked equal to a recorded prefix");
        self.populate(family.clone(), record.attempt, &revision, outcome)
            .await
    }

    /// Populate from `family` to the target, visiting only the blocks that carry family work
    /// and then the target itself, so the marker ends at the target. The target's block
    /// completes the record.
    pub(super) async fn populate(
        &mut self,
        mut family: FamilyMarker,
        attempt: i64,
        revision: &Revision,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let from = family
            .current
            .as_ref()
            .map_or(0, |marker| marker.number + 1);
        let mut blocks =
            input::work_blocks(self.pool, self.chain_id, from, self.target.number).await?;
        if blocks.last() != Some(&self.target.number) {
            blocks.push(self.target.number);
        }
        for (visited, number) in (0_u64..).zip(blocks) {
            if !self.budget.take(outcome) {
                return Ok(());
            }
            // A rebuild grows the families from empty faster than autovacuum samples them, and
            // plans made on empty-table statistics scan whole families per key. Refresh the
            // statistics after 1, 2, 4, 8, ... rebuilt blocks.
            if visited > 0 && visited.is_power_of_two() {
                analyze(self.pool, self.chain_id).await;
            }
            let completes = number == self.target.number;
            let plan = block::Plan {
                predecessor: family.current.as_ref(),
                sequence: family.sequence,
                contiguous: false,
                bootstrap: !completes,
                revision,
                role: block::Role::Rebuild { attempt, completes },
            };
            let (next, stats) = block::apply(self.pool, self.chain_id, number, &plan, self.options)
                .await
                .map_err(|error| at_block(number, error))?;
            outcome.record(stats);
            family = next;
        }
        Ok(())
    }

    /// Clear every family row, undo row and the marker of the chain and write the rebuild's
    /// intent, in one transaction fenced on the marker and record the run planned from.
    pub(super) async fn reset(
        &self,
        reason: Reason,
        attempt: i64,
        family: &FamilyMarker,
        planned: Option<&Record>,
    ) -> Result<(FamilyMarker, Revision)> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| ProjectError::database("failed to begin a family reset", error))?;
        let locked = marker::lock(&mut transaction, self.chain_id).await?;
        marker::require(
            self.chain_id,
            &locked,
            family.current.as_ref(),
            family.sequence,
        )?;
        let record = repair::lock(&mut transaction, self.chain_id).await?;
        repair::require_unchanged(self.chain_id, record.as_ref(), planned)?;
        let token = input::token_in(&mut transaction, self.chain_id).await?;
        let revision = token
            .revision()
            .ok_or_else(|| block::revision_error(self.chain_id, 0, None, &(None, None)))?;
        for table in tables::JOURNALLED
            .iter()
            .map(|table| table.name)
            .chain(tables::DERIVED)
            .chain(["project_family_undo"])
        {
            sqlx::query(&format!(
                "/* project:families.reset_{table} */ DELETE FROM {table} WHERE chain_id = $1"
            ))
            .bind(self.chain_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| ProjectError::database(format!("failed to reset {table}"), error))?;
        }
        let reset = FamilyMarker {
            sequence: locked.sequence + 1,
            bootstrap: true,
            ..FamilyMarker::default()
        };
        marker::advance(&mut transaction, self.chain_id, &reset).await?;
        repair::begin_rebuild(
            &mut transaction,
            self.chain_id,
            attempt,
            reason,
            self.target,
            &revision,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|error| ProjectError::database("failed to commit a family reset", error))?;
        Ok((reset, revision))
    }

    /// Open an undo-then-replay, fenced on the marker and record the run planned from.
    pub(super) async fn begin_undo(
        &self,
        family: &FamilyMarker,
        planned: Option<&Record>,
        repair: &NewRepair<'_>,
    ) -> Result<()> {
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                ProjectError::database("failed to begin a repair record", error)
            })?;
        let locked = marker::lock(&mut transaction, self.chain_id).await?;
        marker::require(
            self.chain_id,
            &locked,
            family.current.as_ref(),
            family.sequence,
        )?;
        let record = repair::lock(&mut transaction, self.chain_id).await?;
        repair::require_unchanged(self.chain_id, record.as_ref(), planned)?;
        repair::begin_undo(&mut transaction, self.chain_id, repair).await?;
        transaction
            .commit()
            .await
            .map_err(|error| ProjectError::database("failed to commit a repair record", error))
    }

    /// A record transition with no block or undo of its own, fenced on the marker and on the
    /// record's attempt and state.
    pub(super) async fn transition(
        &self,
        family: &FamilyMarker,
        record: &Record,
        transition: Transition,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database("failed to begin a repair transition", error)
        })?;
        let locked = marker::lock(&mut transaction, self.chain_id).await?;
        marker::require(
            self.chain_id,
            &locked,
            family.current.as_ref(),
            family.sequence,
        )?;
        let current = repair::lock(&mut transaction, self.chain_id).await?;
        repair::require_state(
            self.chain_id,
            current.as_ref(),
            record.attempt,
            record.state,
        )?;
        match transition {
            Transition::LowerTarget(target) => {
                repair::lower_undo_target(&mut transaction, self.chain_id, target).await?;
            }
            Transition::Reopen(target) => {
                repair::reopen_undo(&mut transaction, self.chain_id, target).await?;
            }
            Transition::Complete => {
                let published = locked.current.as_ref().ok_or_else(|| {
                    ProjectError::transient("a repair cannot complete without a marker")
                })?;
                repair::complete(
                    &mut transaction,
                    self.chain_id,
                    published,
                    locked.sequence,
                    &self.options.input_content_hash,
                )
                .await?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| ProjectError::database("failed to commit a repair transition", error))
    }
}

pub(super) enum Transition {
    LowerTarget(i64),
    Reopen(i64),
    Complete,
}

/// Refresh the planner statistics of every family table. A failure only costs plan quality, so
/// it is logged and the rebuild continues.
async fn analyze(pool: &sqlx::PgPool, chain_id: &str) {
    for table in tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
        .chain(["project_family_undo"])
    {
        if let Err(error) = sqlx::query(&format!(
            "/* project:families.rebuild.analyze */ ANALYZE {table}"
        ))
        .execute(pool)
        .await
        {
            tracing::warn!(
                target: "bigname_project::families",
                chain_id,
                table,
                %error,
                "could not refresh the statistics of a family table during a rebuild"
            );
        }
    }
}
