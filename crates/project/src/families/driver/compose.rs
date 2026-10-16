//! The composition pass of a rebuild in ranges (docs/projections.md, "Owned key families"). The
//! ranges at or below the switch point write the family rows only. Before the first generation
//! above it, with the repair record's `composed_block` still unset, a fenced transition moves the
//! record to composing; chunks then compose the pass's streams (families/compose.rs) at the
//! marker's block, each in its own transaction and each one block of the run's budget, and a
//! fenced transition ends the pass: the record returns to rebuilding with `composed_block` set,
//! and one marker journal row under the pass block ends the undo journal there. The transition
//! that begins the pass also writes its name work list, which the names stream then pages
//! through, and the pass end drops it. The pass end spends one block of the budget too. Every generation
//! after that composes and journals as a block always did. A run that stops anywhere in the pass
//! leaves the record saying where, and the next run continues from it.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{Run, transitions::analyze_tables};
use crate::{
    Marker, ProjectError, Result,
    families::{
        FamilyOutcome, block,
        compose::{self, Stream},
        input::{self, Revision},
        manifests,
        marker::{self, Composition, FamilyMarker},
        repair::{self, Record, State},
        tables,
        undo::ENDS_JOURNAL,
    },
};

/// What one chunk of the pass composes.
enum Work<'a> {
    Names(&'a [(i64, String, compose::Reach)]),
    Pairs(Vec<(String, String)>),
    /// The catalogue window after this block; its end is planned inside the chunk.
    Window(i64),
}

impl Run<'_> {
    /// Run the composition pass, or what is left of it, before the rebuild's first generation
    /// above the switch point. Returns the record as that generation must find it, or `None`
    /// when the run's budget ran out first.
    pub(super) async fn compose(
        &mut self,
        family: &FamilyMarker,
        record: &Record,
        revision: &Revision,
        manifests: &manifests::History,
        outcome: &mut FamilyOutcome,
    ) -> Result<Option<Record>> {
        let mut record = record.clone();
        let Some(pass) = family.current.clone() else {
            // Nothing was applied in a range, so there is nothing to compose.
            let mut transaction = self.fenced(family, &record, revision).await?;
            repair::end_compose(&mut transaction, self.chain_id, -1).await?;
            commit(transaction).await?;
            return Ok(Some(ended(record, -1)));
        };
        // The manifest set is constant across a rebuild: a manifest sync is adopted by a redo,
        // never at a block. The pass composes with the set the marker recorded.
        let key = manifests.at(pass.number).key;
        if family.admission_manifests.as_deref() != Some(key.as_str()) {
            return Err(ProjectError::data_integrity(format!(
                "chain {}'s family marker at {} recorded manifest set {:?}, the run read {key}",
                self.chain_id, pass.number, family.admission_manifests
            )));
        }
        if record.state == State::Rebuilding {
            if self.budget.left == 0 {
                outcome.budget_exhausted = true;
                return Ok(None);
            }
            let mut transaction = self.fenced(family, &record, revision).await?;
            repair::begin_compose(&mut transaction, self.chain_id).await?;
            let names =
                compose::prepare_names(&mut transaction, self.chain_id, pass.number).await?;
            commit(transaction).await?;
            analyze_tables(self.pool, self.chain_id, &[tables::COMPOSE_NAME_WORK]).await;
            record.state = State::Composing;
            record.compose_stream = Some(Stream::Names.index());
            record.compose_cursor = None;
            tracing::info!(
                target: "bigname_project::families",
                chain_id = self.chain_id,
                block = pass.number,
                names,
                "the rebuild's composition pass began"
            );
        }
        let composition = Composition::recorded(family);
        let done = pass.number.to_string();
        let size = self.options.compose_chunk_keys.max(1);
        loop {
            let stream = Stream::of(record.compose_stream);
            let cursor = record.compose_cursor.clone();
            if stream == Stream::Windows && cursor.as_deref() == Some(done.as_str()) {
                break;
            }
            if self.budget.left == 0 {
                outcome.budget_exhausted = true;
                return Ok(None);
            }
            let limit = usize::try_from(self.budget.left)
                .unwrap_or(usize::MAX)
                .saturating_mul(size);
            let within_budget = match stream {
                Stream::Names => {
                    let ids =
                        compose::names(self.pool, self.chain_id, cursor.as_deref(), limit).await?;
                    let last = ids.len() < limit;
                    let mut chunks = Vec::new();
                    for chunk in ids.chunks(size) {
                        let at = chunk.last().map(|(serial, _, _)| serial.to_string());
                        chunks.push((Work::Names(chunk), at));
                    }
                    self.chunks(
                        family,
                        &mut record,
                        revision,
                        &composition,
                        chunks,
                        last,
                        outcome,
                    )
                    .await?
                }
                Stream::Nodes | Stream::RecordIds => {
                    let pairs =
                        compose::pairs(self.pool, self.chain_id, stream, cursor.as_deref(), limit)
                            .await?;
                    let last = pairs.len() < limit;
                    let mut chunks = Vec::new();
                    for chunk in pairs.chunks(size) {
                        let at = chunk.last().map(compose::pair_cursor);
                        chunks.push((Work::Pairs(chunk.to_vec()), at));
                    }
                    self.chunks(
                        family,
                        &mut record,
                        revision,
                        &composition,
                        chunks,
                        last,
                        outcome,
                    )
                    .await?
                }
                Stream::Windows => {
                    let after = match cursor.as_deref() {
                        Some(cursor) => cursor.parse().map_err(|error| {
                            ProjectError::data_integrity(format!(
                                "composition window cursor {cursor}: {error}"
                            ))
                        })?,
                        None => -1,
                    };
                    let chunks = vec![(Work::Window(after), None)];
                    self.chunks(
                        family,
                        &mut record,
                        revision,
                        &composition,
                        chunks,
                        false,
                        outcome,
                    )
                    .await?
                }
            };
            if !within_budget {
                return Ok(None);
            }
        }
        if !self.budget.take(outcome) {
            return Ok(None);
        }
        let mut transaction = self.fenced(family, &record, revision).await?;
        let header = self.pass_block(&mut transaction, &pass).await?;
        let mut image = family.journal_image();
        image[ENDS_JOURNAL] = Value::Bool(false);
        block::insert_journal(
            &mut transaction,
            self.chain_id,
            &header,
            vec![json!({"family": "marker", "key": self.chain_id, "before_image": image})],
        )
        .await?;
        compose::clear_names(&mut transaction, self.chain_id).await?;
        repair::end_compose(&mut transaction, self.chain_id, pass.number).await?;
        commit(transaction).await?;
        outcome.undo_rows += 1;
        outcome.compose_chunks += 1;
        tracing::info!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            block = pass.number,
            "the rebuild's composition pass ended"
        );
        Ok(Some(ended(record, pass.number)))
    }

    /// Commit `chunks` in order, each taking one block of the budget. The last one moves to the
    /// next stream when `last` says the stream has no keys after it; with no chunk at all that
    /// move is a step of its own, which spends nothing. After the pass's 1st, 2nd, 4th, 8th, ...
    /// chunk, counted on the repair record across runs, the composed tables' planner statistics
    /// are refreshed: they grow from empty during the pass. Returns false when the budget ran
    /// out first.
    #[allow(clippy::too_many_arguments)]
    async fn chunks(
        &mut self,
        family: &FamilyMarker,
        record: &mut Record,
        revision: &Revision,
        composition: &Composition,
        chunks: Vec<(Work<'_>, Option<String>)>,
        last: bool,
        outcome: &mut FamilyOutcome,
    ) -> Result<bool> {
        let stream = Stream::of(record.compose_stream);
        if chunks.is_empty() {
            let mut transaction = self.fenced(family, record, revision).await?;
            let next = stream.next();
            repair::compose_step(&mut transaction, self.chain_id, next.index(), None, false)
                .await?;
            commit(transaction).await?;
            record.compose_stream = Some(next.index());
            record.compose_cursor = None;
            return Ok(true);
        }
        let count = chunks.len();
        for (index, (work, at)) in chunks.into_iter().enumerate() {
            if !self.budget.take(outcome) {
                return Ok(false);
            }
            let (next, cursor) = if last && index + 1 == count {
                (stream.next(), None)
            } else {
                (stream, at)
            };
            let pass = family
                .current
                .as_ref()
                .expect("a pass with chunks has a block");
            let mut transaction = self.fenced(family, record, revision).await?;
            let mut header = self.pass_block(&mut transaction, pass).await?;
            header.journal = false;
            let mut stats = block::BlockStats::default();
            let cursor = match work {
                Work::Names(ids) => {
                    compose::names_chunk(
                        &mut transaction,
                        self.chain_id,
                        &header,
                        composition,
                        ids,
                        &mut stats,
                    )
                    .await?;
                    cursor
                }
                Work::Pairs(pairs) => {
                    compose::pairs_chunk(
                        &mut transaction,
                        self.chain_id,
                        stream,
                        composition,
                        pairs,
                    )
                    .await?;
                    cursor
                }
                Work::Window(after) => {
                    let end = compose::window_end(
                        &mut transaction,
                        self.chain_id,
                        after,
                        pass.number,
                        self.options.max_range_events,
                    )
                    .await?;
                    let mut window = input::read_block(&mut transaction, self.chain_id, end)
                        .await?
                        .ok_or_else(|| {
                            ProjectError::transient(format!(
                                "block {end} of chain {} is not readable for its catalogue window",
                                self.chain_id
                            ))
                        })?;
                    window.journal = false;
                    compose::window_chunk(
                        &mut transaction,
                        self.chain_id,
                        &window,
                        after,
                        &mut stats,
                    )
                    .await?;
                    Some(end.to_string())
                }
            };
            let committed = repair::compose_step(
                &mut transaction,
                self.chain_id,
                next.index(),
                cursor.as_deref(),
                true,
            )
            .await?;
            commit(transaction).await?;
            record.compose_stream = Some(next.index());
            record.compose_cursor = cursor;
            outcome.compose_chunks += 1;
            outcome.add(stats);
            if committed.is_power_of_two() {
                analyze_tables(self.pool, self.chain_id, &tables::COMPOSED).await;
                outcome.statistics_refreshes += 1;
            }
            tracing::debug!(
                target: "bigname_project::families",
                chain_id = self.chain_id,
                stream = ?next,
                cursor = ?record.compose_cursor,
                "committed a chunk of the rebuild's composition pass"
            );
        }
        Ok(true)
    }

    /// A pass transaction past its fences: the marker the pass composes at, generation and all,
    /// the input revision read inside the transaction, and the repair record as the run planned
    /// it, attempt, state and cursor included.
    async fn fenced(
        &self,
        family: &FamilyMarker,
        record: &Record,
        revision: &Revision,
    ) -> Result<Transaction<'static, Postgres>> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database("failed to begin a composition chunk", error)
        })?;
        let locked = marker::lock(&mut transaction, self.chain_id).await?;
        marker::require(
            self.chain_id,
            &locked,
            family.current.as_ref(),
            family.sequence,
        )?;
        let token = input::token_in(&mut transaction, self.chain_id).await?;
        let read = token.revision();
        if read.as_ref() != Some(revision) || record.prefix_revision.as_ref() != Some(revision) {
            return Err(block::revision_error(
                self.chain_id,
                family.current.as_ref().map_or(0, |marker| marker.number),
                read.as_ref(),
                revision,
            ));
        }
        let locked = repair::lock(&mut transaction, self.chain_id).await?;
        repair::require_unchanged(self.chain_id, locked.as_ref(), Some(record))?;
        Ok(transaction)
    }

    /// The pass block's lineage row, which must still be the marker's.
    async fn pass_block(
        &self,
        transaction: &mut Transaction<'static, Postgres>,
        pass: &Marker,
    ) -> Result<input::BlockHeader> {
        input::read_block(transaction, self.chain_id, pass.number)
            .await?
            .filter(|header| header.hash == pass.hash)
            .ok_or_else(|| {
                ProjectError::transient(format!(
                    "the composition pass block {} of chain {} is not readable",
                    pass.number, self.chain_id
                ))
            })
    }
}

/// The record after the pass ended at `composed_block`.
fn ended(record: Record, composed_block: i64) -> Record {
    Record {
        state: State::Rebuilding,
        composed_block: Some(composed_block),
        compose_stream: None,
        compose_cursor: None,
        ..record
    }
}

async fn commit(transaction: Transaction<'static, Postgres>) -> Result<()> {
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a composition chunk", error))
}
