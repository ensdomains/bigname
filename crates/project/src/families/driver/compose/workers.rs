//! The names stream of the composition pass on `compose_workers` workers. Each worker repeats one
//! chunk transaction: its fence (the family marker in share mode at the generation the run planned
//! from, the input revision, and the repair record as planned, read without a lock), a claim of
//! the work list's next ids no chunk has composed, skipping those another open chunk holds, then
//! the chunk with its mark (`compose::names_chunk`), and commit. A chunk writes keys of its own
//! names only, so chunks run side by side. The coordinator alone writes the repair record: one
//! short transaction counts each committed chunk. Once every worker found nothing left to claim,
//! it moves the record to the next stream under the marker `FOR UPDATE`, which waits for any open
//! chunk, after requiring every id of the list composed. One worker is the serial pass and runs on
//! the coordinator's own task and connection.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use sqlx::{PgPool, Postgres, Transaction};
use tokio::{sync::mpsc, task::JoinSet};

use super::{Run, commit, pass_block};
use crate::{
    ProjectError, Result,
    families::{
        FamilyOutcome,
        block::{self, BlockStats},
        compose::{self, Stream},
        driver::transitions::analyze_tables,
        input::{self, Revision},
        marker::{self, Composition, FamilyMarker},
        repair::{self, Record},
        tables,
    },
};

/// Why a worker stopped without an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Drained {
    /// Its claim found no id left.
    Empty,
    /// It claimed ids with no block of the run's budget left.
    Budget,
    /// Another worker or the coordinator failed.
    Stopped,
}

/// What a worker holds for the run: the generation, input revision and record it fences on.
struct Worker {
    pool: PgPool,
    chain_id: String,
    family: FamilyMarker,
    record: Record,
    revision: Revision,
    composition: Composition,
    size: usize,
    budget: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Worker {
    /// The worker's fence inside `transaction`.
    async fn fence(&self, transaction: &mut Transaction<'static, Postgres>) -> Result<()> {
        let chain_id = self.chain_id.as_str();
        let locked = marker::lock(transaction, chain_id).await?;
        marker::require(
            chain_id,
            &locked,
            self.family.current.as_ref(),
            self.family.sequence,
        )?;
        let read = input::token_in(transaction, chain_id).await?.revision();
        if read.as_ref() != Some(&self.revision)
            || self.record.prefix_revision.as_ref() != Some(&self.revision)
        {
            return Err(block::revision_error(
                chain_id,
                self.family
                    .current
                    .as_ref()
                    .map_or(0, |marker| marker.number),
                read.as_ref(),
                &self.revision,
            ));
        }
        let record = repair::read(&mut **transaction, chain_id).await?;
        repair::require_composing(chain_id, record.as_ref(), &self.record)
    }

    /// Claim, compose and commit one chunk, or say why the worker is done. The claim comes before
    /// the budget, so a drained stream spends nothing. A chunk that does not commit is rolled
    /// back before this returns, so no claim outlives the worker's transaction.
    async fn chunk(&self) -> Result<std::result::Result<BlockStats, Drained>> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database("failed to begin a composition chunk", error)
        })?;
        match self.compose(&mut transaction).await {
            Ok(Ok(stats)) => {
                commit(transaction).await?;
                Ok(Ok(stats))
            }
            Ok(Err(drained)) => {
                transaction.rollback().await.map_err(|error| {
                    ProjectError::database("failed to roll back a composition chunk", error)
                })?;
                Ok(Err(drained))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn compose(
        &self,
        transaction: &mut Transaction<'static, Postgres>,
    ) -> Result<std::result::Result<BlockStats, Drained>> {
        self.fence(transaction).await?;
        let ids = compose::claim(transaction, &self.chain_id, self.size).await?;
        if ids.is_empty() {
            return Ok(Err(Drained::Empty));
        }
        if !take(&self.budget) {
            return Ok(Err(Drained::Budget));
        }
        let pass = self
            .family
            .current
            .as_ref()
            .expect("a pass with chunks has a block");
        // Stub S2 needs a second connection beside the claim's, so it acts on pools that have one.
        if self.pool.options().get_max_connections() > 1 {
            let serials: Vec<i64> = ids.iter().map(|(serial, _, _)| *serial).collect();
            sqlx::query("UPDATE project_compose_name_work SET composed = true WHERE chain_id = $1 AND serial = ANY($2)")
            .bind(&self.chain_id).bind(&serials).execute(&mut **transaction).await
            .map_err(|error| ProjectError::database("stub S2 mark", error))?;
            let early = std::mem::replace(
                transaction,
                self.pool
                    .begin()
                    .await
                    .map_err(|error| ProjectError::database("stub S2 begin", error))?,
            );
            commit(early).await?;
            self.fence(transaction).await?;
        }
        let mut header = pass_block(transaction, &self.chain_id, pass).await?;
        header.journal = false;
        let mut stats = BlockStats::default();
        compose::names_chunk(
            transaction,
            &self.chain_id,
            &header,
            &self.composition,
            &ids,
            &mut stats,
        )
        .await?;
        Ok(Ok(stats))
    }

    /// Commit chunks until the stream is drained, the budget is spent or the pass stops, sending
    /// each committed chunk's statistics to the coordinator.
    async fn work(
        self: Arc<Self>,
        committed: mpsc::UnboundedSender<BlockStats>,
    ) -> Result<Drained> {
        while !self.stop.load(Ordering::SeqCst) {
            match self.chunk().await {
                Ok(Ok(stats)) => {
                    let _ = committed.send(stats);
                }
                Ok(Err(drained)) => return Ok(drained),
                Err(error) => {
                    self.stop.store(true, Ordering::SeqCst);
                    return Err(error);
                }
            }
        }
        Ok(Drained::Stopped)
    }
}

/// Spend one block of the run's budget, or say none is left.
fn take(budget: &AtomicU64) -> bool {
    budget
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

/// The pass's workers need a connection each and its coordinator one more. A smaller pool is
/// refused before anything is claimed: it would leave the coordinator waiting behind its own
/// workers. One worker runs on the coordinator's connection.
pub(super) fn require_pool(chain_id: &str, pool: &PgPool, workers: u16) -> Result<()> {
    let connections = pool.options().get_max_connections();
    let needed = u32::from(workers) + 1;
    if workers > 1 && connections < needed {
        return Err(ProjectError::configuration(format!(
            "chain {chain_id}'s composition pass runs {workers} workers and needs {needed} \
             database connections, one more for its coordinator, but the pool has {connections}"
        )));
    }
    Ok(())
}

impl Run<'_> {
    /// Run the names stream to its end, or until the budget runs out (false). Returns with no
    /// worker in flight, whatever happened.
    pub(super) async fn names(
        &mut self,
        family: &FamilyMarker,
        record: &mut Record,
        revision: &Revision,
        composition: &Composition,
        outcome: &mut FamilyOutcome,
    ) -> Result<bool> {
        let workers = self.options.compose_workers.max(1);
        let budget = Arc::new(AtomicU64::new(self.budget.left));
        let worker = Arc::new(Worker {
            pool: self.pool.clone(),
            chain_id: self.chain_id.to_owned(),
            family: family.clone(),
            record: record.clone(),
            revision: revision.clone(),
            composition: composition.clone(),
            size: self.options.compose_chunk_keys.max(1),
            budget: Arc::clone(&budget),
            stop: Arc::new(AtomicBool::new(false)),
        });
        let drained = if workers == 1 {
            self.serial(&worker, family, record, outcome).await
        } else {
            self.parallel(&worker, workers, family, record, outcome)
                .await
        };
        self.budget.left = budget.load(Ordering::SeqCst);
        match drained? {
            Drained::Empty => {}
            Drained::Budget | Drained::Stopped => {
                outcome.budget_exhausted = true;
                return Ok(false);
            }
        }
        let mut transaction = self.fenced(family, record, revision).await?;
        compose::require_composed(&mut transaction, self.chain_id, "names stream end").await?;
        let next = Stream::Names.next();
        repair::compose_step(&mut transaction, self.chain_id, next.index(), None, false).await?;
        commit(transaction).await?;
        record.compose_stream = Some(next.index());
        record.compose_cursor = None;
        Ok(true)
    }

    async fn serial(
        &mut self,
        worker: &Worker,
        family: &FamilyMarker,
        record: &Record,
        outcome: &mut FamilyOutcome,
    ) -> Result<Drained> {
        loop {
            match worker.chunk().await? {
                Ok(stats) => self.acknowledge(family, record, stats, outcome).await?,
                Err(drained) => return Ok(drained),
            }
        }
    }

    /// Spawn the workers, count each chunk they commit, and join every one of them before
    /// returning. The first error, a worker's or an acknowledgement's, stops the others at their
    /// next chunk and is returned once they all have. Chunks committed after a worker's error are
    /// still counted while the coordinator's own fence holds.
    async fn parallel(
        &mut self,
        worker: &Arc<Worker>,
        workers: u16,
        family: &FamilyMarker,
        record: &Record,
        outcome: &mut FamilyOutcome,
    ) -> Result<Drained> {
        let (sender, mut committed) = mpsc::unbounded_channel();
        let mut running = JoinSet::new();
        for _ in 0..workers {
            running.spawn(Arc::clone(worker).work(sender.clone()));
        }
        drop(sender);
        let mut error: Option<ProjectError> = None;
        let mut counting = true;
        let mut drained = Drained::Empty;
        let mut moved = false;
        loop {
            tokio::select! {
                Some(stats) = committed.recv() => {
                    if counting
                        && let Err(failed) = self.acknowledge(family, record, stats, outcome).await
                    {
                        worker.stop.store(true, Ordering::SeqCst);
                        counting = false;
                        error.get_or_insert(failed);
                    }
                }
                Some(joined) = running.join_next() => match joined {
                    Ok(Ok(Drained::Empty)) => {
                        if !moved {
                            moved = true;
                            let mut early = self.pool.begin().await.map_err(|error| ProjectError::database("stub S4 begin", error))?;
                            marker::lock_shared(&mut early, self.chain_id).await?;
                            repair::compose_step(&mut early, self.chain_id, Stream::Names.next().index(), None, false).await?;
                            commit(early).await?;
                        }
                    }
                    Ok(Ok(stopped)) => drained = stopped,
                    Ok(Err(failed)) => {
                        error.get_or_insert(failed);
                    }
                    Err(join) => {
                        worker.stop.store(true, Ordering::SeqCst);
                        error.get_or_insert(ProjectError::transient(format!(
                            "a composition pass worker of chain {} stopped: {join}",
                            self.chain_id
                        )));
                    }
                },
                else => break,
            }
        }
        match error {
            Some(error) => Err(error),
            None => Ok(drained),
        }
    }

    /// Count a committed chunk of the names stream on the repair record, the coordinator's own
    /// transaction. It takes the marker in share mode, as the workers do, so it waits for none
    /// of them. After the pass's 1st, 2nd, 4th, 8th, ... chunk the composed tables' statistics
    /// are refreshed.
    async fn acknowledge(
        &self,
        family: &FamilyMarker,
        record: &Record,
        stats: BlockStats,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database("failed to begin a composition acknowledgement", error)
        })?;
        let locked = marker::lock_shared(&mut transaction, self.chain_id).await?;
        marker::require(
            self.chain_id,
            &locked,
            family.current.as_ref(),
            family.sequence,
        )?;
        let locked = repair::lock(&mut transaction, self.chain_id).await?;
        repair::require_unchanged(self.chain_id, locked.as_ref(), Some(record))?;
        let committed = repair::compose_step(
            &mut transaction,
            self.chain_id,
            Stream::Names.index(),
            None,
            true,
        )
        .await?;
        commit(transaction).await?;
        outcome.compose_chunks += 1;
        outcome.add(stats);
        if committed.is_power_of_two() {
            analyze_tables(self.pool, self.chain_id, &tables::COMPOSED).await;
            outcome.statistics_refreshes += 1;
        }
        tracing::debug!(
            target: "bigname_project::families",
            chain_id = self.chain_id,
            committed,
            "committed a chunk of the rebuild's composition pass names stream"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "workers_tests.rs"]
mod workers_tests;
