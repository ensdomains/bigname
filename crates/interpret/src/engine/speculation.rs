//! Read and prepare a bounded window of complete batches, then validate one at its turn.
use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use tokio::sync::{Mutex, Semaphore, watch};

use super::{BatchRequest, Engine, FullStateReason, Marker, RunMode, StateLoader};
use crate::{InterpretError, Result, load};

#[path = "speculation_stats.rs"]
mod stats;
pub use stats::SpeculationStats;

pub(super) struct Speculation {
    pub(super) workers: NonZeroU32,
    pub(super) queue: Mutex<Queue>,
    permits: Arc<Semaphore>,
    counters: Arc<stats::Counters>,
}

impl Speculation {
    pub(super) fn new(workers: NonZeroU32) -> Self {
        Self {
            workers,
            queue: Mutex::new(Queue::default()),
            permits: Arc::new(Semaphore::new(workers.get() as usize)),
            counters: Arc::default(),
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.workers.get() > 1
    }

    pub(super) fn clear(&mut self) {
        self.queue.get_mut().clear();
    }

    pub(super) fn stats(&self) -> SpeculationStats {
        self.counters.snapshot()
    }

    pub(super) fn log_success(&self, chain_id: &str, from_block: i64, to_block: i64) {
        if !self.enabled() {
            return;
        }
        let stats = self.stats();
        tracing::debug!(
            chain_id,
            from_block,
            to_block,
            prepared = stats.prepared,
            accepted = stats.accepted,
            retried = stats.retried,
            fallback = stats.fallback,
            validation_ms = stats.validation_nanoseconds / 1_000_000,
            peak_active_workers = stats.peak_active_workers,
            "interpret speculative preparation totals after ordered batch"
        );
    }
}

#[derive(Default)]
pub(super) struct Queue {
    pass: Option<Pass>,
    expected_resume: Option<Marker>,
    batches: VecDeque<Candidate>,
}

#[derive(Eq, PartialEq)]
struct Pass {
    chain_id: String,
    from_block: i64,
    to_block: i64,
    mode: RunMode,
}

struct Candidate {
    from_block: i64,
    to_block: i64,
    result: Result<load::lookahead::Attempt>,
}

impl Queue {
    pub(super) fn begin(&mut self, request: &BatchRequest) {
        let pass = Pass {
            chain_id: request.chain_id.clone(),
            from_block: request.from_block,
            to_block: request.to_block,
            mode: request.mode,
        };
        if self.pass.as_ref() != Some(&pass) || self.expected_resume != request.resume_current {
            self.clear();
            self.pass = Some(pass);
            self.expected_resume = request.resume_current.clone();
        }
    }

    pub(super) fn clear(&mut self) {
        self.batches.clear();
        self.pass = None;
        self.expected_resume = None;
    }

    pub(super) fn advance(&mut self, current: &Marker, complete: bool) {
        if complete {
            self.clear();
        } else {
            self.expected_resume = Some(current.clone());
        }
    }
}

impl Engine {
    pub(super) async fn speculative_input(
        &self,
        queue: &mut Queue,
        request: &BatchRequest,
        markers: &[(i64, String)],
    ) -> Result<Option<load::lookahead::Attempt>> {
        let counters = &self.speculation.counters;
        if self.force_full_state_loader {
            queue.batches.clear();
            counters.fallback.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(load::lookahead::Attempt::FullStateRequired(
                StateLoader::FullState {
                    reason: FullStateReason::OperatorOverride,
                },
            )));
        }
        let from_block = markers.first().expect("non-empty batch markers").0;
        let to_block = markers.last().expect("non-empty batch markers").0;
        if queue.batches.front().is_some_and(|candidate| {
            candidate.from_block != from_block || candidate.to_block != to_block
        }) {
            queue.batches.clear();
        }
        if queue.batches.is_empty()
            && let Err(error) = self.prepare_window(queue, request, markers).await
        {
            counters.fallback.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(chain_id = request.chain_id, %error, "speculative preparation unavailable; loading the current batch serially");
            return Ok(None);
        }
        let candidate = queue.batches.pop_front().expect("prepared current batch");
        match candidate.result {
            Ok(load::lookahead::Attempt::Loaded(loaded)) => {
                let started = Instant::now();
                let valid = load::lookahead::validate_speculative(
                    &self.pool,
                    &loaded,
                    request
                        .resume_current
                        .as_ref()
                        .map(|marker| (marker.number, marker.hash.as_str())),
                    self.lookahead_statement_timeout_secs,
                )
                .await;
                counters.validations.fetch_add(1, Ordering::Relaxed);
                counters.validation_nanoseconds.fetch_add(
                    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                match valid {
                    Ok(true) => {
                        counters.accepted.fetch_add(1, Ordering::Relaxed);
                        Ok(Some(load::lookahead::Attempt::Loaded(loaded)))
                    }
                    result => {
                        counters.retried.fetch_add(1, Ordering::Relaxed);
                        if let Err(error) = result {
                            tracing::debug!(chain_id = request.chain_id, %error, "speculative validation failed; reloading the batch serially");
                        }
                        Ok(None)
                    }
                }
            }
            Ok(load::lookahead::Attempt::FullStateRequired(choice)) => {
                queue.batches.clear();
                counters.fallback.fetch_add(1, Ordering::Relaxed);
                Ok(Some(load::lookahead::Attempt::FullStateRequired(choice)))
            }
            Err(error) => {
                counters.fallback.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(chain_id = request.chain_id, %error, "speculative interpretation failed; reloading the batch serially");
                Ok(None)
            }
        }
    }

    async fn prepare_window(
        &self,
        queue: &mut Queue,
        request: &BatchRequest,
        current: &[(i64, String)],
    ) -> Result<()> {
        let batch_size = self.blocks_per_batch.get() as usize;
        let current_end = current.last().expect("non-empty batch markers").0;
        let mut markers = current.to_vec();
        if current_end < request.to_block {
            let additional = i64::from(self.blocks_per_batch.get())
                .saturating_mul(i64::from(self.speculation.workers.get() - 1));
            let future = load::canonical_markers(
                &self.pool,
                &request.chain_id,
                current_end.saturating_add(1),
                request.to_block,
                additional,
            )
            .await?;
            super::validate_contiguous_markers(
                &request.chain_id,
                current_end.saturating_add(1),
                &future,
            )?;
            markers.extend(future);
        }
        let runtime = tokio::runtime::Handle::current();
        // Closing this sender asks dropped windows' workers to stop at their next await.
        // Running CPU work still owns its permit until it returns, even after cancellation.
        let (_cancel_window, cancellation) = watch::channel(());
        let mut jobs = Vec::new();
        let mut resume = request.resume_current.clone();
        for batch in markers.chunks(batch_size) {
            let (from_block, _) = batch.first().expect("non-empty marker chunk");
            let (to_block, hash) = batch.last().expect("non-empty marker chunk");
            let (from_block, to_block) = (*from_block, *to_block);
            let permit = Arc::clone(&self.speculation.permits)
                .acquire_owned()
                .await
                .map_err(|_| InterpretError::transient("interpret worker permits closed"))?;
            let pool = self.pool.clone();
            let chain_id = request.chain_id.clone();
            let capacity = self.state_cache_capacity;
            let timeout = self.lookahead_statement_timeout_secs;
            let runtime = runtime.clone();
            let counters = Arc::clone(&self.speculation.counters);
            let mut cancellation = cancellation.clone();
            let predecessor = resume;
            let job = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _active = counters.enter();
                let result = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancellation.changed() => Err(InterpretError::transient("speculative interpretation was cancelled")),
                        result = load::lookahead::speculative_batch_input(
                            &pool,
                            &chain_id,
                            from_block,
                            to_block,
                            predecessor.as_ref().map(|marker| (marker.number, marker.hash.as_str())),
                            capacity,
                            timeout,
                        ) => result,
                    }
                });
                if matches!(&result, Ok(load::lookahead::Attempt::Loaded(_))) {
                    counters.prepared.fetch_add(1, Ordering::Relaxed);
                }
                result
            });
            jobs.push((from_block, to_block, job));
            resume = Some(Marker {
                number: to_block,
                hash: hash.clone(),
            });
        }
        for (from_block, to_block, job) in jobs {
            let result = job.await.unwrap_or_else(|error| {
                Err(InterpretError::transient(format!(
                    "speculative interpretation worker failed: {error}"
                )))
            });
            queue.batches.push_back(Candidate {
                from_block,
                to_block,
                result,
            });
        }
        Ok(())
    }
}
