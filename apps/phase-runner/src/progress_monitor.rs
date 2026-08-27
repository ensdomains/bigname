use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tracing::warn;

use crate::{
    heads::BlockMarker,
    phase::{
        IngestCursor, PhaseBatchOutcome, PhaseContext, PhaseName, PhaseProgress, RedoAttemptFence,
        RunMode,
    },
};

const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(900);

#[derive(Clone)]
pub struct RunnerPhaseProgress {
    inner: Arc<Inner>,
}

struct Inner {
    states: Mutex<BTreeMap<ProgressKey, ProgressState>>,
    stale_after: Duration,
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProgressKey {
    chain: String,
    phase: PhaseName,
    mode: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CursorIdentity {
    current: Option<BlockMarker>,
    ingest: Vec<IngestCursorIdentity>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IngestCursorIdentity {
    source_key: String,
    next_block_number: i64,
    last_processed: Option<(i64, String)>,
    redo_loaded_boundary: Option<(i64, String)>,
}

#[derive(Clone)]
struct PendingBatch {
    starting_cursor: CursorIdentity,
    committed_at: Instant,
    quiet_until_confirmed: bool,
}

#[derive(Default)]
struct ProgressState {
    epoch: Option<RedoAttemptFence>,
    last_cursor: Option<CursorIdentity>,
    pending: Option<PendingBatch>,
    confirmed: i64,
    first_pinned_commit: Option<Instant>,
    last_successful_commit: Option<Instant>,
}

pub(crate) struct ProgressToken {
    key: ProgressKey,
    epoch: Option<RedoAttemptFence>,
    starting_cursor: CursorIdentity,
}

pub(crate) struct ProgressSnapshot {
    pub(crate) chain: String,
    pub(crate) phase: PhaseName,
    pub(crate) mode: &'static str,
    pub(crate) batches: i64,
    pub(crate) age_seconds: i64,
}

impl Default for RunnerPhaseProgress {
    fn default() -> Self {
        Self::new(DEFAULT_STALE_AFTER)
    }
}

impl RunnerPhaseProgress {
    pub fn new(stale_after: Duration) -> Self {
        assert!(!stale_after.is_zero(), "progress expiry must be positive");
        Self::with_clock(stale_after, Arc::new(Instant::now))
    }

    fn with_clock(stale_after: Duration, now: Arc<dyn Fn() -> Instant + Send + Sync>) -> Self {
        Self {
            inner: Arc::new(Inner {
                states: Mutex::new(BTreeMap::new()),
                stale_after,
                now,
            }),
        }
    }

    pub fn seed_chain(&self, chain: &str) {
        let mut states = self.states();
        for phase in PhaseName::ALL {
            for mode in ["normal", "redo", "recompute_flags"] {
                states
                    .entry(ProgressKey {
                        chain: chain.to_owned(),
                        phase,
                        mode,
                    })
                    .or_default();
            }
        }
    }

    pub fn observation(&self, chain: &str, phase: PhaseName, mode: &RunMode) -> (i64, i64) {
        self.snapshot()
            .into_iter()
            .find(|sample| {
                sample.chain == chain && sample.phase == phase && sample.mode == mode.as_str()
            })
            .map(|sample| (sample.batches, sample.age_seconds))
            .unwrap_or((0, 0))
    }

    pub(crate) fn begin_batch(&self, context: &PhaseContext) -> ProgressToken {
        let now = self.now();
        let key = ProgressKey {
            chain: context.chain_id.clone(),
            phase: context.phase,
            mode: context.mode.as_str(),
        };
        let epoch = context.redo_attempt;
        let cursor = CursorIdentity::from_context(context);
        let mut warning = None;
        {
            let mut states = self.states();
            let state = states.entry(key.clone()).or_default();
            if state.epoch != epoch {
                state.reset();
                state.epoch = epoch;
            }
            state.expire(now, self.inner.stale_after);
            if let Some(pending) = state.pending.take() {
                if cursor == pending.starting_cursor {
                    state.confirmed = state.confirmed.saturating_add(1);
                    state
                        .first_pinned_commit
                        .get_or_insert(pending.committed_at);
                    if matches!(state.confirmed, 2 | 3) {
                        warning = Some((
                            state.confirmed,
                            elapsed_seconds(state.first_pinned_commit, now),
                            format!("{cursor:?}"),
                        ));
                    }
                } else {
                    state.reset();
                    state.epoch = epoch;
                }
            }
            state.last_cursor = Some(cursor.clone());
        }
        if let Some((batches, age, cursor_summary)) = warning {
            warn!(
                chain_id = key.chain,
                phase = %key.phase,
                mode = key.mode,
                cursor_summary,
                batches_since_cursor_advance = batches,
                cursor_stall_age_seconds = age,
                "phase batches committed without durable cursor progress"
            );
        }
        ProgressToken {
            key,
            epoch,
            starting_cursor: cursor,
        }
    }

    pub(crate) fn record_committed(&self, token: ProgressToken, outcome: &PhaseBatchOutcome) {
        let now = self.now();
        let work_bearing = is_work_bearing(&token, outcome);
        let mut states = self.states();
        let state = states.entry(token.key).or_default();
        if state.epoch != token.epoch {
            state.reset();
            state.epoch = token.epoch;
        }
        if work_bearing {
            let quiet_until_confirmed = matches!(outcome, PhaseBatchOutcome::Complete(progress)
                if progress.current != token.starting_cursor.current);
            state.pending = Some(PendingBatch {
                starting_cursor: token.starting_cursor,
                committed_at: now,
                quiet_until_confirmed,
            });
            state.last_successful_commit = Some(now);
        } else {
            state.reset();
            state.epoch = token.epoch;
        }
    }

    pub(crate) fn clear_phase(&self, chain: &str, phase: PhaseName) {
        for (key, state) in self.states().iter_mut() {
            if key.chain == chain && key.phase == phase {
                let epoch = state.epoch;
                state.reset();
                state.epoch = epoch;
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<ProgressSnapshot> {
        let now = self.now();
        self.states()
            .iter_mut()
            .map(|(key, state)| {
                state.expire(now, self.inner.stale_after);
                let quiet = state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.quiet_until_confirmed);
                ProgressSnapshot {
                    chain: key.chain.clone(),
                    phase: key.phase,
                    mode: key.mode,
                    batches: if quiet { 0 } else { state.confirmed },
                    age_seconds: if quiet {
                        0
                    } else {
                        elapsed_seconds(state.first_pinned_commit, now)
                    },
                }
            })
            .collect()
    }

    fn now(&self) -> Instant {
        (self.inner.now)()
    }

    fn states(&self) -> std::sync::MutexGuard<'_, BTreeMap<ProgressKey, ProgressState>> {
        self.inner
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ProgressState {
    fn reset(&mut self) {
        self.last_cursor = None;
        self.pending = None;
        self.confirmed = 0;
        self.first_pinned_commit = None;
        self.last_successful_commit = None;
    }

    fn expire(&mut self, now: Instant, stale_after: Duration) {
        if self
            .last_successful_commit
            .is_some_and(|last| now.saturating_duration_since(last) > stale_after)
        {
            let epoch = self.epoch;
            self.reset();
            self.epoch = epoch;
        }
    }
}

impl CursorIdentity {
    fn from_context(context: &PhaseContext) -> Self {
        let mut ingest = if context.phase == PhaseName::Ingest {
            context
                .resume
                .ingest_cursors
                .iter()
                .map(IngestCursorIdentity::from)
                .collect()
        } else {
            Vec::new()
        };
        ingest.sort();
        Self {
            current: context.resume.current.clone(),
            ingest,
        }
    }
}

impl From<&IngestCursor> for IngestCursorIdentity {
    fn from(cursor: &IngestCursor) -> Self {
        Self {
            source_key: cursor.source_key.clone(),
            next_block_number: cursor.next_block_number,
            last_processed: marker_tuple(cursor.last_processed.as_ref()),
            redo_loaded_boundary: marker_tuple(cursor.redo_loaded_boundary.as_ref()),
        }
    }
}

fn marker_tuple(marker: Option<&BlockMarker>) -> Option<(i64, String)> {
    marker.map(|marker| (marker.number, marker.hash.clone()))
}

fn is_work_bearing(token: &ProgressToken, outcome: &PhaseBatchOutcome) -> bool {
    match outcome {
        PhaseBatchOutcome::Continue(_) => true,
        PhaseBatchOutcome::Idle(_) => false,
        PhaseBatchOutcome::Complete(progress) => {
            !(progress_is_empty(progress)
                || token.key.phase == PhaseName::Live
                    && token.key.mode == "normal"
                    && progress.current == progress.target)
        }
    }
}

fn progress_is_empty(progress: &PhaseProgress) -> bool {
    progress.current.is_none()
        && progress
            .source_progress
            .iter()
            .all(|source| source.current.is_none() && source.redo_loaded_boundary.is_none())
}

fn elapsed_seconds(since: Option<Instant>, now: Instant) -> i64 {
    since
        .map(|since| {
            i64::try_from(now.saturating_duration_since(since).as_secs()).unwrap_or(i64::MAX)
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase::{BlockRange, PhaseResume, SourceProgress};

    #[derive(Clone)]
    struct Clock(Arc<Mutex<Instant>>);

    impl Clock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Instant::now())))
        }

        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().unwrap();
            *now += duration;
        }
    }

    fn tracker(clock: &Clock) -> RunnerPhaseProgress {
        let clock = clock.clone();
        RunnerPhaseProgress::with_clock(Duration::from_secs(900), Arc::new(move || clock.now()))
    }

    fn marker(number: i64, hash: &str) -> BlockMarker {
        BlockMarker::new(number, hash).unwrap()
    }

    fn context(phase: PhaseName, mode: RunMode, current: Option<BlockMarker>) -> PhaseContext {
        let redo_attempt = mode.range().map(|execution_range| RedoAttemptFence {
            generation: 1,
            execution_range,
        });
        PhaseContext {
            chain_id: "chain".into(),
            phase,
            mode,
            redo_attempt,
            sources: Arc::from([]),
            available_heads: None,
            live_handoff: None,
            resume: PhaseResume {
                current,
                ..PhaseResume::default()
            },
        }
    }

    fn work(current: Option<BlockMarker>) -> PhaseBatchOutcome {
        PhaseBatchOutcome::Continue(PhaseProgress {
            current,
            ..PhaseProgress::default()
        })
    }

    fn commit(tracker: &RunnerPhaseProgress, context: &PhaseContext, outcome: PhaseBatchOutcome) {
        let token = tracker.begin_batch(context);
        tracker.record_committed(token, &outcome);
    }

    fn observed(tracker: &RunnerPhaseProgress, phase: PhaseName, mode: &RunMode) -> (i64, i64) {
        tracker.observation("chain", phase, mode)
    }

    #[test]
    fn unchanged_commits_are_confirmed_one_batch_late() {
        let tracker = tracker(&Clock::new());
        let pinned = context(PhaseName::Interpret, RunMode::Normal, None);
        commit(&tracker, &pinned, work(Some(marker(9, "reported-advance"))));
        assert_eq!(
            observed(&tracker, PhaseName::Interpret, &RunMode::Normal),
            (0, 0)
        );

        for expected in 1..=3 {
            commit(&tracker, &pinned, work(None));
            assert_eq!(
                observed(&tracker, PhaseName::Interpret, &RunMode::Normal).0,
                expected
            );
        }
    }

    #[test]
    fn durable_number_hash_and_backward_changes_reset_but_target_does_not() {
        let tracker = tracker(&Clock::new());
        let mut pinned = context(PhaseName::Project, RunMode::Normal, Some(marker(5, "a")));
        commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        pinned.resume.target = Some(marker(10, "target-a"));
        let token = tracker.begin_batch(&pinned);
        assert_eq!(
            observed(&tracker, PhaseName::Project, &RunMode::Normal).0,
            1
        );
        tracker.record_committed(token, &work(pinned.resume.current.clone()));

        for moved in [marker(5, "b"), marker(4, "older"), marker(6, "newer")] {
            pinned.resume.current = Some(moved);
            tracker.begin_batch(&pinned);
            assert_eq!(
                observed(&tracker, PhaseName::Project, &RunMode::Normal),
                (0, 0)
            );
            commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        }
    }

    #[test]
    fn ingest_source_and_redo_boundary_movement_reset_a_pinned_summary() {
        let tracker = tracker(&Clock::new());
        let mut ingest = context(
            PhaseName::Ingest,
            RunMode::Normal,
            Some(marker(7, "summary")),
        );
        ingest.resume.ingest_cursors = Arc::from([IngestCursor {
            source_key: "source".into(),
            next_block_number: 2,
            target_block_number: Some(10),
            last_processed: Some(marker(1, "one")),
            redo_loaded_boundary: None,
        }]);
        commit(&tracker, &ingest, work(ingest.resume.current.clone()));
        commit(&tracker, &ingest, work(ingest.resume.current.clone()));
        assert_eq!(observed(&tracker, PhaseName::Ingest, &RunMode::Normal).0, 1);

        Arc::make_mut(&mut ingest.resume.ingest_cursors)[0].next_block_number = 3;
        tracker.begin_batch(&ingest);
        assert_eq!(
            observed(&tracker, PhaseName::Ingest, &RunMode::Normal),
            (0, 0)
        );
        commit(&tracker, &ingest, work(ingest.resume.current.clone()));
        Arc::make_mut(&mut ingest.resume.ingest_cursors)[0].redo_loaded_boundary =
            Some(marker(2, "boundary"));
        tracker.begin_batch(&ingest);
        assert_eq!(
            observed(&tracker, PhaseName::Ingest, &RunMode::Normal),
            (0, 0)
        );
    }

    #[test]
    fn redo_generation_or_execution_range_change_resets() {
        let tracker = tracker(&Clock::new());
        let mut redo = context(
            PhaseName::Interpret,
            RunMode::Redo(BlockRange::new(1, 9).unwrap()),
            Some(marker(1, "one")),
        );
        commit(&tracker, &redo, work(redo.resume.current.clone()));
        commit(&tracker, &redo, work(redo.resume.current.clone()));
        assert_eq!(observed(&tracker, PhaseName::Interpret, &redo.mode).0, 1);
        redo.redo_attempt.as_mut().unwrap().generation = 2;
        tracker.begin_batch(&redo);
        assert_eq!(observed(&tracker, PhaseName::Interpret, &redo.mode), (0, 0));

        commit(&tracker, &redo, work(redo.resume.current.clone()));
        commit(&tracker, &redo, work(redo.resume.current.clone()));
        redo.redo_attempt.as_mut().unwrap().execution_range = BlockRange::new(2, 9).unwrap();
        tracker.begin_batch(&redo);
        assert_eq!(observed(&tracker, PhaseName::Interpret, &redo.mode), (0, 0));
    }

    #[test]
    fn empty_idle_and_caught_up_live_outcomes_clear_evidence() {
        let tracker = tracker(&Clock::new());
        let mut live = context(PhaseName::Live, RunMode::Normal, Some(marker(5, "five")));
        commit(&tracker, &live, work(live.resume.current.clone()));
        commit(&tracker, &live, work(live.resume.current.clone()));
        assert_eq!(observed(&tracker, PhaseName::Live, &RunMode::Normal).0, 1);

        for outcome in [
            PhaseBatchOutcome::Idle(PhaseProgress::default()),
            PhaseBatchOutcome::Complete(PhaseProgress::default()),
            PhaseBatchOutcome::Complete(PhaseProgress {
                current: live.resume.current.clone(),
                target: live.resume.current.clone(),
                ..PhaseProgress::default()
            }),
        ] {
            let token = tracker.begin_batch(&live);
            tracker.record_committed(token, &outcome);
            assert_eq!(
                observed(&tracker, PhaseName::Live, &RunMode::Normal),
                (0, 0)
            );
            commit(&tracker, &live, work(live.resume.current.clone()));
            commit(&tracker, &live, work(live.resume.current.clone()));
        }
        live.resume.target = Some(marker(6, "six"));
        commit(&tracker, &live, work(live.resume.current.clone()));
        assert!(observed(&tracker, PhaseName::Live, &RunMode::Normal).0 >= 1);
    }

    #[test]
    fn capacity_clear_expiry_and_error_without_commit_do_not_accumulate() {
        let clock = Clock::new();
        let tracker = tracker(&clock);
        let pinned = context(PhaseName::Verify, RunMode::Normal, Some(marker(1, "one")));
        commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        let unused_error_token = tracker.begin_batch(&pinned);
        assert_eq!(observed(&tracker, PhaseName::Verify, &RunMode::Normal).0, 1);
        drop(unused_error_token);
        tracker.begin_batch(&pinned);
        assert_eq!(observed(&tracker, PhaseName::Verify, &RunMode::Normal).0, 1);

        commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        clock.advance(Duration::from_secs(901));
        assert_eq!(
            observed(&tracker, PhaseName::Verify, &RunMode::Normal),
            (0, 0)
        );
        commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        commit(&tracker, &pinned, work(pinned.resume.current.clone()));
        tracker.clear_phase("chain", PhaseName::Verify);
        assert_eq!(
            observed(&tracker, PhaseName::Verify, &RunMode::Normal),
            (0, 0)
        );
    }

    #[test]
    fn source_progress_makes_an_ingest_completion_work_bearing() {
        let tracker = tracker(&Clock::new());
        let ingest = context(PhaseName::Ingest, RunMode::Normal, None);
        let token = tracker.begin_batch(&ingest);
        tracker.record_committed(
            token,
            &PhaseBatchOutcome::Complete(PhaseProgress {
                source_progress: vec![SourceProgress {
                    source_key: "source".into(),
                    current: Some(marker(1, "one")),
                    target: None,
                    redo_loaded_boundary: None,
                }],
                ..PhaseProgress::default()
            }),
        );
        tracker.begin_batch(&ingest);
        assert_eq!(observed(&tracker, PhaseName::Ingest, &RunMode::Normal).0, 1);
    }
}

#[cfg(test)]
#[path = "progress_monitor_completion_tests.rs"]
mod completion_tests;
