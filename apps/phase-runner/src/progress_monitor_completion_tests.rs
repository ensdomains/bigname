use super::*;
use crate::phase::{IngestCursor, PhaseResume, SourceProgress};
fn marker(number: i64, hash: &str) -> BlockMarker {
    BlockMarker::new(number, hash).unwrap()
}
fn context(phase: PhaseName, current: BlockMarker) -> PhaseContext {
    PhaseContext {
        chain_id: "chain".into(),
        phase,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Arc::from([]),
        available_heads: None,
        live_handoff: None,
        resume: PhaseResume {
            current: Some(current),
            ..PhaseResume::default()
        },
    }
}
fn assert_advancing_completion_is_quiet(pinned: &PhaseContext, progress: PhaseProgress) {
    let now = Arc::new(Mutex::new(Instant::now()));
    let clock = Arc::clone(&now);
    let tracker = RunnerPhaseProgress::with_clock(
        DEFAULT_STALE_AFTER,
        Arc::new(move || *clock.lock().unwrap()),
    );
    let observed = || tracker.observation("chain", pinned.phase, &RunMode::Normal);
    for _ in 0..2 {
        let token = tracker.begin_batch(pinned);
        tracker.record_committed(
            token,
            &PhaseBatchOutcome::Continue(PhaseProgress::default()),
        );
    }
    let token = tracker.begin_batch(pinned);
    *now.lock().unwrap() += Duration::from_secs(7);
    tracker.record_committed(token, &PhaseBatchOutcome::Complete(progress.clone()));
    assert_eq!(observed(), (2, 7));
    let token = tracker.begin_batch(pinned);
    assert_eq!(observed().0, 3);
    tracker.record_committed(token, &PhaseBatchOutcome::Complete(progress));
    assert_eq!(observed().0, 3);
}
#[test]
fn advancing_final_completion_is_quiet_until_durable_confirmation() {
    let pinned = context(PhaseName::Project, marker(1, "one"));
    assert_advancing_completion_is_quiet(
        &pinned,
        PhaseProgress {
            current: Some(marker(2, "two")),
            ..PhaseProgress::default()
        },
    );
}
#[test]
fn final_ingest_next_block_progress_is_quiet_when_markers_stay_pinned() {
    let mut pinned = context(PhaseName::Ingest, marker(9, "summary"));
    pinned.resume.ingest_cursors = Arc::from([IngestCursor {
        source_key: "source".into(),
        next_block_number: 1,
        target_block_number: Some(2),
        last_processed: Some(marker(1, "one")),
        redo_loaded_boundary: None,
    }]);
    assert_advancing_completion_is_quiet(
        &pinned,
        PhaseProgress {
            current: pinned.resume.current.clone(),
            source_progress: vec![SourceProgress {
                source_key: "source".into(),
                current: Some(marker(1, "one")),
                target: Some(marker(1, "one")),
                redo_loaded_boundary: None,
            }],
            ..PhaseProgress::default()
        },
    );
}
