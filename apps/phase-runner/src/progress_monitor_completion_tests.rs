use super::*;
use crate::phase::PhaseResume;

fn marker(number: i64, hash: &str) -> BlockMarker {
    BlockMarker::new(number, hash).unwrap()
}

#[test]
fn advancing_final_completion_is_quiet_until_durable_confirmation() {
    let tracker = RunnerPhaseProgress::default();
    let pinned = PhaseContext {
        chain_id: "chain".into(),
        phase: PhaseName::Project,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Arc::from([]),
        available_heads: None,
        live_handoff: None,
        resume: PhaseResume {
            current: Some(marker(1, "one")),
            ..PhaseResume::default()
        },
    };
    for _ in 0..2 {
        let token = tracker.begin_batch(&pinned);
        tracker.record_committed(
            token,
            &PhaseBatchOutcome::Continue(PhaseProgress::default()),
        );
    }
    let token = tracker.begin_batch(&pinned);
    tracker.record_committed(
        token,
        &PhaseBatchOutcome::Complete(PhaseProgress {
            current: Some(marker(2, "two")),
            ..PhaseProgress::default()
        }),
    );
    assert_eq!(
        tracker.observation("chain", PhaseName::Project, &RunMode::Normal),
        (0, 0)
    );
    tracker.begin_batch(&pinned);
    assert_eq!(
        tracker
            .observation("chain", PhaseName::Project, &RunMode::Normal)
            .0,
        3
    );
}
