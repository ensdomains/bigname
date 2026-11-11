//! The cursor of the event count and leads streams, without a database: each partition once, in
//! order, the last one never written as a cursor, and a resume under another split or server
//! version refused.
use super::{Step, Steps, next};
use crate::families::{
    MAX_COMPOSE_BUILDER_BLOCK_SPANS,
    compose::{counts, leads},
    history_catalogue::build::Layout,
};

const LAYOUT: Layout = Layout {
    spans: 3,
    partitions: 2,
};
const VERSION: i32 = 170_004;

/// Every partition of a stream of `steps` under `layout`, in order, from no cursor to the last.
fn walk(steps: Steps, layout: Layout) -> Vec<Step> {
    let mut walked = Vec::new();
    let mut text: Option<String> = None;
    loop {
        let step = next(text.as_deref(), steps, layout, VERSION, "stream").expect("the next one");
        walked.push(step);
        if step.last(steps) {
            return walked;
        }
        text = Some(step.text(steps));
    }
}

fn named(steps: Steps, walked: &[Step]) -> Vec<(&'static str, u16)> {
    walked
        .iter()
        .map(|step| (step.name(steps), step.part))
        .collect()
}

#[test]
fn the_event_count_stream_reads_every_span_then_merges_every_partition() {
    assert_eq!(
        named(counts::STEPS, &walk(counts::STEPS, LAYOUT)),
        [
            ("spans", 0),
            ("spans", 1),
            ("spans", 2),
            ("merge", 0),
            ("merge", 1)
        ]
    );
    // S + K transactions.
    let default = Layout {
        spans: 64,
        partitions: 16,
    };
    assert_eq!(walk(counts::STEPS, default).len(), 64 + 16);
}

#[test]
fn the_leads_stream_runs_every_partition_once() {
    assert_eq!(
        named(leads::STEPS, &walk(leads::STEPS, LAYOUT)),
        [("leads", 0), ("leads", 1)]
    );
    let one = Layout {
        spans: 1,
        partitions: 1,
    };
    let walked = walk(leads::STEPS, one);
    assert_eq!(walked.len(), 1);
    assert!(walked[0].last(leads::STEPS));
}

#[test]
fn a_cursor_round_trips_and_a_resume_under_another_split_or_server_is_refused() {
    let first = next(None, counts::STEPS, LAYOUT, VERSION, "event count stream").unwrap();
    let text = first.text(counts::STEPS);
    let second = next(
        Some(&text),
        counts::STEPS,
        LAYOUT,
        VERSION,
        "event count stream",
    )
    .unwrap();
    assert_eq!((second.index, second.part), (0, 1));
    let other = Layout {
        spans: 4,
        partitions: 2,
    };
    let refused = next(
        Some(&text),
        counts::STEPS,
        other,
        VERSION,
        "event count stream",
    )
    .expect_err("another split is refused");
    assert!(refused.to_string().contains("(3, 2)"), "{refused}");
    next(
        Some(&text),
        counts::STEPS,
        LAYOUT,
        VERSION + 1,
        "event count stream",
    )
    .expect_err("another server version is refused");
    // A cursor of another stream's step, or the last partition's, was never written.
    let foreign =
        r#"{"step":"keys","part":0,"spans":3,"partitions":2,"server_version_num":170004}"#;
    next(
        Some(foreign),
        counts::STEPS,
        LAYOUT,
        VERSION,
        "event count stream",
    )
    .expect_err("a catalogue build cursor is not an event count cursor");
    let last = walk(counts::STEPS, LAYOUT).pop().unwrap();
    next(
        Some(&last.text(counts::STEPS)),
        counts::STEPS,
        LAYOUT,
        VERSION,
        "event count stream",
    )
    .expect_err("the last partition moves the pass on and is never a cursor");
}

#[test]
fn a_first_partition_is_refused_for_a_split_past_the_bounds() {
    for layout in [
        Layout {
            spans: 0,
            partitions: 2,
        },
        Layout {
            spans: MAX_COMPOSE_BUILDER_BLOCK_SPANS + 1,
            partitions: 2,
        },
        Layout {
            spans: 3,
            partitions: 3,
        },
    ] {
        next(None, leads::STEPS, layout, VERSION, "leads stream")
            .expect_err("a split past the bounds is refused");
    }
}

#[test]
fn the_spans_cover_the_blocks_to_the_pass_without_overlap() {
    for (pass, spans) in [(0_i64, 1_u16), (10, 3), (299, 64), (5, 8)] {
        let mut covered = Vec::new();
        for span in 0..spans {
            let (from, to) = counts::bounds(pass, span, spans);
            covered.extend(from..to);
        }
        assert_eq!(covered, (0..=pass).collect::<Vec<_>>(), "{pass} in {spans}");
    }
}
