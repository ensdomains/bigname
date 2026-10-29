//! The pass's stream order and the worker fence's record compare, without a database.
use super::Stream;
use crate::{
    Marker,
    families::repair::{Record, State, require_composing},
};

#[test]
fn the_streams_run_in_the_order_of_next() {
    let mut order = vec![Stream::Names];
    while let Some(last) = order.last().copied()
        && last != Stream::Catalogue
    {
        order.push(last.next());
    }
    let values: Vec<i16> = order.iter().map(|stream| stream.index()).collect();
    assert_eq!(values, [0, 1, 2, 4, 3]);
    for value in 0..=4 {
        assert_eq!(Stream::of(Some(value)).index(), value);
    }
    assert_eq!(Stream::of(Some(4)), Stream::Inventories);
    assert_eq!(Stream::of(None), Stream::Names);
    assert_eq!(Stream::Catalogue.next(), Stream::Catalogue);
}

fn composing() -> Record {
    Record {
        attempt: 7,
        reason: "operator redo".to_owned(),
        trusted_base: None,
        replay_target: Marker {
            number: 30,
            hash: "0x1e".to_owned(),
        },
        state: State::Composing,
        prefix_revision: Some((Some("h1".to_owned()), Some(1))),
        invalidation_from: None,
        pending_undo_target: None,
        completed_sequence: None,
        completed_marker: None,
        completed_input_hash: None,
        reset_sequence: Some(3),
        composed_block: None,
        compose_stream: Some(0),
        compose_cursor: None,
    }
}

#[test]
fn a_worker_fence_passes_on_the_record_it_planned_from() {
    let planned = composing();
    assert!(require_composing("chain", Some(&planned.clone()), &planned).is_ok());
}

// The coordinator moves `compose_chunks` and `updated_at` under live workers, and neither is a
// field of the record, so the fence excludes none. Every field refuses when it alone differs.
#[test]
fn a_worker_fence_refuses_a_record_that_differs_in_any_field() {
    let planned = composing();
    let changes: [(&str, fn(&mut Record)); 15] = [
        ("attempt", |record| record.attempt += 1),
        ("reason", |record| record.reason = "orphaned".to_owned()),
        ("trusted_base", |record| {
            record.trusted_base = Some(Marker {
                number: 1,
                hash: "0x01".to_owned(),
            })
        }),
        ("replay_target", |record| record.replay_target.number += 1),
        ("state", |record| record.state = State::Rebuilding),
        ("prefix_revision", |record| {
            record.prefix_revision = Some((Some("h2".to_owned()), Some(1)))
        }),
        ("invalidation_from", |record| record.invalidation_from = Some(4)),
        ("pending_undo_target", |record| {
            record.pending_undo_target = Some(4)
        }),
        ("completed_sequence", |record| {
            record.completed_sequence = Some(4)
        }),
        ("completed_marker", |record| {
            record.completed_marker = Some(Marker {
                number: 4,
                hash: "0x04".to_owned(),
            })
        }),
        ("completed_input_hash", |record| {
            record.completed_input_hash = Some("p".to_owned())
        }),
        ("reset_sequence", |record| record.reset_sequence = Some(4)),
        ("composed_block", |record| record.composed_block = Some(4)),
        ("compose_stream", |record| record.compose_stream = Some(1)),
        ("compose_cursor", |record| {
            record.compose_cursor = Some("4".to_owned())
        }),
    ];
    for (field, change) in changes {
        let mut read = planned.clone();
        change(&mut read);
        assert!(
            require_composing("chain", Some(&read), &planned).is_err(),
            "a record that differs in {field} alone is refused"
        );
    }
    assert!(require_composing("chain", None, &planned).is_err());
    let mut rebuilding = planned.clone();
    rebuilding.state = State::Rebuilding;
    assert!(
        require_composing("chain", Some(&rebuilding), &rebuilding).is_err(),
        "a worker never fences on a record that is not composing"
    );
}
