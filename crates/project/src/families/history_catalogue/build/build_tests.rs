//! The catalogue build's cursor, partition order and persisted relations, without a database.
use uuid::Uuid;

use super::{Cursor, Layout, PersistedRelation, STEPS, Step, analyzed, is_done, next, spans};
use crate::families::{FamilyOptions, MAX_COMPOSE_BUILDER_BLOCK_SPANS};

const LAYOUT: Layout = Layout {
    spans: 3,
    partitions: 2,
};

/// Every partition of a build of `layout`, in order, from no cursor to the terminal one.
fn walk(layout: Layout) -> Vec<Cursor> {
    let mut cursors = Vec::new();
    let mut text: Option<String> = None;
    while !is_done(text.as_deref()).expect("a cursor the build wrote parses") {
        let cursor = next(text.as_deref(), layout, 170_004).expect("the next partition");
        text = Some(cursor.text());
        cursors.push(cursor);
    }
    cursors
}

#[test]
fn the_build_runs_every_partition_of_every_step_once_in_order() {
    let cursors = walk(LAYOUT);
    let seen: Vec<(&str, u16)> = cursors
        .iter()
        .map(|cursor| (cursor.step.name(), cursor.part))
        .collect();
    assert_eq!(
        seen,
        [
            ("keys", 0),
            ("spans", 0),
            ("spans", 1),
            ("spans", 2),
            ("edges", 0),
            ("edges", 1),
            ("sources", 0),
            ("sources", 1),
            ("edge_bounds", 0),
            ("edge_bounds", 1),
            ("name_anchors", 0),
            ("name_anchors", 1),
            ("resource_anchors", 0),
            ("resource_anchors", 1),
            ("done", 0),
        ]
    );
    // 1 + S + 5K + 1 transactions.
    assert_eq!(
        walk(Layout {
            spans: 64,
            partitions: 16
        })
        .len(),
        1 + 64 + 5 * 16 + 1
    );
    let analyzing: Vec<(&str, &[&str])> = cursors
        .iter()
        .filter(|cursor| !analyzed(cursor).is_empty())
        .map(|cursor| (cursor.step.name(), analyzed(cursor)))
        .collect();
    assert_eq!(
        analyzing,
        [
            ("spans", &["project_compose_history_work"][..]),
            ("edges", &["project_history_source_edge"][..]),
            ("sources", &["project_history_source"][..]),
            ("edge_bounds", &["project_history_source_edge"][..]),
            ("resource_anchors", &["project_address_history_anchor"][..]),
        ]
    );
}

#[test]
fn a_build_resumed_with_other_partitions_or_on_another_server_is_refused() {
    let cursor = Cursor {
        step: Step::Sources,
        part: 1,
        layout: LAYOUT,
        server_version_num: 170_004,
    };
    let text = cursor.text();
    let fields: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(
        fields,
        serde_json::json!({"step": "sources", "part": 1, "spans": 3, "partitions": 2,
                           "server_version_num": 170_004})
    );
    assert_eq!(Cursor::parse(&text).ok(), Some(cursor));
    assert_eq!(
        next(Some(&text), LAYOUT, 170_004).map(|c| c.step).ok(),
        Some(Step::EdgeBounds)
    );
    for (layout, version) in [
        (
            Layout {
                spans: 4,
                partitions: 2,
            },
            170_004,
        ),
        (
            Layout {
                spans: 3,
                partitions: 4,
            },
            170_004,
        ),
        (LAYOUT, 170_005),
    ] {
        let error = next(Some(&text), layout, version)
            .expect_err("refused")
            .to_string();
        assert!(
            error.contains("(3, 2) on server version 170004")
                && error.contains(&format!(
                    "({}, {}) on server version {version}",
                    layout.spans, layout.partitions
                )),
            "{error}"
        );
    }
    assert!(next(Some("7"), LAYOUT, 170_004).is_err());
    assert!(is_done(Some("not json")).is_err());
}

#[test]
fn the_block_spans_cover_every_block_to_the_pass_once() {
    for pass in [0, 1, 5, 299, 300, 24_000_000] {
        for spans_count in [1, 2, 3, 64, 1000] {
            let mut expected = 0;
            for span in 0..spans_count {
                let (from, to) = spans::bounds(pass, span, spans_count);
                assert_eq!(
                    from, expected,
                    "pass {pass}, {spans_count} spans, span {span}"
                );
                assert!(to >= from);
                expected = to;
            }
            assert_eq!(expected, pass + 1, "pass {pass}, {spans_count} spans");
        }
    }
}

#[test]
fn a_persisted_relation_reads_back_as_written() {
    let relation = PersistedRelation {
        address: "0x00000000000000000000000000000000000000aA".to_owned(),
        namespace: "ens".to_owned(),
        resource_id: Uuid::from_u128(0x387),
        relation: "token_holder".to_owned(),
    };
    let json = relation.to_json();
    assert_eq!(
        json,
        serde_json::json!({"address": "0x00000000000000000000000000000000000000aA",
                           "namespace": "ens", "relation": "token_holder",
                           "resource_id": "00000000-0000-0000-0000-000000000387"})
    );
    assert_eq!(PersistedRelation::from_json(&json), Some(relation));
}

// Every block span writes its partials under its own work table part: two spans that shared one
// would each clear the other's partials, and the sources would lose those spans' events. A span
// number the `smallint` part cannot hold apart is refused, not saturated.
#[test]
fn every_block_span_has_its_own_part_and_one_past_the_bound_is_refused() {
    let most = MAX_COMPOSE_BUILDER_BLOCK_SPANS;
    for (span, part) in [(0, 0), (1, 1), (most - 1, 16_383)] {
        assert_eq!(spans::span_part(span).ok(), Some(part), "span {span}");
    }
    for (first, second) in [(32_767, 32_768), (32_767, 39_999), (most, most + 1)] {
        if let (Ok(one), Ok(other)) = (spans::span_part(first), spans::span_part(second)) {
            assert_ne!(one, other, "spans {first} and {second} share a part");
        }
    }
    for span in [most, 32_767, 32_768, 40_000, u16::MAX] {
        assert!(
            spans::span_part(span).is_err(),
            "span {span} must be refused, it is {:?}",
            spans::span_part(span)
        );
    }
}

// The build's options keep the span count within the bound, as they keep the key partitions.
#[test]
fn the_build_options_bound_the_block_spans() {
    let most = MAX_COMPOSE_BUILDER_BLOCK_SPANS;
    for (asked, kept) in [
        (0, 1),
        (3, 3),
        (most, most),
        (most + 1, most),
        (40_000, most),
        (u16::MAX, most),
    ] {
        let options = FamilyOptions::new("keccak256:00").with_compose_builder_partitions(asked, 16);
        assert_eq!(
            options.compose_builder_block_spans, kept,
            "{asked} spans asked"
        );
    }
}

// The steps the build walks are the steps a cursor parses, each under its own name, so a step
// added to the enum cannot print or parse as another.
#[test]
fn every_step_is_walked_once_and_named_apart() {
    let mut walked = vec![Step::Keys];
    while let Some(&last) = walked.last().filter(|step| **step != Step::Done) {
        walked.push(last.after());
    }
    assert_eq!(walked, STEPS);
    let names: std::collections::BTreeSet<&str> = STEPS.iter().map(|step| step.name()).collect();
    assert_eq!(names.len(), STEPS.len());
}
